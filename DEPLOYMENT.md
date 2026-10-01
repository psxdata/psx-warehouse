# Deployment Guide

How to run this warehouse — purely locally against a DuckDB file, against
MotherDuck, on a local cron schedule, or deployed to your own GCP project.

## Contents

- [Local development (DuckDB)](#local-development-duckdb)
- [Local development (MotherDuck)](#local-development-motherduck)
- [Local cron scheduling](#local-cron-scheduling)
- [Production deployment (GCP)](#production-deployment-gcp)
  - [Architecture](#architecture)
  - [Prerequisites](#prerequisites)
  - [GCP bootstrap (one-time, manual)](#gcp-bootstrap-one-time-manual)
  - [Deploying the infrastructure](#deploying-the-infrastructure)
  - [Verifying a deployment](#verifying-a-deployment)
- [CI/CD](#cicd)
- [Known limitation: dbt writes into the raw dataset](#known-limitation-dbt-writes-into-the-raw-dataset)

## Local development (DuckDB)

The fastest way to run the whole pipeline — no account, no token, just a
local file.

**Prerequisites:** [uv](https://docs.astral.sh/uv/). Nothing else.

```bash
uv sync --extra dev --extra dbt
cp dbt/profiles.example.yml ~/.dbt/profiles.yml
```

```bash
BACKEND=motherduck uv run python -m extract.main   # writes ./warehouse.duckdb by default
cd dbt && uv run --project .. dbt build --target dev
```

Set `DUCKDB_PATH` if you want the file somewhere other than
`./warehouse.duckdb` (e.g. a path mounted into a Docker container as a
volume, if you're running extraction inside one). Whatever `DUCKDB_PATH`
resolves to is passed straight to `duckdb.connect()` — it's just a file path,
nothing DuckDB-server-specific to configure.

Browse the lineage graph locally:

```bash
uv run --project .. dbt docs generate --target dev
uv run --project .. dbt docs serve
```

## Local development (MotherDuck)

Same pipeline, against MotherDuck's hosted DuckDB instead of a local file —
useful if you want your local dev data shared across machines or visible in
MotherDuck's own UI.

**Prerequisites:** [uv](https://docs.astral.sh/uv/), a free
[MotherDuck](https://motherduck.com/) account and token.

```bash
uv sync --extra dev --extra dbt
cp dbt/profiles.example.yml ~/.dbt/profiles.yml
```

```bash
MOTHERDUCK_TOKEN=... MD_DATABASE=your_db uv run python -m extract.main
cd dbt && uv run --project .. dbt build --target dev_motherduck
```

Setting `MOTHERDUCK_TOKEN` is what switches `extract/motherduck_io.py` from
local-file mode to MotherDuck mode — see its `load_config()` docstring for
the exact rule. Never point either local mode at production BigQuery; that's
what the GCP deployment below is for.

See [CONTRIBUTING.md](CONTRIBUTING.md) if you're adding a new backend rather
than running an existing one.

## Local cron scheduling

`scripts/run_local.sh` runs the same two steps (extract, then `dbt build`)
as the production Cloud Run Job — it's the local equivalent of what Cloud
Scheduler triggers. Point it at either local mode above via the same env
vars.

**Linux/macOS (cron):**

```bash
crontab -e
```

```cron
# Daily at 6 PM. Cron runs with a minimal PATH, so reference uv by its full
# path (`which uv`) rather than assuming your shell's PATH applies.
0 18 * * * cd /path/to/psx-warehouse && DUCKDB_PATH=/path/to/psx-warehouse/warehouse.duckdb /usr/local/bin/uv run sh scripts/run_local.sh >> /path/to/psx-warehouse/cron.log 2>&1
```

Redirect to a log file as shown above and rotate it yourself (`logrotate`,
or just truncate periodically) — cron doesn't do this for you, and the
extraction job's own logging has no built-in rotation either.

**Windows (Task Scheduler):** create a Basic Task, trigger daily at your
chosen time, action "Start a program", program `sh.exe` (from Git Bash or
WSL), arguments `scripts/run_local.sh`, "Start in" set to the repo root.

## Production deployment (GCP)

### Architecture

This section only applies to the GCP deployment — the local modes above
have no orchestration at all, just the two commands run by hand or via cron.

```
Cloud Scheduler (weekdays Mon-Fri, 6 PM PKT / Asia/Karachi)
        |  triggers via OAuth, as the runner service account below
        v
Cloud Run Job (runs the exact image digest CI last deployed -- see "CI/CD"
               below; :latest is still published to Docker Hub but the Job
               never pulls it directly)
  1. python -m extract.main        -- scrape PSX, write raw.* tables
  2. dbt build --target prod       -- seed + staging + intermediate + marts
        |
        v
BigQuery (raw dataset; staging/intermediate/marts currently share it too --
          see "Known limitation" below)
```

Two service accounts split this by function, kept separate on purpose:

- `psx-warehouse-runner` — the Job's own runtime identity. Cloud Scheduler
  authenticates as it to invoke the Job, and the Job itself runs as it
  (attached directly, no downloadable key — both `extract/bigquery_io.py`
  and dbt-bigquery's `oauth` auth method resolve Application Default
  Credentials from Cloud Run's metadata server automatically). It can read
  and write BigQuery and execute the Job, but has no permission to change
  the Job's own spec.
- `psx-warehouse-deployer` — used only by GitHub Actions CI, via Workload
  Identity Federation (no downloadable key, no long-lived secret), to update
  the Job's image to the digest just built. It holds a custom IAM role
  limited to `run.jobs.get`/`run.jobs.update` (plus `iam.serviceAccountUser`
  scoped narrowly to the runner service account, which `gcloud run jobs
  update` requires) — it cannot execute the Job or touch BigQuery.

They're kept separate for least privilege: the CI credential shouldn't be
able to do anything the Job's own runtime identity can do (run the Job,
read/write BigQuery), and vice versa (the runtime identity can't redeploy
the Job).

### Prerequisites

- A GCP project, with a billing account linked. Terraform does not create
  the project itself — see "GCP bootstrap" below.
- [gcloud CLI](https://cloud.google.com/sdk/docs/install) and
  [Terraform](https://developer.hashicorp.com/terraform/downloads) installed
  locally.
- [uv](https://docs.astral.sh/uv/) and Docker, to build and test the image
  before it ships.
- Three GitHub repo secrets, if you fork this: `DOCKERHUB_USERNAME`,
  `DOCKERHUB_TOKEN` (for `docker-publish.yml`), `MOTHERDUCK_TOKEN` (for
  `dbt-docs.yml`).
- Three GitHub repo **variables** too (not secrets — none of these are
  sensitive), for `docker-publish.yml`'s `deploy` job: `GCP_PROJECT` (your
  GCP project ID), plus `GCP_WORKLOAD_IDENTITY_PROVIDER` and
  `GCP_DEPLOYER_SA_EMAIL`, both read from Terraform outputs after you apply
  (see "Deploying the infrastructure" below). Skip these and the `deploy`
  job fails on every push to `main` — harmlessly, since the image still
  publishes and only the Job redeploy is skipped.

### GCP bootstrap (one-time, manual)

Terraform (`infra/`) manages everything *inside* a project — the project's
own existence and billing link are outside its scope on purpose (there's no
clean unattended path for either at personal-project scale).

1. Create the GCP project (console or `gcloud projects create`); note the
   Project ID.
2. Link a billing account: Billing -> Link a billing account. Consider
   setting a small budget alert (Billing -> Budgets & Alerts) — the design
   stays inside the free tier (~1.5GB/month of BigQuery scans against a
   1TB/month free allowance) but a linked billing account means real spend
   is now *possible*.
3. `gcloud auth application-default login` — gives Terraform's `google`
   provider your own credentials to plan/apply with.

### Deploying the infrastructure

```bash
cd infra
terraform init
```

Create `infra/terraform.tfvars` (gitignored) with the one variable that has
no default:

```hcl
gcp_project = "your-real-project-id"
```

(Every other variable — region, dataset name/location, image reference,
schedule — already has a sensible default; override only if you genuinely
want something different. See `infra/variables.tf`.)

```bash
terraform plan
terraform apply
```

**Don't use `-target` to work around a partial failure.** It's tempting when
some resources are blocked (see below), but `-target` on a resource that
references any single instance of a `for_each`'d resource pulls the *entire*
`for_each` set into its operation — including instances you didn't touch and
don't want retried. A plain `terraform apply` with no `-target` flags
correctly creates everything whose real dependencies are satisfied and
leaves only the genuinely-blocked resources pending, because the normal
execution graph (unlike `-target`'s closure computation) respects
per-instance dependencies correctly. If you ever do need `-target` for a
real emergency, quote each value in PowerShell (`-target="type.name"`) —
unquoted values get mis-tokenized by PowerShell/Terraform on Windows.

**If billing isn't fully active yet:** APIs that don't need it (`bigquery`,
`iam`, `cloudresourcemanager`, `serviceusage`) enable fine, and everything
depending only on those (the service account, its two BigQuery IAM bindings,
the `raw` dataset) creates successfully. `run.googleapis.com` and
`cloudscheduler.googleapis.com` — and anything depending on them (the Cloud
Run Job, its invoker binding, the Scheduler job) — fail with:

```
Error 400: Billing account for project '...' is not found.
```

That's expected, not a config bug. Once billing is fully linked, re-run the
same plain `terraform apply` — already-created resources are a no-op, and
only the previously-blocked resources get created.

If `google_project_service` itself fails to enable anything (a fresh project
without the Service Usage API already on), the fallback is:

```bash
gcloud services enable serviceusage.googleapis.com
```

**Starting over:**

```bash
terraform destroy          # tears down what Terraform actually created
rm -rf infra/.terraform
terraform init             # fresh provider download
terraform apply
```

Never delete `infra/terraform.tfstate` by hand instead of running `destroy`
— that makes Terraform *forget* resources without deleting them in GCP,
orphaning real cloud resources it no longer tracks. Leave
`infra/.terraform.lock.hcl` alone when clearing the cache — it pins the
exact provider version for reproducibility; deleting it lets `init` resolve
a potentially different one.

**If you forked this repo**, set the GitHub repo variables now, using the
two new Terraform outputs:

```bash
terraform output workload_identity_provider
terraform output deployer_service_account_email
```

Set these as `GCP_WORKLOAD_IDENTITY_PROVIDER` and `GCP_DEPLOYER_SA_EMAIL`,
plus `GCP_PROJECT` (the same project ID from `terraform.tfvars`), as GitHub
repo variables (Settings -> Secrets and variables -> Actions -> Variables
tab) — see "Prerequisites" above for why.

### Verifying a deployment

```bash
gcloud run jobs execute psx-warehouse-extract --region us-central1
```

Watch the execution in Cloud Run's own console/logs. Confirm rows land in
`raw.*` and the dbt layers build, then let Cloud Scheduler's weekday cadence
take over unsupervised.

## CI/CD

- `ci.yml` — lint, test, `dbt parse` on every push/PR to `main`.
- `docker-publish.yml` — builds and pushes `mtauha/psx-warehouse:latest`
  (+ a short-sha tag) to Docker Hub, then redeploys the Cloud Run Job to
  that exact image digest via Workload Identity Federation, on every push
  to `main`.
- `dbt-docs.yml` — publishes dbt docs to GitHub Pages
  (https://psxdata.github.io/psx-warehouse/) on every `dbt/` change.

## Known limitation: dbt writes into the `raw` dataset

As currently configured, dbt has no `+schema:`/`generate_schema_name`
override, so `staging`/`intermediate`/`marts` objects all land in the same
dataset as the raw extraction tables (`raw` by default) — not in separate
datasets, despite what the layer names might suggest. This causes no
collisions today (no name overlaps between raw tables and dbt models) and
predates the Terraform work entirely. If you want genuine dataset
separation, that's an open design decision — not something this guide's
setup does for you.
