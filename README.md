# PSX Analytics Warehouse

[![CI](https://github.com/psxdata/psx-warehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/psxdata/psx-warehouse/actions/workflows/ci.yml)
[![Docker Publish](https://github.com/psxdata/psx-warehouse/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/psxdata/psx-warehouse/actions/workflows/docker-publish.yml)
[![dbt docs status](https://github.com/psxdata/psx-warehouse/actions/workflows/dbt-docs.yml/badge.svg)](https://github.com/psxdata/psx-warehouse/actions/workflows/dbt-docs.yml)
[![Docker Hub](https://img.shields.io/badge/docker-mtauha%2Fpsx--warehouse-blue?logo=docker)](https://hub.docker.com/r/mtauha/psx-warehouse)
[![License: MIT](https://img.shields.io/github/license/psxdata/psx-warehouse)](LICENSE.md)
[![dbt docs](https://img.shields.io/badge/dbt%20docs-view-orange)](https://psxdata.github.io/psx-warehouse/)

dbt-core + BigQuery analytics layer on top of the [`psxdata`](https://github.com/psxdata/psxdata) SDK, turning raw Pakistan Stock Exchange (PSX) OHLCV data into tested, analysis-ready marts.

Every run: pull the current KSE-100 constituents and their full OHLCV history
from PSX, load it into an append-only, hash-diffed raw layer (no restatement
ever silently overwritten), then transform it through dbt into ticker
dimensions, sector/index rollups, valuation snapshots, and fact tables ready
to query.

## Quickstart

```bash
uv sync --extra dev --extra dbt
BACKEND=motherduck uv run python -m extract.main   # writes ./warehouse.duckdb, no account needed
cd dbt && uv run --project .. dbt build --target dev
```

Full walkthrough (local DuckDB/MotherDuck setup, scheduling a local cron
sync, or deploying your own copy to GCP): [DEPLOYMENT.md](DEPLOYMENT.md).

## Layout

- `extract/` — Python extraction from the `psxdata` SDK into raw tables.
  Ships with BigQuery and MotherDuck backends; see
  [CONTRIBUTING.md](CONTRIBUTING.md) to add another.
- `dbt/` — dbt-core project (staging / intermediate / marts), targeting
  MotherDuck/DuckDB for local dev and BigQuery for production:
  - `models/staging/` — one staging model per raw source (`stg_stock_history`,
    `stg_symbols`, `stg_sectors`, `stg_index_constituents`, `stg_screener`)
  - `snapshots/` — `dim_tickers`, a Type-2 snapshot of ticker attributes
    (with delisting handling)
  - `models/marts/` — three Type-1 dimensions (`dim_sectors`, `dim_indices`,
    `dim_date`) and five fact tables (`fact_ohlcv`, `fact_restatement_history`,
    `fact_index_membership`, `fact_sector_daily`, `fact_valuation_daily`)
- `infra/` — Terraform for the production GCP deployment: one service
  account, its IAM bindings, a BigQuery dataset, a Cloud Run Job, and a
  Cloud Scheduler job. See [DEPLOYMENT.md](DEPLOYMENT.md#production-deployment-gcp).

## Guides

- [DEPLOYMENT.md](DEPLOYMENT.md) — run it locally against MotherDuck, or
  deploy your own copy to GCP.
- [CONTRIBUTING.md](CONTRIBUTING.md) — add a new raw-storage backend.

## CI/CD

- `ci.yml` — lint, test, and `dbt parse` on every push/PR to `main`.
- `docker-publish.yml` — builds and pushes `mtauha/psx-warehouse:latest`
  (+ a short-sha tag) to Docker Hub, then redeploys the Cloud Run Job to
  that exact image digest via Workload Identity Federation, on every push
  to `main`.
- `dbt-docs.yml` — publishes the dbt lineage docs to
  [GitHub Pages](https://psxdata.github.io/psx-warehouse/) on every `dbt/`
  change.

## License

[MIT](LICENSE.md)
