resource "google_service_account" "deployer" {
  project      = var.gcp_project
  account_id   = "psx-warehouse-deployer"
  display_name = "GitHub Actions CI -- Cloud Run Job deploy identity"

  depends_on = [google_project_service.required["iam.googleapis.com"]]
}

# Custom role instead of roles/run.developer: omits run.jobs.run / run.jobs.runWithOverrides,
# which would let this SA execute the Job immediately with an arbitrary overridden image,
# bypassing the digest-pinned spec (independent security review, 2026-09-18).
resource "google_project_iam_custom_role" "cloud_run_job_deployer" {
  project     = var.gcp_project
  role_id     = "psxWarehouseJobDeployer"
  title       = "psx-warehouse Cloud Run Job deployer"
  description = "Update a Cloud Run Job's spec (image) without permission to run/delete it."
  permissions = [
    "run.jobs.get",
    "run.jobs.update",
    "run.operations.get",
    "resourcemanager.projects.get",
  ]
}

resource "google_cloud_run_v2_job_iam_member" "deployer_updates_extract" {
  project  = var.gcp_project
  location = var.region
  name     = google_cloud_run_v2_job.extract.name
  role     = google_project_iam_custom_role.cloud_run_job_deployer.id
  member   = "serviceAccount:${google_service_account.deployer.email}"
}

# Required for `gcloud run jobs update` to succeed at all: Cloud Run validates actAs against
# the SA in the full request spec on every update call, not just when that field changes.
# Scoped to the runner SA specifically, not project-wide.
resource "google_service_account_iam_member" "deployer_acts_as_runner" {
  service_account_id = google_service_account.runner.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.deployer.email}"
}

resource "google_iam_workload_identity_pool" "github" {
  project                   = var.gcp_project
  workload_identity_pool_id = "github-actions"
  display_name              = "GitHub Actions"

  depends_on = [google_project_service.required["iam.googleapis.com"]]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  project                            = var.gcp_project
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-actions"
  display_name                       = "GitHub Actions OIDC"

  attribute_mapping = {
    "google.subject"          = "assertion.sub"
    "attribute.repository"    = "assertion.repository"
    "attribute.repository_id" = "assertion.repository_id"
    "attribute.ref"           = "assertion.ref"
  }

  # Pinned to this repo AND the main branch -- not just the repo alone (independent security
  # review, 2026-09-18: repo-only would let any future workflow requesting id-token: write
  # silently inherit deploy access). The repo is matched on its immutable numeric ID rather
  # than its name, so a transfer/rename doesn't break trust and a recycled name can't inherit it.
  attribute_condition = "attribute.repository_id == \"${var.github_repository_id}\" && attribute.ref == \"refs/heads/main\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account_iam_member" "github_actions_impersonates_deployer" {
  service_account_id = google_service_account.deployer.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository_id/${var.github_repository_id}"
}
