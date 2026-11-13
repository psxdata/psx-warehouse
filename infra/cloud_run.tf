resource "google_cloud_run_v2_job" "extract" {
  project             = var.gcp_project
  name                = "psx-warehouse-extract"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.runner.email
      timeout         = "3600s"
      max_retries     = 1

      containers {
        image = var.docker_image

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }

        env {
          name  = "GCP_PROJECT"
          value = var.gcp_project
        }

        env {
          name  = "BQ_DATASET"
          value = var.bq_dataset
        }

        env {
          name  = "RAW_BQ_DATASET"
          value = var.bq_dataset
        }
      }
    }
  }

  depends_on = [
    google_project_service.required["run.googleapis.com"],
    google_project_iam_member.runner_bigquery_data_editor,
    google_project_iam_member.runner_bigquery_job_user,
    google_artifact_registry_repository.dockerhub,
  ]

  lifecycle {
    # image is deployed by CI after the first apply (see .github/workflows/docker-publish.yml's
    # `deploy` job) -- Terraform only sets it at creation. var.docker_image is otherwise unused
    # after that; do not expect `terraform apply` to change this field again.
    ignore_changes = [template[0].template[0].containers[0].image]
  }
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_invokes" {
  project  = var.gcp_project
  location = var.region
  name     = google_cloud_run_v2_job.extract.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.runner.email}"
}
