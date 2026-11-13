# Pull-through cache for Docker Hub in this project. Cloud Run pulls docker.io
# images through the shared mirror.gcr.io, whose upstream quota is not ours to
# raise; from 2026-11-01 it answered 429 for this image and the extract could
# not start. Pulling through a remote repository uses this project's quota, and
# the cached image survives Docker Hub hiccups. Images are still published only
# to Docker Hub.
resource "google_artifact_registry_repository" "dockerhub" {
  project       = var.gcp_project
  location      = var.region
  repository_id = "dockerhub"
  description   = "Remote repository caching Docker Hub images for the Cloud Run Job."
  format        = "DOCKER"
  mode          = "REMOTE_REPOSITORY"

  remote_repository_config {
    description = "Docker Hub"
    docker_repository {
      public_repository = "DOCKER_HUB"
    }
  }

  # Keep the last few cached digests (rollback targets) and drop the rest after
  # 90 days. An evicted digest is fetched from Docker Hub again on next use.
  cleanup_policies {
    id     = "keep-recent"
    action = "KEEP"
    most_recent_versions {
      keep_count = 5
    }
  }
  cleanup_policies {
    id     = "delete-old"
    action = "DELETE"
    condition {
      older_than = "7776000s"
    }
  }

  depends_on = [google_project_service.required["artifactregistry.googleapis.com"]]
}

# Cloud Run's deploy docs list Artifact Registry Reader on the image's
# repository for whoever updates the job; scoped to this repository only.
resource "google_artifact_registry_repository_iam_member" "deployer_reads_dockerhub" {
  project    = var.gcp_project
  location   = google_artifact_registry_repository.dockerhub.location
  repository = google_artifact_registry_repository.dockerhub.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.deployer.email}"
}
