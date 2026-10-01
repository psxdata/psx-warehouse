variable "gcp_project" {
  description = "GCP project ID the warehouse infra is deployed into."
  type        = string
}

variable "region" {
  description = "GCP region for the Cloud Run Job and Cloud Scheduler job."
  type        = string
  default     = "us-central1"
}

variable "bq_dataset" {
  description = "BigQuery raw dataset name."
  type        = string
  default     = "raw"
}

variable "bq_location" {
  description = "BigQuery dataset location."
  type        = string
  default     = "US"
}

variable "docker_image" {
  description = "Public Docker Hub image the Cloud Run Job pulls."
  type        = string
  default     = "docker.io/mtauha/psx-warehouse:latest"
}

variable "github_repository_id" {
  description = "Immutable numeric GitHub repository ID (psxdata/psx-warehouse) that Workload Identity Federation trusts."
  type        = string
  default     = "1350817881"
}

variable "schedule_cron" {
  description = "Cron expression for the extraction run, weekdays (Mon-Fri) only -- PSX doesn't trade on weekends."
  type        = string
  default     = "0 18 * * 1-5"
}

variable "schedule_timezone" {
  description = "Time zone for schedule_cron."
  type        = string
  default     = "Asia/Karachi"
}
