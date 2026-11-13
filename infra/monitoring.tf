# Alerting for the daily extract.
#
# The dbt recency tests inside the job can only fail when the job actually
# starts. These policies cover the two ways it doesn't finish:
#   1. Cloud Scheduler can't start it (e.g. the job is not Ready after a bad
#      deploy, so every :run call returns HTTP 400).
#   2. The job starts and the execution fails.

resource "google_monitoring_notification_channel" "alert_email" {
  project      = var.gcp_project
  display_name = "psx-warehouse alerts"
  type         = "email"

  labels = {
    email_address = var.alert_email
  }

  depends_on = [google_project_service.required["monitoring.googleapis.com"]]
}

resource "google_monitoring_alert_policy" "scheduler_trigger_failed" {
  project      = var.gcp_project
  display_name = "psx-warehouse: scheduler failed to start the extract"
  combiner     = "OR"
  severity     = "ERROR"

  conditions {
    display_name = "Cloud Scheduler attempt for ${google_cloud_scheduler_job.extract_daily.name} failed"

    condition_matched_log {
      filter = join(" AND ", [
        "resource.type=\"cloud_scheduler_job\"",
        "resource.labels.job_id=\"${google_cloud_scheduler_job.extract_daily.name}\"",
        "jsonPayload.@type=\"type.googleapis.com/google.cloud.scheduler.logging.AttemptFinished\"",
        "severity>=ERROR",
      ])
    }
  }

  alert_strategy {
    notification_rate_limit {
      period = "3600s"
    }
    auto_close = "86400s"
  }

  documentation {
    mime_type = "text/markdown"
    content   = <<-EOT
      Cloud Scheduler could not start the Cloud Run job `${google_cloud_run_v2_job.extract.name}`, so no extract ran and the warehouse did not refresh.

      Check the job's condition first: `gcloud run jobs describe ${google_cloud_run_v2_job.extract.name} --region ${var.region}`.
      A job that is not Ready (for example after a failed image deploy) rejects every run with HTTP 400.
    EOT
  }

  notification_channels = [google_monitoring_notification_channel.alert_email.id]
}

resource "google_monitoring_alert_policy" "extract_execution_failed" {
  project      = var.gcp_project
  display_name = "psx-warehouse: extract execution failed"
  combiner     = "OR"
  severity     = "ERROR"

  conditions {
    display_name = "Failed execution of ${google_cloud_run_v2_job.extract.name}"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"cloud_run_job\"",
        "resource.labels.job_name = \"${google_cloud_run_v2_job.extract.name}\"",
        "metric.type = \"run.googleapis.com/job/completed_execution_count\"",
        "metric.labels.result = \"failed\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "86400s"
  }

  documentation {
    mime_type = "text/markdown"
    content   = <<-EOT
      An execution of the Cloud Run job `${google_cloud_run_v2_job.extract.name}` failed after its retries, so the warehouse did not refresh.

      List recent executions: `gcloud run jobs executions list --job ${google_cloud_run_v2_job.extract.name} --region ${var.region}`, then read that execution's logs.
    EOT
  }

  notification_channels = [google_monitoring_notification_channel.alert_email.id]
}
