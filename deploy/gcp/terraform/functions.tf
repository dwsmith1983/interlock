# -----------------------------------------------------------------------------
# GCS bucket for Cloud Function source code
# -----------------------------------------------------------------------------

resource "google_storage_bucket" "function_source" {
  name                        = "${var.project_id}-${var.collection_name}-gcf-source"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}

# -----------------------------------------------------------------------------
# Source archive — staged by deploy/gcp/build.sh
# -----------------------------------------------------------------------------

data "archive_file" "function_source" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = "${path.module}/.build/source.zip"
}

resource "google_storage_bucket_object" "function_source" {
  name   = "source-${data.archive_file.function_source.output_md5}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.function_source.output_path
}

# -----------------------------------------------------------------------------
# Core Cloud Functions (orchestrator, evaluator, trigger, run-checker)
# All share: same source, same env vars, Firestore + Pub/Sub access
# -----------------------------------------------------------------------------

resource "google_cloudfunctions2_function" "core" {
  for_each = local.core_functions

  name     = "${var.collection_name}-${each.key}"
  location = var.region

  build_config {
    runtime     = "go122"
    entry_point = local.entry_points[each.key]

    source {
      storage_source {
        bucket = google_storage_bucket.function_source.name
        object = google_storage_bucket_object.function_source.name
      }
    }
  }

  service_config {
    max_instance_count    = var.function_max_instances
    available_memory      = "${var.function_memory}M"
    timeout_seconds       = var.function_timeout
    service_account_email = google_service_account.function[each.key].email

    environment_variables = local.common_env
  }
}

# -----------------------------------------------------------------------------
# stream-router Cloud Function (separate — different env vars, Eventarc trigger)
# Only needs WORKFLOW_NAME; no Firestore access.
# -----------------------------------------------------------------------------

resource "google_cloudfunctions2_function" "stream_router" {
  name     = "${var.collection_name}-stream-router"
  location = var.region

  build_config {
    runtime     = "go122"
    entry_point = local.entry_points["stream-router"]

    source {
      storage_source {
        bucket = google_storage_bucket.function_source.name
        object = google_storage_bucket_object.function_source.name
      }
    }
  }

  service_config {
    max_instance_count    = var.function_max_instances
    available_memory      = "${var.function_memory}M"
    timeout_seconds       = var.function_timeout
    service_account_email = google_service_account.function["stream-router"].email

    environment_variables = {
      WORKFLOW_NAME = google_workflows_workflow.pipeline.id
    }
  }
}

# -----------------------------------------------------------------------------
# Eventarc trigger: Firestore document writes → stream-router
# Fires on any document write in the interlock collection.
# The stream-router filters for MARKER documents in application code.
# -----------------------------------------------------------------------------

resource "google_eventarc_trigger" "firestore_stream" {
  name     = "${var.collection_name}-firestore-stream"
  location = var.region

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.firestore.document.v1.written"
  }

  matching_criteria {
    attribute = "database"
    value     = var.firestore_database
  }

  destination {
    cloud_run_service {
      service = google_cloudfunctions2_function.stream_router.service_config[0].service
      region  = var.region
    }
  }

  service_account = google_service_account.eventarc.email

  depends_on = [
    google_project_iam_member.eventarc_receive,
    google_cloud_run_v2_service_iam_member.eventarc_invoke,
  ]
}
