# -----------------------------------------------------------------------------
# Cloud Function service accounts (one per function)
# -----------------------------------------------------------------------------

resource "google_service_account" "function" {
  for_each     = local.all_functions
  account_id   = "${var.collection_name}-${each.key}"
  display_name = "Interlock ${each.key} Cloud Function"
}

# -----------------------------------------------------------------------------
# Firestore read/write — orchestrator, evaluator, trigger
# -----------------------------------------------------------------------------

resource "google_project_iam_member" "firestore_rw" {
  for_each = toset(["orchestrator", "evaluator", "trigger"])
  project  = var.project_id
  role     = "roles/datastore.user"
  member   = "serviceAccount:${google_service_account.function[each.key].email}"
}

# -----------------------------------------------------------------------------
# Firestore read-only — run-checker
# -----------------------------------------------------------------------------

resource "google_project_iam_member" "firestore_ro" {
  project = var.project_id
  role    = "roles/datastore.viewer"
  member  = "serviceAccount:${google_service_account.function["run-checker"].email}"
}

# -----------------------------------------------------------------------------
# Pub/Sub publish — orchestrator, trigger
# -----------------------------------------------------------------------------

resource "google_project_iam_member" "pubsub_publish" {
  for_each = toset(["orchestrator", "trigger"])
  project  = var.project_id
  role     = "roles/pubsub.publisher"
  member   = "serviceAccount:${google_service_account.function[each.key].email}"
}

# -----------------------------------------------------------------------------
# Cloud Workflows execution — stream-router starts workflow executions
# -----------------------------------------------------------------------------

resource "google_project_iam_member" "workflows_invoker" {
  project = var.project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${google_service_account.function["stream-router"].email}"
}

# -----------------------------------------------------------------------------
# Cloud Workflows service account
# Needs to invoke Cloud Functions (via Cloud Run) and publish to Pub/Sub
# -----------------------------------------------------------------------------

resource "google_service_account" "workflow" {
  account_id   = "${var.collection_name}-workflow"
  display_name = "Interlock Cloud Workflows"
}

resource "google_project_iam_member" "workflow_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.workflow.email}"
}

# Grant the workflow SA permission to invoke each core Cloud Function
resource "google_cloud_run_v2_service_iam_member" "workflow_invoke" {
  for_each = local.core_functions
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.core[each.key].service_config[0].service
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.workflow.email}"
}

# -----------------------------------------------------------------------------
# Eventarc service account (for Firestore → stream-router trigger)
# -----------------------------------------------------------------------------

resource "google_service_account" "eventarc" {
  account_id   = "${var.collection_name}-eventarc"
  display_name = "Interlock Eventarc trigger"
}

resource "google_project_iam_member" "eventarc_receive" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.eventarc.email}"
}

# Grant the Eventarc SA permission to invoke the stream-router Cloud Function
resource "google_cloud_run_v2_service_iam_member" "eventarc_invoke" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.stream_router.service_config[0].service
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.eventarc.email}"
}

# -----------------------------------------------------------------------------
# Conditional trigger permissions (opt-in per trigger type)
# -----------------------------------------------------------------------------

# --- Dataproc ---

resource "google_project_iam_member" "dataproc_trigger" {
  count   = var.enable_dataproc_trigger ? 1 : 0
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.function["trigger"].email}"
}

resource "google_project_iam_member" "dataproc_run_checker" {
  count   = var.enable_dataproc_trigger ? 1 : 0
  project = var.project_id
  role    = "roles/dataproc.viewer"
  member  = "serviceAccount:${google_service_account.function["run-checker"].email}"
}

# --- Dataproc Serverless ---

resource "google_project_iam_member" "dataproc_serverless_trigger" {
  count   = var.enable_dataproc_serverless_trigger ? 1 : 0
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.function["trigger"].email}"
}

resource "google_project_iam_member" "dataproc_serverless_run_checker" {
  count   = var.enable_dataproc_serverless_trigger ? 1 : 0
  project = var.project_id
  role    = "roles/dataproc.viewer"
  member  = "serviceAccount:${google_service_account.function["run-checker"].email}"
}

# --- BigQuery ---

resource "google_project_iam_member" "bigquery_trigger" {
  count   = var.enable_bigquery_trigger ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.function["trigger"].email}"
}

resource "google_project_iam_member" "bigquery_run_checker" {
  count   = var.enable_bigquery_trigger ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.function["run-checker"].email}"
}

# --- Cloud Workflows (nested orchestration) ---

resource "google_project_iam_member" "cloud_workflows_trigger" {
  count   = var.enable_cloud_workflows_trigger ? 1 : 0
  project = var.project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${google_service_account.function["trigger"].email}"
}

resource "google_project_iam_member" "cloud_workflows_run_checker" {
  count   = var.enable_cloud_workflows_trigger ? 1 : 0
  project = var.project_id
  role    = "roles/workflows.viewer"
  member  = "serviceAccount:${google_service_account.function["run-checker"].email}"
}
