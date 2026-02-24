resource "google_workflows_workflow" "pipeline" {
  name            = "${var.collection_name}-pipeline"
  region          = var.region
  service_account = google_service_account.workflow.id

  source_contents = templatefile(var.workflow_path, {
    orchestrator_url = google_cloudfunctions2_function.core["orchestrator"].url
    evaluator_url    = google_cloudfunctions2_function.core["evaluator"].url
    trigger_url      = google_cloudfunctions2_function.core["trigger"].url
    run_checker_url  = google_cloudfunctions2_function.core["run-checker"].url
    pubsub_topic     = google_pubsub_topic.alerts.id
  })
}
