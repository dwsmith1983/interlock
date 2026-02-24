output "firestore_database" {
  description = "Firestore database name"
  value       = var.firestore_database
}

output "collection_name" {
  description = "Firestore collection name"
  value       = var.collection_name
}

output "topic_id" {
  description = "Pub/Sub alert topic full resource name"
  value       = google_pubsub_topic.alerts.id
}

output "workflow_name" {
  description = "Cloud Workflows workflow full resource name"
  value       = google_workflows_workflow.pipeline.id
}

output "function_urls" {
  description = "Cloud Function URLs by name"
  value = merge(
    { for name in local.core_functions : name => google_cloudfunctions2_function.core[name].url },
    { "stream-router" = google_cloudfunctions2_function.stream_router.url }
  )
}
