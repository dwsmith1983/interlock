resource "google_pubsub_topic" "alerts" {
  name = "${var.collection_name}-alerts"
}
