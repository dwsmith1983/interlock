output "state_bucket" {
  description = "GCS bucket for Terraform state"
  value       = google_storage_bucket.state.name
}
