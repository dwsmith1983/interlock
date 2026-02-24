variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-central1"
}

variable "collection_name" {
  description = "Firestore collection name (also used as prefix for all resources)"
  type        = string
  default     = "interlock"
}

variable "firestore_database" {
  description = "Firestore database name"
  type        = string
  default     = "(default)"
}

variable "create_firestore_database" {
  description = "Create the Firestore database (set false if it already exists)"
  type        = bool
  default     = true
}

variable "function_memory" {
  description = "Memory in MB for all Cloud Functions"
  type        = number
  default     = 256
}

variable "function_timeout" {
  description = "Timeout in seconds for all Cloud Functions"
  type        = number
  default     = 60
}

variable "function_max_instances" {
  description = "Maximum concurrent instances per Cloud Function"
  type        = number
  default     = 10
}

variable "readiness_ttl" {
  description = "TTL for readiness records (Go duration string)"
  type        = string
  default     = "1h"
}

variable "retention_ttl" {
  description = "TTL for retention records (Go duration string)"
  type        = string
  default     = "168h"
}

variable "evaluator_base_url" {
  description = "Base URL for HTTP evaluator (optional)"
  type        = string
  default     = ""
}

variable "source_dir" {
  description = "Path to staged Go source directory for Cloud Functions"
  type        = string
  default     = "../dist/source"
}

variable "workflow_path" {
  description = "Path to Cloud Workflows YAML definition file"
  type        = string
  default     = "../workflow.yaml"
}

variable "destroy_on_delete" {
  description = "Allow terraform destroy to delete the Firestore database (use for testing)"
  type        = bool
  default     = false
}

# Opt-in trigger permissions

variable "enable_dataproc_trigger" {
  description = "Grant Dataproc permissions to trigger and run-checker functions"
  type        = bool
  default     = false
}

variable "enable_dataproc_serverless_trigger" {
  description = "Grant Dataproc Serverless (Batches) permissions to trigger and run-checker functions"
  type        = bool
  default     = false
}

variable "enable_bigquery_trigger" {
  description = "Grant BigQuery permissions to trigger and run-checker functions"
  type        = bool
  default     = false
}

variable "enable_cloud_workflows_trigger" {
  description = "Grant Cloud Workflows permissions to trigger and run-checker functions"
  type        = bool
  default     = false
}
