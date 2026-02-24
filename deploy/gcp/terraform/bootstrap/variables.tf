variable "project_name" {
  description = "Project name used for state bucket naming"
  type        = string
  default     = "interlock"
}

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for the state bucket"
  type        = string
  default     = "us-central1"
}
