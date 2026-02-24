terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    # Configure via: terraform init -backend-config=backend.hcl
    # Or override inline:
    #   bucket = "interlock-terraform-state"
    #   prefix = "interlock/terraform"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "current" {}

locals {
  core_functions = toset(["orchestrator", "evaluator", "trigger", "run-checker"])
  all_functions  = toset(["orchestrator", "evaluator", "trigger", "run-checker", "stream-router"])

  entry_points = {
    "orchestrator"  = "Orchestrator"
    "evaluator"     = "Evaluator"
    "trigger"       = "Trigger"
    "run-checker"   = "RunChecker"
    "stream-router" = "StreamRouter"
  }

  common_env = merge(
    {
      PROJECT_ID    = var.project_id
      COLLECTION    = var.collection_name
      PUBSUB_TOPIC  = google_pubsub_topic.alerts.id
      READINESS_TTL = var.readiness_ttl
      RETENTION_TTL = var.retention_ttl
    },
    var.evaluator_base_url != "" ? { EVALUATOR_BASE_URL = var.evaluator_base_url } : {}
  )
}
