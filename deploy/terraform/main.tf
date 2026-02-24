terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    # Configure via: terraform init -backend-config=backend.hcl
    # Or override inline:
    #   bucket         = "interlock-terraform-state"
    #   key            = "interlock/terraform.tfstate"
    #   region         = "ap-southeast-1"
    #   dynamodb_table = "interlock-terraform-locks"
    #   encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name

  core_lambdas = toset(["orchestrator", "evaluator", "trigger", "run-checker"])
  all_lambdas  = toset(["orchestrator", "evaluator", "trigger", "run-checker", "stream-router", "watchdog"])

  common_env = merge(
    {
      TABLE_NAME    = var.table_name
      SNS_TOPIC_ARN = aws_sns_topic.alerts.arn
      ARCHETYPE_DIR = "/opt/archetypes"
      READINESS_TTL = var.readiness_ttl
      RETENTION_TTL = var.retention_ttl
    },
    var.evaluator_base_url != "" ? { EVALUATOR_BASE_URL = var.evaluator_base_url } : {}
  )
}
