variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "ap-southeast-1"
}

variable "table_name" {
  description = "DynamoDB table name (also used as prefix for all resources)"
  type        = string
  default     = "interlock"
}

variable "lambda_memory_size" {
  description = "Memory size in MB for all Lambda functions"
  type        = number
  default     = 256
}

variable "lambda_timeout" {
  description = "Timeout in seconds for all Lambda functions"
  type        = number
  default     = 60
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
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

variable "lambda_dist_dir" {
  description = "Path to Lambda binary distribution directory"
  type        = string
  default     = "../dist/lambda"
}

variable "layer_dist_dir" {
  description = "Path to Lambda layer distribution directory"
  type        = string
  default     = "../dist/layer"
}

variable "asl_path" {
  description = "Path to Step Function ASL definition file"
  type        = string
  default     = "../statemachine.asl.json"
}

variable "destroy_on_delete" {
  description = "Allow terraform destroy to delete the DynamoDB table (use for testing)"
  type        = bool
  default     = false
}

# Opt-in trigger permissions

variable "enable_glue_trigger" {
  description = "Grant Glue permissions to trigger and run-checker Lambdas"
  type        = bool
  default     = false
}

variable "enable_emr_trigger" {
  description = "Grant EMR permissions to trigger and run-checker Lambdas"
  type        = bool
  default     = false
}

variable "enable_emr_serverless_trigger" {
  description = "Grant EMR Serverless permissions to trigger and run-checker Lambdas"
  type        = bool
  default     = false
}

variable "enable_sfn_trigger" {
  description = "Grant Step Functions permissions to trigger and run-checker Lambdas"
  type        = bool
  default     = false
}
