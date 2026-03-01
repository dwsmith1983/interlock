variable "environment" {
  description = "Environment name (e.g., staging, production)"
  type        = string
}

variable "dist_path" {
  description = "Path to the directory containing Lambda zip files"
  type        = string
}

variable "pipelines_path" {
  description = "Path to directory containing pipeline YAML configuration files"
  type        = string
}

variable "calendars_path" {
  description = "Path to directory containing calendar YAML files (optional)"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "lambda_memory_size" {
  description = "Memory size for Lambda functions (MB)"
  type        = number
  default     = 128
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 30
}

variable "watchdog_schedule" {
  description = "EventBridge schedule expression for watchdog (e.g., rate(5 minutes))"
  type        = string
  default     = "rate(5 minutes)"
}

variable "sfn_timeout_seconds" {
  description = "Step Functions execution timeout in seconds (default 12h)"
  type        = number
  default     = 43200
}
