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
  description = "Step Functions execution timeout in seconds (default 4h)"
  type        = number
  default     = 14400
}

variable "trigger_max_attempts" {
  description = "Max retry attempts for the Trigger state"
  type        = number
  default     = 3
}

variable "events_table_ttl_days" {
  description = "TTL in days for records in the events table"
  type        = number
  default     = 90
}

variable "slack_bot_token" {
  description = "Slack Bot API token for alert notifications (empty = logging only)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "slack_channel_id" {
  description = "Slack channel ID for alert notifications"
  type        = string
  default     = ""
}

variable "enable_glue_trigger" {
  description = "Enable IAM permissions for Glue job triggering"
  type        = bool
  default     = false
}

variable "enable_emr_trigger" {
  description = "Enable IAM permissions for EMR job triggering"
  type        = bool
  default     = false
}

variable "enable_emr_serverless_trigger" {
  description = "Enable IAM permissions for EMR Serverless job triggering"
  type        = bool
  default     = false
}

variable "enable_sfn_trigger" {
  description = "Enable IAM permissions for nested Step Functions execution"
  type        = bool
  default     = false
}
