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

variable "slack_secret_arn" {
  description = "ARN of Secrets Manager secret containing Slack bot token (overrides slack_bot_token env var)"
  type        = string
  default     = ""
}

variable "slack_channel_id" {
  description = "Slack channel ID for alert notifications"
  type        = string
  default     = ""
}

variable "lambda_concurrency" {
  description = "Reserved concurrent executions per Lambda function"
  type = object({
    stream_router    = number
    orchestrator     = number
    sla_monitor      = number
    watchdog         = number
    event_sink       = number
    alert_dispatcher = number
  })
  default = {
    stream_router    = 10
    orchestrator     = 10
    sla_monitor      = 5
    watchdog         = 2
    event_sink       = 5
    alert_dispatcher = 3
  }
}

variable "kms_key_arn" {
  description = "ARN of a KMS key for encrypting DynamoDB tables and SQS queues at rest. Empty = AWS-managed encryption."
  type        = string
  default     = ""
}

variable "sns_alarm_topic_arn" {
  description = "SNS topic ARN for CloudWatch alarm notifications (empty = alarms fire but no notifications)"
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

variable "enable_lambda_trigger" {
  description = "Enable IAM permissions for Lambda-invoked job triggering"
  type        = bool
  default     = false
}

variable "lambda_trigger_arns" {
  description = "ARNs of Lambda functions the orchestrator may invoke as pipeline triggers"
  type        = list(string)
  default     = []
}

variable "glue_job_arns" {
  description = "ARNs of Glue jobs that the orchestrator Lambda can start. Required when enable_glue_trigger is true."
  type        = list(string)
  default     = []
}

variable "emr_cluster_arns" {
  description = "ARNs of EMR clusters the orchestrator can submit steps to. Required when enable_emr_trigger is true."
  type        = list(string)
  default     = []
}

variable "emr_serverless_app_arns" {
  description = "ARNs of EMR Serverless applications. Required when enable_emr_serverless_trigger is true."
  type        = list(string)
  default     = []
}

variable "sfn_trigger_arns" {
  description = "ARNs of Step Functions the orchestrator can start. Required when enable_sfn_trigger is true."
  type        = list(string)
  default     = []
}
