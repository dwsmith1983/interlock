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
  description = "EventBridge schedule expression for the watchdog (e.g., rate(5 minutes)). Keep the interval well under 6 hours, or SLA alerts can be missed"
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

  validation {
    condition     = var.events_table_ttl_days >= 1
    error_message = "events_table_ttl_days must be at least 1."
  }
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
  description = "Slack channel ID for alert notifications (required when slack_bot_token or slack_secret_arn is set; all three empty = logging only)"
  type        = string
  default     = ""
}

variable "trigger_secrets_arn" {
  description = "Full ARN (with its random 6-character suffix, no wildcards) of a Secrets Manager secret in the deployment's region holding a JSON object of INTERLOCK_ env vars for trigger auth, loaded by the orchestrator at cold start (empty = none)"
  type        = string
  default     = ""

  validation {
    condition     = var.trigger_secrets_arn == "" || can(regex("^arn:aws[a-z-]*:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@-]+$", var.trigger_secrets_arn))
    error_message = "trigger_secrets_arn must be the full Secrets Manager secret ARN, including its random 6-character suffix, in the deployment's region and without wildcards, or empty."
  }
}

variable "orchestrator_env" {
  description = "Extra INTERLOCK_ env vars for the orchestrator. Values are stored in Terraform state and shown in plans and the Lambda console, so not for secrets: use trigger_secrets_arn for tokens."
  type        = map(string)
  default     = {}

  validation {
    condition     = alltrue([for k in keys(var.orchestrator_env) : can(regex("^INTERLOCK_[A-Z0-9_]+$", k))])
    error_message = "Every orchestrator_env key must match ^INTERLOCK_[A-Z0-9_]+$."
  }

  validation {
    condition     = length(setintersection(keys(var.orchestrator_env), ["INTERLOCK_PIPELINE_ID", "INTERLOCK_SCHEDULE_ID", "INTERLOCK_DATE", "INTERLOCK_RUN_ID"])) == 0
    error_message = "orchestrator_env must not set INTERLOCK_PIPELINE_ID, INTERLOCK_SCHEDULE_ID, INTERLOCK_DATE or INTERLOCK_RUN_ID: each run sets its own run identity."
  }
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
  description = "ARN of a customer managed KMS key for the DynamoDB tables and SQS queues. Empty = AWS managed keys, and SQS-managed encryption for the alert queues."
  type        = string
  default     = ""

  validation {
    condition     = var.kms_key_arn == "" || can(regex("^arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:key/[A-Za-z0-9-]+$", var.kms_key_arn))
    error_message = "kms_key_arn must be a KMS key ARN (not an alias, key ID or wildcard), or empty."
  }
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
  description = "ARNs of EMR Serverless applications the orchestrator can start job runs on; it can poll their job runs. Required when enable_emr_serverless_trigger is true."
  type        = list(string)
  default     = []
}

variable "emr_serverless_execution_role_arns" {
  description = "ARNs of the IAM runtime roles EMR Serverless job runs execute under; the orchestrator can pass them to EMR Serverless only. Required when enable_emr_serverless_trigger is true."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for a in var.emr_serverless_execution_role_arns : can(regex("^arn:aws[a-z-]*:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+$", a))])
    error_message = "emr_serverless_execution_role_arns must be IAM role ARNs (arn:<partition>:iam::<account>:role/<optional path/><name>), no wildcards."
  }
}

variable "sfn_trigger_arns" {
  description = "ARNs of state machines the orchestrator can start; alias and version ARNs are accepted, and it can poll their executions. Required when enable_sfn_trigger is true."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for a in var.sfn_trigger_arns : can(regex("^arn:[^:]+:states:[^:]+:[^:]+:stateMachine:[^:]+(:[^:]+)?$", a))])
    error_message = "sfn_trigger_arns must be state machine ARNs (arn:<partition>:states:<region>:<account>:stateMachine:<name>, optionally followed by :<alias or version>), not execution ARNs."
  }
}
