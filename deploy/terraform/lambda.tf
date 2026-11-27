# -----------------------------------------------------------------------------
# Lambda functions, IAM roles, log groups, event source mappings, and DLQs
#
# Cross-file references (defined in stepfunctions.tf / eventbridge.tf):
#   aws_sfn_state_machine.pipeline   — Step Functions state machine
#   aws_cloudwatch_event_bus.interlock — custom EventBridge bus
# -----------------------------------------------------------------------------

locals {
  lambda_names = toset(["stream-router", "orchestrator", "sla-monitor", "watchdog", "event-sink", "alert-dispatcher"])

  # The events table is left out: only the event-sink and alert-dispatcher use
  # it, and they have their own policies.
  table_arns = [
    aws_dynamodb_table.control.arn,
    aws_dynamodb_table.joblog.arn,
    aws_dynamodb_table.rerun.arn,
  ]

  table_and_index_arns = flatten([
    for arn in local.table_arns : [arn, "${arn}/index/*"]
  ])

  # The watchdog never reads the rerun table.
  watchdog_read_arns = flatten([
    for arn in [aws_dynamodb_table.control.arn, aws_dynamodb_table.joblog.arn] : [arn, "${arn}/index/*"]
  ])
}

# -----------------------------------------------------------------------------
# Shared IAM assume-role policy (Lambda service)
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# -----------------------------------------------------------------------------
# IAM roles — one per Lambda function
# -----------------------------------------------------------------------------

resource "aws_iam_role" "lambda" {
  for_each           = local.lambda_names
  name               = "${var.environment}-interlock-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  for_each   = local.lambda_names
  role       = aws_iam_role.lambda[each.key].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# -----------------------------------------------------------------------------
# CloudWatch log groups
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "lambda" {
  for_each          = local.lambda_names
  name              = "/aws/lambda/${var.environment}-interlock-${each.key}"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# =============================================================================
# Lambda functions
# =============================================================================

# -----------------------------------------------------------------------------
# event-sink — writes all EventBridge events to the centralized events table
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "event_sink" {
  function_name                  = "${var.environment}-interlock-event-sink"
  role                           = aws_iam_role.lambda["event-sink"].arn
  handler                        = "bootstrap"
  runtime                        = "provided.al2023"
  architectures                  = ["arm64"]
  memory_size                    = 128
  timeout                        = 30
  filename                       = "${var.dist_path}/event-sink.zip"
  source_code_hash               = filebase64sha256("${var.dist_path}/event-sink.zip")
  reserved_concurrent_executions = var.lambda_concurrency.event_sink

  environment {
    variables = {
      EVENTS_TABLE    = aws_dynamodb_table.events.name
      EVENTS_TTL_DAYS = var.events_table_ttl_days
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda["event-sink"]]
}

# -----------------------------------------------------------------------------
# alert-dispatcher — sends Slack notifications from SQS alert queue
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "alert_dispatcher" {
  function_name                  = "${var.environment}-interlock-alert-dispatcher"
  role                           = aws_iam_role.lambda["alert-dispatcher"].arn
  handler                        = "bootstrap"
  runtime                        = "provided.al2023"
  architectures                  = ["arm64"]
  memory_size                    = 128
  timeout                        = 30
  filename                       = "${var.dist_path}/alert-dispatcher.zip"
  source_code_hash               = filebase64sha256("${var.dist_path}/alert-dispatcher.zip")
  reserved_concurrent_executions = var.lambda_concurrency.alert_dispatcher

  environment {
    variables = merge(
      var.slack_secret_arn == "" ? { SLACK_BOT_TOKEN = var.slack_bot_token } : {},
      {
        SLACK_CHANNEL_ID = var.slack_channel_id
        SLACK_SECRET_ARN = var.slack_secret_arn
        EVENTS_TABLE     = aws_dynamodb_table.events.name
        EVENTS_TTL_DAYS  = var.events_table_ttl_days
        ALERT_QUEUE_URL  = aws_sqs_queue.alert.url
      }
    )
  }

  lifecycle {
    precondition {
      condition     = var.slack_channel_id != "" || (var.slack_bot_token == "" && var.slack_secret_arn == "")
      error_message = "slack_channel_id is required when slack_bot_token or slack_secret_arn is set."
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda["alert-dispatcher"]]
}

# -----------------------------------------------------------------------------
# 1. stream-router — processes DynamoDB stream events from control + joblog
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "stream_router" {
  function_name                  = "${var.environment}-interlock-stream-router"
  role                           = aws_iam_role.lambda["stream-router"].arn
  handler                        = "bootstrap"
  runtime                        = "provided.al2023"
  architectures                  = ["arm64"]
  memory_size                    = var.lambda_memory_size
  timeout                        = 60
  filename                       = "${var.dist_path}/stream-router.zip"
  source_code_hash               = filebase64sha256("${var.dist_path}/stream-router.zip")
  reserved_concurrent_executions = var.lambda_concurrency.stream_router

  environment {
    variables = {
      CONTROL_TABLE       = aws_dynamodb_table.control.name
      JOBLOG_TABLE        = aws_dynamodb_table.joblog.name
      RERUN_TABLE         = aws_dynamodb_table.rerun.name
      STATE_MACHINE_ARN   = aws_sfn_state_machine.pipeline.arn
      EVENT_BUS_NAME      = aws_cloudwatch_event_bus.interlock.name
      SFN_TIMEOUT_SECONDS = tostring(var.sfn_timeout_seconds)
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda["stream-router"]]
}

# -----------------------------------------------------------------------------
# 2. orchestrator — invoked by Step Functions, evaluates + triggers jobs
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "orchestrator" {
  function_name                  = "${var.environment}-interlock-orchestrator"
  role                           = aws_iam_role.lambda["orchestrator"].arn
  handler                        = "bootstrap"
  runtime                        = "provided.al2023"
  architectures                  = ["arm64"]
  memory_size                    = var.lambda_memory_size
  timeout                        = 120
  filename                       = "${var.dist_path}/orchestrator.zip"
  source_code_hash               = filebase64sha256("${var.dist_path}/orchestrator.zip")
  reserved_concurrent_executions = var.lambda_concurrency.orchestrator

  environment {
    variables = merge(var.orchestrator_env, {
      CONTROL_TABLE  = aws_dynamodb_table.control.name
      JOBLOG_TABLE   = aws_dynamodb_table.joblog.name
      RERUN_TABLE    = aws_dynamodb_table.rerun.name
      EVENT_BUS_NAME = aws_cloudwatch_event_bus.interlock.name
      # CompleteTrigger starts retries. Built from the name: the state machine
      # references this function, so its ARN attribute would be a cycle.
      STATE_MACHINE_ARN   = "arn:${data.aws_partition.current.partition}:states:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:stateMachine:${var.environment}-interlock-pipeline"
      SFN_TIMEOUT_SECONDS = tostring(var.sfn_timeout_seconds)
    }, var.trigger_secrets_arn != "" ? { TRIGGER_SECRETS_ARN = var.trigger_secrets_arn } : {})
  }

  depends_on = [aws_cloudwatch_log_group.lambda["orchestrator"]]
}

# -----------------------------------------------------------------------------
# 3. sla-monitor — lightweight SLA deadline checks, invoked by Step Functions
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "sla_monitor" {
  function_name                  = "${var.environment}-interlock-sla-monitor"
  role                           = aws_iam_role.lambda["sla-monitor"].arn
  handler                        = "bootstrap"
  runtime                        = "provided.al2023"
  architectures                  = ["arm64"]
  memory_size                    = var.lambda_memory_size
  timeout                        = 30
  filename                       = "${var.dist_path}/sla-monitor.zip"
  source_code_hash               = filebase64sha256("${var.dist_path}/sla-monitor.zip")
  reserved_concurrent_executions = var.lambda_concurrency.sla_monitor

  environment {
    variables = {
      CONTROL_TABLE        = aws_dynamodb_table.control.name
      JOBLOG_TABLE         = aws_dynamodb_table.joblog.name
      RERUN_TABLE          = aws_dynamodb_table.rerun.name
      EVENT_BUS_NAME       = aws_cloudwatch_event_bus.interlock.name
      SLA_MONITOR_ARN      = "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.environment}-interlock-sla-monitor"
      SCHEDULER_ROLE_ARN   = aws_iam_role.scheduler_sla.arn
      SCHEDULER_GROUP_NAME = aws_scheduler_schedule_group.sla.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda["sla-monitor"]]
}

# -----------------------------------------------------------------------------
# 4. watchdog — scans for missed schedules, invoked by EventBridge schedule
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "watchdog" {
  function_name                  = "${var.environment}-interlock-watchdog"
  role                           = aws_iam_role.lambda["watchdog"].arn
  handler                        = "bootstrap"
  runtime                        = "provided.al2023"
  architectures                  = ["arm64"]
  memory_size                    = var.lambda_memory_size
  timeout                        = 60
  filename                       = "${var.dist_path}/watchdog.zip"
  source_code_hash               = filebase64sha256("${var.dist_path}/watchdog.zip")
  reserved_concurrent_executions = var.lambda_concurrency.watchdog

  environment {
    variables = {
      CONTROL_TABLE        = aws_dynamodb_table.control.name
      JOBLOG_TABLE         = aws_dynamodb_table.joblog.name
      RERUN_TABLE          = aws_dynamodb_table.rerun.name
      STATE_MACHINE_ARN    = aws_sfn_state_machine.pipeline.arn
      EVENT_BUS_NAME       = aws_cloudwatch_event_bus.interlock.name
      SLA_MONITOR_ARN      = aws_lambda_function.sla_monitor.arn
      SCHEDULER_ROLE_ARN   = aws_iam_role.scheduler_sla.arn
      SCHEDULER_GROUP_NAME = aws_scheduler_schedule_group.sla.name
      SFN_TIMEOUT_SECONDS  = tostring(var.sfn_timeout_seconds)
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda["watchdog"]]
}

# =============================================================================
# IAM policies — least-privilege per function
# =============================================================================

# -----------------------------------------------------------------------------
# DynamoDB read/write: stream-router, orchestrator (control, joblog, rerun)
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "dynamodb_rw" {
  statement {
    actions = [
      "dynamodb:GetItem",
      "dynamodb:BatchGetItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:ConditionCheckItem",
      "dynamodb:DescribeTable",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:BatchWriteItem",
    ]
    resources = local.table_and_index_arns
  }
}

resource "aws_iam_role_policy" "dynamodb_rw" {
  for_each = toset(["stream-router", "orchestrator"])
  name     = "dynamodb-rw"
  role     = aws_iam_role.lambda[each.key].id
  policy   = data.aws_iam_policy_document.dynamodb_rw.json
}

# -----------------------------------------------------------------------------
# DynamoDB mixed: watchdog (read control, joblog; write control only)
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "dynamodb_watchdog" {
  statement {
    sid = "ReadControlJoblog"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:BatchGetItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:ConditionCheckItem",
      "dynamodb:DescribeTable",
    ]
    resources = local.watchdog_read_arns
  }

  statement {
    sid = "WriteControlTable"
    actions = [
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
    ]
    resources = [
      aws_dynamodb_table.control.arn,
      "${aws_dynamodb_table.control.arn}/index/*",
    ]
  }
}

resource "aws_iam_role_policy" "dynamodb_watchdog" {
  name   = "dynamodb-mixed"
  role   = aws_iam_role.lambda["watchdog"].id
  policy = data.aws_iam_policy_document.dynamodb_watchdog.json
}

# -----------------------------------------------------------------------------
# DynamoDB read — sla-monitor (control + joblog, read-only for alert suppression)
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "dynamodb_sla_read" {
  statement {
    sid = "ReadControlJoblog"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:Query",
    ]
    resources = [
      aws_dynamodb_table.control.arn,
      aws_dynamodb_table.joblog.arn,
    ]
  }
}

resource "aws_iam_role_policy" "dynamodb_sla_read" {
  name   = "dynamodb-read"
  role   = aws_iam_role.lambda["sla-monitor"].id
  policy = data.aws_iam_policy_document.dynamodb_sla_read.json
}

# -----------------------------------------------------------------------------
# EventBridge PutEvents — all 4 functions
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "eventbridge_put" {
  statement {
    actions   = ["events:PutEvents"]
    resources = [aws_cloudwatch_event_bus.interlock.arn]
  }
}

resource "aws_iam_role_policy" "eventbridge_put" {
  for_each = toset(["stream-router", "orchestrator", "sla-monitor", "watchdog"])
  name     = "eventbridge-put"
  role     = aws_iam_role.lambda[each.key].id
  policy   = data.aws_iam_policy_document.eventbridge_put.json
}

# -----------------------------------------------------------------------------
# DynamoDB write — event-sink (events table only)
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy" "event_sink_dynamodb" {
  name = "dynamodb-events-write"
  role = aws_iam_role.lambda["event-sink"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["dynamodb:PutItem"]
      Resource = [aws_dynamodb_table.events.arn]
    }]
  })
}

# -----------------------------------------------------------------------------
# SQS SendMessage: event-sink (events it could not write)
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy" "event_sink_dlq" {
  name = "sqs-dlq"
  role = aws_iam_role.lambda["event-sink"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:SendMessage"]
      Resource = [aws_sqs_queue.event_sink_dlq.arn]
    }]
  })
}

# -----------------------------------------------------------------------------
# SQS receive and retry delay: alert-dispatcher (alert queue)
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy" "alert_dispatcher_sqs" {
  name = "sqs-alert-receive"
  role = aws_iam_role.lambda["alert-dispatcher"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"]
      Resource = [aws_sqs_queue.alert.arn]
    }]
  })
}

resource "aws_iam_role_policy" "alert_dispatcher_dynamodb" {
  name = "dynamodb-events-thread"
  role = aws_iam_role.lambda["alert-dispatcher"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["dynamodb:GetItem", "dynamodb:PutItem"]
      Resource = [aws_dynamodb_table.events.arn]
    }]
  })
}

# -----------------------------------------------------------------------------
# Secrets Manager read — alert-dispatcher (Slack bot token, opt-in)
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy" "secrets_alert_dispatcher" {
  count = var.slack_secret_arn != "" ? 1 : 0
  name  = "secrets-read"
  role  = aws_iam_role.lambda["alert-dispatcher"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "ReadSlackSecret"
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = var.slack_secret_arn
    }]
  })
}

# -----------------------------------------------------------------------------
# Secrets Manager read: orchestrator (trigger auth tokens, opt-in)
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy" "orchestrator_trigger_secrets" {
  count = var.trigger_secrets_arn != "" ? 1 : 0
  name  = "trigger-secrets-read"
  role  = aws_iam_role.lambda["orchestrator"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "ReadTriggerSecrets"
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = var.trigger_secrets_arn
    }]
  })
}

# -----------------------------------------------------------------------------
# DynamoDB Streams — stream-router (control + joblog streams)
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "dynamodb_streams" {
  statement {
    actions = [
      "dynamodb:GetRecords",
      "dynamodb:GetShardIterator",
      "dynamodb:DescribeStream",
      "dynamodb:ListStreams",
      "dynamodb:ListShards",
    ]
    resources = [
      aws_dynamodb_table.control.stream_arn,
      aws_dynamodb_table.joblog.stream_arn,
    ]
  }
}

resource "aws_iam_role_policy" "dynamodb_streams" {
  name   = "dynamodb-streams"
  role   = aws_iam_role.lambda["stream-router"].id
  policy = data.aws_iam_policy_document.dynamodb_streams.json
}

# -----------------------------------------------------------------------------
# Step Functions StartExecution: stream-router, watchdog, orchestrator
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "sfn_start" {
  statement {
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.pipeline.arn]
  }
}

resource "aws_iam_role_policy" "sfn_start" {
  for_each = toset(["stream-router", "watchdog", "orchestrator"])
  name     = "sfn-start"
  role     = aws_iam_role.lambda[each.key].id
  policy   = data.aws_iam_policy_document.sfn_start.json
}

# -----------------------------------------------------------------------------
# SQS SendMessage — stream-router (on-failure destinations for DLQs)
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "sqs_dlq" {
  statement {
    actions = ["sqs:SendMessage"]
    resources = [
      aws_sqs_queue.stream_router_control_dlq.arn,
      aws_sqs_queue.stream_router_joblog_dlq.arn,
    ]
  }
}

resource "aws_iam_role_policy" "sqs_dlq" {
  name   = "sqs-dlq"
  role   = aws_iam_role.lambda["stream-router"].id
  policy = data.aws_iam_policy_document.sqs_dlq.json
}

# -----------------------------------------------------------------------------
# KMS: with a customer managed key, every Lambda role can use it. DynamoDB
# decrypts the table key as the caller on every table or stream read and write;
# SQS needs Decrypt to receive and Decrypt plus GenerateDataKey to send. One
# action list for all roles is deliberate: only stream-router and event-sink
# send to an encrypted queue and need GenerateDataKey, the rest need only Decrypt.
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy" "kms" {
  for_each = var.kms_key_arn != "" ? local.lambda_names : toset([])
  name     = "kms"
  role     = aws_iam_role.lambda[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["kms:Decrypt", "kms:GenerateDataKey"]
      Resource = [var.kms_key_arn]
    }]
  })
}

# =============================================================================
# SQS Dead-Letter Queues for stream event source mappings and the event-sink
# =============================================================================

resource "aws_sqs_queue" "stream_router_control_dlq" {
  name                              = "${var.environment}-interlock-sr-control-dlq"
  message_retention_seconds         = 1209600 # 14 days
  kms_master_key_id                 = var.kms_key_arn != "" ? var.kms_key_arn : "alias/aws/sqs"
  kms_data_key_reuse_period_seconds = 300
  tags                              = var.tags
}

resource "aws_sqs_queue" "stream_router_joblog_dlq" {
  name                              = "${var.environment}-interlock-sr-joblog-dlq"
  message_retention_seconds         = 1209600 # 14 days
  kms_master_key_id                 = var.kms_key_arn != "" ? var.kms_key_arn : "alias/aws/sqs"
  kms_data_key_reuse_period_seconds = 300
  tags                              = var.tags
}

resource "aws_sqs_queue" "event_sink_dlq" {
  name                              = "${var.environment}-interlock-event-sink-dlq"
  message_retention_seconds         = 1209600 # 14 days
  kms_master_key_id                 = var.kms_key_arn != "" ? var.kms_key_arn : "alias/aws/sqs"
  kms_data_key_reuse_period_seconds = 300
  tags                              = var.tags
}

# =============================================================================
# Failure destination: event-sink
# =============================================================================

# EventBridge invokes the event-sink asynchronously. Lambda sends an event that
# failed every try, or grew too old waiting for one, to the queue as the
# function's role. Retries and the maximum event age stay at Lambda's defaults.
resource "aws_lambda_function_event_invoke_config" "event_sink" {
  function_name = aws_lambda_function.event_sink.function_name

  destination_config {
    on_failure {
      destination = aws_sqs_queue.event_sink_dlq.arn
    }
  }

  depends_on = [aws_iam_role_policy.event_sink_dlq]
}

# =============================================================================
# Event source mappings: DynamoDB Streams → stream-router
# =============================================================================

resource "aws_lambda_event_source_mapping" "control_stream" {
  event_source_arn               = aws_dynamodb_table.control.stream_arn
  function_name                  = aws_lambda_function.stream_router.arn
  starting_position              = "LATEST"
  batch_size                     = 10
  bisect_batch_on_function_error = true
  maximum_retry_attempts         = 3
  function_response_types        = ["ReportBatchItemFailures"]

  # Defense in depth. The primary guard is in handleRecord
  # (internal/lambda/stream/handler.go), which skips REMOVE records; these
  # filters stop them being delivered or billed at all. Multiple filter blocks
  # are OR-ed: writes always pass, and deletes pass only for the CONFIG row so
  # the handler can still invalidate the config cache. Operator deletes and TTL
  # expiries of SENSOR#, JOB#, RERUN_REQUEST# and TRIGGER# rows never arrive.
  filter_criteria {
    filter {
      pattern = jsonencode({ eventName = ["INSERT", "MODIFY"] })
    }
    filter {
      pattern = jsonencode({ eventName = ["REMOVE"], dynamodb = { Keys = { SK = { S = ["CONFIG"] } } } })
    }
  }

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.stream_router_control_dlq.arn
    }
  }
}

resource "aws_lambda_event_source_mapping" "joblog_stream" {
  event_source_arn               = aws_dynamodb_table.joblog.stream_arn
  function_name                  = aws_lambda_function.stream_router.arn
  starting_position              = "LATEST"
  batch_size                     = 10
  bisect_batch_on_function_error = true
  maximum_retry_attempts         = 3
  function_response_types        = ["ReportBatchItemFailures"]

  # Only writes. Unlike the control table, the joblog table holds no CONFIG
  # rows (store.ScanConfigs scans the control table only), so there is no
  # CONFIG-delete escape hatch to add here. This filter also stops the 30-day
  # JOB# TTL expiries from invoking the function.
  filter_criteria {
    filter {
      pattern = jsonencode({ eventName = ["INSERT", "MODIFY"] })
    }
  }

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.stream_router_joblog_dlq.arn
    }
  }
}

# =============================================================================
# Security checks
# =============================================================================

check "slack_token_deprecation" {
  assert {
    condition     = var.slack_bot_token == "" || var.slack_secret_arn != ""
    error_message = "DEPRECATED: Passing a plaintext Slack bot token is deprecated. Use var.slack_secret_arn with an AWS Secrets Manager ARN instead. Plaintext path still works but will be removed in a future version."
  }
}

# =============================================================================
# Conditional trigger permissions for orchestrator (opt-in per trigger type)
# =============================================================================

# --- Glue ---

resource "aws_iam_role_policy" "glue_trigger" {
  count = var.enable_glue_trigger ? 1 : 0
  name  = "glue-trigger"
  role  = aws_iam_role.lambda["orchestrator"].id

  lifecycle {
    precondition {
      condition     = length(var.glue_job_arns) > 0
      error_message = "glue_job_arns must be non-empty when enable_glue_trigger is true."
    }
  }

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun"]
        Resource = var.glue_job_arns
      },
      {
        Sid    = "GlueLogVerification"
        Effect = "Allow"
        Action = ["logs:FilterLogEvents"]
        Resource = [
          "arn:aws:logs:*:*:log-group:/aws-glue/jobs/logs-v2:*",
        ]
      }
    ]
  })
}

# --- EMR ---

resource "aws_iam_role_policy" "emr_trigger" {
  count = var.enable_emr_trigger ? 1 : 0
  name  = "emr-trigger"
  role  = aws_iam_role.lambda["orchestrator"].id

  lifecycle {
    precondition {
      condition     = length(var.emr_cluster_arns) > 0
      error_message = "emr_cluster_arns must be non-empty when enable_emr_trigger is true."
    }
  }

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["elasticmapreduce:AddJobFlowSteps", "elasticmapreduce:DescribeStep"]
      Resource = var.emr_cluster_arns
    }]
  })
}

# --- EMR Serverless ---

resource "aws_iam_role_policy" "emr_serverless_trigger" {
  count = var.enable_emr_serverless_trigger ? 1 : 0
  name  = "emr-serverless-trigger"
  role  = aws_iam_role.lambda["orchestrator"].id

  lifecycle {
    precondition {
      condition     = length(var.emr_serverless_app_arns) > 0
      error_message = "emr_serverless_app_arns must be non-empty when enable_emr_serverless_trigger is true."
    }
    precondition {
      condition     = length(var.emr_serverless_execution_role_arns) > 0
      error_message = "emr_serverless_execution_role_arns must be non-empty when enable_emr_serverless_trigger is true."
    }
  }

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "emr-serverless:StartJobRun"
        Resource = var.emr_serverless_app_arns
      },
      {
        # GetJobRun is authorized on the job run, not the application.
        Effect   = "Allow"
        Action   = "emr-serverless:GetJobRun"
        Resource = [for a in var.emr_serverless_app_arns : "${a}/jobruns/*"]
      },
      {
        # StartJobRun hands the job run its runtime role.
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = var.emr_serverless_execution_role_arns
        Condition = {
          StringEquals = { "iam:PassedToService" = "emr-serverless.amazonaws.com" }
        }
      },
    ]
  })
}

# --- Step Functions (nested execution) ---

resource "aws_iam_role_policy" "sfn_trigger" {
  count = var.enable_sfn_trigger ? 1 : 0
  name  = "sfn-trigger"
  role  = aws_iam_role.lambda["orchestrator"].id

  lifecycle {
    precondition {
      condition     = length(var.sfn_trigger_arns) > 0
      error_message = "sfn_trigger_arns must be non-empty when enable_sfn_trigger is true."
    }
  }

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "states:StartExecution"
        Resource = var.sfn_trigger_arns
      },
      {
        # DescribeExecution is authorized on the execution ARN, which carries
        # the state machine name but not an alias or version suffix.
        Effect = "Allow"
        Action = "states:DescribeExecution"
        Resource = [
          for a in var.sfn_trigger_arns :
          "${join(":", slice(split(":", a), 0, 5))}:execution:${split(":", a)[6]}:*"
        ]
      },
    ]
  })
}

# --- Lambda ---

resource "aws_iam_role_policy" "lambda_trigger" {
  count = var.enable_lambda_trigger ? 1 : 0
  name  = "lambda-trigger"
  role  = aws_iam_role.lambda["orchestrator"].id

  lifecycle {
    precondition {
      condition     = length(var.lambda_trigger_arns) > 0
      error_message = "lambda_trigger_arns must be non-empty when enable_lambda_trigger is true."
    }
  }

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["lambda:InvokeFunction"]
      Resource = var.lambda_trigger_arns
    }]
  })
}
