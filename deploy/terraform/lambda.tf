# -----------------------------------------------------------------------------
# Lambda functions, IAM roles, log groups, event source mappings, and DLQs
#
# Cross-file references (defined in stepfunctions.tf / eventbridge.tf):
#   aws_sfn_state_machine.pipeline   — Step Functions state machine
#   aws_cloudwatch_event_bus.interlock — custom EventBridge bus
# -----------------------------------------------------------------------------

locals {
  lambda_names = toset(["stream-router", "orchestrator", "sla-monitor", "watchdog", "event-sink", "alert-dispatcher"])

  table_arns = [
    aws_dynamodb_table.control.arn,
    aws_dynamodb_table.joblog.arn,
    aws_dynamodb_table.rerun.arn,
    aws_dynamodb_table.events.arn,
  ]

  table_and_index_arns = flatten([
    for arn in local.table_arns : [arn, "${arn}/index/*"]
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
  function_name    = "${var.environment}-interlock-event-sink"
  role             = aws_iam_role.lambda["event-sink"].arn
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  memory_size      = 128
  timeout          = 30
  filename         = "${var.dist_path}/event-sink.zip"
  source_code_hash = filebase64sha256("${var.dist_path}/event-sink.zip")

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
  function_name    = "${var.environment}-interlock-alert-dispatcher"
  role             = aws_iam_role.lambda["alert-dispatcher"].arn
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  memory_size      = 128
  timeout          = 30
  filename         = "${var.dist_path}/alert-dispatcher.zip"
  source_code_hash = filebase64sha256("${var.dist_path}/alert-dispatcher.zip")

  environment {
    variables = {
      SLACK_BOT_TOKEN  = var.slack_bot_token
      SLACK_CHANNEL_ID = var.slack_channel_id
      EVENTS_TABLE     = aws_dynamodb_table.events.name
      EVENTS_TTL_DAYS  = var.events_table_ttl_days
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda["alert-dispatcher"]]
}

# -----------------------------------------------------------------------------
# 1. stream-router — processes DynamoDB stream events from control + joblog
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "stream_router" {
  function_name    = "${var.environment}-interlock-stream-router"
  role             = aws_iam_role.lambda["stream-router"].arn
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  memory_size      = var.lambda_memory_size
  timeout          = 60
  filename         = "${var.dist_path}/stream-router.zip"
  source_code_hash = filebase64sha256("${var.dist_path}/stream-router.zip")

  environment {
    variables = {
      CONTROL_TABLE     = aws_dynamodb_table.control.name
      JOBLOG_TABLE      = aws_dynamodb_table.joblog.name
      RERUN_TABLE       = aws_dynamodb_table.rerun.name
      STATE_MACHINE_ARN = aws_sfn_state_machine.pipeline.arn
      EVENT_BUS_NAME    = aws_cloudwatch_event_bus.interlock.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda["stream-router"]]
}

# -----------------------------------------------------------------------------
# 2. orchestrator — invoked by Step Functions, evaluates + triggers jobs
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "orchestrator" {
  function_name    = "${var.environment}-interlock-orchestrator"
  role             = aws_iam_role.lambda["orchestrator"].arn
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  memory_size      = var.lambda_memory_size
  timeout          = 120
  filename         = "${var.dist_path}/orchestrator.zip"
  source_code_hash = filebase64sha256("${var.dist_path}/orchestrator.zip")

  environment {
    variables = {
      CONTROL_TABLE  = aws_dynamodb_table.control.name
      JOBLOG_TABLE   = aws_dynamodb_table.joblog.name
      RERUN_TABLE    = aws_dynamodb_table.rerun.name
      EVENT_BUS_NAME = aws_cloudwatch_event_bus.interlock.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda["orchestrator"]]
}

# -----------------------------------------------------------------------------
# 3. sla-monitor — lightweight SLA deadline checks, invoked by Step Functions
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "sla_monitor" {
  function_name    = "${var.environment}-interlock-sla-monitor"
  role             = aws_iam_role.lambda["sla-monitor"].arn
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  memory_size      = var.lambda_memory_size
  timeout          = 30
  filename         = "${var.dist_path}/sla-monitor.zip"
  source_code_hash = filebase64sha256("${var.dist_path}/sla-monitor.zip")

  environment {
    variables = {
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
  function_name    = "${var.environment}-interlock-watchdog"
  role             = aws_iam_role.lambda["watchdog"].arn
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  memory_size      = var.lambda_memory_size
  timeout          = 60
  filename         = "${var.dist_path}/watchdog.zip"
  source_code_hash = filebase64sha256("${var.dist_path}/watchdog.zip")

  environment {
    variables = {
      CONTROL_TABLE  = aws_dynamodb_table.control.name
      JOBLOG_TABLE   = aws_dynamodb_table.joblog.name
      RERUN_TABLE    = aws_dynamodb_table.rerun.name
      EVENT_BUS_NAME = aws_cloudwatch_event_bus.interlock.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda["watchdog"]]
}

# =============================================================================
# IAM policies — least-privilege per function
# =============================================================================

# -----------------------------------------------------------------------------
# DynamoDB read/write — stream-router, orchestrator (all 3 tables)
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
# DynamoDB mixed — watchdog (read all 3 tables, write control only)
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "dynamodb_watchdog" {
  statement {
    sid = "ReadAllTables"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:BatchGetItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:ConditionCheckItem",
      "dynamodb:DescribeTable",
    ]
    resources = local.table_and_index_arns
  }

  statement {
    sid = "WriteControlTable"
    actions = [
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
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
# SQS receive — alert-dispatcher (alert queue)
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy" "alert_dispatcher_sqs" {
  name = "sqs-alert-receive"
  role = aws_iam_role.lambda["alert-dispatcher"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
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
# Step Functions StartExecution — stream-router
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "sfn_start" {
  statement {
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.pipeline.arn]
  }
}

resource "aws_iam_role_policy" "sfn_start" {
  name   = "sfn-start"
  role   = aws_iam_role.lambda["stream-router"].id
  policy = data.aws_iam_policy_document.sfn_start.json
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

# =============================================================================
# SQS Dead-Letter Queues for stream event source mappings
# =============================================================================

resource "aws_sqs_queue" "stream_router_control_dlq" {
  name                      = "${var.environment}-interlock-sr-control-dlq"
  message_retention_seconds = 1209600 # 14 days
  tags                      = var.tags
}

resource "aws_sqs_queue" "stream_router_joblog_dlq" {
  name                      = "${var.environment}-interlock-sr-joblog-dlq"
  message_retention_seconds = 1209600 # 14 days
  tags                      = var.tags
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

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.stream_router_joblog_dlq.arn
    }
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

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["glue:StartJobRun", "glue:GetJobRun"]
      Resource = "*"
    }]
  })
}

# --- EMR ---

resource "aws_iam_role_policy" "emr_trigger" {
  count = var.enable_emr_trigger ? 1 : 0
  name  = "emr-trigger"
  role  = aws_iam_role.lambda["orchestrator"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["elasticmapreduce:AddJobFlowSteps", "elasticmapreduce:DescribeStep"]
      Resource = "*"
    }]
  })
}

# --- EMR Serverless ---

resource "aws_iam_role_policy" "emr_serverless_trigger" {
  count = var.enable_emr_serverless_trigger ? 1 : 0
  name  = "emr-serverless-trigger"
  role  = aws_iam_role.lambda["orchestrator"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["emr-serverless:StartJobRun", "emr-serverless:GetJobRun"]
      Resource = "*"
    }]
  })
}

# --- Step Functions (nested execution) ---

resource "aws_iam_role_policy" "sfn_trigger" {
  count = var.enable_sfn_trigger ? 1 : 0
  name  = "sfn-trigger"
  role  = aws_iam_role.lambda["orchestrator"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution", "states:DescribeExecution"]
      Resource = "*"
    }]
  })
}
