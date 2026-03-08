# -----------------------------------------------------------------------------
# CloudWatch alarms — Lambda errors, SFN failures, DLQ depth, stream lag
# -----------------------------------------------------------------------------

locals {
  # Map of Terraform resource key → Lambda function resource for alarm iteration.
  lambda_functions = {
    stream_router    = aws_lambda_function.stream_router
    orchestrator     = aws_lambda_function.orchestrator
    sla_monitor      = aws_lambda_function.sla_monitor
    watchdog         = aws_lambda_function.watchdog
    event_sink       = aws_lambda_function.event_sink
    alert_dispatcher = aws_lambda_function.alert_dispatcher
  }

  # DLQ resources keyed by a short label.
  dlq_queues = {
    sr_control = aws_sqs_queue.stream_router_control_dlq
    sr_joblog  = aws_sqs_queue.stream_router_joblog_dlq
    alert      = aws_sqs_queue.alert_dlq
  }

  # DynamoDB stream event source mappings keyed by table name.
  stream_mappings = {
    control = aws_lambda_event_source_mapping.control_stream
    joblog  = aws_lambda_event_source_mapping.joblog_stream
  }

  alarm_actions = var.sns_alarm_topic_arn != "" ? [var.sns_alarm_topic_arn] : []
}

# =============================================================================
# 1. Lambda Error Alarms — one per function
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each = local.lambda_functions

  alarm_name          = "${var.environment}-interlock-${each.key}-errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "${each.key} Lambda errors detected"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = each.value.function_name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

# =============================================================================
# 2. Step Functions Execution Failure Alarm
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "sfn_failures" {
  alarm_name          = "${var.environment}-interlock-sfn-failures"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Step Functions pipeline execution failures detected"
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.pipeline.arn
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

# =============================================================================
# 3. DLQ Message Count Alarms — fires when any message lands in a DLQ
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "dlq_messages" {
  for_each = local.dlq_queues

  alarm_name          = "${var.environment}-interlock-dlq-${each.key}-depth"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Messages visible in ${each.key} dead-letter queue"
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = each.value.name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

# =============================================================================
# 4. DynamoDB Stream Iterator Age Alarms — detects stream processing lag
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "stream_iterator_age" {
  for_each = local.stream_mappings

  alarm_name          = "${var.environment}-interlock-stream-${each.key}-iterator-age"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "IteratorAge"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Maximum"
  threshold           = 300000 # 5 minutes in milliseconds
  alarm_description   = "DynamoDB ${each.key} stream iterator age exceeds 5 minutes"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName       = aws_lambda_function.stream_router.function_name
    EventSourceMapping = each.value.uuid
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}
