resource "aws_cloudwatch_event_bus" "interlock" {
  name = "${var.environment}-interlock-events"
  tags = var.tags
}

# Watchdog schedule
resource "aws_cloudwatch_event_rule" "watchdog" {
  name                = "${var.environment}-interlock-watchdog"
  description         = "Periodic watchdog for stale triggers and missed schedules"
  schedule_expression = var.watchdog_schedule
  event_bus_name      = "default" # scheduled rules use default bus
  tags                = var.tags
}

resource "aws_cloudwatch_event_target" "watchdog" {
  rule      = aws_cloudwatch_event_rule.watchdog.name
  target_id = "watchdog-lambda"
  arn       = aws_lambda_function.watchdog.arn
}

resource "aws_lambda_permission" "watchdog_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.watchdog.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.watchdog.arn
}

# -----------------------------------------------------------------------------
# EventBridge rule — ALL events → event-sink Lambda (centralized logging)
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "all_events" {
  name           = "${var.environment}-interlock-all-events"
  description    = "Route all interlock events to the event-sink Lambda"
  event_bus_name = aws_cloudwatch_event_bus.interlock.name
  tags           = var.tags

  event_pattern = jsonencode({
    source = ["interlock"]
  })
}

resource "aws_cloudwatch_event_target" "event_sink" {
  rule           = aws_cloudwatch_event_rule.all_events.name
  event_bus_name = aws_cloudwatch_event_bus.interlock.name
  target_id      = "event-sink-lambda"
  arn            = aws_lambda_function.event_sink.arn
}

resource "aws_lambda_permission" "event_sink_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.event_sink.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.all_events.arn
}

# -----------------------------------------------------------------------------
# EventBridge rule — alert events → SQS queue (Slack delivery)
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "alert_events" {
  name           = "${var.environment}-interlock-alert-events"
  description    = "Route alert-worthy interlock events to SQS for Slack delivery"
  event_bus_name = aws_cloudwatch_event_bus.interlock.name
  tags           = var.tags

  event_pattern = jsonencode({
    source      = ["interlock"]
    detail-type = [
      "SLA_WARNING",
      "SLA_BREACH",
      "SLA_MET",
      "JOB_FAILED",
      "VALIDATION_EXHAUSTED",
      "RETRY_EXHAUSTED",
      "INFRA_FAILURE",
      "SFN_TIMEOUT",
      "SCHEDULE_MISSED",
      "DATA_DRIFT",
    ]
  })
}

resource "aws_cloudwatch_event_target" "alert_sqs" {
  rule           = aws_cloudwatch_event_rule.alert_events.name
  event_bus_name = aws_cloudwatch_event_bus.interlock.name
  target_id      = "alert-sqs"
  arn            = aws_sqs_queue.alert.arn
}
