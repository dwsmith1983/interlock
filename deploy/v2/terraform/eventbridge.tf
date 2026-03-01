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
