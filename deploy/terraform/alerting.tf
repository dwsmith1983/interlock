# -----------------------------------------------------------------------------
# SQS alert queue — receives alert-worthy EventBridge events for Slack delivery
# -----------------------------------------------------------------------------

resource "aws_sqs_queue" "alert_dlq" {
  name                      = "${var.environment}-interlock-alert-dlq"
  message_retention_seconds = 1209600 # 14 days
  tags                      = var.tags
}

resource "aws_sqs_queue" "alert" {
  name                       = "${var.environment}-interlock-alerts"
  message_retention_seconds  = 86400 # 1 day
  visibility_timeout_seconds = 60
  tags                       = var.tags

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.alert_dlq.arn
    maxReceiveCount     = 3
  })
}

# Allow EventBridge to send messages to the alert queue.
resource "aws_sqs_queue_policy" "alert" {
  queue_url = aws_sqs_queue.alert.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.alert.arn
      Condition = {
        ArnEquals = {
          "aws:SourceArn" = aws_cloudwatch_event_rule.alert_events.arn
        }
      }
    }]
  })
}
