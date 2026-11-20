# -----------------------------------------------------------------------------
# SQS alert queue — receives alert-worthy EventBridge events for Slack delivery
#
# EventBridge writes to these queues as events.amazonaws.com, which the AWS
# managed SQS key (alias/aws/sqs) does not allow, and its key policy cannot be
# changed. So the queues use SQS-managed encryption (SSE-SQS) by default. With
# kms_key_arn set they use that key, whose policy must let events.amazonaws.com
# call kms:Decrypt and kms:GenerateDataKey.
# -----------------------------------------------------------------------------

resource "aws_sqs_queue" "alert_dlq" {
  name                              = "${var.environment}-interlock-alert-dlq"
  message_retention_seconds         = 1209600 # 14 days
  kms_master_key_id                 = var.kms_key_arn != "" ? var.kms_key_arn : null
  kms_data_key_reuse_period_seconds = var.kms_key_arn != "" ? 300 : null
  sqs_managed_sse_enabled           = var.kms_key_arn == "" ? true : null
  tags                              = var.tags
}

resource "aws_sqs_queue" "alert" {
  name                              = "${var.environment}-interlock-alerts"
  message_retention_seconds         = 86400 # 1 day
  visibility_timeout_seconds        = 60
  kms_master_key_id                 = var.kms_key_arn != "" ? var.kms_key_arn : null
  kms_data_key_reuse_period_seconds = var.kms_key_arn != "" ? 300 : null
  sqs_managed_sse_enabled           = var.kms_key_arn == "" ? true : null
  tags                              = var.tags

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
          "aws:SourceArn" = [
            aws_cloudwatch_event_rule.alert_events.arn,
            aws_cloudwatch_event_rule.cw_alarm_alert.arn,
          ]
        }
      }
    }]
  })
}

# -----------------------------------------------------------------------------
# SQS → alert-dispatcher Lambda event source mapping
# -----------------------------------------------------------------------------

resource "aws_lambda_event_source_mapping" "alert_dispatcher" {
  event_source_arn        = aws_sqs_queue.alert.arn
  function_name           = aws_lambda_function.alert_dispatcher.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
}
