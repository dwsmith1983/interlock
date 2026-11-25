# EventBridge must be able to write to the alert queues. It cannot use the AWS
# managed SQS key (its key policy cannot grant events.amazonaws.com), so the
# queues use SQS-managed encryption unless a customer key is given.

mock_provider "aws" {
  source = "./tests/mocks"
}

variables {
  environment    = "test"
  dist_path      = "tests/fixtures/dist"
  pipelines_path = ""
}

run "default_alert_queues_use_sqs_managed_sse" {
  command = plan

  assert {
    condition     = aws_sqs_queue.alert.sqs_managed_sse_enabled == true
    error_message = "The alert queue must use SQS-managed encryption when no KMS key is given."
  }

  assert {
    condition     = aws_sqs_queue.alert_dlq.sqs_managed_sse_enabled == true
    error_message = "The alert DLQ must use SQS-managed encryption when no KMS key is given."
  }

  assert {
    condition     = aws_sqs_queue.alert.kms_master_key_id == null
    error_message = "The alert queue must not set a KMS key when none is given."
  }

  assert {
    condition     = aws_sqs_queue.alert_dlq.kms_master_key_id == null
    error_message = "The alert DLQ must not set a KMS key when none is given."
  }
}

run "cmk_alert_queues_use_the_key" {
  command = plan

  variables {
    kms_key_arn = "arn:aws:kms:eu-west-1:123456789012:key/test"
  }

  assert {
    condition     = aws_sqs_queue.alert.kms_master_key_id == var.kms_key_arn
    error_message = "The alert queue must use the given KMS key."
  }

  assert {
    condition     = aws_sqs_queue.alert_dlq.kms_master_key_id == var.kms_key_arn
    error_message = "The alert DLQ must use the given KMS key."
  }
}

run "events_table_ttl_days_rejects_zero" {
  command = plan

  variables {
    events_table_ttl_days = 0
  }

  expect_failures = [var.events_table_ttl_days]
}

run "events_table_ttl_days_accepts_one" {
  command = plan

  variables {
    events_table_ttl_days = 1
  }

  assert {
    condition     = aws_lambda_function.alert_dispatcher.environment[0].variables["EVENTS_TTL_DAYS"] == "1"
    error_message = "The alert-dispatcher must get events_table_ttl_days as EVENTS_TTL_DAYS."
  }
}
