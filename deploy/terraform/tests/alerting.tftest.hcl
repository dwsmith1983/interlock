# EventBridge must be able to write to the alert queues. It cannot use the AWS
# managed SQS key (its key policy cannot grant events.amazonaws.com), so the
# queues use SQS-managed encryption unless a customer key is given.

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
    }
  }

  mock_data "aws_region" {
    defaults = {
      name = "eu-west-1"
    }
  }

  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
    }
  }

  # The provider still validates policy arguments, so the generated documents
  # must be JSON objects rather than the mock's random strings.
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
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
