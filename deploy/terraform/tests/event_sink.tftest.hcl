# An event the event-sink could not write goes to a queue, and a message in the
# queue raises an alarm.

mock_provider "aws" {
  source = "./tests/mocks"
}

variables {
  environment    = "test"
  dist_path      = "tests/fixtures/dist"
  pipelines_path = ""
}

# The destination and the policy hold values a plan knows only when overridden.
run "unwritten_events_go_to_the_queue" {
  command = plan

  override_resource {
    target          = aws_sqs_queue.event_sink_dlq
    override_during = plan
    values = {
      arn = "arn:aws:sqs:eu-west-1:123456789012:event-sink-dlq"
    }
  }

  override_resource {
    target          = aws_iam_role.lambda["event-sink"]
    override_during = plan
    values = {
      id  = "event-sink-role"
      arn = "arn:aws:iam::123456789012:role/event-sink-role"
    }
  }

  assert {
    condition     = aws_sqs_queue.event_sink_dlq.name == "test-interlock-event-sink-dlq"
    error_message = "The event-sink's queue must be named {env}-interlock-event-sink-dlq."
  }

  assert {
    condition     = aws_sqs_queue.event_sink_dlq.message_retention_seconds == 1209600
    error_message = "The event-sink's queue must keep messages for 14 days."
  }

  assert {
    condition     = aws_lambda_function_event_invoke_config.event_sink.function_name == aws_lambda_function.event_sink.function_name
    error_message = "The failure destination must belong to the event-sink."
  }

  assert {
    condition     = aws_lambda_function_event_invoke_config.event_sink.destination_config[0].on_failure[0].destination == aws_sqs_queue.event_sink_dlq.arn
    error_message = "Events the event-sink could not write must go to its queue."
  }

  # The mocked plan does not fill in the provider's default, so null means the
  # module leaves the setting alone.
  assert {
    condition     = aws_lambda_function_event_invoke_config.event_sink.maximum_retry_attempts == null
    error_message = "The event-sink must keep Lambda's default number of retries."
  }

  assert {
    condition     = aws_lambda_function_event_invoke_config.event_sink.maximum_event_age_in_seconds == null
    error_message = "The event-sink must keep Lambda's default maximum event age."
  }

  assert {
    condition     = aws_iam_role_policy.event_sink_dlq.role == "event-sink-role"
    error_message = "The queue permission must be on the event-sink's role."
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.event_sink_dlq.policy).Statement == [{
      Effect   = "Allow"
      Action   = ["sqs:SendMessage"]
      Resource = [aws_sqs_queue.event_sink_dlq.arn]
    }]
    error_message = "The event-sink must be allowed to send to its queue, and only that."
  }
}

run "event_sink_queue_has_a_depth_alarm" {
  command = plan

  assert {
    condition     = aws_cloudwatch_metric_alarm.dlq_messages["event_sink"].dimensions["QueueName"] == aws_sqs_queue.event_sink_dlq.name
    error_message = "The event-sink's queue must have a depth alarm."
  }

  # The alarm rules match the prefix, so the alarm reaches Slack.
  assert {
    condition     = aws_cloudwatch_metric_alarm.dlq_messages["event_sink"].alarm_name == "test-interlock-dlq-event_sink-depth"
    error_message = "The event-sink queue's alarm must be named like the other queue alarms."
  }

  assert {
    condition     = toset(keys(aws_cloudwatch_metric_alarm.dlq_messages)) == toset(["sr_control", "sr_joblog", "alert", "event_sink"])
    error_message = "Every dead-letter queue must have a depth alarm."
  }
}

run "event_sink_queue_uses_the_aws_managed_key" {
  command = plan

  assert {
    condition     = aws_sqs_queue.event_sink_dlq.kms_master_key_id == "alias/aws/sqs"
    error_message = "The event-sink's queue must use the AWS managed SQS key when no KMS key is given."
  }
}

run "event_sink_queue_uses_the_given_key" {
  command = plan

  variables {
    kms_key_arn = "arn:aws:kms:eu-west-1:123456789012:key/test"
  }

  assert {
    condition     = aws_sqs_queue.event_sink_dlq.kms_master_key_id == var.kms_key_arn
    error_message = "The event-sink's queue must use the given KMS key."
  }
}
