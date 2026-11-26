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

# The policies hold the queues' ARNs, which a plan knows only when overridden.
run "failed_alerts_wait_for_the_dispatchers_delay" {
  command = plan

  override_resource {
    target          = aws_sqs_queue.alert
    override_during = plan
    values = {
      arn = "arn:aws:sqs:eu-west-1:123456789012:alerts"
      url = "https://sqs.eu-west-1.amazonaws.com/123456789012/alerts"
    }
  }

  override_resource {
    target          = aws_sqs_queue.alert_dlq
    override_during = plan
    values = {
      arn = "arn:aws:sqs:eu-west-1:123456789012:alert-dlq"
    }
  }

  assert {
    condition     = aws_sqs_queue.alert.visibility_timeout_seconds == 180
    error_message = "The alert queue's visibility timeout must be six times the dispatcher's timeout."
  }

  assert {
    condition     = jsondecode(aws_sqs_queue.alert.redrive_policy).maxReceiveCount == 10
    error_message = "An alert must be tried 10 times before it goes to the DLQ."
  }

  assert {
    condition = anytrue([
      for s in jsondecode(aws_iam_role_policy.alert_dispatcher_sqs.policy).Statement :
      contains(s.Action, "sqs:ChangeMessageVisibility") && s.Resource == [aws_sqs_queue.alert.arn]
    ])
    error_message = "The dispatcher must be able to change the visibility of alert queue messages."
  }

  assert {
    condition     = aws_lambda_function.alert_dispatcher.environment[0].variables["ALERT_QUEUE_URL"] == aws_sqs_queue.alert.url
    error_message = "The dispatcher must get the alert queue's URL as ALERT_QUEUE_URL."
  }
}

run "alarm_events_carry_the_alarms_time" {
  command = plan

  assert {
    condition     = aws_cloudwatch_event_target.cw_alarm_event_sink.input_transformer[0].input_paths["time"] == "$.time"
    error_message = "The event-sink's alarm target must read the alarm event's time."
  }

  assert {
    condition = jsondecode(
      aws_cloudwatch_event_target.cw_alarm_event_sink.input_transformer[0].input_template
    ).detail.timestamp == "<time>"
    error_message = "The event-sink must get the alarm's time as the event timestamp."
  }
}

# An alert EventBridge could not hand to the alert queue is dropped, so each
# alert rule has an alarm on its FailedInvocations. Metrics of a custom-bus rule
# carry EventBusName and RuleName; those of a default-bus rule carry RuleName.
run "alert_rules_alarm_on_failed_invocations" {
  command = plan

  assert {
    condition = alltrue([
      for k in ["alert_events", "cw_alarm_alert"] :
      aws_cloudwatch_metric_alarm.alert_rule_failed_invocations[k].namespace == "AWS/Events" &&
      aws_cloudwatch_metric_alarm.alert_rule_failed_invocations[k].metric_name == "FailedInvocations" &&
      aws_cloudwatch_metric_alarm.alert_rule_failed_invocations[k].threshold == 1 &&
      aws_cloudwatch_metric_alarm.alert_rule_failed_invocations[k].treat_missing_data == "notBreaching"
    ])
    error_message = "Each alert rule must have an alarm on one or more FailedInvocations."
  }

  assert {
    condition = aws_cloudwatch_metric_alarm.alert_rule_failed_invocations["alert_events"].dimensions == tomap({
      EventBusName = "test-interlock-events"
      RuleName     = "test-interlock-alert-events"
    })
    error_message = "The alert_events alarm must watch that rule on the interlock bus."
  }

  assert {
    condition = aws_cloudwatch_metric_alarm.alert_rule_failed_invocations["cw_alarm_alert"].dimensions == tomap({
      RuleName = "test-interlock-cw-alarm-alert"
    })
    error_message = "The cw_alarm_alert alarm must watch that rule by RuleName only, as on the default bus."
  }

  # The alarm rules match the prefix, so the alarm reaches the events table.
  assert {
    condition = alltrue([
      for a in values(aws_cloudwatch_metric_alarm.alert_rule_failed_invocations) :
      startswith(a.alarm_name, "test-interlock-")
    ])
    error_message = "The rule alarms must be named with the environment's interlock- prefix."
  }
}

run "alert_rule_alarms_notify_the_sns_topic" {
  command = plan

  variables {
    sns_alarm_topic_arn = "arn:aws:sns:eu-west-1:123456789012:ops"
  }

  assert {
    condition = alltrue([
      for a in values(aws_cloudwatch_metric_alarm.alert_rule_failed_invocations) :
      contains(a.alarm_actions, var.sns_alarm_topic_arn)
    ])
    error_message = "The rule alarms must notify sns_alarm_topic_arn."
  }
}
