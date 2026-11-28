# The watchdog runs on a schedule. If the schedule stalls or is disabled, its
# Errors alarm stays OK (no invocations, no errors), so a heartbeat alarm goes
# to ALARM when the watchdog has not been invoked for watchdog_heartbeat_minutes.

mock_provider "aws" {
  source = "./tests/mocks"
}

variables {
  environment    = "test"
  dist_path      = "tests/fixtures/dist"
  pipelines_path = ""
}

run "heartbeat_alarm_watches_watchdog_invocations" {
  command = plan

  variables {
    sns_alarm_topic_arn = "arn:aws:sns:eu-west-1:123456789012:ops"
  }

  assert {
    condition     = aws_cloudwatch_metric_alarm.watchdog_heartbeat.alarm_name == "test-interlock-watchdog-heartbeat"
    error_message = "The heartbeat alarm must be named with the interlock- prefix, so the alarm rule sends it to Slack."
  }

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.watchdog_heartbeat.namespace == "AWS/Lambda" &&
      aws_cloudwatch_metric_alarm.watchdog_heartbeat.metric_name == "Invocations" &&
      aws_cloudwatch_metric_alarm.watchdog_heartbeat.statistic == "Sum" &&
      aws_cloudwatch_metric_alarm.watchdog_heartbeat.period == 300
    )
    error_message = "The heartbeat alarm must sum the watchdog's Lambda Invocations over 5-minute periods."
  }

  assert {
    condition = aws_cloudwatch_metric_alarm.watchdog_heartbeat.dimensions == tomap({
      FunctionName = "test-interlock-watchdog"
    })
    error_message = "The heartbeat alarm must watch the watchdog function."
  }

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.watchdog_heartbeat.evaluation_periods == 3 &&
      aws_cloudwatch_metric_alarm.watchdog_heartbeat.datapoints_to_alarm == 3
    )
    error_message = "The default 15-minute heartbeat must alarm after 3 empty 5-minute periods."
  }

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.watchdog_heartbeat.comparison_operator == "LessThanThreshold" &&
      aws_cloudwatch_metric_alarm.watchdog_heartbeat.threshold == 1
    )
    error_message = "The heartbeat alarm must fire on fewer than one invocation per period."
  }

  # No invocations means no datapoints, which must count as breaching.
  assert {
    condition     = aws_cloudwatch_metric_alarm.watchdog_heartbeat.treat_missing_data == "breaching"
    error_message = "Missing Invocations data must count as breaching."
  }

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.watchdog_heartbeat.alarm_actions == toset([var.sns_alarm_topic_arn]) &&
      aws_cloudwatch_metric_alarm.watchdog_heartbeat.ok_actions == toset([var.sns_alarm_topic_arn])
    )
    error_message = "The heartbeat alarm must notify sns_alarm_topic_arn on ALARM and OK."
  }
}

run "heartbeat_minutes_set_the_periods" {
  command = plan

  variables {
    watchdog_heartbeat_minutes = 30
  }

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.watchdog_heartbeat.evaluation_periods == 6 &&
      aws_cloudwatch_metric_alarm.watchdog_heartbeat.datapoints_to_alarm == 6
    )
    error_message = "A 30-minute heartbeat must alarm after 6 empty 5-minute periods."
  }
}

run "heartbeat_must_cover_two_watchdog_intervals" {
  command = plan

  variables {
    watchdog_schedule          = "rate(10 minutes)"
    watchdog_heartbeat_minutes = 15
  }

  expect_failures = [aws_cloudwatch_metric_alarm.watchdog_heartbeat]
}

run "heartbeat_accepts_two_hourly_intervals" {
  command = plan

  variables {
    watchdog_schedule          = "rate(1 hour)"
    watchdog_heartbeat_minutes = 120
  }

  assert {
    condition     = aws_cloudwatch_metric_alarm.watchdog_heartbeat.evaluation_periods == 24
    error_message = "A 120-minute heartbeat must alarm after 24 empty 5-minute periods."
  }
}

run "heartbeat_rejects_one_hourly_interval" {
  command = plan

  variables {
    watchdog_schedule          = "rate(1 hour)"
    watchdog_heartbeat_minutes = 90
  }

  expect_failures = [aws_cloudwatch_metric_alarm.watchdog_heartbeat]
}

# A cron gap is not computed; the variable's description covers it.
run "heartbeat_skips_the_check_for_cron" {
  command = plan

  variables {
    watchdog_schedule = "cron(0 * * * ? *)"
  }

  assert {
    condition     = aws_cloudwatch_metric_alarm.watchdog_heartbeat.evaluation_periods == 3
    error_message = "A cron schedule must not be checked against the heartbeat."
  }
}

run "heartbeat_minutes_rejects_non_multiple_of_five" {
  command = plan

  variables {
    watchdog_heartbeat_minutes = 12
  }

  expect_failures = [var.watchdog_heartbeat_minutes]
}

run "heartbeat_minutes_rejects_five" {
  command = plan

  variables {
    watchdog_heartbeat_minutes = 5
  }

  expect_failures = [var.watchdog_heartbeat_minutes]
}

run "heartbeat_minutes_rejects_over_a_day" {
  command = plan

  variables {
    watchdog_heartbeat_minutes = 1445
  }

  expect_failures = [var.watchdog_heartbeat_minutes]
}
