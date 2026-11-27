# Only the event-sink and the alert-dispatcher use the events table, so the
# stream-router, orchestrator and watchdog roles get no access to it. The
# watchdog never reads the rerun table either.

mock_provider "aws" {
  source = "./tests/mocks"
}

variables {
  environment    = "test"
  dist_path      = "tests/fixtures/dist"
  pipelines_path = ""
}

# Table ARNs are known in a plan only when overridden.
run "events_table_only_for_the_roles_that_use_it" {
  command = plan

  override_resource {
    target          = aws_dynamodb_table.control
    override_during = plan
    values = {
      arn = "arn:aws:dynamodb:eu-west-1:123456789012:table/test-interlock-control"
    }
  }

  override_resource {
    target          = aws_dynamodb_table.joblog
    override_during = plan
    values = {
      arn = "arn:aws:dynamodb:eu-west-1:123456789012:table/test-interlock-joblog"
    }
  }

  override_resource {
    target          = aws_dynamodb_table.rerun
    override_during = plan
    values = {
      arn = "arn:aws:dynamodb:eu-west-1:123456789012:table/test-interlock-rerun"
    }
  }

  override_resource {
    target          = aws_dynamodb_table.events
    override_during = plan
    values = {
      arn = "arn:aws:dynamodb:eu-west-1:123456789012:table/test-interlock-events"
    }
  }

  assert {
    condition = toset(data.aws_iam_policy_document.dynamodb_rw.statement[0].resources) == toset([
      aws_dynamodb_table.control.arn, "${aws_dynamodb_table.control.arn}/index/*",
      aws_dynamodb_table.joblog.arn, "${aws_dynamodb_table.joblog.arn}/index/*",
      aws_dynamodb_table.rerun.arn, "${aws_dynamodb_table.rerun.arn}/index/*",
    ])
    error_message = "The stream-router and orchestrator must reach the control, joblog and rerun tables and their indexes, and not the events table."
  }

  assert {
    condition     = length(data.aws_iam_policy_document.dynamodb_rw.statement) == 1
    error_message = "The stream-router and orchestrator DynamoDB policy must have one statement."
  }

  assert {
    condition = toset(one([
      for s in data.aws_iam_policy_document.dynamodb_watchdog.statement : s.resources if contains(s.actions, "dynamodb:GetItem")
      ])) == toset([
      aws_dynamodb_table.control.arn, "${aws_dynamodb_table.control.arn}/index/*",
      aws_dynamodb_table.joblog.arn, "${aws_dynamodb_table.joblog.arn}/index/*",
    ])
    error_message = "The watchdog must read the control and joblog tables and their indexes, and not the rerun or events table."
  }

  assert {
    condition     = length(data.aws_iam_policy_document.dynamodb_watchdog.statement) == 2
    error_message = "The watchdog DynamoDB policy must have one read and one write statement."
  }

  assert {
    condition = toset(one([
      for s in data.aws_iam_policy_document.dynamodb_watchdog.statement : s.resources if contains(s.actions, "dynamodb:PutItem")
      ])) == toset([
      aws_dynamodb_table.control.arn, "${aws_dynamodb_table.control.arn}/index/*",
    ])
    error_message = "The watchdog must write the control table only."
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.event_sink_dynamodb.policy).Statement == [{
      Effect   = "Allow"
      Action   = ["dynamodb:PutItem"]
      Resource = [aws_dynamodb_table.events.arn]
    }]
    error_message = "The event-sink must keep PutItem on the events table."
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.alert_dispatcher_dynamodb.policy).Statement == [{
      Effect   = "Allow"
      Action   = ["dynamodb:GetItem", "dynamodb:PutItem"]
      Resource = [aws_dynamodb_table.events.arn]
    }]
    error_message = "The alert-dispatcher must keep GetItem and PutItem on the events table."
  }
}
