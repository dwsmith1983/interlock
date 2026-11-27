# Only the stream-router, orchestrator, sla-monitor and watchdog publish to the
# Interlock bus. The bus policy names exactly their roles, since in one account
# a resource policy naming a role grants access on its own, and the same four
# get PutEvents in their identity policies.

mock_provider "aws" {
  source = "./tests/mocks"
}

variables {
  environment    = "test"
  dist_path      = "tests/fixtures/dist"
  pipelines_path = ""
}

# Role and bus ARNs are known in a plan only when overridden. Each role gets
# its own ARN so the principals can be told apart.
run "only_publishers_may_put_events_on_the_bus" {
  command = plan

  override_resource {
    target          = aws_cloudwatch_event_bus.interlock
    override_during = plan
    values = {
      arn = "arn:aws:events:eu-west-1:123456789012:event-bus/test-interlock-events"
    }
  }

  override_resource {
    target          = aws_iam_role.lambda["stream-router"]
    override_during = plan
    values = {
      id  = "stream-router-role"
      arn = "arn:aws:iam::123456789012:role/stream-router-role"
    }
  }

  override_resource {
    target          = aws_iam_role.lambda["orchestrator"]
    override_during = plan
    values = {
      id  = "orchestrator-role"
      arn = "arn:aws:iam::123456789012:role/orchestrator-role"
    }
  }

  override_resource {
    target          = aws_iam_role.lambda["sla-monitor"]
    override_during = plan
    values = {
      id  = "sla-monitor-role"
      arn = "arn:aws:iam::123456789012:role/sla-monitor-role"
    }
  }

  override_resource {
    target          = aws_iam_role.lambda["watchdog"]
    override_during = plan
    values = {
      id  = "watchdog-role"
      arn = "arn:aws:iam::123456789012:role/watchdog-role"
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

  override_resource {
    target          = aws_iam_role.lambda["alert-dispatcher"]
    override_during = plan
    values = {
      id  = "alert-dispatcher-role"
      arn = "arn:aws:iam::123456789012:role/alert-dispatcher-role"
    }
  }

  assert {
    condition = jsondecode(aws_cloudwatch_event_bus_policy.interlock_bus.policy).Statement == [{
      Sid    = "AllowInterlockLambdas"
      Effect = "Allow"
      Principal = {
        AWS = [
          "arn:aws:iam::123456789012:role/orchestrator-role",
          "arn:aws:iam::123456789012:role/sla-monitor-role",
          "arn:aws:iam::123456789012:role/stream-router-role",
          "arn:aws:iam::123456789012:role/watchdog-role",
        ]
      }
      Action   = "events:PutEvents"
      Resource = aws_cloudwatch_event_bus.interlock.arn
    }]
    error_message = "The bus policy must let only the stream-router, orchestrator, sla-monitor and watchdog roles put events."
  }

  assert {
    condition     = toset(keys(aws_iam_role_policy.eventbridge_put)) == toset(["stream-router", "orchestrator", "sla-monitor", "watchdog"])
    error_message = "Only the stream-router, orchestrator, sla-monitor and watchdog may have the PutEvents policy."
  }

  assert {
    condition = toset([for p in aws_iam_role_policy.eventbridge_put : p.role]) == toset([
      "stream-router-role", "orchestrator-role", "sla-monitor-role", "watchdog-role",
    ])
    error_message = "The PutEvents policies must sit on the four publishing roles."
  }
}
