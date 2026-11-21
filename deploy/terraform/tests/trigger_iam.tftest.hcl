# Status polls are authorized on the run, not on what started it:
# DescribeExecution needs execution ARNs and GetJobRun needs job-run ARNs.

mock_provider "aws" {
  source = "./tests/mocks"
}

variables {
  environment    = "test"
  dist_path      = "tests/fixtures/dist"
  pipelines_path = ""
}

run "sfn_describe_execution_on_execution_arns" {
  command = plan

  variables {
    enable_sfn_trigger = true
    sfn_trigger_arns = [
      "arn:aws:states:eu-west-1:123456789012:stateMachine:etl",
      "arn:aws:states:eu-west-1:123456789012:stateMachine:report:prod",
    ]
  }

  assert {
    condition = toset(flatten([
      for s in jsondecode(aws_iam_role_policy.sfn_trigger[0].policy).Statement :
      flatten([s.Resource]) if contains(flatten([s.Action]), "states:StartExecution")
      ])) == toset([
      "arn:aws:states:eu-west-1:123456789012:stateMachine:etl",
      "arn:aws:states:eu-west-1:123456789012:stateMachine:report:prod",
    ])
    error_message = "StartExecution must be granted on the configured state machine ARNs."
  }

  assert {
    condition = toset(flatten([
      for s in jsondecode(aws_iam_role_policy.sfn_trigger[0].policy).Statement :
      flatten([s.Resource]) if contains(flatten([s.Action]), "states:DescribeExecution")
      ])) == toset([
      "arn:aws:states:eu-west-1:123456789012:execution:etl:*",
      "arn:aws:states:eu-west-1:123456789012:execution:report:*",
    ])
    error_message = "DescribeExecution must be granted on the executions of each state machine."
  }

  assert {
    condition = length([
      for s in jsondecode(aws_iam_role_policy.sfn_trigger[0].policy).Statement : s
      if contains(flatten([s.Action]), "states:DescribeExecution") &&
      anytrue([for r in flatten([s.Resource]) : strcontains(r, ":stateMachine:")])
    ]) == 0
    error_message = "DescribeExecution must not be granted on a state machine ARN."
  }
}

run "emr_serverless_get_job_run_on_job_run_arns" {
  command = plan

  variables {
    enable_emr_serverless_trigger      = true
    emr_serverless_app_arns            = ["arn:aws:emr-serverless:eu-west-1:123456789012:/applications/00abc"]
    emr_serverless_execution_role_arns = ["arn:aws:iam::123456789012:role/emr-job"]
  }

  assert {
    condition = toset(flatten([
      for s in jsondecode(aws_iam_role_policy.emr_serverless_trigger[0].policy).Statement :
      flatten([s.Resource]) if contains(flatten([s.Action]), "emr-serverless:StartJobRun")
    ])) == toset(["arn:aws:emr-serverless:eu-west-1:123456789012:/applications/00abc"])
    error_message = "StartJobRun must be granted on the configured application ARNs."
  }

  assert {
    condition = toset(flatten([
      for s in jsondecode(aws_iam_role_policy.emr_serverless_trigger[0].policy).Statement :
      flatten([s.Resource]) if contains(flatten([s.Action]), "emr-serverless:GetJobRun")
    ])) == toset(["arn:aws:emr-serverless:eu-west-1:123456789012:/applications/00abc/jobruns/*"])
    error_message = "GetJobRun must be granted on the job runs of each application."
  }
}

# StartJobRun hands the job its runtime role, so the orchestrator must be
# allowed to pass exactly those roles, and only to EMR Serverless.
run "emr_serverless_pass_role_on_execution_roles" {
  command = plan

  variables {
    enable_emr_serverless_trigger = true
    emr_serverless_app_arns       = ["arn:aws:emr-serverless:eu-west-1:123456789012:/applications/00abc"]
    emr_serverless_execution_role_arns = [
      "arn:aws:iam::123456789012:role/emr-job",
      "arn:aws:iam::123456789012:role/team/emr-report",
    ]
  }

  assert {
    condition = [
      for s in jsondecode(aws_iam_role_policy.emr_serverless_trigger[0].policy).Statement : {
        effect    = s.Effect
        actions   = flatten([s.Action])
        resources = toset(flatten([s.Resource]))
        condition = try(s.Condition, null)
      } if contains(flatten([s.Action]), "iam:PassRole")
      ] == [{
        effect  = "Allow"
        actions = ["iam:PassRole"]
        resources = toset([
          "arn:aws:iam::123456789012:role/emr-job",
          "arn:aws:iam::123456789012:role/team/emr-report",
        ])
        condition = { StringEquals = { "iam:PassedToService" = "emr-serverless.amazonaws.com" } }
    }]
    error_message = "One statement must allow iam:PassRole alone on the execution roles, only when passed to emr-serverless.amazonaws.com."
  }
}

run "emr_serverless_execution_roles_must_be_role_arns" {
  command = plan

  variables {
    emr_serverless_execution_role_arns = ["arn:aws:iam::123456789012:user/emr-job"]
  }

  expect_failures = [var.emr_serverless_execution_role_arns]
}

run "emr_serverless_execution_roles_reject_wildcard" {
  command = plan

  variables {
    emr_serverless_execution_role_arns = ["arn:aws:iam::123456789012:role/*"]
  }

  expect_failures = [var.emr_serverless_execution_role_arns]
}

run "emr_serverless_execution_roles_reject_path_wildcard" {
  command = plan

  variables {
    emr_serverless_execution_role_arns = ["arn:aws:iam::123456789012:role/team/*"]
  }

  expect_failures = [var.emr_serverless_execution_role_arns]
}

run "emr_serverless_execution_roles_reject_trailing_text" {
  command = plan

  variables {
    emr_serverless_execution_role_arns = ["arn:aws:iam::123456789012:role/emr-job extra"]
  }

  expect_failures = [var.emr_serverless_execution_role_arns]
}

run "emr_serverless_trigger_needs_execution_roles" {
  command = plan

  variables {
    enable_emr_serverless_trigger = true
    emr_serverless_app_arns       = ["arn:aws:emr-serverless:eu-west-1:123456789012:/applications/00abc"]
  }

  expect_failures = [aws_iam_role_policy.emr_serverless_trigger]
}

run "sfn_trigger_arns_must_be_state_machines" {
  command = plan

  variables {
    sfn_trigger_arns = ["arn:aws:states:eu-west-1:123456789012:execution:etl:abc"]
  }

  expect_failures = [var.sfn_trigger_arns]
}

run "sfn_trigger_arns_reject_trailing_segments" {
  command = plan

  variables {
    sfn_trigger_arns = ["arn:aws:states:eu-west-1:123456789012:stateMachine:report:3:junk"]
  }

  expect_failures = [var.sfn_trigger_arns]
}

run "glue_log_check_reads_logs_v2_only" {
  command = plan

  variables {
    enable_glue_trigger = true
    glue_job_arns       = ["arn:aws:glue:eu-west-1:123456789012:job/etl"]
  }

  assert {
    condition = toset(flatten([
      for s in jsondecode(aws_iam_role_policy.glue_trigger[0].policy).Statement :
      flatten([s.Resource]) if contains(flatten([s.Action]), "logs:FilterLogEvents")
      ])) == toset([
      "arn:aws:logs:*:*:log-group:/aws-glue/jobs/logs-v2:*",
    ])
    error_message = "FilterLogEvents must be granted on the job insights log group only."
  }
}
