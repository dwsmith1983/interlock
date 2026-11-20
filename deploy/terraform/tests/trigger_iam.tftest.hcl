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
    enable_emr_serverless_trigger = true
    emr_serverless_app_arns       = ["arn:aws:emr-serverless:eu-west-1:123456789012:/applications/00abc"]
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
