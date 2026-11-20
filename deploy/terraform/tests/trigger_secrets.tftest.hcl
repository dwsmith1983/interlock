# Trigger auth tokens reach the orchestrator either from a Secrets Manager
# secret named by trigger_secrets_arn or as plain env vars in orchestrator_env.

mock_provider "aws" {
  source = "./tests/mocks"
}

variables {
  environment    = "test"
  dist_path      = "tests/fixtures/dist"
  pipelines_path = ""
}

run "trigger_secret_env_and_grant" {
  command = plan

  variables {
    trigger_secrets_arn = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:interlock-triggers-AbCdEf"
  }

  assert {
    condition     = aws_lambda_function.orchestrator.environment[0].variables["TRIGGER_SECRETS_ARN"] == var.trigger_secrets_arn
    error_message = "The orchestrator must get TRIGGER_SECRETS_ARN when trigger_secrets_arn is set."
  }

  assert {
    condition     = length(jsondecode(aws_iam_role_policy.orchestrator_trigger_secrets[0].policy).Statement) == 1
    error_message = "The trigger secret policy must hold a single statement."
  }

  assert {
    condition = alltrue([
      for s in jsondecode(aws_iam_role_policy.orchestrator_trigger_secrets[0].policy).Statement :
      s.Effect == "Allow" &&
      flatten([s.Action]) == ["secretsmanager:GetSecretValue"] &&
      flatten([s.Resource]) == [var.trigger_secrets_arn]
    ])
    error_message = "The orchestrator may only read the configured trigger secret."
  }
}

run "no_trigger_secret" {
  command = plan

  assert {
    condition     = !contains(keys(aws_lambda_function.orchestrator.environment[0].variables), "TRIGGER_SECRETS_ARN")
    error_message = "TRIGGER_SECRETS_ARN must be unset without trigger_secrets_arn."
  }

  assert {
    condition     = length(aws_iam_role_policy.orchestrator_trigger_secrets) == 0
    error_message = "No trigger secret policy may be created without trigger_secrets_arn."
  }
}

run "orchestrator_env_merged" {
  command = plan

  variables {
    orchestrator_env = { INTERLOCK_API_URL = "https://example.com" }
  }

  assert {
    condition     = aws_lambda_function.orchestrator.environment[0].variables["INTERLOCK_API_URL"] == "https://example.com"
    error_message = "orchestrator_env entries must reach the orchestrator."
  }

  assert {
    condition     = contains(keys(aws_lambda_function.orchestrator.environment[0].variables), "CONTROL_TABLE")
    error_message = "orchestrator_env must not replace the fixed variables."
  }
}

run "orchestrator_env_rejects_other_keys" {
  command = plan

  variables {
    orchestrator_env = { INTERLOCK_token = "x" }
  }

  expect_failures = [var.orchestrator_env]
}

run "orchestrator_env_rejects_unprefixed_keys" {
  command = plan

  variables {
    orchestrator_env = { AWS_REGION = "x" }
  }

  expect_failures = [var.orchestrator_env]
}

run "trigger_secrets_arn_rejects_wildcard" {
  command = plan

  variables {
    trigger_secrets_arn = "*"
  }

  expect_failures = [var.trigger_secrets_arn]
}

run "trigger_secrets_arn_rejects_wildcard_name" {
  command = plan

  variables {
    trigger_secrets_arn = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:*"
  }

  expect_failures = [var.trigger_secrets_arn]
}
