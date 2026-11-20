# With a customer managed key, every Lambda role reads or writes a table or
# queue encrypted with it, so every role needs the key. DynamoDB and SQS call
# KMS as the caller: Decrypt for any read or write, GenerateDataKey for an SQS
# send.

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

run "cmk_grants_every_lambda_role" {
  command = plan

  variables {
    kms_key_arn = "arn:aws:kms:eu-west-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"
  }

  assert {
    condition     = length(aws_iam_role_policy.kms) == 6
    error_message = "Every Lambda role must get a KMS policy when kms_key_arn is set."
  }

  assert {
    condition     = toset(keys(aws_iam_role_policy.kms)) == local.lambda_names
    error_message = "The KMS policies must cover exactly the Lambda roles."
  }

  assert {
    condition = alltrue([
      for p in aws_iam_role_policy.kms :
      length(jsondecode(p.policy).Statement) == 1
    ])
    error_message = "Each KMS policy must hold a single statement."
  }

  assert {
    condition = alltrue([
      for p in aws_iam_role_policy.kms :
      jsondecode(p.policy).Statement[0].Effect == "Allow" &&
      jsondecode(p.policy).Statement[0].Resource == [var.kms_key_arn]
    ])
    error_message = "Each KMS policy must allow use of the configured key only."
  }

  assert {
    condition = alltrue([
      for p in aws_iam_role_policy.kms :
      toset(jsondecode(p.policy).Statement[0].Action) == toset(["kms:Decrypt", "kms:GenerateDataKey"])
    ])
    error_message = "Each KMS policy must grant kms:Decrypt and kms:GenerateDataKey."
  }
}

run "kms_key_arn_rejects_wildcard" {
  command = plan

  variables {
    kms_key_arn = "*"
  }

  expect_failures = [var.kms_key_arn]
}

run "kms_key_arn_rejects_alias_arn" {
  command = plan

  variables {
    kms_key_arn = "arn:aws:kms:eu-west-1:123456789012:alias/interlock"
  }

  expect_failures = [var.kms_key_arn]
}

run "no_cmk_no_kms_policy" {
  command = plan

  assert {
    condition     = length(aws_iam_role_policy.kms) == 0
    error_message = "No KMS policy may be created without kms_key_arn."
  }
}
