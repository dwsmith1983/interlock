# Mock data shared by every test file through mock_provider "aws" { source }.

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
