# Deletion protection is on for all four tables by default, and
# deletion_protection = false turns it off for all of them so that
# terraform destroy can remove a deployment.

mock_provider "aws" {
  source = "./tests/mocks"
}

variables {
  environment    = "test"
  dist_path      = "tests/fixtures/dist"
  pipelines_path = ""
}

run "tables_protected_by_default" {
  command = plan

  assert {
    condition = alltrue([
      aws_dynamodb_table.control.deletion_protection_enabled,
      aws_dynamodb_table.joblog.deletion_protection_enabled,
      aws_dynamodb_table.rerun.deletion_protection_enabled,
      aws_dynamodb_table.events.deletion_protection_enabled,
    ])
    error_message = "All four tables must have deletion protection on by default."
  }
}

run "tables_unprotected_when_turned_off" {
  command = plan

  variables {
    deletion_protection = false
  }

  assert {
    condition = !anytrue([
      aws_dynamodb_table.control.deletion_protection_enabled,
      aws_dynamodb_table.joblog.deletion_protection_enabled,
      aws_dynamodb_table.rerun.deletion_protection_enabled,
      aws_dynamodb_table.events.deletion_protection_enabled,
    ])
    error_message = "deletion_protection = false must turn deletion protection off on all four tables."
  }
}
