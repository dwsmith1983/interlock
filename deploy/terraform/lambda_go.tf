# -----------------------------------------------------------------------------
# Core Lambdas (orchestrator, evaluator, trigger, run-checker)
# Shared pattern: provided.al2023, arm64, archetype layer, common env vars
# -----------------------------------------------------------------------------

data "archive_file" "core_lambda" {
  for_each    = local.core_lambdas
  type        = "zip"
  source_file = "${var.lambda_dist_dir}/${each.key}/bootstrap"
  output_path = "${path.module}/.build/${each.key}.zip"
}

resource "aws_cloudwatch_log_group" "core" {
  for_each          = local.core_lambdas
  name              = "/aws/lambda/${var.table_name}-${each.key}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "core" {
  for_each = local.core_lambdas

  function_name    = "${var.table_name}-${each.key}"
  role             = aws_iam_role.lambda[each.key].arn
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  memory_size      = var.lambda_memory_size
  timeout          = var.lambda_timeout
  filename         = data.archive_file.core_lambda[each.key].output_path
  source_code_hash = data.archive_file.core_lambda[each.key].output_base64sha256

  layers = [aws_lambda_layer_version.archetypes.arn]

  environment {
    variables = local.common_env
  }

  depends_on = [aws_cloudwatch_log_group.core]
}

# -----------------------------------------------------------------------------
# stream-router (separate to break SFN → Lambda dependency cycle)
# Only env var is STATE_MACHINE_ARN; no archetype layer needed.
# -----------------------------------------------------------------------------

data "archive_file" "stream_router" {
  type        = "zip"
  source_file = "${var.lambda_dist_dir}/stream-router/bootstrap"
  output_path = "${path.module}/.build/stream-router.zip"
}

resource "aws_cloudwatch_log_group" "stream_router" {
  name              = "/aws/lambda/${var.table_name}-stream-router"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "stream_router" {
  function_name    = "${var.table_name}-stream-router"
  role             = aws_iam_role.lambda["stream-router"].arn
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  memory_size      = var.lambda_memory_size
  timeout          = var.lambda_timeout
  filename         = data.archive_file.stream_router.output_path
  source_code_hash = data.archive_file.stream_router.output_base64sha256

  environment {
    variables = {
      STATE_MACHINE_ARN = aws_sfn_state_machine.pipeline.arn
    }
  }

  depends_on = [aws_cloudwatch_log_group.stream_router]
}

# -----------------------------------------------------------------------------
# Event source mapping: DynamoDB Stream → stream-router
# -----------------------------------------------------------------------------

resource "aws_lambda_event_source_mapping" "dynamodb_stream" {
  event_source_arn  = aws_dynamodb_table.main.stream_arn
  function_name     = aws_lambda_function.stream_router.arn
  starting_position = "LATEST"
  batch_size        = 10
}
