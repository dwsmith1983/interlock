# -----------------------------------------------------------------------------
# Lambda execution roles (one per function)
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  for_each           = local.all_lambdas
  name               = "${var.table_name}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  for_each   = local.all_lambdas
  role       = aws_iam_role.lambda[each.key].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# -----------------------------------------------------------------------------
# DynamoDB read/write — orchestrator, evaluator, trigger
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "dynamodb_rw" {
  statement {
    actions = [
      "dynamodb:GetItem",
      "dynamodb:BatchGetItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:ConditionCheckItem",
      "dynamodb:DescribeTable",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:BatchWriteItem",
    ]
    resources = [
      aws_dynamodb_table.main.arn,
      "${aws_dynamodb_table.main.arn}/index/*",
    ]
  }
}

resource "aws_iam_role_policy" "dynamodb_rw" {
  for_each = toset(["orchestrator", "evaluator", "trigger"])
  name     = "dynamodb-rw"
  role     = aws_iam_role.lambda[each.key].id
  policy   = data.aws_iam_policy_document.dynamodb_rw.json
}

# -----------------------------------------------------------------------------
# DynamoDB read-only — run-checker
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "dynamodb_ro" {
  statement {
    actions = [
      "dynamodb:GetItem",
      "dynamodb:BatchGetItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:ConditionCheckItem",
      "dynamodb:DescribeTable",
    ]
    resources = [
      aws_dynamodb_table.main.arn,
      "${aws_dynamodb_table.main.arn}/index/*",
    ]
  }
}

resource "aws_iam_role_policy" "dynamodb_ro" {
  name   = "dynamodb-ro"
  role   = aws_iam_role.lambda["run-checker"].id
  policy = data.aws_iam_policy_document.dynamodb_ro.json
}

# -----------------------------------------------------------------------------
# DynamoDB read/write — watchdog (needs read + lock write)
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy" "dynamodb_watchdog" {
  name   = "dynamodb-rw"
  role   = aws_iam_role.lambda["watchdog"].id
  policy = data.aws_iam_policy_document.dynamodb_rw.json
}

# -----------------------------------------------------------------------------
# SNS publish — orchestrator, trigger
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "sns_publish" {
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
}

resource "aws_iam_role_policy" "sns_publish" {
  for_each = toset(["orchestrator", "trigger", "watchdog"])
  name     = "sns-publish"
  role     = aws_iam_role.lambda[each.key].id
  policy   = data.aws_iam_policy_document.sns_publish.json
}

# -----------------------------------------------------------------------------
# stream-router — DynamoDB Streams + SFN StartExecution
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "stream_router" {
  statement {
    sid = "DynamoDBStreams"
    actions = [
      "dynamodb:GetRecords",
      "dynamodb:GetShardIterator",
      "dynamodb:DescribeStream",
      "dynamodb:ListStreams",
      "dynamodb:ListShards",
    ]
    resources = [aws_dynamodb_table.main.stream_arn]
  }

  statement {
    sid       = "StartExecution"
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.pipeline.arn]
  }
}

resource "aws_iam_role_policy" "stream_router" {
  name   = "stream-router"
  role   = aws_iam_role.lambda["stream-router"].id
  policy = data.aws_iam_policy_document.stream_router.json
}

# -----------------------------------------------------------------------------
# Step Function execution role
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = "${var.table_name}-sfn"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

data "aws_iam_policy_document" "sfn_policy" {
  statement {
    sid     = "InvokeLambda"
    actions = ["lambda:InvokeFunction"]
    resources = [
      aws_lambda_function.core["orchestrator"].arn,
      aws_lambda_function.core["evaluator"].arn,
      aws_lambda_function.core["trigger"].arn,
      aws_lambda_function.core["run-checker"].arn,
    ]
  }

  statement {
    sid       = "PublishAlerts"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
}

resource "aws_iam_role_policy" "sfn" {
  name   = "sfn-policy"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn_policy.json
}

# -----------------------------------------------------------------------------
# Conditional trigger permissions (opt-in per trigger type)
# -----------------------------------------------------------------------------

# --- Glue ---

resource "aws_iam_role_policy" "glue_trigger" {
  count = var.enable_glue_trigger ? 1 : 0
  name  = "glue-trigger"
  role  = aws_iam_role.lambda["trigger"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["glue:StartJobRun", "glue:GetJobRun"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy" "glue_run_checker" {
  count = var.enable_glue_trigger ? 1 : 0
  name  = "glue-run-checker"
  role  = aws_iam_role.lambda["run-checker"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["glue:GetJobRun"]
      Resource = "*"
    }]
  })
}

# --- EMR ---

resource "aws_iam_role_policy" "emr_trigger" {
  count = var.enable_emr_trigger ? 1 : 0
  name  = "emr-trigger"
  role  = aws_iam_role.lambda["trigger"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["elasticmapreduce:AddJobFlowSteps", "elasticmapreduce:DescribeStep"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy" "emr_run_checker" {
  count = var.enable_emr_trigger ? 1 : 0
  name  = "emr-run-checker"
  role  = aws_iam_role.lambda["run-checker"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["elasticmapreduce:DescribeStep"]
      Resource = "*"
    }]
  })
}

# --- EMR Serverless ---

resource "aws_iam_role_policy" "emr_serverless_trigger" {
  count = var.enable_emr_serverless_trigger ? 1 : 0
  name  = "emr-serverless-trigger"
  role  = aws_iam_role.lambda["trigger"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["emr-serverless:StartJobRun", "emr-serverless:GetJobRun"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy" "emr_serverless_run_checker" {
  count = var.enable_emr_serverless_trigger ? 1 : 0
  name  = "emr-serverless-run-checker"
  role  = aws_iam_role.lambda["run-checker"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["emr-serverless:GetJobRun"]
      Resource = "*"
    }]
  })
}

# --- Step Functions (nested) ---

resource "aws_iam_role_policy" "sfn_trigger" {
  count = var.enable_sfn_trigger ? 1 : 0
  name  = "sfn-trigger"
  role  = aws_iam_role.lambda["trigger"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution", "states:DescribeExecution"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_run_checker" {
  count = var.enable_sfn_trigger ? 1 : 0
  name  = "sfn-run-checker"
  role  = aws_iam_role.lambda["run-checker"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:DescribeExecution"]
      Resource = "*"
    }]
  })
}
