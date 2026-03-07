resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.environment}-interlock-pipeline"
  role_arn = aws_iam_role.sfn.arn

  definition = templatefile("${path.module}/../statemachine.asl.json", {
    orchestrator_arn     = aws_lambda_function.orchestrator.arn
    sla_monitor_arn      = aws_lambda_function.sla_monitor.arn
    sfn_timeout_seconds  = var.sfn_timeout_seconds
    trigger_max_attempts = var.trigger_max_attempts
  })

  tags = var.tags
}

resource "aws_iam_role" "sfn" {
  name = "${var.environment}-interlock-sfn"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = var.tags
}

resource "aws_iam_role_policy" "sfn" {
  name = "invoke-lambdas"
  role = aws_iam_role.sfn.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "lambda:InvokeFunction"
      Resource = [
        aws_lambda_function.orchestrator.arn,
        aws_lambda_function.sla_monitor.arn,
      ]
    }]
  })
}
