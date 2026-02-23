resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.table_name}-pipeline"
  type     = "STANDARD"
  role_arn = aws_iam_role.sfn.arn

  definition = templatefile(var.asl_path, {
    OrchestratorFunctionArn = aws_lambda_function.core["orchestrator"].arn
    EvaluatorFunctionArn    = aws_lambda_function.core["evaluator"].arn
    TriggerFunctionArn      = aws_lambda_function.core["trigger"].arn
    RunCheckerFunctionArn   = aws_lambda_function.core["run-checker"].arn
    AlertTopicArn           = aws_sns_topic.alerts.arn
  })
}
