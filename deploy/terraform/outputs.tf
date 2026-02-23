output "table_name" {
  description = "DynamoDB table name"
  value       = aws_dynamodb_table.main.name
}

output "topic_arn" {
  description = "SNS alert topic ARN"
  value       = aws_sns_topic.alerts.arn
}

output "state_machine_arn" {
  description = "Step Function state machine ARN"
  value       = aws_sfn_state_machine.pipeline.arn
}
