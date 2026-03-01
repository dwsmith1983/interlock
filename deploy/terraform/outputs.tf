output "control_table_name" {
  description = "Name of the control DynamoDB table"
  value       = aws_dynamodb_table.control.name
}

output "control_table_arn" {
  description = "ARN of the control DynamoDB table"
  value       = aws_dynamodb_table.control.arn
}

output "joblog_table_name" {
  description = "Name of the job log DynamoDB table"
  value       = aws_dynamodb_table.joblog.name
}

output "rerun_table_name" {
  description = "Name of the rerun DynamoDB table"
  value       = aws_dynamodb_table.rerun.name
}

output "event_bus_name" {
  description = "Name of the EventBridge event bus"
  value       = aws_cloudwatch_event_bus.interlock.name
}

output "event_bus_arn" {
  description = "ARN of the EventBridge event bus"
  value       = aws_cloudwatch_event_bus.interlock.arn
}

output "sfn_arn" {
  description = "ARN of the pipeline state machine"
  value       = aws_sfn_state_machine.pipeline.arn
}

output "sfn_name" {
  description = "Name of the pipeline state machine"
  value       = aws_sfn_state_machine.pipeline.name
}
