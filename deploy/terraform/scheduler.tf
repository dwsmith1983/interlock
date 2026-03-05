# -----------------------------------------------------------------------------
# EventBridge Scheduler — one-time SLA alert schedules
#
# The sla-monitor Lambda creates and deletes one-time schedules dynamically.
# This file provides the schedule group and execution role.
# -----------------------------------------------------------------------------

resource "aws_scheduler_schedule_group" "sla" {
  name = "${var.environment}-interlock-sla"
  tags = var.tags
}

# Execution role for Scheduler to invoke the sla-monitor Lambda
resource "aws_iam_role" "scheduler_sla" {
  name = "${var.environment}-interlock-scheduler-sla"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = var.tags
}

resource "aws_iam_role_policy" "scheduler_invoke_sla" {
  name = "invoke-sla-monitor"
  role = aws_iam_role.scheduler_sla.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.sla_monitor.arn
    }]
  })
}

# IAM policy for sla-monitor Lambda to manage Scheduler schedules
data "aws_iam_policy_document" "scheduler_manage" {
  statement {
    actions = [
      "scheduler:CreateSchedule",
      "scheduler:DeleteSchedule",
      "scheduler:GetSchedule",
    ]
    resources = [
      "arn:aws:scheduler:*:*:schedule/${aws_scheduler_schedule_group.sla.name}/*",
    ]
  }

  statement {
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.scheduler_sla.arn]
  }
}

resource "aws_iam_role_policy" "sla_monitor_scheduler" {
  name   = "scheduler-manage"
  role   = aws_iam_role.lambda["sla-monitor"].id
  policy = data.aws_iam_policy_document.scheduler_manage.json
}

# IAM policy for watchdog Lambda to create Scheduler schedules
resource "aws_iam_role_policy" "watchdog_scheduler" {
  name = "scheduler-create"
  role = aws_iam_role.lambda["watchdog"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["scheduler:CreateSchedule"]
        Resource = ["arn:aws:scheduler:*:*:schedule/${aws_scheduler_schedule_group.sla.name}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = [aws_iam_role.scheduler_sla.arn]
      }
    ]
  })
}
