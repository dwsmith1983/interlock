---
title: Alerting
weight: 1
description: EventBridge-based event publishing, detail types, and consumer patterns.
---

Interlock publishes all lifecycle and alert events to a custom EventBridge event bus. Consumers subscribe to events by creating EventBridge rules that match on detail types. There are no built-in alert sinks -- you route events to whatever targets your organization uses (SNS, SQS, Lambda, CloudWatch Logs, etc.).

## EventBridge Event Bus

The Terraform module creates a custom event bus named `{environment}-interlock-events`. All four Lambda functions publish events to this bus using `events:PutEvents`.

The bus name is available as a Terraform output:

```hcl
output "event_bus_name" {
  value = module.interlock.event_bus_name
}

output "event_bus_arn" {
  value = module.interlock.event_bus_arn
}
```

## Detail Types

Each event is published with a `detail-type` field that classifies the event. Use these values in EventBridge rule patterns for filtering and routing.

### SLA Events

Published by the **sla-monitor** Lambda from the SLA monitoring branch of the Step Functions state machine.

| Detail Type | Meaning | When |
|---|---|---|
| `SLA_WARNING` | SLA warning threshold reached | Pipeline has not completed by the warning timestamp |
| `SLA_BREACH` | SLA deadline exceeded | Pipeline has not completed by the breach timestamp |
| `SLA_MET` | Job completed before SLA warning deadline | Pipeline completed before any SLA alert fired |

### Lifecycle Events

Published by the **orchestrator** Lambda during the evaluation and execution branches.

| Detail Type | Meaning | When |
|---|---|---|
| `VALIDATION_PASSED` | All validation rules passed | Readiness evaluation succeeds, before trigger |
| `VALIDATION_EXHAUSTED` | Evaluation window closed without passing | Max evaluation time exceeded |
| `JOB_TRIGGERED` | Pipeline job was triggered | Trigger fired successfully |
| `JOB_COMPLETED` | Triggered job completed successfully | Job polling detects success |
| `JOB_FAILED` | Triggered job failed | Job polling detects failure |
| `JOB_TIMEOUT` | Triggered job timed out | Job polling detects timeout |
| `RETRY_EXHAUSTED` | All retry attempts consumed | Job failed `maxRetries` times without success |

### Watchdog Events

Published by the **watchdog** Lambda, invoked on an EventBridge schedule (default: every 5 minutes).

| Detail Type | Meaning | When |
|---|---|---|
| `SCHEDULE_MISSED` | No evaluation started by the schedule deadline | Watchdog detects absence of expected pipeline activity |

## Event Payload Structure

Events follow the standard EventBridge envelope format:

```json
{
  "version": "0",
  "id": "abc123",
  "source": "interlock",
  "detail-type": "SLA_WARNING",
  "time": "2026-03-01T10:00:00Z",
  "region": "us-east-1",
  "resources": [],
  "detail": {
    "pipelineId": "gold-revenue",
    "scheduleId": "daily",
    "date": "2026-03-01",
    "message": "Pipeline gold-revenue has not completed by warning deadline 10:00 UTC"
  }
}
```

The `detail` object contains pipeline-specific context. The exact fields vary by detail type but always include `pipelineId`.

## Creating EventBridge Rules

Subscribe to events by creating EventBridge rules that match on `detail-type`. Route matched events to any supported EventBridge target.

### Route SLA Alerts to SNS

```hcl
resource "aws_cloudwatch_event_rule" "sla_alerts" {
  name           = "interlock-sla-alerts"
  event_bus_name = module.interlock.event_bus_name

  event_pattern = jsonencode({
    "detail-type" = ["SLA_WARNING", "SLA_BREACH"]
  })
}

resource "aws_cloudwatch_event_target" "sla_to_sns" {
  rule           = aws_cloudwatch_event_rule.sla_alerts.name
  event_bus_name = module.interlock.event_bus_name
  target_id      = "sla-to-sns"
  arn            = aws_sns_topic.alerts.arn
}
```

### Route All Events to CloudWatch Logs

```hcl
resource "aws_cloudwatch_event_rule" "all_events" {
  name           = "interlock-all-events"
  event_bus_name = module.interlock.event_bus_name

  event_pattern = jsonencode({
    source = ["interlock"]
  })
}

resource "aws_cloudwatch_event_target" "to_cloudwatch" {
  rule           = aws_cloudwatch_event_rule.all_events.name
  event_bus_name = module.interlock.event_bus_name
  target_id      = "to-cloudwatch"
  arn            = aws_cloudwatch_log_group.interlock_events.arn
}
```

### Route Job Failures to a Lambda for Custom Handling

```hcl
resource "aws_cloudwatch_event_rule" "job_failures" {
  name           = "interlock-job-failures"
  event_bus_name = module.interlock.event_bus_name

  event_pattern = jsonencode({
    "detail-type" = ["JOB_FAILED", "JOB_TIMEOUT"]
  })
}

resource "aws_cloudwatch_event_target" "failure_handler" {
  rule           = aws_cloudwatch_event_rule.job_failures.name
  event_bus_name = module.interlock.event_bus_name
  target_id      = "failure-handler"
  arn            = aws_lambda_function.failure_handler.arn
}
```

### Filter by Pipeline ID

```hcl
resource "aws_cloudwatch_event_rule" "gold_pipeline_events" {
  name           = "interlock-gold-pipeline"
  event_bus_name = module.interlock.event_bus_name

  event_pattern = jsonencode({
    source      = ["interlock"]
    detail = {
      pipelineId = ["gold-revenue"]
    }
  })
}
```

## Consumer Patterns

Since EventBridge supports many target types, you can build any alert delivery pattern:

| Pattern | EventBridge Target | Use Case |
|---|---|---|
| PagerDuty / Slack | SNS topic with subscription | On-call alerting for SLA breaches |
| Audit log | CloudWatch Logs log group | Compliance and debugging |
| Custom processing | Lambda function | Enrich, deduplicate, or aggregate events |
| Queue for batch processing | SQS queue | Downstream systems that process events in batches |
| Cross-account delivery | EventBridge in another account | Centralized observability |
