---
title: Alerting
weight: 1
description: EventBridge-based event publishing, detail types, and consumer patterns.
---

Interlock publishes all lifecycle and alert events to a custom EventBridge event bus. The framework includes two built-in consumers: an **event-sink** Lambda that logs all events to a DynamoDB events table, and an **alert-dispatcher** Lambda that delivers Slack notifications with message threading. You can also create custom rules to route events to SNS, SQS, Lambda, CloudWatch Logs, or any other EventBridge target.

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

Published by the **sla-monitor** Lambda via EventBridge Scheduler callbacks and during SLA cleanup in the Step Functions state machine.

| Detail Type | Meaning | When |
|---|---|---|
| `SLA_WARNING` | SLA warning threshold reached | Pipeline has not completed by the warning timestamp |
| `SLA_BREACH` | SLA deadline exceeded | Pipeline has not completed by the breach timestamp |
| `SLA_MET` | Job completed before SLA warning deadline | Pipeline completed before any SLA alert fired |

### Lifecycle Events

Published by the **orchestrator** and **stream-router** Lambdas during the pipeline lifecycle.

| Detail Type | Meaning | When |
|---|---|---|
| `VALIDATION_PASSED` | All validation rules passed | Readiness evaluation succeeds, before trigger |
| `VALIDATION_EXHAUSTED` | Evaluation window closed without passing | Max evaluation time exceeded |
| `JOB_TRIGGERED` | Pipeline job was triggered | Trigger fired successfully |
| `JOB_COMPLETED` | Triggered job completed successfully | Job polling detects success |
| `JOB_FAILED` | Triggered job failed | Job polling detects failure |
| `JOB_TIMEOUT` | Triggered job timed out | Job polling detects timeout |
| `RETRY_EXHAUSTED` | All retry attempts consumed | Job failed `maxRetries` (or `maxCodeRetries` for permanent failures) times without success |
| `JOB_POLL_EXHAUSTED` | Job polling window exceeded | Orchestrator stopped checking job status after `jobPollWindowSeconds` elapsed without a terminal result |
| `INFRA_FAILURE` | Unrecoverable infrastructure error | Step Functions execution reaches Fail state |
| `SFN_TIMEOUT` | Step Functions execution timed out | Global `TimeoutSeconds` exceeded (configurable via `sfn_timeout_seconds` Terraform variable) |
| `DATA_DRIFT` | Post-run drift detected | Post-run evaluation detected data quality drift against baseline |

### Rerun Events

Published by the **stream-router** when processing rerun requests and late data arrivals.

| Detail Type | Meaning | When |
|---|---|---|
| `LATE_DATA_ARRIVAL` | Sensor updated after job completed | Sensor `updatedAt` is newer than joblog `completedAt` |
| `RERUN_REJECTED` | Rerun request rejected by circuit breaker | No new sensor data since last job completion |

### Watchdog Events

Published by the **watchdog** Lambda, invoked on an EventBridge schedule (default: every 5 minutes).

| Detail Type | Meaning | When |
|---|---|---|
| `SCHEDULE_MISSED` | No evaluation started by the schedule deadline | Watchdog detects absence of expected pipeline activity |
| `TRIGGER_RECOVERED` | Sensor trigger condition met but no trigger existed | Watchdog re-evaluated sensor data and self-healed a missed trigger |

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

## Built-In Observability

### Event Sink

The Terraform module deploys an `event-sink` Lambda that captures all EventBridge events to a DynamoDB events table. This provides a queryable audit log without any additional configuration.

The events table uses:
- **PK**: `PIPELINE#{pipelineId}` — all events for a pipeline
- **SK**: `{tsMillis}#{eventType}` — sorted by timestamp
- **GSI1**: `eventType` → `timestamp` — query all events of a given type

Records expire after a configurable TTL (default: 90 days via `events_ttl_days` variable).

### Alert Dispatcher (Slack)

The module deploys an `alert-dispatcher` Lambda that reads from an SQS alert queue and posts formatted notifications to Slack using the Bot API (`chat.postMessage`).

**Configuration:**

| Variable | Description |
|---|---|
| `slack_bot_token` | Bot token with `chat:write` scope (sensitive) |
| `slack_channel_id` | Channel ID to post alerts to |

**Message threading**: alerts for the same pipeline, schedule, and date are grouped into a single Slack thread. The first alert creates the thread; subsequent alerts reply in-thread. Thread records are stored in the events table and expire with the same TTL.

**SLA warning suppression**: when an SLA breach has already occurred, the `fire-alert` handler suppresses the corresponding `SLA_WARNING` to prevent duplicate notifications.

## Consumer Patterns

Since EventBridge supports many target types, you can build any alert delivery pattern:

| Pattern | EventBridge Target | Use Case |
|---|---|---|
| PagerDuty / Slack | SNS topic with subscription | On-call alerting for SLA breaches |
| Audit log | CloudWatch Logs log group | Compliance and debugging |
| Custom processing | Lambda function | Enrich, deduplicate, or aggregate events |
| Queue for batch processing | SQS queue | Downstream systems that process events in batches |
| Cross-account delivery | EventBridge in another account | Centralized observability |
