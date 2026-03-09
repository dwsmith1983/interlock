---
title: Terraform
weight: 1
description: Deploy Interlock as a reusable Terraform module on AWS.
---

Interlock ships as a reusable Terraform module in `deploy/terraform/`. The module creates all required AWS infrastructure: DynamoDB tables, Lambda functions, Step Functions, EventBridge, and IAM roles. Your project consumes the module and provides pipeline YAML files -- no framework code runs in your repository.

## Prerequisites

- Terraform >= 1.5
- AWS CLI v2 configured with appropriate credentials
- AWS provider >= 5.0
- Go 1.24+ (to build Lambda binaries)

## Quick Start

### 1. Build Lambda Binaries

```bash
./deploy/build.sh
```

This cross-compiles the 6 Lambda handlers (`stream-router`, `orchestrator`, `sla-monitor`, `watchdog`, `event-sink`, `alert-dispatcher`) for `linux/arm64` and outputs them to `deploy/dist/`.

### 2. Write Pipeline Configs

Create YAML files in a `pipelines/` directory. Each file defines one pipeline:

```yaml
# pipelines/gold-revenue.yaml
pipeline:
  id: gold-revenue
  owner: analytics-team
  description: Gold-tier revenue aggregation pipeline

schedule:
  cron: "0 8 * * *"
  timezone: UTC
  evaluation:
    window: 1h
    interval: 5m

sla:
  deadline: "10:00"
  expectedDuration: 30m

validation:
  trigger: "ALL"
  rules:
    - key: upstream-complete
      check: equals
      field: status
      value: ready
    - key: row-count
      check: gte
      field: count
      value: 1000

job:
  type: glue
  config:
    jobName: gold-revenue-etl
  maxRetries: 2
  jobPollWindowSeconds: 3600
```

See [Pipeline Configuration](../../configuration/pipelines/) for the full YAML schema, including per-source rerun limits and failure classification.

### 3. Use the Module

Reference the Interlock module from your Terraform configuration:

```hcl
module "interlock" {
  source = "path/to/interlock/deploy/terraform"

  environment    = "production"
  dist_path      = "path/to/interlock/deploy/dist"
  pipelines_path = "${path.module}/pipelines"
  calendars_path = "${path.module}/calendars"

  tags = {
    Project     = "interlock"
    Environment = "production"
  }
}
```

### 4. Deploy

```bash
terraform init
terraform plan
terraform apply
```

## Module Variables

| Variable | Type | Default | Description |
|---|---|---|---|
| `environment` | string | -- | Environment name (e.g., `staging`, `production`). Prefixes all resource names. |
| `dist_path` | string | -- | Path to the directory containing Lambda zip files |
| `pipelines_path` | string | -- | Path to directory containing pipeline YAML configuration files |
| `calendars_path` | string | `""` | Path to directory containing calendar YAML files (optional) |
| `tags` | map(string) | `{}` | Tags to apply to all resources |
| `lambda_memory_size` | number | `128` | Memory size for Lambda functions (MB) |
| `log_retention_days` | number | `30` | CloudWatch log retention in days |
| `watchdog_schedule` | string | `rate(5 minutes)` | EventBridge schedule expression for the watchdog Lambda |
| `sfn_timeout_seconds` | number | `14400` | Step Functions global execution timeout in seconds (default 4h). Set this to accommodate your longest pipeline's total window (evaluation + job poll + post-run) |
| `trigger_max_attempts` | number | `3` | Max infrastructure retry attempts for the Trigger state (exponential backoff: 30s, 60s, 120s, 240s) |
| `enable_glue_trigger` | bool | `false` | Grant orchestrator Lambda permission to start Glue jobs |
| `enable_emr_trigger` | bool | `false` | Grant orchestrator Lambda permission to submit EMR steps |
| `enable_emr_serverless_trigger` | bool | `false` | Grant orchestrator Lambda permission to start EMR Serverless jobs |
| `enable_sfn_trigger` | bool | `false` | Grant orchestrator Lambda permission to start Step Functions executions |
| `enable_lambda_trigger` | bool | `false` | Grant orchestrator Lambda permission to invoke Lambda functions |
| `lambda_trigger_arns` | list(string) | `["*"]` | Lambda function ARNs the orchestrator may invoke (scoped IAM). Only used when `enable_lambda_trigger` is `true` |
| `slack_bot_token` | string (sensitive) | `""` | Slack Bot API token with `chat:write` scope for alert-dispatcher |
| `slack_channel_id` | string | `""` | Slack channel ID for alert notifications |
| `slack_secret_arn` | string | `""` | AWS Secrets Manager ARN containing the Slack bot token. When set, alert-dispatcher reads the token from Secrets Manager instead of the `slack_bot_token` environment variable |
| `events_table_ttl_days` | number | `90` | TTL in days for events table records |
| `lambda_concurrency` | object | See below | Reserved concurrent executions per Lambda function |
| `sns_alarm_topic_arn` | string | `""` | SNS topic ARN for CloudWatch alarm notifications. When set, all alarms send to this topic |

#### Lambda Concurrency Defaults

The `lambda_concurrency` variable is an object with per-function keys:

```hcl
lambda_concurrency = {
  stream_router    = 10
  orchestrator     = 10
  sla_monitor      = 5
  watchdog         = 2
  event_sink       = 5
  alert_dispatcher = 3
}
```

Set any key to `-1` to use unreserved concurrency (Lambda default).

## Secrets Manager

By default, alert-dispatcher reads the Slack bot token from the `SLACK_BOT_TOKEN` environment variable. For production deployments, store the token in AWS Secrets Manager and pass the ARN:

```hcl
module "interlock" {
  source = "path/to/interlock/deploy/terraform"

  slack_secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:interlock/slack-token-AbCdEf"
  slack_channel_id = "C0123456789"
  # ...
}
```

When `slack_secret_arn` is set, the module automatically grants `secretsmanager:GetSecretValue` to the alert-dispatcher Lambda role. The token must have the `xoxb-` prefix (Bot token).

## CloudWatch Alarms

The module creates CloudWatch alarms across four categories. All alarms conditionally route to an SNS topic when `sns_alarm_topic_arn` is set.

| Category | Alarms | Threshold |
|---|---|---|
| Lambda errors | One per function (6 total) | `Errors >= 1` per 5-minute period |
| Step Functions failures | One for the pipeline state machine | `ExecutionsFailed >= 1` per 5-minute period |
| DLQ depth | One per dead-letter queue (3 total) | `ApproximateNumberOfMessagesVisible >= 1` |
| Stream iterator age | One per DynamoDB Stream mapping (2 total) | `IteratorAge >= 300,000ms` (5 minutes) |

CloudWatch alarm state changes are reshaped via EventBridge input transformers into `INFRA_ALARM` events and routed to both event-sink and alert-dispatcher. This means infrastructure alerts appear in the events table and Slack alongside pipeline lifecycle events — no additional Go code required.

```hcl
module "interlock" {
  source = "path/to/interlock/deploy/terraform"

  sns_alarm_topic_arn = aws_sns_topic.ops_alerts.arn
  # ...
}
```

## What Gets Created

### DynamoDB Tables

The module creates 4 tables, all using on-demand billing:

| Table | Name Pattern | Purpose |
|---|---|---|
| Control | `{env}-interlock-control` | Pipeline configs, sensor data, locks, evaluation state |
| Job Log | `{env}-interlock-joblog` | Job execution history and run status |
| Rerun | `{env}-interlock-rerun` | Rerun requests and tracking |
| Events | `{env}-interlock-events` | Centralized event log for dashboards and audit (GSI1: eventType → timestamp) |

All tables have TTL enabled on the `ttl` attribute. The control and joblog tables have DynamoDB Streams enabled (`NEW_IMAGE`) to drive the stream-router Lambda.

### Lambda Functions

| Function | Name Pattern | Purpose |
|---|---|---|
| stream-router | `{env}-interlock-stream-router` | Processes DynamoDB stream events, starts Step Functions executions |
| orchestrator | `{env}-interlock-orchestrator` | Evaluates validation rules, triggers jobs, polls job status |
| sla-monitor | `{env}-interlock-sla-monitor` | Checks SLA deadlines, emits breach events |
| watchdog | `{env}-interlock-watchdog` | Scans for missed schedules and stale triggers |
| event-sink | `{env}-interlock-event-sink` | Captures all EventBridge events to the events table |
| alert-dispatcher | `{env}-interlock-alert-dispatcher` | Formats and delivers Slack notifications from the alert queue |

All Lambdas run on `provided.al2023` (custom runtime) with `arm64` architecture.

### Pipeline Config Loader

The module reads all `*.yaml` and `*.yml` files from `pipelines_path` and writes them as CONFIG rows to the control table. Each pipeline's YAML content is stored as a string attribute, keyed by `PIPELINE#{id}` / `CONFIG`.

This means pipeline configuration is managed entirely through Terraform -- update a YAML file, run `terraform apply`, and the new config is live.

### Step Functions

A single state machine (`{env}-interlock-pipeline`) orchestrates the full pipeline lifecycle:

1. **Evaluate** -- orchestrator Lambda evaluates validation rules
2. **Wait** -- if not ready, wait for the configured interval
3. **Re-evaluate** -- loop until ready or window expires
4. **Trigger** -- orchestrator starts the configured job
5. **Poll** -- orchestrator checks job status at intervals
6. **Complete** -- emit success/failure event to EventBridge

SLA monitoring uses EventBridge Scheduler — one-time schedule entries fire warning and breach alerts at exact timestamps, independent of the main evaluation loop.

The ASL definition is at `deploy/statemachine.asl.json` and uses `templatefile()` to substitute Lambda ARNs.

### EventBridge

| Resource | Name Pattern | Purpose |
|---|---|---|
| Custom event bus | `{env}-interlock-events` | All Interlock events (SLA breaches, completions, failures, missed schedules) |
| Watchdog schedule | `{env}-interlock-watchdog` | Invokes the watchdog Lambda on a recurring schedule |

All 4 Lambda functions publish events to the custom bus. Create EventBridge rules on this bus to route events to your alerting systems (SNS, Slack, PagerDuty, etc.).

### IAM

Each Lambda gets its own execution role with least-privilege policies:

| Role | Permissions |
|---|---|
| stream-router | DynamoDB read/write (all 3 tables), DynamoDB Streams (control + joblog), Step Functions `StartExecution`, EventBridge `PutEvents`, SQS `SendMessage` (DLQs) |
| orchestrator | DynamoDB read/write (all 3 tables), EventBridge `PutEvents`, conditional trigger permissions |
| sla-monitor | EventBridge `PutEvents` |
| watchdog | DynamoDB read (all 3 tables), DynamoDB write (control only), EventBridge `PutEvents` |

The Step Functions execution role has permission to invoke the orchestrator and sla-monitor Lambdas.

### Dead-Letter Queues

Two SQS DLQs capture failed DynamoDB stream processing:

| Queue | Source |
|---|---|
| `{env}-interlock-sr-control-dlq` | Control table stream failures |
| `{env}-interlock-sr-joblog-dlq` | Job log table stream failures |

Stream event source mappings use `bisect_batch_on_function_error` with 3 retry attempts before routing to the DLQ.

## Module Outputs

| Output | Description |
|---|---|
| `control_table_name` | Name of the control DynamoDB table |
| `control_table_arn` | ARN of the control DynamoDB table |
| `joblog_table_name` | Name of the job log DynamoDB table |
| `rerun_table_name` | Name of the rerun DynamoDB table |
| `event_bus_name` | Name of the EventBridge event bus |
| `event_bus_arn` | ARN of the EventBridge event bus |
| `sfn_arn` | ARN of the pipeline state machine |
| `sfn_name` | Name of the pipeline state machine |

Use these outputs to integrate Interlock with your existing infrastructure (e.g., create EventBridge rules, grant external services write access to the control table).

## Tear Down

```bash
terraform destroy
```

This removes all resources created by the module. DynamoDB tables with data will be deleted -- ensure you have backups if needed.
