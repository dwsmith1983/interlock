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

This cross-compiles the 4 Lambda handlers (`stream-router`, `orchestrator`, `sla-monitor`, `watchdog`) for `linux/arm64` and outputs them to `deploy/dist/`.

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
```

See [Pipeline Configuration](../../configuration/pipelines/) for the full YAML schema.

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
| `sfn_timeout_seconds` | number | `43200` | Step Functions execution timeout in seconds (default 12h) |
| `enable_glue_trigger` | bool | `false` | Grant orchestrator Lambda permission to start Glue jobs |
| `enable_emr_trigger` | bool | `false` | Grant orchestrator Lambda permission to submit EMR steps |
| `enable_emr_serverless_trigger` | bool | `false` | Grant orchestrator Lambda permission to start EMR Serverless jobs |
| `enable_sfn_trigger` | bool | `false` | Grant orchestrator Lambda permission to start Step Functions executions |

## What Gets Created

### DynamoDB Tables

The module creates 3 tables, all using on-demand billing:

| Table | Name Pattern | Purpose |
|---|---|---|
| Control | `{env}-interlock-control` | Pipeline configs, sensor data, locks, evaluation state |
| Job Log | `{env}-interlock-joblog` | Job execution history and run status |
| Rerun | `{env}-interlock-rerun` | Rerun requests and tracking |

All tables have TTL enabled on the `ttl` attribute. The control and joblog tables have DynamoDB Streams enabled (`NEW_IMAGE`) to drive the stream-router Lambda.

### Lambda Functions

| Function | Name Pattern | Purpose |
|---|---|---|
| stream-router | `{env}-interlock-stream-router` | Processes DynamoDB stream events, starts Step Functions executions |
| orchestrator | `{env}-interlock-orchestrator` | Evaluates validation rules, triggers jobs, polls job status |
| sla-monitor | `{env}-interlock-sla-monitor` | Checks SLA deadlines, emits breach events |
| watchdog | `{env}-interlock-watchdog` | Scans for missed schedules and stale triggers |

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

The SLA monitor runs as a parallel branch, checking deadlines independently of the main evaluation loop.

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
