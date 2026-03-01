---
title: AWS Architecture
weight: 2
description: 3 DynamoDB tables, 4 Lambda functions, Step Functions, EventBridge, and Terraform module.
---

Interlock runs as a fully serverless, event-driven system on AWS. External processes push sensor data into DynamoDB. DynamoDB Streams trigger a routing Lambda that starts Step Function executions. The orchestrator and SLA monitor Lambdas handle the pipeline lifecycle within the state machine. A watchdog Lambda runs independently on a schedule to detect silent failures.

## High-Level Flow

```
External processes ──→ DynamoDB control table (SENSOR# writes)
                              │
                       DynamoDB Stream
                              │
                              ↓
                    stream-router Lambda ──→ Step Functions execution
                                                     │
                                          ┌──────────┴──────────┐
                                          ↓                     ↓
                                    Evaluation            SLA Monitoring
                                      Branch                 Branch
                                          │                     │
                                    orchestrator          sla-monitor
                                     (Lambda)              (Lambda)
                                          │
                              ┌───────────┼──────────┐
                              ↓           ↓          ↓
                          Evaluate     Trigger    CheckJob
                          (rules)      (job)     (poll status)

EventBridge schedule ──→ watchdog Lambda ──→ DynamoDB (scan for stale/missed)
                                           ──→ EventBridge (publish alerts)
```

## DynamoDB Tables

Interlock uses three DynamoDB tables, each with a composite primary key (`PK`, `SK`) and pay-per-request billing:

### Control Table

The primary state table. Stores pipeline configs, sensor data, trigger locks, and evaluation state.

| Entity | PK | SK | Purpose |
|---|---|---|---|
| Pipeline config | `PIPELINE#{id}` | `CONFIG` | YAML config stored as JSON |
| Sensor data | `PIPELINE#{id}` | `SENSOR#{key}` | External sensor writes |
| Trigger lock | `PIPELINE#{id}` | `TRIGGER#{schedule}#{date}` | Dedup lock for executions |

DynamoDB Streams are enabled with `NEW_IMAGE` view type. The stream-router Lambda processes sensor writes and config changes from this stream.

### Job Log Table

Append-only log of job execution events. Each record represents a lifecycle event for a triggered job.

| Entity | PK | SK | Purpose |
|---|---|---|---|
| Job event | `PIPELINE#{id}` | `JOB#{schedule}#{date}#{timestamp}` | success/fail/timeout record |

DynamoDB Streams are enabled. The stream-router processes `JOB#` records to handle automatic retries on failure.

### Rerun Table

Tracks retry attempts for failed job executions.

| Entity | PK | SK | Purpose |
|---|---|---|---|
| Rerun record | `PIPELINE#{id}` | `RERUN#{schedule}#{date}#{attempt}` | Retry attempt metadata |

All three tables have TTL enabled on a `ttl` attribute for automatic cleanup of expired records.

## Lambda Functions

### stream-router

Processes DynamoDB Stream events from the control and joblog tables. Routes records by SK prefix:

| SK Prefix | Action |
|---|---|
| `SENSOR#` | Evaluate trigger condition; if matched and not excluded, acquire trigger lock and start Step Function execution |
| `CONFIG` | Invalidate the config cache |
| `JOB#` | On failure/timeout: check retry limit, write rerun record, restart SFN. On success: publish `JOB_COMPLETED` event |

The stream-router maintains a config cache to avoid redundant DynamoDB reads. Trigger lock acquisition uses conditional writes with a 24-hour TTL to prevent duplicate executions for the same pipeline/schedule/date.

Event source mappings from both control and joblog streams use batch size 10, bisect-on-error, max 3 retries, and SQS dead-letter queues for poison records.

### orchestrator

Multi-mode dispatcher invoked by Step Functions. Each invocation specifies a `mode` field:

| Mode | Purpose |
|---|---|
| `evaluate` | Load config and sensor data, evaluate validation rules, publish `VALIDATION_PASSED` if all pass |
| `trigger` | Build trigger config from `job` section, execute trigger (Glue, EMR, HTTP, etc.), return run ID |
| `check-job` | Query joblog table for latest event; return `success`, `fail`, `timeout`, or empty (still running) |
| `post-run` | Evaluate post-run validation rules if configured |
| `validation-exhausted` | Publish `VALIDATION_EXHAUSTED` event when the evaluation window closes |

Supported trigger types: `http`, `command`, `airflow`, `glue`, `emr`, `emr-serverless`, `step-function`, `databricks`.

### sla-monitor

Lightweight SLA deadline calculator and alert publisher, invoked by the SLA monitoring branch of the Step Functions state machine. Two modes:

| Mode | Purpose |
|---|---|
| `calculate` | Compute warning and breach timestamps from deadline, expected duration, and timezone |
| `fire-alert` | Publish `SLA_WARNING` or `SLA_BREACH` event to EventBridge |

The SLA monitor does not read from DynamoDB -- it receives all necessary data as input from the Step Function.

### watchdog

Invoked by an EventBridge scheduled rule (default: every 5 minutes). Runs two independent scans:

1. **Stale triggers** -- scans for `TRIGGER#` records with `RUNNING` status whose TTL has expired. Publishes `SFN_TIMEOUT` events and sets status to `FAILED_FINAL`.
2. **Missed schedules** -- loads all cron-scheduled pipeline configs, checks for missing `TRIGGER#` records for today's date. Publishes `SCHEDULE_MISSED` events for pipelines past their expected start time.

See [Watchdog](../watchdog) for the full algorithm.

## Step Functions State Machine

The state machine orchestrates the pipeline lifecycle using two parallel branches:

### Evaluation Branch (7 states)

1. **InitEvalLoop** -- initialize elapsed-seconds counter
2. **Evaluate** -- invoke orchestrator with `mode=evaluate`, returns `passed` or `not_ready`
3. **IsReady** -- route on evaluation result: passed goes to Trigger, otherwise WaitInterval
4. **WaitInterval** -- wait `evaluationIntervalSeconds` before retrying
5. **IncrementElapsed** -- track total elapsed time using `States.MathAdd`
6. **CheckWindowExhausted** -- if elapsed >= window, go to ValidationExhausted
7. **Trigger** -- invoke orchestrator with `mode=trigger`, then poll via WaitForJob/CheckJob/IsJobDone loop

### SLA Monitoring Branch (5 states)

1. **CheckSLAConfig** -- skip branch if no SLA configured
2. **CalcDeadlines** -- invoke sla-monitor with `mode=calculate`
3. **WaitForWarning** -- wait until warning timestamp
4. **FireSLAWarning** -- invoke sla-monitor with `mode=fire-alert, alertType=SLA_WARNING`
5. **WaitForBreach/FireSLABreach** -- same pattern for breach deadline

### Error Handling

Every Task state includes Retry and Catch blocks:

- **Retry**: `IntervalSeconds: 2`, `MaxAttempts: 3`, `BackoffRate: 2` for Lambda service errors
- **Catch**: `States.ALL` routes to a branch-level Fail state (`EvalBranchFailed` or `SLABranchFailed`)

The top-level Parallel state catches any branch failure and routes to `InfraFailure`.

### ARN Substitution

The ASL template at `deploy/statemachine.asl.json` uses two substitution variables, resolved by Terraform's `templatefile()`:

- `${orchestrator_arn}`
- `${sla_monitor_arn}`

## EventBridge

A custom event bus (`{environment}-interlock-events`) receives all framework events. All four Lambda functions have `events:PutEvents` permission on this bus.

The watchdog Lambda is invoked by a scheduled rule on the default event bus (EventBridge scheduled rules require the default bus).

Consumers create rules on the custom bus to route events to their targets. Example rule pattern:

```json
{
  "source": ["interlock"],
  "detail-type": ["SLA_BREACH", "SCHEDULE_MISSED"]
}
```

This replaces the previous SNS topic approach. EventBridge provides native filtering, fan-out to multiple targets, and cross-account delivery without framework changes.

## IAM

Each Lambda function has its own IAM role with least-privilege policies:

| Function | DynamoDB | EventBridge | Other |
|---|---|---|---|
| stream-router | Read/write all 3 tables + stream access | PutEvents | `states:StartExecution`, SQS SendMessage (DLQs) |
| orchestrator | Read/write all 3 tables | PutEvents | Conditional trigger permissions (Glue, EMR, etc.) |
| sla-monitor | None | PutEvents | -- |
| watchdog | Read all 3 tables, write control only | PutEvents | -- |

Trigger permissions for the orchestrator are opt-in via Terraform variables (`enable_glue_trigger`, `enable_emr_trigger`, etc.).

## Terraform Module

The entire infrastructure deploys as a reusable Terraform module. No framework code needs to live in the user's repository -- only pipeline config YAML files.

### Module Structure

```
deploy/
├── statemachine.asl.json    # Step Functions ASL definition
├── build.sh                 # Lambda build script (Go → zip)
└── terraform/
    ├── main.tf              # Provider, module-level config
    ├── dynamodb.tf          # 3 tables (control, joblog, rerun)
    ├── lambda.tf            # 4 functions, IAM, DLQs, event source mappings
    ├── stepfunctions.tf     # State machine + execution role
    ├── eventbridge.tf       # Custom bus + watchdog schedule
    ├── config_loader.tf     # Pipeline YAML loading
    ├── variables.tf         # Input variables
    └── outputs.tf           # Table names, function ARNs, bus name
```

### Key Variables

| Variable | Description | Default |
|---|---|---|
| `environment` | Environment prefix for all resource names | -- (required) |
| `dist_path` | Path to Lambda zip files | -- (required) |
| `lambda_memory_size` | Memory allocation for all Lambdas | `128` |
| `log_retention_days` | CloudWatch log retention | `14` |
| `watchdog_schedule` | EventBridge schedule expression | `rate(5 minutes)` |
| `enable_glue_trigger` | Add Glue permissions to orchestrator | `false` |
| `enable_emr_trigger` | Add EMR permissions to orchestrator | `false` |
| `enable_emr_serverless_trigger` | Add EMR Serverless permissions | `false` |
| `enable_sfn_trigger` | Add Step Functions permissions | `false` |

### Deployment

```bash
# Build Lambda binaries
./deploy/build.sh

# Deploy infrastructure
cd deploy/terraform
terraform init
terraform apply -var="environment=prod" -var="dist_path=../../dist"
```

Pipeline configs are loaded by the `config_loader.tf` module and written to the control table as `CONFIG` records.
