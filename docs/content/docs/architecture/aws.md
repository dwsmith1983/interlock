---
title: AWS Architecture
weight: 2
description: 4 DynamoDB tables, 6 Lambda functions, Step Functions, EventBridge, and Terraform module.
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
                                               Sequential flow:
                                               Evaluate → Trigger
                                                     │
                                               SLA Scheduling
                                            (EventBridge Scheduler)
                                                     │
                                               Poll job status
                                                     │
                                               SLA Cleanup → Done

EventBridge schedule ──→ watchdog Lambda ──→ DynamoDB (scan for stale/missed)
                                           ──→ EventBridge (publish alerts)

EventBridge rules ──→ event-sink Lambda ──→ events table (all events)
                  ──→ SQS alert queue ──→ alert-dispatcher Lambda ──→ Slack
```

## DynamoDB Tables

Interlock uses four DynamoDB tables, each with a composite primary key (`PK`, `SK`) and pay-per-request billing:

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

### Events Table

Centralized log of all EventBridge events. The event-sink Lambda writes every event to this table. Used by dashboards and audit queries.

| Entity | PK | SK | Purpose |
|---|---|---|---|
| Event | `PIPELINE#{id}` | `{tsMillis}#{eventType}` | Timestamped event record |
| Thread | `PIPELINE#{id}` | `THREAD#{schedule}#{date}` | Slack thread storage for alert-dispatcher |

GSI1 (`eventType` → `timestamp`) enables querying all events of a specific type across pipelines.

All four tables have TTL enabled on a `ttl` attribute for automatic cleanup of expired records.

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

Manages SLA deadlines using EventBridge Scheduler. Creates one-time schedule entries that fire alerts at exact warning and breach timestamps. Invoked by the Step Functions state machine at two points:

| When | Action |
|---|---|
| After trigger (or validation exhaustion) | Creates two one-time EventBridge Scheduler entries — one for warning, one for breach. Entries auto-delete after firing. |
| On job completion (or all retries exhausted) | Cancels unfired Scheduler entries. Publishes `SLA_MET` if the job completed before the warning deadline. |

When a Scheduler entry fires, it invokes this Lambda to publish the corresponding `SLA_WARNING` or `SLA_BREACH` event to EventBridge.

### watchdog

Invoked by an EventBridge scheduled rule (default: every 5 minutes). Runs two independent scans:

1. **Stale triggers** -- scans for `TRIGGER#` records with `RUNNING` status whose TTL has expired. Publishes `SFN_TIMEOUT` events and sets status to `FAILED_FINAL`.
2. **Missed schedules** -- loads all cron-scheduled pipeline configs, checks for missing `TRIGGER#` records for today's date. Publishes `SCHEDULE_MISSED` events for pipelines past their expected start time.

See [Watchdog](../watchdog) for the full algorithm.

### event-sink

Receives EventBridge events via a rule that matches all `source: "interlock"` events. Writes each event to the events table with the pipeline ID as PK and a composite `{tsMillis}#{eventType}` as SK. TTL is set based on `EVENTS_TTL_DAYS` (default: 90).

### alert-dispatcher

Processes messages from the SQS alert queue. Formats pipeline events into Slack Block Kit messages and posts them using the Slack Bot API (`chat.postMessage`).

**Threading**: looks up existing thread records in the events table (`THREAD#{scheduleId}#{date}`). If a thread exists for the pipeline-day, replies in-thread. Otherwise, posts a new message and saves the thread timestamp for subsequent alerts.

**Error handling**: Slack API errors return batch item failures so SQS retries individual messages. Thread lookup/save errors are logged but don't fail the message delivery.

## Step Functions State Machine

The state machine orchestrates the pipeline lifecycle as a sequential flow of 18 states. SLA monitoring is handled by EventBridge Scheduler rather than a parallel branch.

### State Flow

```
InitEvalLoop → Evaluate → IsReady
  → (passed) Trigger → CheckSLAConfig → ScheduleSLAAlerts → HasTriggerResult
      → WaitForJob → CheckJob → IsJobDone
          → (terminal) CheckCancelSLA → CancelSLASchedules → Done
          → (running) WaitForJob (loop)
  → (not ready) WaitInterval → IncrementElapsed → CheckWindowExhausted
      → (window remaining) Evaluate (loop)
      → (window exhausted) ValidationExhausted → CheckSLAConfig → ... → Done
```

### Evaluation Loop (7 states)

1. **InitEvalLoop** — initialize elapsed-seconds counter
2. **Evaluate** — invoke orchestrator with `mode=evaluate`
3. **IsReady** — if `passed`, go to Trigger; otherwise WaitInterval
4. **WaitInterval** — configurable delay between evaluation attempts
5. **IncrementElapsed** — track total elapsed time via `States.MathAdd`
6. **CheckWindowExhausted** — if elapsed >= window, go to ValidationExhausted
7. **ValidationExhausted** — publish `VALIDATION_EXHAUSTED` event

### Trigger and Job Polling (7 states)

1. **Trigger** — invoke orchestrator with `mode=trigger`. Infrastructure failures retry 4 times with exponential backoff (30s, 60s, 120s, 240s)
2. **CheckSLAConfig** — if SLA configured, schedule alerts; otherwise skip
3. **ScheduleSLAAlerts** — invoke sla-monitor to create one-time EventBridge Scheduler entries
4. **HasTriggerResult** — if a job was triggered, poll for completion; otherwise finish
5. **WaitForJob** — configurable delay between job status checks
6. **CheckJob** — invoke orchestrator with `mode=check-job`
7. **IsJobDone** — route on terminal events (success/fail/timeout) or keep polling

### SLA Cleanup (2 states)

1. **CheckCancelSLA** — if SLA was scheduled, cancel unfired entries
2. **CancelSLASchedules** — invoke sla-monitor to delete Scheduler entries and record final SLA outcome

### Terminal States (2 states)

1. **InfraFailure** — Fail state for unrecoverable infrastructure errors
2. **Done** — Succeed state

### Error Handling

Every Task state includes Retry and Catch blocks:

- **Default Retry**: `IntervalSeconds: 2`, `MaxAttempts: 3`, `BackoffRate: 2` for Lambda service errors
- **Trigger Retry**: `IntervalSeconds: 30`, `MaxAttempts: 4`, `BackoffRate: 2` — infrastructure failures (e.g., Glue concurrency limits) get a longer retry budget
- **Catch**: unrecoverable errors route to `InfraFailure` (Fail state). Trigger exhaustion routes to `CheckCancelSLA` for graceful SLA cleanup before termination.

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
| stream-router | Read/write control, joblog, rerun + stream access | PutEvents | `states:StartExecution`, SQS SendMessage (DLQs) |
| orchestrator | Read/write control, joblog, rerun | PutEvents | Conditional trigger permissions (Glue, EMR, etc.) |
| sla-monitor | None | PutEvents | `scheduler:CreateSchedule`, `scheduler:DeleteSchedule` |
| watchdog | Read control, joblog, rerun; write control only | PutEvents | -- |
| event-sink | Write events table | -- | -- |
| alert-dispatcher | Read/write events table (thread storage) | -- | SQS ReceiveMessage/DeleteMessage |

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
    ├── dynamodb.tf          # 4 tables (control, joblog, rerun, events)
    ├── lambda.tf            # 6 functions, IAM, DLQs, event source mappings
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
| `enable_glue_trigger` | Add Glue + CloudWatch Logs RCA permissions to orchestrator | `false` |
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
