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
| `SENSOR#` (post-run) | If sensor matches a `postRun.rules[].key` and trigger is COMPLETED: compare against baseline, publish drift/pass/fail events, request rerun on drift. If trigger is RUNNING: publish informational `POST_RUN_DRIFT_INFLIGHT` event |

The stream-router maintains a config cache to avoid redundant DynamoDB reads. Pipeline configs are validated at load time (`ValidatePipelineConfig`) -- invalid configs are logged and skipped.

Trigger lock acquisition uses conditional writes to prevent duplicate executions for the same pipeline/schedule/date. Lock TTL is dynamically computed from the `SFN_TIMEOUT_SECONDS` environment variable plus a 30-minute buffer (default: 4h30m). Terminal trigger statuses (`COMPLETED`, `FAILED_FINAL`) have their TTL removed so records persist indefinitely.

Event source mappings from both control and joblog streams use batch size 10, bisect-on-error, max 3 retries, and SQS dead-letter queues for poison records.

### orchestrator

Multi-mode dispatcher invoked by Step Functions. Each invocation specifies a `mode` field:

| Mode | Purpose |
|---|---|
| `evaluate` | Load config and sensor data, evaluate validation rules, publish `VALIDATION_PASSED` if all pass |
| `trigger` | Build trigger config from `job` section, execute trigger (Glue, EMR, HTTP, etc.), return run ID |
| `check-job` | Query joblog table for latest event; return `success`, `fail`, `timeout`, or empty (still running). Propagates `FailureCategory` to the joblog when a failure is detected |
| `validation-exhausted` | Publish `VALIDATION_EXHAUSTED` event when the evaluation window closes |
| `job-poll-exhausted` | Publish `JOB_POLL_EXHAUSTED` event, write timeout joblog entry, set trigger to `FAILED_FINAL` when the job poll window expires |
| `complete-trigger` | Set trigger status to `COMPLETED` (on success) or `FAILED_FINAL` (on failure/timeout). Ensures trigger records reflect terminal state |

Supported trigger types: `http`, `command`, `airflow`, `glue`, `emr`, `emr-serverless`, `step-function`, `databricks`, `lambda`.

### sla-monitor

Manages SLA deadlines using EventBridge Scheduler. Creates one-time schedule entries that fire alerts at exact warning and breach timestamps. Invoked by the Step Functions state machine at two points:

| When | Action |
|---|---|
| After trigger (or validation exhaustion) | Creates two one-time EventBridge Scheduler entries — one for warning, one for breach. Entries auto-delete after firing. |
| On job completion (or all retries exhausted) | Cancels unfired Scheduler entries. Publishes `SLA_MET` if the job completed before the warning deadline. |

When a Scheduler entry fires, it invokes this Lambda to publish the corresponding `SLA_WARNING` or `SLA_BREACH` event to EventBridge.

### watchdog

Invoked by an EventBridge scheduled rule (default: every 5 minutes). Runs three independent scans:

1. **Stale triggers** -- scans for `TRIGGER#` records with `RUNNING` status whose TTL has expired. Publishes `SFN_TIMEOUT` events and sets status to `FAILED_FINAL`.
2. **Missed schedules** -- loads all cron-scheduled pipeline configs, checks for missing `TRIGGER#` records for today's date. Publishes `SCHEDULE_MISSED` events for pipelines past their expected start time.
3. **Missing post-run sensors** -- for pipelines with `postRun` config and a completed trigger, checks whether post-run sensors have arrived within the `sensorTimeout` grace period. Publishes `POST_RUN_SENSOR_MISSING` events.

See [Watchdog](../watchdog) for the full algorithm.

### event-sink

Receives EventBridge events via a rule that matches all `source: "interlock"` events. Writes each event to the events table with the pipeline ID as PK and a composite `{tsMillis}#{eventType}` as SK. TTL is set based on `EVENTS_TTL_DAYS` (default: 90).

### alert-dispatcher

Processes messages from the SQS alert queue. Formats pipeline events into Slack Block Kit messages and posts them using the Slack Bot API (`chat.postMessage`).

**Threading**: looks up existing thread records in the events table (`THREAD#{scheduleId}#{date}`). If a thread exists for the pipeline-day, replies in-thread. Otherwise, posts a new message and saves the thread timestamp for subsequent alerts.

**Secrets Manager**: when `SLACK_SECRET_ARN` is set, the Slack bot token is read from Secrets Manager at cold start instead of the `SLACK_BOT_TOKEN` environment variable. The module conditionally grants `secretsmanager:GetSecretValue` to the alert-dispatcher role.

**Error handling**: Slack API errors return batch item failures so SQS retries individual messages. Thread lookup/save errors are logged but don't fail the message delivery.

## Step Functions State Machine

The state machine orchestrates the pipeline lifecycle as a sequential flow. A global `TimeoutSeconds` (configurable via the `sfn_timeout_seconds` Terraform variable, default 4h) caps the entire execution time as a hard safety bound. SLA monitoring is handled by EventBridge Scheduler rather than a parallel branch.

### State Flow

```
InitEvalLoop → Evaluate → IsReady
  → (passed) Trigger → HasTriggerResult
      → InitJobPollLoop → WaitForJob → CheckJob → IsJobDone
          → (terminal) CompleteTrigger → CheckCancelSLA → CancelSLASchedules → Done
          → (running) IncrementJobPollElapsed → CheckJobPollExhausted
              → (window remaining) WaitForJob (loop)
              → (exhausted) JobPollExhausted → InjectTimeoutEvent → CompleteTrigger → ...
  → (not ready) WaitInterval → IncrementElapsed → CheckWindowExhausted
      → (window remaining) Evaluate (loop)
      → (window exhausted) ValidationExhausted → CheckCancelSLA → ... → Done
```

### Evaluation Loop (7 states)

1. **InitEvalLoop** — initialize elapsed-seconds counter
2. **Evaluate** — invoke orchestrator with `mode=evaluate`
3. **IsReady** — if `passed`, go to Trigger; otherwise WaitInterval
4. **WaitInterval** — configurable delay between evaluation attempts
5. **IncrementElapsed** — track total elapsed time via `States.MathAdd`
6. **CheckWindowExhausted** — if elapsed >= window, go to ValidationExhausted
7. **ValidationExhausted** — publish `VALIDATION_EXHAUSTED` event

### Trigger and Job Polling (11 states)

1. **Trigger** — invoke orchestrator with `mode=trigger`. Infrastructure failures retry up to `trigger_max_attempts` times (default 3) with exponential backoff (30s, 60s, 120s, 240s)
2. **HasTriggerResult** — if a job was triggered, poll for completion; otherwise finish
3. **InitJobPollLoop** — initialize job poll elapsed counter
4. **WaitForJob** — configurable delay between job status checks
5. **CheckJob** — invoke orchestrator with `mode=check-job`
6. **IsJobDone** — route on terminal events (success/fail/timeout) or keep polling
7. **IncrementJobPollElapsed** — track total job polling time via `States.MathAdd`
8. **CheckJobPollExhausted** — if elapsed >= `jobPollWindowSeconds`, go to JobPollExhausted
9. **JobPollExhausted** — invoke orchestrator with `mode=job-poll-exhausted`
10. **InjectTimeoutEvent** — inject `event: timeout` for CompleteTrigger routing
11. **CompleteTrigger** — invoke orchestrator with `mode=complete-trigger` to set terminal trigger status

### SLA Cleanup (2 states)

1. **CheckCancelSLA** — if SLA was scheduled, cancel unfired entries
2. **CancelSLASchedules** — invoke sla-monitor to delete Scheduler entries and record final SLA outcome

### Terminal States (2 states)

1. **InfraFailure** — Fail state for unrecoverable infrastructure errors
2. **Done** — Succeed state

### Error Handling

Every Task state includes Retry and Catch blocks:

- **Default Retry**: `IntervalSeconds: 2`, `MaxAttempts: 3`, `BackoffRate: 2` for Lambda service errors
- **Trigger Retry**: `IntervalSeconds: 30`, `MaxAttempts: trigger_max_attempts` (default 3), `BackoffRate: 2` — infrastructure failures (e.g., Glue concurrency limits) get a longer retry budget
- **Catch**: unrecoverable errors route to `InfraFailure` (Fail state). Trigger exhaustion routes to `CheckCancelSLA` for graceful SLA cleanup before termination.

### ARN Substitution

The ASL template at `deploy/statemachine.asl.json` uses substitution variables resolved by Terraform's `templatefile()`:

- `${orchestrator_arn}` — orchestrator Lambda ARN
- `${sla_monitor_arn}` — SLA monitor Lambda ARN
- `${sfn_timeout_seconds}` — global execution timeout (default 14400 / 4h)
- `${trigger_max_attempts}` — trigger infrastructure retry count (default 3)

## CloudWatch Alarms

The Terraform module creates CloudWatch alarms across four categories to monitor infrastructure health:

| Category | Alarms | Metric | Threshold |
|---|---|---|---|
| Lambda errors | 6 (one per function) | `Errors` | `>= 1` per 5-minute period |
| SFN failures | 1 (pipeline state machine) | `ExecutionsFailed` | `>= 1` per 5-minute period |
| DLQ depth | 3 (control, joblog, alert queues) | `ApproximateNumberOfMessagesVisible` | `>= 1` |
| Stream iterator age | 2 (control, joblog streams) | `IteratorAge` | `>= 300,000ms` (5 min) |

Alarm state changes are reshaped into `INFRA_ALARM` events via EventBridge input transformers and routed to event-sink and alert-dispatcher. Optionally, alarms also publish to an SNS topic via the `sns_alarm_topic_arn` variable.

See [Alerting](../../reference/alerting/#cloudwatch-alarms) for details on the event flow and consumer patterns.

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
| alert-dispatcher | Read/write events table (thread storage) | -- | SQS ReceiveMessage/DeleteMessage, conditional `secretsmanager:GetSecretValue` |

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
