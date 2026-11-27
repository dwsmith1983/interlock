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
| Trigger lock | `PIPELINE#{id}` | `TRIGGER#{schedule}#{date}` | Dedup lock for executions; `startedAt` and `completedAt` mark the job's launch and the run's completion |
| Expected run date | `PIPELINE#{id}` | `RUN#{runKey}` | Written by the user; drives the SLA schedules, start-by and cutoff checks |
| Rerun request | `PIPELINE#{id}` | `RERUN_REQUEST#{schedule}#{date}` | Written by the user (`manual`) or by Interlock (`late-data`, `data-drift`); the stream-router acts on it |

Interlock's own bookkeeping rows expire: the `SENSOR#` rows it writes (the alert markers `schedule-missed-check#`, `irregular-missed-check#`, `postrun-check#`, `relative-sla-warning-check#`, `relative-sla-breach-check#` and `manual-rerun-alert-<reason>#`, plus `first-sensor-arrival#{date}` and `postrun-baseline#{date}`) and the `late-data` and `data-drift` rerun requests carry a `ttl` 31 days after their latest write. That is the longest post-run watch window (720h, counted from `completedAt`, during which the baseline is read) plus a day. Seeding one rule's entry into an existing baseline row keeps that row's `ttl`. Sensor rows and rerun requests the user writes, `CONFIG`, `TRIGGER#` and `RUN#` rows get no `ttl` from Interlock.

DynamoDB Streams are enabled with `NEW_IMAGE` view type. The stream-router Lambda processes sensor writes and config changes from this stream.

### Job Log Table

Append-only log of job execution events. Each record represents a lifecycle event for a triggered job.

| Entity | PK | SK | Purpose |
|---|---|---|---|
| Job event | `PIPELINE#{id}` | `JOB#{schedule}#{date}#{timestamp}` | success/fail/timeout record; `runId` names the run it belongs to |

DynamoDB Streams are enabled. The stream-router publishes `JOB_COMPLETED` and `JOB_FAILED` from `JOB#` records; retries are started by the run's CompleteTrigger.

### Rerun Table

Tracks retry attempts for failed job executions.

| Entity | PK | SK | Purpose |
|---|---|---|---|
| Rerun record | `PIPELINE#{id}` | `RERUN#{schedule}#{date}#{attempt}` | Retry attempt metadata |

### Events Table

Centralized log of all EventBridge events. The event-sink Lambda writes every event to this table. Used by dashboards and audit queries.

| Entity | PK | SK | Purpose |
|---|---|---|---|
| Event | `PIPELINE#{id}` | `{tsMillis}#{eventType}#{hash}` | Event record with its details, see [Event Sink](../../reference/alerting/#event-sink) |
| Thread | `PIPELINE#{id}` | `THREAD#{schedule}#{date}`, or `THREADDAY#{YYYY-MM-DD}` (UTC) for alerts without a run date | Slack thread storage for alert-dispatcher |

GSI1 (`eventType` → `timestamp`) enables querying all events of a specific type across pipelines.

All four tables have TTL enabled on a `ttl` attribute for automatic cleanup of expired records.

## Lambda Functions

### stream-router

Processes DynamoDB Stream events from the control and joblog tables. Routes records by SK prefix:

| SK Prefix | Action |
|---|---|
| `SENSOR#` | Evaluate trigger condition; if matched and not excluded, acquire trigger lock and start Step Function execution. For a `COMPLETED` date inside its post-run watch window (from the `completedAt` complete-trigger records), the write is late data: `LATE_DATA_ARRIVAL`, and a `late-data` rerun request while `job.maxAutoRerunAge` (default `120h`) has not passed since `completedAt`, or, once it has, `MANUAL_RERUN_REQUIRED` (then `MANUAL_RERUN_STILL_REQUIRED` for later writes until a new completion); after the window it is ignored |
| `CONFIG` | Invalidate the config cache |
| `JOB#` | On success: publish `JOB_COMPLETED`. On fail/timeout: publish `JOB_FAILED` (retries belong to the run's CompleteTrigger). Dry-run pipelines: publish what a retry would do |
| `RERUN_REQUEST#` | Check the calendar, the rerun budget, a `RUNNING` date and the circuit breaker; on acceptance, reset the trigger lock, write the rerun row and start the execution, then delete the post-run baseline and record `rerun-accepted` and `RERUN_ACCEPTED`. If the start fails, the rerun row is deleted and the date's earlier status restored, so no budget is spent and a stream retry (up to 3, then the dead-letter queue) can start it; a row that cannot be deleted still counts against the budget. A failed restore, a crash between the reset and the start, or a retry that finds the finished execution of a start that reported an error leaves the date `RUNNING` until the watchdog's stale-trigger check closes it with `SFN_TIMEOUT` (stale deadline `staleAt`: `sfn_timeout_seconds` plus 30 minutes from the latest attempt, 4h30m by default). Dry-run pipelines: publish what a rerun would do |
| `SENSOR#` (post-run) | If sensor matches a `postRun.rules[].key` and trigger is COMPLETED: inside the date's post-run watch window (from `completedAt`), compare against baseline, publish drift/pass/fail events, request rerun on drift while `job.maxAutoRerunAge` has not passed; after the window the sensor is ignored. If trigger is RUNNING: publish informational `POST_RUN_DRIFT_INFLIGHT` event |

The stream-router maintains a config cache to avoid redundant DynamoDB reads. Pipeline configs are parsed and validated at load time (`ValidatePipelineConfig`): a bad `CONFIG` row is skipped and logged as a warning (the watchdog also publishes `CONFIG_INVALID`), and every other pipeline keeps running.

Trigger lock acquisition uses conditional writes to prevent duplicate executions for the same pipeline/schedule/date. A trigger row never carries `ttl`, so DynamoDB never deletes it. While it is `RUNNING` it holds a stale deadline, `staleAt`, computed from the `SFN_TIMEOUT_SECONDS` environment variable plus a 30-minute buffer (default: 4h30m) from the latest attempt; the watchdog closes a row past it. Terminal trigger statuses (`COMPLETED`, `FAILED_FINAL`) have `staleAt` removed, and records persist indefinitely.

Event source mappings from both control and joblog streams use batch size 10, bisect-on-error, max 3 retries, and SQS dead-letter queues for poison records.

### orchestrator

Multi-mode dispatcher invoked by Step Functions. Each invocation specifies a `mode` field:

| Mode | Purpose |
|---|---|
| `evaluate` | Load config and sensor data, evaluate validation rules, publish `VALIDATION_PASSED` if all pass |
| `trigger` | Build trigger config from `job` section, execute trigger (Glue, EMR, HTTP, etc.), return run ID: the job API's run ID for polled types, otherwise the Step Functions execution name |
| `check-job` | Query joblog table for the newest `success`, `fail` or `timeout` row of this run's `runId` (rows of earlier runs are ignored); otherwise poll the job API. Return the event, or empty (still running). Propagates `FailureCategory` to the joblog when a polled failure is detected |
| `validation-exhausted` | When the evaluation window closes: before the cutoff, release a first run (`evaluation-released` row, trigger row deleted, `EVALUATION_RELEASED`, status `released`); for a rerun, publish `VALIDATION_EXHAUSTED` and set `FAILED_FINAL` (`exhausted`); at or after the cutoff, publish `SENSOR_DEADLINE_EXPIRED` and set `FAILED_FINAL` (`cutoff`) |
| `infra-failure` | After a failed `evaluate`, `validation-exhausted` or `check-job` step: publish `INFRA_FAILURE` and set a `RUNNING` trigger row `FAILED_FINAL`. A missing or finished row, or one a later execution holds because this one released its lock, is left alone |
| `job-poll-exhausted` | Write a `timeout` joblog row for the run when the job poll window expires |
| `complete-trigger` | On success set the trigger to `COMPLETED`. On fail/timeout start the next attempt (`status: retrying`), which waits in its own execution, while `maxRetries` lasts, else set `FAILED_FINAL` and publish `RETRY_EXHAUSTED` (`PIPELINE_EXCLUDED` for an excluded date, `INFRA_FAILURE` when the next execution cannot start) |

Supported trigger types: `http`, `airflow`, `glue`, `emr`, `emr-serverless`, `step-function`, `databricks`, `lambda`.

**Secrets Manager**: when `TRIGGER_SECRETS_ARN` is set, the orchestrator reads that secret's `INTERLOCK_` variables into its environment at cold start (see [Setting `INTERLOCK_` values](../../configuration/triggers/#setting-interlock_-values)). The module conditionally grants `secretsmanager:GetSecretValue` on that secret to the orchestrator role.

### sla-monitor

Publishes the SLA alerts. The watchdog creates the one-time EventBridge Scheduler entries (warning and breach) for each `RUN#` date, at most 6 hours before its warning; sla-monitor is invoked:

| When | Action |
|---|---|
| A Scheduler entry fires | Publishes `SLA_WARNING` or `SLA_BREACH`, unless the run has finished (the trigger row is `COMPLETED` or `FAILED_FINAL`; without a trigger row, the date's newest job result decides). Entries auto-delete after firing. |
| The run ends without starting a retry (state machine) | Cancels unfired Scheduler entries. Publishes `SLA_MET` if the run completed before the breach time, and `SLA_BREACH` once the breach time has passed. |


### watchdog

Invoked by an EventBridge scheduled rule (default: every 5 minutes). Runs nine independent checks in a table-driven loop:

1. **Stale triggers** -- scans for `TRIGGER#` records with `RUNNING` status whose stale deadline (`staleAt`) has passed. Publishes `SFN_TIMEOUT` and then sets status to `FAILED_FINAL`; a failed publish leaves the row `RUNNING`, so the next pass retries.
2. **Missed schedules** -- for each `RUN#` date of a cron pipeline whose start-by time (the cron time) has passed, checks the trigger row for the job-started mark. Publishes `SCHEDULE_MISSED` once per date when the job has not started.
3. **Missed inclusion schedules** -- checks pipelines with inclusion calendar config for missing triggers on scheduled dates, dated up to 8 days before the current date in `schedule.timezone` and at most the 3 most recent. Publishes `IRREGULAR_SCHEDULE_MISSED` once per date, deduplicated by a marker written only after a successful publish, so a failed publish is retried on the next pass.
4. **Sensor-trigger reconciliation** -- re-evaluates trigger conditions for sensor-triggered pipelines, cron ones included. Self-heals missed triggers caused by silent completion-write failures. Past the date's `schedule.trigger.cutoff` it starts nothing and closes the date with `FAILED_FINAL` and one `SENSOR_DEADLINE_EXPIRED`.
5. **SLA scheduling** -- proactively creates EventBridge Scheduler entries for each expected run date (`RUN#<runKey>` row, written by the user) of a pipeline with an `sla.deadline` once its warning is at most 6 hours away, ensuring warnings and breaches fire even when data never arrives. A pipeline with no `RUN#` rows gets none.
6. **Cutoffs** -- for each expected run date (`RUN#` row) whose `schedule.trigger.cutoff` passed with no trigger row, writes `FAILED_FINAL` and publishes `SENSOR_DEADLINE_EXPIRED` once.
7. **Missing post-run sensors** -- for pipelines with `postRun` config and a completed trigger, checks whether post-run sensors have arrived within the `sensorTimeout` grace period, counted from the trigger row's `completedAt`. Publishes `POST_RUN_SENSOR_MISSING` once per date, deduplicated by a marker written only after a successful publish, so a failed publish is retried on the next pass.
8. **Relative SLA** -- for pipelines with `maxDuration` SLA config, checks each run date (daily or hourly) with a first sensor arrival, from 8 days before to 2 days after the current UTC date. While the run has not finished, publishes `RELATIVE_SLA_WARNING` once at first sensor arrival + `maxDuration` - `expectedDuration` (75% of `maxDuration` without `expectedDuration`), then `RELATIVE_SLA_BREACH` once at arrival + `maxDuration`. Each is deduplicated per date by a marker written only after a successful publish, so a failed publish is retried on the next pass. Nothing is sent once the breach time is more than 24 hours past, so a date whose breach was not published by then (a watchdog outage that long, say) never gets its `RELATIVE_SLA_BREACH`.
9. **Invalid configs** -- publishes `CONFIG_INVALID` for each `CONFIG` row the config cache rejected, once per broken config version and day, at most 20 per pass.

If any check fails, the watchdog publishes a `WATCHDOG_DEGRADED` event listing the failed checks and returns an aggregate error. Individual check failures do not prevent the remaining checks from running.

See [Watchdog](../watchdog) for the full algorithm.

### event-sink

Receives EventBridge events via a rule that matches all `source: "interlock"` events. Writes each event to the events table with the pipeline ID as PK and a composite `{tsMillis}#{eventType}#{hash}` as SK, where `hash` is taken from the event's content, and keeps the event's details in the `detail` map. TTL is set based on `EVENTS_TTL_DAYS` (default: 90).

**Error handling**: EventBridge invokes the function asynchronously. An event that fails every try, or exceeds the maximum event age, goes to the event-sink DLQ through the function's on-failure destination. Retries and the maximum event age stay at Lambda's defaults (2 retries, 6 hours). See [Unwritten Events](../../deployment/terraform/#unwritten-events).

### alert-dispatcher

Processes messages from the SQS alert queue. Formats pipeline events into Slack Block Kit messages and posts them using the Slack Bot API (`chat.postMessage`).

**Threading**: looks up the alert's thread record in the events table. The key is `THREAD#{scheduleId}#{date}` for an alert with a run date and `THREADDAY#{YYYY-MM-DD}` for one without, where the day is the UTC day of the SQS `SentTimestamp` (the same on every redelivery). If the record holds a thread in the configured channel, replies in it. A reply in a day thread sets `reply_broadcast`, so it also shows in the channel. Otherwise, posts a new message and saves its timestamp and channel over any earlier record.

**Secrets Manager**: when `SLACK_SECRET_ARN` is set, the Slack bot token is read from Secrets Manager at cold start instead of the `SLACK_BOT_TOKEN` environment variable. The module conditionally grants `secretsmanager:GetSecretValue` to the alert-dispatcher role.

**Error handling**: a failed message is returned as a batch item failure. Before that, the dispatcher calls `ChangeMessageVisibility` to set how long SQS hides it: the try's doubling wait (60 to 900 seconds), or Slack's `Retry-After` (up to 3600 seconds) if that is longer, plus jitter. The try number is the message's `ApproximateReceiveCount`. If that call fails, the queue's visibility timeout (180 seconds) applies. After 10 tries SQS moves the message to the alert DLQ (see [Delivery retries](../../reference/alerting/#alert-dispatcher-slack)). Thread lookup/save errors are logged but don't fail the message delivery.

## Step Functions State Machine

The state machine orchestrates the pipeline lifecycle as a sequential flow. A global `TimeoutSeconds` (configurable via the `sfn_timeout_seconds` Terraform variable, default 4h) caps the entire execution time as a hard safety bound. SLA monitoring is handled by EventBridge Scheduler rather than a parallel branch.

### State Flow

```
CheckRetryBackoff
  → (retry) RetryBackoff → InitEvalLoop
  → (any other run) InitEvalLoop

InitEvalLoop → Evaluate → IsReady
  → (passed) Trigger → HasTriggerResult
      → InitJobPollLoop → WaitForJob → CheckJob → IsJobDone
          → (terminal) CompleteTrigger → IsRetrying
              → (retrying) Done
              → CheckCancelSLA → CancelSLASchedules → Done
          → (running) IncrementJobPollElapsed → CheckJobPollExhausted
              → (window remaining) WaitForJob (loop)
              → (exhausted) JobPollExhausted → InjectTimeoutEvent → CompleteTrigger → ...
  → (not ready) WaitInterval → IncrementElapsed → CheckWindowExhausted
      → (window remaining) Evaluate (loop)
      → (window exhausted) ValidationExhausted (release, end or cutoff) → Done
  → (cutoff) ValidationExhausted → Done

Evaluate, ValidationExhausted or CheckJob fails after its retries
  → RecordInfraFailure → InfraFailure
```

### Retry Backoff (2 states)

1. **CheckRetryBackoff**: if `retryDelaySeconds` is above 0 (a retry after a job failure), go to RetryBackoff; otherwise InitEvalLoop
2. **RetryBackoff**: wait `retryDelaySeconds` (60 doubling per retry, at most 900), then InitEvalLoop

### Evaluation Loop (7 states)

1. **InitEvalLoop** — initialize elapsed-seconds counter
2. **Evaluate** — invoke orchestrator with `mode=evaluate`
3. **IsReady**: if `passed`, go to Trigger; if `cutoff` (the cutoff passed and the date is not a rerun), go to ValidationExhausted, which sets `FAILED_FINAL` and publishes `SENSOR_DEADLINE_EXPIRED`; otherwise WaitInterval
4. **WaitInterval** — configurable delay between evaluation attempts
5. **IncrementElapsed** — track total elapsed time via `States.MathAdd`
6. **CheckWindowExhausted** — if elapsed >= window, go to ValidationExhausted
7. **ValidationExhausted**: end the execution without SLA cleanup, since no job ran. Before the cutoff, a first run is released (`evaluation-released` joblog row, trigger row deleted, `EVALUATION_RELEASED`), so a later sensor write or watchdog pass can start it again and its SLA schedules still fire. For a rerun it publishes `VALIDATION_EXHAUSTED` and sets `FAILED_FINAL`, and sla-monitor suppresses the unfired SLA schedules because the run is finished. At or after the cutoff it publishes `SENSOR_DEADLINE_EXPIRED` and then sets `FAILED_FINAL`

### Trigger and Job Polling (13 states)

1. **Trigger**: invoke orchestrator with `mode=trigger`. A job that could not be started is retried up to `trigger_max_attempts` times after the first attempt (default 3, so four attempts, waiting 30s, 60s and 120s between them)
2. **HasTriggerResult**: if a job was triggered, poll for completion; otherwise go to FailValidationExhausted
3. **TriggerRetryExhausted**: after the Trigger retries run out, invoke orchestrator with `mode=trigger-exhausted` (records the failure, publishes `RETRY_EXHAUSTED` and sets the trigger row `FAILED_FINAL`; a `RERUN_REQUEST` runs the date again), then go to CheckCancelSLA
4. **InitJobPollLoop**: initialize job poll elapsed counter
5. **WaitForJob**: configurable delay between job status checks
6. **CheckJob**: invoke orchestrator with `mode=check-job`
7. **IsJobDone**: route on terminal events (success/fail/timeout) or keep polling
8. **IncrementJobPollElapsed**: track total job polling time via `States.MathAdd`
9. **CheckJobPollExhausted**: if elapsed >= `jobPollWindowSeconds`, go to JobPollExhausted
10. **JobPollExhausted**: invoke orchestrator with `mode=job-poll-exhausted` (best effort)
11. **InjectTimeoutEvent**: inject `event: timeout` for CompleteTrigger routing
12. **CompleteTrigger**: invoke orchestrator with `mode=complete-trigger`, passing the run ID and execution name; it sets the terminal status or starts the next attempt. If it still fails after its retries, go to CheckSLAForCompleteTriggerFailure
13. **IsRetrying**: if CompleteTrigger started a retry, finish without cancelling the SLA (the retry owns it); otherwise go to CheckCancelSLA

### SLA Cleanup (4 states)

1. **CheckCancelSLA** — if SLA was scheduled, cancel unfired entries
2. **CancelSLASchedules** — invoke sla-monitor to delete Scheduler entries and record final SLA outcome
3. **CheckSLAForCompleteTriggerFailure**: if SLA was configured, go to CancelSLAOnCompleteTriggerFailure; otherwise CompleteTriggerFailed
4. **CancelSLAOnCompleteTriggerFailure**: best-effort SLA cancellation, then CompleteTriggerFailed

### Infrastructure Failures (1 state)

1. **RecordInfraFailure**: after Evaluate, ValidationExhausted or CheckJob fails, invoke orchestrator with `mode=infra-failure` (publishes `INFRA_FAILURE` and sets the trigger row `FAILED_FINAL`; a `RERUN_REQUEST` runs the date again), then go to InfraFailure. It skips the SLA cleanup: sla-monitor suppresses the unfired SLA schedules because the run is finished. If it fails itself, the row stays `RUNNING` and the watchdog closes it with `SFN_TIMEOUT` once its stale deadline passes

### Terminal States (4 states)

1. **InfraFailure** — Fail state for unrecoverable infrastructure errors
2. **FailValidationExhausted**: Fail state when Trigger returned no result
3. **CompleteTriggerFailed**: Fail state when CompleteTrigger failed after its retries; the trigger row may still be `RUNNING`
4. **Done**: Succeed state

### Error Handling

Every Task state includes Retry and Catch blocks:

- **Throttle Retry**: `IntervalSeconds: 2`, `MaxAttempts: 7`, `BackoffRate: 2`, `MaxDelaySeconds: 60` (waits of 2, 4, 8, 16, 32, 60 and 60 seconds) for `Lambda.TooManyRequestsException`, first on every Task state and counted apart from the retries below, so a throttled orchestrator call on Trigger uses none of `trigger_max_attempts`. Its 182 seconds outlast the orchestrator's 120-second timeout. The delays are not randomised, since random ones could total less than that timeout, so executions throttled at the same moment retry together
- **Default Retry**: `IntervalSeconds: 2`, `MaxAttempts: 3`, `BackoffRate: 2` for Lambda service errors
- **Trigger Retry**: `IntervalSeconds: 30`, `MaxAttempts: trigger_max_attempts` (default 3), `BackoffRate: 2`: failures to start the job (e.g., Glue concurrency limits, a `lambda` job that could not be invoked) get a longer retry budget. `MaxAttempts` counts retries, so the default allows four attempts. A job that started and then failed is not retried here: CompleteTrigger applies `job.maxRetries`
- **Catch**: unrecoverable errors in Evaluate, ValidationExhausted and CheckJob route to `RecordInfraFailure`, then `InfraFailure` (Fail state). Trigger exhaustion routes to `CheckCancelSLA` for graceful SLA cleanup before termination.

### ARN Substitution

The ASL template at `deploy/statemachine.asl.json` uses substitution variables resolved by Terraform's `templatefile()`:

- `${orchestrator_arn}` — orchestrator Lambda ARN
- `${sla_monitor_arn}` — SLA monitor Lambda ARN
- `${sfn_timeout_seconds}` — global execution timeout (default 14400 / 4h)
- `${trigger_max_attempts}`: trigger infrastructure retries after the first attempt (default 3)

## CloudWatch Alarms

The Terraform module creates CloudWatch alarms across five categories to monitor infrastructure health:

| Category | Alarms | Metric | Threshold |
|---|---|---|---|
| Lambda errors | 6 (one per function) | `Errors` | `>= 1` per 5-minute period |
| SFN failures | 1 (pipeline state machine) | `ExecutionsFailed` | `>= 1` per 5-minute period |
| DLQ depth | 4 (control, joblog, alert and event-sink queues) | `ApproximateNumberOfMessagesVisible` | `>= 1` |
| Stream iterator age | 1 (stream-router, covers the control and joblog streams) | `IteratorAge` | `>= 300,000ms` (5 min) |
| Alert rule delivery | 2 (`alert_events` and `cw_alarm_alert` rules to the alert queue) | `FailedInvocations` | `>= 1` per 5-minute period |

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
| orchestrator | Read/write control, joblog, rerun | PutEvents | `states:StartExecution` (failure retries), conditional trigger permissions (Glue, EMR, etc.) |
| sla-monitor | Read control, joblog | PutEvents | `scheduler:CreateSchedule`, `scheduler:DeleteSchedule` |
| watchdog | Read control, joblog, rerun; write and delete in control only | PutEvents | `states:StartExecution` (reconciliation), `scheduler:CreateSchedule` (SLA schedules) |
| event-sink | Write events table | -- | SQS SendMessage (its DLQ) |
| alert-dispatcher | Read/write events table (thread storage) | -- | SQS ReceiveMessage/DeleteMessage/ChangeMessageVisibility, conditional `secretsmanager:GetSecretValue` |

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
| `log_retention_days` | CloudWatch log retention | `30` |
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
terraform apply -var="environment=prod" -var="dist_path=../dist"
```

Pipeline configs are loaded by the `config_loader.tf` module and written to the control table as `CONFIG` records.
