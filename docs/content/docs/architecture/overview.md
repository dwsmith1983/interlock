---
title: Overview
weight: 1
description: STAMP safety model, declarative validation rules, sensor-driven evaluation, and the pipeline lifecycle.
---

## STAMP Model

Interlock applies [STAMP](https://psas.scripts.mit.edu/home/stamp/) (Systems-Theoretic Accident Model and Processes) to data pipeline operations. In STAMP, accidents result from inadequate enforcement of safety constraints -- not just component failures. Interlock maps this to data pipelines:

| STAMP Concept | Interlock Mapping |
|---|---|
| Safety constraint | Validation rule (declarative check against sensor data) |
| Control structure | Pipeline config (YAML defining schedule, rules, job, SLA) |
| Controller | Orchestrator Lambda + Step Functions (evaluates rules, triggers jobs) |
| Controlled process | Pipeline (data transformation job) |
| Feedback | Sensor data written to DynamoDB by external processes |
| Absence of control action | Watchdog Lambda (missed schedule and stale trigger detection) |

## Sensor-Driven Model

Interlock does not run evaluator subprocesses or make HTTP calls to check readiness. Instead, external processes push sensor data into the DynamoDB control table. The framework reads this data and evaluates it against declarative validation rules.

```
External process ──→ DynamoDB control table (SENSOR# records)
                              │
                     DynamoDB Stream
                              │
                              ↓
                     stream-router Lambda
                              │
                    ┌─────────┴──────────┐
                    ↓                    ↓
          Sensor matches            Config change
          trigger condition         → invalidate cache
                    │
                    ↓
          Start Step Function execution
```

Sensor records use a composite key structure:

- **PK**: `PIPELINE#{pipelineId}`
- **SK**: `SENSOR#{sensorKey}`

Any system that can write to DynamoDB can serve as a sensor source: Lambda functions, Glue jobs, Airflow tasks, Step Function states, or external APIs via the AWS SDK.

## Declarative Validation Rules

Pipeline readiness is determined by validation rules defined in YAML. Each rule checks a sensor key's data against a condition. No external evaluator programs are needed.

### Rule Structure

```yaml
validation:
  trigger: "ALL"    # ALL = every rule must pass, ANY = at least one
  rules:
    - key: orders-landed
      check: exists
    - key: orders-count
      check: gt
      field: count
      value: 0
    - key: source-freshness
      check: age_lt
      field: updatedAt
      value: "2h"
```

### Supported Operators

| Operator | Description | Field Required | Value |
|---|---|---|---|
| `exists` | Sensor key is present in the control table | No | -- |
| `equals` | Field value equals expected value | Yes | Any |
| `gt` | Field value greater than threshold | Yes | Numeric |
| `gte` | Field value greater than or equal to threshold | Yes | Numeric |
| `lt` | Field value less than threshold | Yes | Numeric |
| `lte` | Field value less than or equal to threshold | Yes | Numeric |
| `age_lt` | Time since the field's timestamp ([forms](../../configuration/pipelines/#pushing-sensor-data)) is less than duration | Yes | Duration string (e.g. `"2h"`) |

### Trigger Modes

The `trigger` field controls how individual rule results combine:

- **ALL** -- every rule must pass before the pipeline triggers (logical AND)
- **ANY** -- at least one rule must pass (logical OR)

### Stream Trigger Condition

Every pipeline defines a `schedule.trigger` that fires when a specific sensor write arrives, without waiting for the evaluation loop, and its `cutoff`, the latest automatic start:

```yaml
schedule:
  trigger:
    key: orders-landed
    check: exists
    cutoff: "23:00"
```

When the stream-router Lambda sees a `SENSOR#` record matching this key, it evaluates the trigger condition immediately. If the condition passes, it starts a Step Function execution.

## Pipeline Lifecycle

Each pipeline execution follows this lifecycle, orchestrated by Step Functions:

```
Sensor data arrives
        │
        ▼
stream-router starts SFN
        │
        ▼
   CheckRetryBackoff
   (a failure retry waits first)
        │
        ▼
   Evaluation Loop
   (Evaluate → Wait → Re-evaluate)
        │
   ┌────┴────┐
   ▼         ▼
 Passed    Window Exhausted, or cutoff passed
   │         │
   ▼         ▼
 Trigger   EVALUATION_RELEASED, VALIDATION_EXHAUSTED or SENSOR_DEADLINE_EXPIRED → Done
   │
   ▼
 WaitForJob → CheckJob → IsJobDone
   │
   ▼
CheckCancelSLA → CancelSLASchedules
   │
   ▼
 Done
```

### State Summary

The Step Functions state machine has 31 states:

1. **Evaluation loop**: evaluates validation rules at a configurable interval. When all rules pass, triggers the job. If the evaluation window expires, ValidationExhausted ends the execution without the SLA cleanup, since no job ran. Before the run date's cutoff, a first run is released: the orchestrator writes an `evaluation-released` job row, deletes the trigger row and publishes `EVALUATION_RELEASED` (not an alert), so the next trigger sensor write or watchdog pass can start evaluation again until the cutoff, and the date's SLA schedules still fire. For a rerun it publishes `VALIDATION_EXHAUSTED` and sets `FAILED_FINAL`, and the run's unfired SLA schedules fire and are suppressed because the run is finished. At or after the cutoff, a first run publishes `SENSOR_DEADLINE_EXPIRED` and then sets `FAILED_FINAL`. An evaluation still running at the cutoff, for a first run, ends that way too instead of launching the job.

2. **SLA scheduling**: not a state machine step. The watchdog creates one-time EventBridge Scheduler entries for the SLA warning and breach of each run date that has a `RUN#` row, at most 6 hours before the warning. The Scheduler fires alerts independently at exact timestamps.

3. **Job polling**: polls the triggered job for completion via `check-job` mode. A terminal event (success, fail, timeout) goes to CompleteTrigger. When it starts a retry, the execution ends and the retry's execution keeps the SLA schedules; otherwise the run goes on to SLA cleanup.

4. **SLA cleanup**: cancels unfired SLA Scheduler entries and publishes `SLA_MET` if the run completed before the breach time, and `SLA_BREACH` once the breach time has passed.

## Event System

All lifecycle events are published to a custom EventBridge event bus.

| Event Type | When Published |
|---|---|
| `VALIDATION_PASSED` | All validation rules pass |
| `VALIDATION_EXHAUSTED` | Evaluation window closed without passing, for a rerun; the date ends `FAILED_FINAL` |
| `EVALUATION_RELEASED` | Evaluation window closed before the cutoff; the date can start again (not an alert) |
| `JOB_TRIGGERED` | A normal run publishes two: the stream-router's when it starts the execution, and the orchestrator's when it launches the job. The watchdog's reconciliation also publishes one when it restarts a date whose evaluation was released |
| `JOB_COMPLETED` | Triggered job finished successfully |
| `JOB_FAILED` | Triggered job failed |
| `SLA_WARNING` | SLA warning deadline reached |
| `SLA_BREACH` | SLA breach deadline reached |
| `SLA_MET` | Run completed before the SLA breach time |
| `RETRY_EXHAUSTED` | All retry attempts consumed |
| `SFN_TIMEOUT` | Step Function execution timed out (watchdog) |
| `SCHEDULE_MISSED` | A run date's cron start-by time passed before its job started (watchdog) |
| `SENSOR_DEADLINE_EXPIRED` | A run date passed its `schedule.trigger.cutoff` without starting and is closed |
| `RELATIVE_SLA_WARNING` / `RELATIVE_SLA_BREACH` | A relative SLA (`sla.maxDuration`) is at risk or exceeded (watchdog) |
| `POST_RUN_PASSED` | Post-run rules pass after job completion, inside the watch window |
| `POST_RUN_FAILED` | Post-run rules fail (no drift detected), inside the watch window |
| `POST_RUN_DRIFT` | A post-run sensor's `driftField` drifted from the baseline after completion, inside the watch window, while automatic reruns are allowed → `data-drift` rerun requested. Past `job.maxAutoRerunAge` drift publishes `MANUAL_RERUN_REQUIRED` instead |
| `POST_RUN_BASELINE_CAPTURED` | Baseline sensor snapshot captured on job success |
| `POST_RUN_SENSOR_MISSING` | Post-run sensor not received within `sensorTimeout` of completion (watchdog; only for the daily run date equal to the current UTC date) |
| `INFRA_FAILURE` | Unrecoverable infrastructure error |

See [Alerting](../../reference/alerting/) for every event type and which ones alert.

### Event Payload

All events share a common payload structure:

```json
{
  "source": "interlock",
  "detail-type": "JOB_TRIGGERED",
  "detail": {
    "pipelineId": "silver-orders",
    "scheduleId": "stream",
    "date": "2026-03-01",
    "message": "triggered http job",
    "timestamp": "2026-03-01T09:15:00Z"
  }
}
```

Consumers subscribe to events by creating EventBridge rules that match on `source` and `detail-type`. This enables routing to SNS topics, SQS queues, Lambda functions, or any other EventBridge target without modifying the framework.

## Automatic Retries

When a triggered job fails or times out (including a closed job poll window), the run's own CompleteTrigger step decides whether to retry. It is the only actor that does; the stream-router just publishes `JOB_FAILED` for the `fail` or `timeout` row.

1. If the date is excluded by the calendar, publish `PIPELINE_EXCLUDED` and mark the trigger as `FAILED_FINAL`; an excluded date is never retried
2. Read the number of retries so far from the execution name's `-r{N}` suffix (none means 0), and the failure category from the run's own job log row (`PERMANENT` uses `job.maxCodeRetries`, default 1; `OUTCOME_UNKNOWN` has no budget, see step 3; anything else, or no row, uses `job.maxRetries`)
3. If the category is `OUTCOME_UNKNOWN` (a `lambda` job still running at the end of the orchestrator's wait), mark the trigger as `FAILED_FINAL` without a `RETRY_EXHAUSTED`; the row's `JOB_FAILED` is the alert. If at the limit, mark the trigger as `FAILED_FINAL` and publish a `RETRY_EXHAUSTED` event
4. Otherwise write a re-run record, start the next execution (which keeps the SLA schedules and [waits](../../configuration/pipelines/#retry-budgets) 60 seconds, doubled per retry, at most 15 minutes, before it evaluates) and give the trigger row a fresh stale deadline (`staleAt`) while it is still `RUNNING`. If the execution cannot be started, mark the trigger as `FAILED_FINAL` and publish `INFRA_FAILURE`

A dry-run pipeline, one without a valid config, or a job whose outcome is unknown (`OUTCOME_UNKNOWN`) is never retried: the trigger goes to `FAILED_FINAL`.

The budget belongs to one execution chain. A requested rerun or a reconciler start begins a new chain with a fresh `maxRetries`, and any execution whose name ends in `-r{N}` counts as attempt N, so do not end a hand-started execution's name that way.

A retry is named after the failing execution with its `-r{N}` suffix replaced by `-r{N+1}` (`{execution}-r1`, `{execution}-r2`, ...), so a repeated CompleteTrigger never starts a second copy.

### Infrastructure Trigger Retry

When the job cannot be started (e.g., Glue `ConcurrentRunsExceededException`, or a `lambda` job whose function could not be invoked), the orchestrator logs `infra-trigger-failure` to the joblog table and returns a Lambda error. Step Functions retries the trigger up to `trigger_max_attempts` times (default 3, so four attempts in all), waiting 30s, 60s and 120s. This retry budget is separate from `maxRetries`. If all attempts fail, the date ends `FAILED_FINAL` with `RETRY_EXHAUSTED` (a `RERUN_REQUEST` runs it again), and the execution routes to SLA cleanup and terminates gracefully. A `lambda` job whose function ran and returned an error has started, so it is a job failure (`fail`, category `PERMANENT`) that CompleteTrigger retries under `maxCodeRetries`.

## Post-Run Monitoring

Pipelines can define optional post-run rules that are evaluated reactively when sensor data arrives after job completion. Unlike pre-trigger validation (which runs in the Step Function), post-run monitoring is entirely event-driven via DynamoDB Streams.

```yaml
postRun:
  rules:
    - key: output-row-count
      check: gte
      field: count
      value: 1000
  driftField: count      # field compared with the baseline (default: sensor_count)
  driftThreshold: 10     # a change larger than this is drift (default: 0, any change)
  sensorTimeout: "2h"    # watchdog alerts if sensor hasn't arrived after this long
```

Set `driftField` to a field your post-run sensor carries: with the default, `sensor_count`, a sensor without that field never drifts.

### How It Works

1. **Baseline capture**: When a job completes successfully, the orchestrator reads the sensors that `postRun.rules` name and writes a date-scoped baseline (`postrun-baseline#<date>`) to the control table. The row expires 31 days after its latest capture (or, for a row a seed created, 31 days after it was written), past the longest watch window (720h).
2. **Stream-based evaluation**: When a sensor matching a post-run rule arrives via DynamoDB Stream, the stream-router evaluates it:
   - If the trigger is **RUNNING**, a write while the job runs is not compared. The baseline is captured when the run completes, and later writes are compared with it.
   - If the trigger is **COMPLETED** and the date is inside its watch window, the `driftField` value is compared against the baseline. Drift above `driftThreshold`, until `job.maxAutoRerunAge` has passed, publishes `POST_RUN_DRIFT` and writes a `data-drift` rerun request, which skips the circuit breaker; past the age it publishes `MANUAL_RERUN_REQUIRED` instead. A sensor first written after completion has no baseline value, so that first value becomes its baseline and later writes are compared with it.
3. **Watchdog safety net**: If no post-run sensor arrives within `sensorTimeout` after job completion, the watchdog publishes a `POST_RUN_SENSOR_MISSING` event. It checks only the daily run date equal to the current UTC date, so an earlier date, an hourly run or a timeout that ends after UTC midnight gets none. A sensor has arrived when it exists and either differs from its baseline value or did not exist at completion.

complete-trigger records the completion as `completedAt` on the trigger row. Late data, drift and the post-run checks are acted on only within `postRun.watchWindow` (default `4h`) of it. Automatic reruns (late data or drift) end separately, at the earlier of the window's end and `job.maxAutoRerunAge` (default `120h`): inside the window but past the age, late data is still published with no rerun request, and an automatic request handled past either limit is skipped. Late data and drift past the age or after the window, and a skipped automatic request, publish `MANUAL_RERUN_REQUIRED` once per completion and reason (`POST_RUN_DRIFT` only while a rerun can follow); after the window drift is still compared with the baseline, but nothing else is published or evaluated. The watchdog counts `sensorTimeout` from `completedAt` too. See [Watch window](../../configuration/pipelines/#watch-window).

Post-run rules always use `ALL` mode (every rule must pass).
