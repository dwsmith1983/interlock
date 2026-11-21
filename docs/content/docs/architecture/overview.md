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

A pipeline can also define a `schedule.trigger` that fires when a specific sensor write arrives, without waiting for the evaluation loop:

```yaml
schedule:
  trigger:
    key: orders-landed
    check: exists
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

The Step Functions state machine has 28 states:

1. **Evaluation loop**: evaluates validation rules at a configurable interval. When all rules pass, triggers the job. If the evaluation window expires, ValidationExhausted ends the execution without the SLA cleanup, since no job ran. Before the run date's cutoff, a first run is released: the orchestrator writes an `evaluation-released` job row, deletes the trigger row and publishes `EVALUATION_RELEASED` (not an alert), so the next trigger sensor write or watchdog pass can start evaluation again until the cutoff, and the date's SLA schedules still fire. With no cutoff for the date, or for a rerun, it publishes `VALIDATION_EXHAUSTED` and sets `FAILED_FINAL`, and the run's unfired SLA schedules fire and are suppressed because the run is finished. At or after the cutoff, a first run publishes `SENSOR_DEADLINE_EXPIRED` and then sets `FAILED_FINAL`. An evaluation still running at the cutoff, for a first run, ends that way too instead of launching the job.

2. **SLA scheduling**: not a state machine step. The watchdog creates one-time EventBridge Scheduler entries for the SLA warning and breach of each run date that has a `RUN#` row. The Scheduler fires alerts independently at exact timestamps.

3. **Job polling** — polls the triggered job for completion via `check-job` mode. Terminal events (success, fail, timeout) proceed to SLA cleanup.

4. **SLA cleanup**: cancels unfired SLA Scheduler entries and publishes `SLA_MET` if the run ended before the breach time (`SLA_BREACH` otherwise).

## Event System

All lifecycle events are published to a custom EventBridge event bus. This replaces the previous SNS-based alert system with a unified event stream.

| Event Type | When Published |
|---|---|
| `VALIDATION_PASSED` | All validation rules pass |
| `VALIDATION_EXHAUSTED` | Evaluation window closed without passing, with no cutoff for the date or for a rerun; the date ends `FAILED_FINAL` |
| `EVALUATION_RELEASED` | Evaluation window closed before the cutoff; the date can start again (not an alert) |
| `JOB_TRIGGERED` | Pipeline trigger executed |
| `JOB_COMPLETED` | Triggered job finished successfully |
| `JOB_FAILED` | Triggered job failed |
| `SLA_WARNING` | SLA warning deadline reached |
| `SLA_BREACH` | SLA breach deadline reached |
| `SLA_MET` | Run ended before the SLA breach time |
| `RETRY_EXHAUSTED` | All retry attempts consumed |
| `SFN_TIMEOUT` | Step Function execution timed out (watchdog) |
| `SCHEDULE_MISSED` | A run date's cron start-by time passed before its job started (watchdog) |
| `SENSOR_DEADLINE_EXPIRED` | A run date passed its `schedule.trigger.cutoff` without starting and is closed |
| `RELATIVE_SLA_WARNING` / `RELATIVE_SLA_BREACH` | A relative SLA (`sla.maxDuration`) is at risk or exceeded (watchdog) |
| `POST_RUN_PASSED` | Post-run rules pass after job completion, inside the watch window |
| `POST_RUN_FAILED` | Post-run rules fail (no drift detected), inside the watch window |
| `POST_RUN_DRIFT` | Sensor count drifted from baseline after completion, inside the watch window → rerun requested |
| `POST_RUN_DRIFT_INFLIGHT` | Sensor count drifted while job is still running (informational) |
| `POST_RUN_BASELINE_CAPTURED` | Baseline sensor snapshot captured on job success |
| `POST_RUN_SENSOR_MISSING` | Post-run sensor not received within timeout (watchdog) |
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
2. Read the number of retries so far from the execution name's `-r{N}` suffix (none means 0), and the failure category from the run's own job log row (`PERMANENT` uses `job.maxCodeRetries`, default 1; anything else, or no row, uses `job.maxRetries`)
3. If at the limit, mark the trigger as `FAILED_FINAL` and publish a `RETRY_EXHAUSTED` event
4. Otherwise write a re-run record, start the next execution (which keeps the SLA schedules) and give the trigger lock a fresh TTL while it is still `RUNNING`. If the execution cannot be started, mark the trigger as `FAILED_FINAL` and publish `INFRA_FAILURE`

A dry-run pipeline, or one without a valid config, is never retried: the trigger goes to `FAILED_FINAL`.

The budget belongs to one execution chain. A requested rerun or a reconciler start begins a new chain with a fresh `maxRetries`, and any execution whose name ends in `-r{N}` counts as attempt N, so do not end a hand-started execution's name that way.

A retry is named after the failing execution with its `-r{N}` suffix replaced by `-r{N+1}` (`{execution}-r1`, `{execution}-r2`, ...), so a repeated CompleteTrigger never starts a second copy.

### Infrastructure Trigger Retry

When the job cannot be started (e.g., Glue `ConcurrentRunsExceededException`, or a `lambda` job whose function could not be invoked), the orchestrator logs `infra-trigger-failure` to the joblog table and returns a Lambda error. Step Functions retries the trigger up to `trigger_max_attempts` times (default 3, so four attempts in all), waiting 30s, 60s and 120s. This retry budget is separate from `maxRetries`. If all attempts fail, the execution routes to SLA cleanup and terminates gracefully. A `lambda` job whose function ran and returned an error has started, so it is a job failure (`fail`, category `PERMANENT`) that CompleteTrigger retries under `maxCodeRetries`.

## Post-Run Monitoring

Pipelines can define optional post-run rules that are evaluated reactively when sensor data arrives after job completion. Unlike pre-trigger validation (which runs in the Step Function), post-run monitoring is entirely event-driven via DynamoDB Streams.

```yaml
postRun:
  rules:
    - key: output-row-count
      check: gte
      field: count
      value: 1000
  driftThreshold: 10     # minimum sensor count change to trigger rerun (default: 0)
  sensorTimeout: "2h"    # watchdog alerts if sensor hasn't arrived after this long
```

### How It Works

1. **Baseline capture**: When a job completes successfully, the orchestrator reads all sensors and writes a date-scoped baseline (`postrun-baseline#<date>`) to the control table.
2. **Stream-based evaluation**: When a sensor matching a post-run rule arrives via DynamoDB Stream, the stream-router evaluates it:
   - If the trigger is **RUNNING**, drift is logged as an informational `POST_RUN_DRIFT_INFLIGHT` event (no rerun).
   - If the trigger is **COMPLETED** and the date is inside its watch window, the sensor count is compared against the baseline. Drift above `driftThreshold` triggers a rerun via the circuit breaker.
3. **Watchdog safety net**: If no post-run sensor arrives within `sensorTimeout` after job completion, the watchdog publishes a `POST_RUN_SENSOR_MISSING` event.

complete-trigger records the completion as `completedAt` on the trigger row. Late data, drift and the post-run checks are acted on only within `postRun.watchWindow` (default `4h`) of it, an automatic rerun request (late data or drift) handled after the window is skipped, and the watchdog counts `sensorTimeout` from it. See [Watch window](../../configuration/pipelines/#watch-window).

Post-run rules always use `ALL` mode (every rule must pass).
