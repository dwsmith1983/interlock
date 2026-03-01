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
| `age_lt` | Time since RFC3339 timestamp is less than duration | Yes | Duration string (e.g. `"2h"`) |

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
        ↓
stream-router starts SFN
        │
   ┌────┴────┐
   ↓         ↓
Evaluation  SLA Monitor
   Loop     (parallel)
   │             │
   │      CalcDeadlines
   │             │
   │      WaitForWarning
   │             │
   │      FireSLAWarning
   │             │
   │      WaitForBreach
   │             │
   │      FireSLABreach
   │
   ├─→ Evaluate (validation rules)
   │       │
   │   ┌───┴───┐
   │   ↓       ↓
   │  Ready  Not Ready
   │   │       │
   │   │    Wait → re-evaluate
   │   │       │
   │   │    Window exhausted?
   │   │       │
   │   │    VALIDATION_EXHAUSTED
   │   │
   │   ↓
   │  Trigger job
   │   │
   │  Wait → CheckJob
   │   │
   │  success / fail / timeout
   │
   ↓
Reconcile (merge branch results)
```

### State Summary

The Step Functions state machine uses two parallel branches:

1. **Evaluation branch** -- loops on Evaluate, checking validation rules at a configurable interval. When all rules pass, triggers the job and polls for completion. Exits on success, failure, timeout, or window exhaustion.

2. **SLA monitoring branch** -- calculates warning and breach timestamps from the SLA config, waits until each deadline, and publishes alerts to EventBridge. Skipped entirely if no SLA is configured.

Both branches run concurrently. The evaluation branch handles the actual pipeline lifecycle; the SLA branch is purely observational and never stops execution.

## Event System

All lifecycle events are published to a custom EventBridge event bus. This replaces the previous SNS-based alert system with a unified event stream.

| Event Type | When Published |
|---|---|
| `VALIDATION_PASSED` | All validation rules pass |
| `VALIDATION_EXHAUSTED` | Evaluation window closed without passing |
| `JOB_TRIGGERED` | Pipeline trigger executed |
| `JOB_COMPLETED` | Triggered job finished successfully |
| `JOB_FAILED` | Triggered job failed |
| `SLA_WARNING` | SLA warning deadline reached |
| `SLA_BREACH` | SLA breach deadline reached |
| `RETRY_EXHAUSTED` | All retry attempts consumed |
| `SFN_TIMEOUT` | Step Function execution timed out (watchdog) |
| `SCHEDULE_MISSED` | Cron schedule passed without a trigger (watchdog) |
| `INFRA_FAILURE` | Unrecoverable infrastructure error |

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

When a triggered job fails or times out, the stream-router processes the `JOB#` record from the joblog table stream and decides whether to retry:

1. Count existing re-run records for this pipeline/schedule/date
2. If under `job.maxRetries`, write a re-run record, release and re-acquire the trigger lock, and start a new Step Function execution
3. If at the retry limit, publish a `RETRY_EXHAUSTED` event and mark the trigger as `FAILED_FINAL`

Re-run executions use a unique name (`{pipeline}-{schedule}-{date}-rerun-{attempt}`) to avoid Step Function dedup collisions.

## Post-Run Validation

Pipelines can define optional post-run validation rules that are evaluated after the triggered job completes. These use the same declarative rule syntax as pre-trigger validation:

```yaml
postRun:
  rules:
    - key: output-row-count
      check: gte
      field: count
      value: 1000
```

Post-run rules always use `ALL` mode (every rule must pass).
