---
title: Pipelines
weight: 1
description: Pipeline YAML schema, validation rules DSL, and readiness evaluation.
---

## Pipeline Configuration

A pipeline is defined by a single YAML file that declares identity, schedule, SLA, validation rules, and job execution. Pipeline YAML files are loaded by the Terraform module from the `pipelines_path` directory and stored as CONFIG rows in the control DynamoDB table.

```yaml
pipeline:
  id: gold-revenue
  owner: analytics-team
  description: Gold-tier revenue aggregation pipeline

schedule:
  cron: "0 8 * * *"
  timezone: UTC
  trigger:
    key: upstream-complete
    check: equals
    field: status
    value: ready
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
    - key: freshness
      check: age_lt
      field: updatedAt
      value: 2h

job:
  type: glue
  config:
    jobName: gold-revenue-etl
  maxRetries: 2
```

## Top-Level Sections

### `pipeline`

Identity and ownership metadata.

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | string | yes | Unique pipeline identifier (used as DynamoDB partition key) |
| `owner` | string | yes | Team or individual responsible for the pipeline |
| `description` | string | no | Human-readable description |

### `schedule`

Controls when and how often the pipeline is evaluated. See [Schedules](../schedules/) for full details.

### `sla`

Time-based constraints for pipeline completion. See [Schedules](../schedules/) for full details.

### `validation`

Declarative rules that determine pipeline readiness. See [Validation Rules](#validation-rules) below.

### `dryRun`

Enables observation-only mode. When `true`, Interlock evaluates trigger conditions and validation rules against real sensor data but never starts a Step Function execution or triggers any job. All observations are published as EventBridge events (`DRY_RUN_WOULD_TRIGGER`, `DRY_RUN_LATE_DATA`, `DRY_RUN_SLA_PROJECTION`, `DRY_RUN_DRIFT`, `DRY_RUN_COMPLETED`).

| Field | Type | Default | Description |
|---|---|---|---|
| `dryRun` | bool | `false` | Enable dry-run / shadow mode |

Requires `schedule.trigger` (sensor-driven evaluation) and `job.type` to be configured. Calendar exclusions are still honored. Remove `dryRun: true` to switch to live mode — `DRY_RUN#` markers have a 7-day TTL and don't interfere with `TRIGGER#` rows.

See [Alerting](../../reference/alerting/) for the full dry-run event reference.

### `job`

Defines how to start the downstream job when validation passes. See [Triggers](../triggers/) for all 8 supported job types.

## Validation Rules

The `validation` block replaces the old archetype/trait system with a declarative DSL. External processes push sensor data into DynamoDB, and validation rules evaluate that data to determine readiness.

### How It Works

1. External systems write sensor data to the control table (e.g., `PK=PIPELINE#gold-revenue`, `SK=SENSOR#upstream-complete`)
2. The orchestrator Lambda reads sensor values and evaluates each rule against the current data
3. Rules are combined using the `trigger` mode to produce a readiness decision

### Trigger Modes

| Mode | Description |
|---|---|
| `ALL` | All rules must pass (logical AND) |
| `ANY` | At least one rule must pass (logical OR) |

### Rule Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `key` | string | yes | Sensor key to evaluate (matches the sensor record's SK suffix) |
| `check` | string | yes | Comparison operator (see [Supported Checks](#supported-checks)) |
| `field` | string | conditional | Field within the sensor data to compare (required for all checks except `exists`) |
| `value` | any | conditional | Expected value to compare against (required for all checks except `exists`) |

### Supported Checks

| Check | Description | Example |
|---|---|---|
| `exists` | Sensor key exists (any value) | `check: exists` |
| `equals` | Field equals the expected value | `check: equals`, `field: status`, `value: ready` |
| `gt` | Field is greater than the value (numeric) | `check: gt`, `field: count`, `value: 0` |
| `gte` | Field is greater than or equal to the value (numeric) | `check: gte`, `field: count`, `value: 1000` |
| `lt` | Field is less than the value (numeric) | `check: lt`, `field: errorRate`, `value: 5` |
| `lte` | Field is less than or equal to the value (numeric) | `check: lte`, `field: latency`, `value: 100` |
| `age_lt` | Field timestamp is newer than the duration | `check: age_lt`, `field: updatedAt`, `value: 2h` |
| `age_gt` | Field timestamp is older than the duration | `check: age_gt`, `field: createdAt`, `value: 24h` |

### Example: Event-Driven Pipeline

This pipeline triggers when an `orders-landed` sensor appears, regardless of value:

```yaml
validation:
  trigger: "ANY"
  rules:
    - key: orders-landed
      check: exists
    - key: orders-count
      check: gt
      field: count
      value: 0
```

### Example: Multi-Condition Pipeline

This pipeline requires all upstream checks to pass before triggering:

```yaml
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
    - key: freshness
      check: age_lt
      field: updatedAt
      value: 2h
```

## Retry and Rerun Policy

Automatic retries on job failure and rerun limits are configured in the `job` block:

```yaml
job:
  type: glue
  config:
    jobName: gold-revenue-etl
  maxRetries: 2
  maxCodeRetries: 1
  maxDriftReruns: 2
  maxManualReruns: 3
  jobPollWindowSeconds: 3600
```

### Retry Budgets

Interlock maintains separate retry budgets by failure source, preventing one category from consuming another's budget:

| Field | Type | Default | Range | Description |
|---|---|---|---|---|
| `maxRetries` | int | `0` | 0--10 | Retry attempts for transient/unknown job failures |
| `maxCodeRetries` | int* | `1` | 0--3 | Retry attempts for permanent (code/logic) failures. Only used when the trigger runner reports `FailureCategory: PERMANENT` |
| `maxDriftReruns` | int* | `1` | 0--5 | Rerun budget for automatic drift and late-data reruns |
| `maxManualReruns` | int* | `1` | 0--5 | Rerun budget for manual rerun requests |

Fields marked `int*` use pointer semantics: omit to use the default, set to `0` to explicitly disable.

**Failure classification**: when a job fails, the trigger runner may report a `FailureCategory` (`PERMANENT`, `TRANSIENT`, or `TIMEOUT`). Permanent failures (e.g., application code bugs, schema mismatches) use the `maxCodeRetries` budget. Transient failures (e.g., service throttling, temporary network issues) and unclassified failures use the `maxRetries` budget.

### Job Poll Window

| Field | Type | Default | Range | Description |
|---|---|---|---|---|
| `jobPollWindowSeconds` | int* | `3600` (1h) | 60--86400 | Maximum time to poll for job completion before timing out |

When the poll window elapses without a terminal job status, the orchestrator publishes a `JOB_POLL_EXHAUSTED` event, writes a timeout entry to the joblog, and sets the trigger to `FAILED_FINAL`. This prevents unbounded polling when external jobs hang.

### Configuration Validation

The framework validates all retry and timeout fields at config load time. Invalid configs are logged and skipped (fail-open). Validation bounds:

| Field | Valid Range |
|---|---|
| `maxRetries` | 0--10 |
| `maxCodeRetries` | 0--3 |
| `maxDriftReruns` | 0--5 |
| `maxManualReruns` | 0--5 |
| `jobPollWindowSeconds` | 60--86400 (0 = use default) |

## Post-Run Monitoring

Optional post-run rules are evaluated reactively when sensor data arrives after job completion. Unlike pre-trigger validation (which runs in the Step Function), post-run monitoring is entirely event-driven via DynamoDB Streams.

```yaml
postRun:
  rules:
    - key: output-row-count
      check: gte
      field: count
      value: 1000
  driftThreshold: 10
  sensorTimeout: "2h"
```

### Post-Run Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `rules` | list | -- (required) | Validation rules using the same syntax as pre-trigger rules. Uses `ALL` mode. |
| `driftThreshold` | float* | `0` | Minimum sensor count change to trigger a drift rerun. Set to `0` for any change. |
| `sensorTimeout` | string | `"2h"` | Grace period after job completion before the watchdog alerts on missing sensors. |

### How It Works

1. When a job completes successfully, the orchestrator captures a date-scoped baseline of all sensor values.
2. When a sensor matching a `postRun.rules[].key` arrives via DynamoDB Stream:
   - **Trigger RUNNING**: drift is logged as `POST_RUN_DRIFT_INFLIGHT` (informational, no rerun)
   - **Trigger COMPLETED**: sensor count is compared against the baseline. Drift above `driftThreshold` triggers a rerun via the circuit breaker
3. If no post-run sensor arrives within `sensorTimeout`, the watchdog publishes `POST_RUN_SENSOR_MISSING`.

## Pushing Sensor Data

The framework reads sensor data from DynamoDB but does not write it. External processes are responsible for pushing sensor values. Write a sensor record to the control table:

| Attribute | Value |
|---|---|
| `PK` | `PIPELINE#{pipeline-id}` |
| `SK` | `SENSOR#{key}` |
| Sensor fields | Any fields referenced by validation rules (e.g., `status`, `count`, `updatedAt`) |

When a sensor record is written, the DynamoDB stream triggers the stream-router Lambda, which starts the Step Functions execution for that pipeline.
