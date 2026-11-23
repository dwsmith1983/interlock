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
| `owner` | string | no | Team or individual responsible for the pipeline |
| `description` | string | no | Human-readable description |

The `id` is 1 to 31 characters of `A-Z`, `a-z`, `0-9`, `_` and `-`. Step Functions execution names and EventBridge Scheduler names are built from it, and at that length every one fits its AWS limit as is (the 64-character Scheduler name is the tightest). The file name without `.yaml` or `.yml` must equal the `id`: the Terraform module keys the `CONFIG` row by the file name and fails `terraform plan` when they differ, and a `CONFIG` row whose `PIPELINE#<id>` key differs from its `pipeline.id` is rejected at load.

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
| `age_lt` | Field timestamp ([forms](#pushing-sensor-data)) is newer than the duration | `check: age_lt`, `field: updatedAt`, `value: 2h` |

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

### Rerun Requests for a Running Date

A `RERUN_REQUEST` for a date whose trigger row is still `RUNNING` is rejected with `RERUN_REJECTED` (`a run is in progress for <date>`), whatever its reason (manual, `late-data` or `data-drift`), and spends no budget. The calendar and the budget are checked first, so an excluded date or a spent budget gives those verdicts instead. A run whose execution died stays `RUNNING` until the watchdog's [stale-trigger check](../../architecture/watchdog/#stale-trigger-detection) publishes `SFN_TIMEOUT` and sets it to `FAILED_FINAL`. That happens once the row's stale deadline (`staleAt`) has passed, counted from the run's latest attempt (each retry refreshes it): `sfn_timeout_seconds` plus 30 minutes, 4h30m by default, plus up to one `watchdog_schedule` interval. Nothing shortens the wait. Request the rerun again once the run ends, or after the `SFN_TIMEOUT`.

### A Rerun That Cannot Start

When an accepted rerun's execution fails to start, the date keeps the status it had (`COMPLETED` or `FAILED_FINAL`), no rerun budget is spent, and nothing is published. The stream retries the request up to 3 times (`maximum_retry_attempts`) under the same execution name, so a retry can start the rerun; after the last one the request goes to the stream-router's dead-letter queue and has to be made again.

If the rerun record cannot be deleted, the date still keeps its status, but the record stays and counts against the budget: with a budget of 1 the retry is rejected with `RERUN_REJECTED` as over the limit.

The date is left `RUNNING` when the status restore fails, when the stream-router crashes between the lock reset and the start, or when the first start reached Step Functions but reported an error and that execution finished before the retry arrived (the retry counts it as started). Retries of the first two are rejected with `RERUN_REJECTED`. The date stays `RUNNING` until the watchdog's [stale-trigger check](../../architecture/watchdog/#stale-trigger-detection) publishes `SFN_TIMEOUT` and sets it to `FAILED_FINAL`, once the row's stale deadline (`staleAt`) has passed: `sfn_timeout_seconds` plus 30 minutes from the latest attempt, 4h30m by default, plus up to one `watchdog_schedule` interval. Request the rerun again after the `SFN_TIMEOUT`. When a crash came after the rerun record was written, that rerun counts against the budget.

### Job Poll Window

| Field | Type | Default | Range | Description |
|---|---|---|---|---|
| `jobPollWindowSeconds` | int* | `3600` (1h) | 60--86400 | Maximum time to poll for job completion before timing out |

When the poll window elapses without a terminal job status, the orchestrator writes a `timeout` row for the run to the joblog (published as `JOB_FAILED` with error "job poll window exhausted") and handles it like any other timeout: it retries while `maxRetries` lasts, then sets the trigger to `FAILED_FINAL` and publishes `RETRY_EXHAUSTED`. This prevents unbounded polling when external jobs hang.

### Configuration Validation

The framework validates the pipeline ID and all retry and timeout fields at config load time. A `CONFIG` row that does not load, because it is malformed (such as `"maxRetries": "3"`) or outside the bounds below, is skipped by the stream-router, the watchdog and CompleteTrigger, logged as a warning and reported by the watchdog as `CONFIG_INVALID`; the other pipelines keep running. An execution that is already running reads its config directly at each step, so a config that breaks mid-run does not stop it at once: a config that no longer parses fails the step that reads it (evaluation retries until its window closes, the trigger step fails after its retries), one that parses but fails validation is still used by those steps, and a failed job is not retried (CompleteTrigger finds no valid config and sets `FAILED_FINAL`). Validation bounds:

| Field | Valid Range |
|---|---|
| `pipeline.id` | 1--31 characters of `A-Za-z0-9_-`, equal to the PK's ID |
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
  watchWindow: "6h"
```

### Post-Run Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `rules` | list | -- | Validation rules using the same syntax as pre-trigger rules. Uses `ALL` mode. Required for drift and the post-run checks; a `postRun` block with only `watchWindow` is valid. |
| `driftThreshold` | float* | `0` | Minimum change in `driftField` to trigger a drift rerun. Set to `0` for any change. |
| `driftField` | string | `"sensor_count"` | Sensor field compared with the baseline for drift. |
| `sensorTimeout` | string | `"2h"` | Grace period after job completion before the watchdog alerts on missing sensors. It must be a positive Go duration and, with `rules`, at most the post-run watch window (`postRun.watchWindow`, default `"4h"`). |
| `watchWindow` | string | `"4h"` | How long after a run completes its date is watched for late data, drift and the post-run checks (see [Watch window](#watch-window)). A Go duration above 0 and at most `720h`. |
| `evaluation` | object | -- | Accepted but not read. |

### How It Works

1. When a job completes successfully, the orchestrator captures a date-scoped baseline of all sensor values.
2. When a sensor matching a `postRun.rules[].key` arrives via DynamoDB Stream:
   - **Trigger RUNNING**: drift is logged as `POST_RUN_DRIFT_INFLIGHT` (informational, no rerun)
   - **Trigger COMPLETED**, inside the [watch window](#watch-window): sensor count is compared against the baseline. Drift above `driftThreshold` writes a `data-drift` rerun request, which skips the circuit breaker; otherwise the rules are evaluated (`POST_RUN_PASSED` or `POST_RUN_FAILED`). A sensor first written after completion (such as a new per-period key) has no baseline value: the rules are evaluated, and that first value becomes its baseline for later writes. Only the rule's own key (or its `#<date>` form) seeds a baseline, and only from a write carrying `driftField` as a number
3. If no post-run sensor arrives within `sensorTimeout` of `completedAt`, the watchdog publishes `POST_RUN_SENSOR_MISSING`. A sensor has arrived when it exists and either differs from its baseline value or did not exist at completion; a sensor rewritten with identical values has not.

### Watch window

A completed date is watched for late data, drift and the post-run checks from the run's completion until `watchWindow` later (default `4h`). complete-trigger records the completion as `completedAt` on the trigger row, and a rerun's completion starts a new window. At the limit the watch has ended.

- Inside the window, a trigger sensor written again for a date whose last run succeeded is late data: `LATE_DATA_ARRIVAL` and a `late-data` `RERUN_REQUEST`. Job rows about rerun requests and late data do not count as a run. Once the rerun budget is spent, the late-data event and the rerun rejection (`RERUN_REJECTED`) repeat for each late trigger-sensor write inside the window.
- Inside the window, a post-run sensor is compared with the baseline and checked against `rules`: `POST_RUN_DRIFT` and a `data-drift` `RERUN_REQUEST`, or `POST_RUN_PASSED` / `POST_RUN_FAILED`. A sensor the baseline has no value for is checked against `rules`, and its value becomes the baseline that later writes are compared with. A seed never replaces a baseline value written first by another write; if the baseline then has no value for the rule either (the row was deleted, the capture at completion won without this sensor, or another rule's first write, handled at the same time on a different shard, created the row first), that write is neither seeded nor compared, and the sensor's next write that carries `driftField` seeds it.
- After it, a trigger or post-run sensor write is ignored with no event and no rerun request. A `COMPLETED` row without `completedAt` is not watched. Inflight drift (`POST_RUN_DRIFT_INFLIGHT`, trigger `RUNNING`) is not bounded by the window.
- An automatic `RERUN_REQUEST` (reason `late-data` or `data-drift`) is checked again when the stream-router handles it: for a `COMPLETED` date outside the window it is skipped with no rerun, row or event. A request written just inside the window but handled at or after the limit is skipped too. A manual `RERUN_REQUEST` (any other reason) still runs. Automatic requests skip the circuit breaker, since the stream-router wrote them on a change it had already seen; manual ones go through it.
- The window is independent of `schedule.trigger.cutoff`: a late-data rerun inside the window runs after the cutoff, and a sensor after the window and before the cutoff starts nothing.
- A sensor is judged by when the stream-router handles it, not when it was written.
- Nothing is published when a window ends.
- `POST_RUN_SENSOR_MISSING` counts `sensorTimeout` from `completedAt`, which is why `sensorTimeout` may not exceed the window. It compares each post-run sensor's whole stored record with the baseline, so a change in any field, a timestamp such as `updatedAt` included, counts; when the write happened is not otherwise used.

Dry-run pipelines are not bounded by the window; their late data and drift last as long as the dry-run marker (7-day TTL).

## Pushing Sensor Data

The framework reads sensor data from DynamoDB but does not write it. External processes are responsible for pushing sensor values. Write a sensor record to the control table:

| Attribute | Value |
|---|---|
| `PK` | `PIPELINE#{pipeline-id}` |
| `SK` | `SENSOR#{key}` |
| `data` | A map of the fields referenced by validation rules (e.g., `status`, `count`, `updatedAt`). Rules read only this map, not top-level attributes. |

`age_lt` and the rerun circuit breaker read a timestamp in `data`, such as `updatedAt`, as RFC3339 (`2026-03-10T09:00:00Z`) or as epoch seconds or milliseconds, given as a number or a string of digits. A value below 10^12 is read as seconds. `age_lt` fails on a value in none of these forms. The rerun circuit breaker, which checks manual requests only, compares each sensor's `updatedAt` with the last successful run and ignores one it cannot read; when no sensor has a readable `updatedAt`, it lets the rerun through.

When a sensor record is written, the DynamoDB stream triggers the stream-router Lambda, which starts the Step Functions execution for that pipeline.
