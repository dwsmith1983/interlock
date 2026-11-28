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
    cutoff: "12:00"
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

Enables observation-only mode. When `true`, Interlock evaluates trigger conditions and validation rules against real sensor data but never starts a Step Function execution or triggers any job. All observations are published as EventBridge events (`DRY_RUN_WOULD_TRIGGER`, `DRY_RUN_LATE_DATA`, `DRY_RUN_SLA_PROJECTION`, `DRY_RUN_DRIFT`, `DRY_RUN_COMPLETED`, `DRY_RUN_WOULD_RERUN`, `DRY_RUN_RERUN_REJECTED`, `DRY_RUN_WOULD_RETRY`, `DRY_RUN_RETRY_EXHAUSTED`).

| Field | Type | Default | Description |
|---|---|---|---|
| `dryRun` | bool | `false` | Enable dry-run / shadow mode |

Requires `job.type` to be configured. Calendar exclusions are still honored. Remove `dryRun: true` to switch to live mode; `DRY_RUN#` markers have a 7-day TTL and don't interfere with `TRIGGER#` rows.

See [Alerting](../../reference/alerting/) for the full dry-run event reference.

### `job`

Defines how to start the downstream job when validation passes. See [Triggers](../triggers/) for all 8 supported job types.

## Validation Rules

The `validation` block is a declarative list of rules. External processes push sensor data into DynamoDB, and validation rules evaluate that data to determine readiness.

### How It Works

1. External systems write sensor data to the control table (e.g., `PK=PIPELINE#gold-revenue`, `SK=SENSOR#upstream-complete`)
2. The orchestrator Lambda reads sensor values and evaluates each rule against the current data
3. Rules are combined using the `trigger` mode to produce a readiness decision

### Trigger Modes

| Mode | Description |
|---|---|
| `ALL` | All rules must pass (logical AND). The default when `trigger` is empty. With no rules, the job starts as soon as the trigger condition is met |
| `ANY` | At least one rule must pass (logical OR). Needs at least one rule |

The mode is read without regard to case (`any` is `ANY`). Any other value, or `ANY` with no rules, is rejected as `CONFIG_INVALID`.

### Rule Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `key` | string | yes | Sensor key to evaluate (matches the sensor record's SK suffix, or a per-period `<key>#<period>` row). It may not contain `#`, and may not be one of the names Interlock uses for its own sensor rows: `postrun-baseline`, `postrun-check`, `irregular-missed-check`, `schedule-missed-check`, `first-sensor-arrival`, `relative-sla-warning-check`, `relative-sla-breach-check`, `manual-rerun-alert-late-data`, `manual-rerun-alert-data-drift` |
| `check` | string | yes | Comparison operator, one of those in [Supported Checks](#supported-checks) |
| `field` | string | conditional | Field within the sensor data to compare (required for all checks except `exists`) |
| `value` | any | conditional | Expected value to compare against (required for all checks except `exists`; see [Supported Checks](#supported-checks) for what each check takes) |

A rule that can never pass (an unknown `check` such as `eq`, a missing `key` or `field`, or a `value` its check cannot use) is rejected as `CONFIG_INVALID` when the config loads. The same applies to `schedule.trigger` and `postRun.rules`.

### Supported Checks

| Check | Description | Example |
|---|---|---|
| `exists` | Sensor key exists (any value); `field` and `value` are ignored | `check: exists` |
| `equals` | Field equals the expected value, compared as text; `value` is required | `check: equals`, `field: status`, `value: ready` |
| `gt` | Field is greater than the value (numeric) | `check: gt`, `field: count`, `value: 0` |
| `gte` | Field is greater than or equal to the value (numeric) | `check: gte`, `field: count`, `value: 1000` |
| `lt` | Field is less than the value (numeric) | `check: lt`, `field: errorRate`, `value: 5` |
| `lte` | Field is less than or equal to the value (numeric) | `check: lte`, `field: latency`, `value: 100` |
| `age_lt` | Field timestamp ([forms](#pushing-sensor-data)) is newer than the duration | `check: age_lt`, `field: updatedAt`, `value: 2h` |

For `gt`, `gte`, `lt` and `lte`, `value` must be a finite number or a string holding one (`1000` or `"1000"`). For `age_lt` it must be a positive Go duration such as `2h` or `90m`: `1d` and a bare number of seconds are not durations.

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
  maxAutoRerunAge: "120h"
  jobPollWindowSeconds: 3600
```

### Retry Budgets

Interlock maintains separate retry budgets by failure source, preventing one category from consuming another's budget:

| Field | Type | Default | Range | Description |
|---|---|---|---|---|
| `maxRetries` | int | `0` | 0--10 | Retry attempts for transient/unknown job failures. Each waits before it starts (see Retry delay) |
| `maxCodeRetries` | int* | `1` | 0--3 | Retry attempts for permanent (code/logic) failures. Only used when the trigger runner reports `FailureCategory: PERMANENT`. Each waits before it starts (see Retry delay) |
| `maxDriftReruns` | int* | `1` | 0--5 | Rerun budget for automatic drift and late-data reruns |
| `maxManualReruns` | int* | `1` | 0--5 | Rerun budget for manual rerun requests, per date. Requests are counted by their `reason`: each reason other than `late-data` and `data-drift` has a budget of this size, so a new reason string gets a new budget. Use one reason, such as the default `manual`, to keep the limit |
| `maxAutoRerunAge` | string | `"120h"` | above 0, at most `720h` | How long after a run completes its date may still be rerun automatically for late data or drift. A Go duration. Automatic reruns stop at the earlier of this and the end of the [watch window](#watch-window); the watch itself goes on for the whole window, and late data or drift after the age asks for a manual rerun (`MANUAL_RERUN_REQUIRED`). Job-failure retries and manual requests are not limited by it |

Fields marked `int*` use pointer semantics: omit to use the default, set to `0` to explicitly disable.

**Failure classification**: when a job fails, the trigger runner may report a `FailureCategory` (`PERMANENT`, `TRANSIENT`, or `TIMEOUT`). Permanent failures (e.g., application code bugs, schema mismatches) use the `maxCodeRetries` budget. Transient failures (e.g., service throttling, temporary network issues), timeouts and unclassified failures use the `maxRetries` budget. A fourth category, `OUTCOME_UNKNOWN`, means the job may still be running: Interlock writes it when a [`lambda`](../triggers/#lambda) job is still running at the end of the orchestrator's wait. It is never retried, under either budget: the date ends `FAILED_FINAL`, no `RETRY_EXHAUSTED` is published, and the row's `JOB_FAILED` is the alert.

**Retry delay**: a retry does not start at once. The execution for retry N waits before it evaluates: 60 seconds, doubling each time, to at most 15 minutes (1, 2, 4, 8, 15, 15, ... minutes). N counts every retry since the chain's first execution (a first run, a requested rerun or a watchdog start), whichever budget allowed it, so a permanent failure waits the same way. The date stays `RUNNING` during the wait: SLA alerts due then go out, and a `RERUN_REQUEST` is rejected as in progress. The delays are not randomised. Ten retries wait 105 minutes in total, on top of each attempt's own run time, so check `sla.deadline` against the budget you set.

A pipeline switched to dry-run, or a date added to the exclusions, during the wait does not stop the retry: it still launches the job, as during the evaluation window. A config deleted during the wait ends the date with `VALIDATION_EXHAUSTED` once the evaluation window closes.

**Launch failures**: a job that could not be started at all (for example Glue `ConcurrentRunsExceededException`) is retried by the Trigger state (`trigger_max_attempts`), not under these budgets. When those retries run out the orchestrator publishes `RETRY_EXHAUSTED` and the date ends `FAILED_FINAL`. Nothing starts it again on its own: submit a `RERUN_REQUEST` ([row format](../../guides/shared-deployment/#writing-sensor-data-and-rerun-requests)) to run it, which spends `maxManualReruns`. A rerun of a `COMPLETED` date whose launch runs out of retries ends the date `FAILED_FINAL` the same way, so its post-run monitoring stops; the earlier run's output stands.

### Rerun Requests for a Running Date

A `RERUN_REQUEST` ([row format](../../guides/shared-deployment/#writing-sensor-data-and-rerun-requests)) for a date whose trigger row is still `RUNNING` is rejected with `RERUN_REJECTED` (`a run is in progress for <date>`), whatever its reason (manual, `late-data` or `data-drift`), and spends no budget. The calendar and the budget are checked first, so an excluded date or a spent budget gives those verdicts instead. A run whose execution ended without closing its row (it timed out or was stopped, or the step that would have closed the row failed) stays `RUNNING` until the watchdog's [stale-trigger check](../../architecture/watchdog/#stale-trigger-detection) publishes `SFN_TIMEOUT` and sets it to `FAILED_FINAL`. That happens once the row's stale deadline (`staleAt`) has passed, counted from the run's latest attempt (each retry refreshes it): `sfn_timeout_seconds` plus 30 minutes, 4h30m by default, plus up to one `watchdog_schedule` interval. Nothing shortens the wait. Request the rerun again once the run ends, or after the `SFN_TIMEOUT`.

### A Rerun That Cannot Start

When an accepted rerun's execution fails to start, the date keeps the status it had (`COMPLETED` or `FAILED_FINAL`), no rerun budget is spent, and nothing is published. The stream retries the request up to 3 times (`maximum_retry_attempts`) under the same execution name, so a retry can start the rerun; after the last one the request goes to the stream-router's dead-letter queue and has to be made again, with a new `requestedAt` ([row format](../../guides/shared-deployment/#writing-sensor-data-and-rerun-requests)).

If the rerun record cannot be deleted, the date still keeps its status, but the record stays and counts against the budget: with a budget of 1 the retry is rejected with `RERUN_REJECTED` as over the limit.

The date is left `RUNNING` when the status restore fails, when the stream-router crashes between the lock reset and the start, or when the first start reached Step Functions but reported an error and that execution finished before the retry arrived (the retry counts it as started). Retries of the first two are rejected with `RERUN_REJECTED`. The date stays `RUNNING` until the watchdog's [stale-trigger check](../../architecture/watchdog/#stale-trigger-detection) publishes `SFN_TIMEOUT` and sets it to `FAILED_FINAL`, once the row's stale deadline (`staleAt`) has passed: `sfn_timeout_seconds` plus 30 minutes from the latest attempt, 4h30m by default, plus up to one `watchdog_schedule` interval. Request the rerun again after the `SFN_TIMEOUT`. When a crash came after the rerun record was written, that rerun counts against the budget.

### Job Poll Window

| Field | Type | Default | Range | Description |
|---|---|---|---|---|
| `jobPollWindowSeconds` | int* | `3600` (1h) | 60--86400 | Maximum time to poll for job completion before timing out. It plus `schedule.evaluation.window` plus the longest [retry delay](#retry-budgets) the pipeline can reach (by the larger of `maxRetries` and `maxCodeRetries`, default 1: 1, 2, 4 or 8 minutes for 1 to 4 retries, 15 minutes from 5 up, nothing when both are 0) may not exceed `sfn_timeout_seconds` (default 14400) |

When the poll window elapses without a terminal job status, the orchestrator writes a `timeout` row for the run to the joblog (published as `JOB_FAILED` with error "job poll window exhausted") and handles it like any other timeout: it retries while `maxRetries` lasts, then sets the trigger to `FAILED_FINAL` and publishes `RETRY_EXHAUSTED`. This prevents unbounded polling when external jobs hang.

### Configuration Validation

The framework validates the pipeline ID and all retry and timeout fields at config load time. A `CONFIG` row that does not load, because it is malformed (such as `"maxRetries": "3"`) or outside the bounds below, is skipped by the stream-router, the watchdog and CompleteTrigger, logged as a warning and reported by the watchdog as `CONFIG_INVALID`; the other pipelines keep running. An execution that is already running reads its config directly at each step, so a config that breaks mid-run does not stop it at once: a config that no longer parses fails the step that reads it (evaluation retries until its window closes, the trigger step fails after its retries). One that parses but fails validation is still used by evaluation, but the trigger step validates it again and fails, so once its retries run out the date ends `FAILED_FINAL` with `RETRY_EXHAUSTED` and the job is not launched. A failed job is not retried: CompleteTrigger finds no valid config and sets `FAILED_FINAL`. Validation bounds:

| Field | Valid Range |
|---|---|
| `pipeline.id` | 1--31 characters of `A-Za-z0-9_-`, equal to the PK's ID |
| `maxRetries` | 0--10 |
| `maxCodeRetries` | 0--3 |
| `maxDriftReruns` | 0--5 |
| `maxManualReruns` | 0--5 |
| `maxAutoRerunAge` | Go duration above 0, at most `720h` (any value with any `watchWindow`) |
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
  driftField: count
  driftThreshold: 10
  sensorTimeout: "2h"
  watchWindow: "6h"
```

Here a write of `output-row-count` whose `count` differs from the baseline by more than 10 is drift. Set `driftField` to the field your post-run sensor carries: the default, `sensor_count`, finds no drift in a sensor that has no such field.

### Post-Run Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `rules` | list | -- | Validation rules using the same syntax and [checks](#rule-fields) as pre-trigger rules; a rule that can never pass is rejected as `CONFIG_INVALID`. Uses `ALL` mode. A sensor belongs to a rule when its key is the rule's `key` or `<key>#<period>`. Required for drift and the post-run checks; a `postRun` block with only `watchWindow` is valid. |
| `driftThreshold` | float* | `0` | A change in `driftField`, up or down, larger than this is drift; a change equal to it is not. `0` means any change. |
| `driftField` | string | `"sensor_count"` | Field in the sensor's `data` compared with the baseline for drift. Both values must be numbers, or strings holding one; otherwise no drift is found. |
| `sensorTimeout` | string | `"2h"` | Grace period after job completion before the watchdog alerts on missing sensors. It must be a positive Go duration and, with `rules`, at most the post-run watch window (`postRun.watchWindow`, default `"4h"`). |
| `watchWindow` | string | `"4h"` | How long after a run completes its date is watched for late data, drift and the post-run checks (see [Watch window](#watch-window)). A Go duration above 0 and at most `720h`. Automatic reruns end separately, at `job.maxAutoRerunAge` (default `120h`) when that comes first. |
| `evaluation` | object | -- | Accepted but not read. |

### How It Works

1. When a job completes successfully, the orchestrator captures a date-scoped baseline of the sensors that `postRun.rules` name (a rule's key, or its per-period key for the run's date). Other sensors are not in it, and with none of them present no baseline is written. A post-run sensor written under a per-period key needs `date` (and `hour` for hourly runs) in its data: its arrival is recorded for the date the write's data names, and a key naming another period records nothing.
2. When a sensor matching a `postRun.rules[].key` arrives via DynamoDB Stream:
   - **Trigger RUNNING**: a write while the job runs is not compared. The baseline is captured when the run completes, and later writes are compared with it.
   - **Trigger COMPLETED**, inside the [watch window](#watch-window): the `driftField` value is compared against the baseline. Drift above `driftThreshold` writes a `data-drift` rerun request, which skips the circuit breaker, until `job.maxAutoRerunAge` (past it, `MANUAL_RERUN_REQUIRED`); otherwise the rules are evaluated (`POST_RUN_PASSED` or `POST_RUN_FAILED`). A sensor first written after completion (such as a new per-period key) has no baseline value: the rules are evaluated, and that first value becomes its baseline for later writes. Only the rule's own key (or its `#<date>` form) seeds a baseline, and only from a write carrying `driftField` as a number
3. If no post-run sensor arrives within `sensorTimeout` of `completedAt`, the watchdog publishes `POST_RUN_SENSOR_MISSING`. The watchdog checks only the daily run date that equals the current UTC date (`YYYY-MM-DD`). A run for an earlier date (such as one that processes yesterday's data today), an hourly run (`YYYY-MM-DDThh`), and a run whose timeout ends after UTC midnight get no `POST_RUN_SENSOR_MISSING`. A sensor has arrived when it exists and either differs from its baseline value or did not exist at completion; a sensor rewritten with identical values has not. Once the stream-router has handled a write of a post-run rule's sensor for the date (its own key, or its key with that date or hour) while the trigger row was `COMPLETED`, the date has the `arrived` marker and counts as arrived, so a later rerun of that date gets no `POST_RUN_SENSOR_MISSING`.

### Watch window

A completed date is watched for late data, drift and the post-run checks from the run's completion until `watchWindow` later (default `4h`). complete-trigger records the completion as `completedAt` on the trigger row, and a rerun's completion starts a new window and restarts `job.maxAutoRerunAge` too, since both count from `completedAt`: a manual rerun ten days after the first run opens another `120h` of automatic reruns by default. At the limit the watch has ended.

- Inside the window, a trigger sensor written again for a date whose last run succeeded is late data: `LATE_DATA_ARRIVAL` and, until `job.maxAutoRerunAge` (below), a `late-data` `RERUN_REQUEST`. A `rerun-accepted` job row does not count as a run. Once the rerun budget is spent, the late-data event and the rerun rejection (`RERUN_REJECTED`) repeat for each late trigger-sensor write inside the window until `job.maxAutoRerunAge`; past the age no request is written, so `LATE_DATA_ARRIVAL` repeats with `MANUAL_RERUN_REQUIRED` (once per completion) and then `MANUAL_RERUN_STILL_REQUIRED`. Late data and rejected reruns are recorded in the events table only: late data and rejected reruns write no job row, so the job log does not grow with late writes. `late-data-arrival` and `rerun-rejected` rows written by earlier versions are still skipped when a run's outcome is read, until their 30-day ttl removes them.
- Inside the window, a post-run sensor is compared with the baseline and checked against `rules`: until `job.maxAutoRerunAge`, drift gives `POST_RUN_DRIFT` and a `data-drift` `RERUN_REQUEST`; otherwise `POST_RUN_PASSED` / `POST_RUN_FAILED`. A sensor the baseline has no value for is checked against `rules`, and its value becomes the baseline that later writes are compared with. A seed never replaces a baseline value written first by another write; if the baseline then has no value for the rule either (the row was deleted, the capture at completion won without this sensor, or another rule's first write, handled at the same time on a different shard, created the row first), that write is neither seeded nor compared, and the sensor's next write that carries `driftField` seeds it.
- Watching and automatic reruns end separately. A `late-data` or `data-drift` `RERUN_REQUEST` is written only before `completedAt` plus `job.maxAutoRerunAge` (default `120h`). With a window longer than that age, late data after the age still publishes `LATE_DATA_ARRIVAL`, but the stream-router writes no request, so a pending manual request is left in place. Late data and drift after the age publish [`MANUAL_RERUN_REQUIRED`](../../reference/alerting/#rerun-events) with the job, the run and the write (for drift its numbers and baseline too), once per completion and reason, then `MANUAL_RERUN_STILL_REQUIRED`. Drift after the age publishes no `POST_RUN_DRIFT`: with no rerun its baseline stays as it is, so every later write would repeat it. To rerun such a date, write a manual `RERUN_REQUEST`. The default window (`4h`) ends well before the default age, so the age matters only for a window over `120h` or an age set below the window.
- After the window no rerun request is written and no `LATE_DATA_ARRIVAL`, `POST_RUN_*` event or post-run rule is published or evaluated. A trigger sensor write for a date whose last run succeeded, and a post-run sensor write that drifted from the date's baseline, publish only `MANUAL_RERUN_REQUIRED` (blocked by `postRun.watchWindow`), then `MANUAL_RERUN_STILL_REQUIRED`. A post-run write never seeds or rewrites the baseline after the window; once the baseline row has expired (31 days after it was written) drift is not detected and nothing is recorded, and once the run's job log row has expired (30 days after it was written) the last run is not known to have succeeded, so neither late data nor drift is recorded. A `COMPLETED` row without `completedAt` is not watched.
- An automatic `RERUN_REQUEST` (reason `late-data` or `data-drift`) is checked again when the stream-router handles it: for a `COMPLETED` date outside the window, past `job.maxAutoRerunAge`, or with no `completedAt`, it is skipped with no rerun or row, and the log's `endedBy` says why: `postRun.watchWindow`, `job.maxAutoRerunAge` or `no-completion-time`. Unless the date has no `completedAt`, its last run did not succeed or the calendar excludes it, [`MANUAL_RERUN_REQUIRED`](../../reference/alerting/#rerun-events) (`source: rerun-request`) then asks for a manual rerun, once per completion and reason. A request written just inside the limit but handled at or after it is skipped too. A manual `RERUN_REQUEST` (any other reason) still runs. Automatic requests skip the circuit breaker, since the stream-router wrote them on a change it had already seen; manual ones go through it.
- The window is independent of `schedule.trigger.cutoff`: a late-data rerun inside the window runs after the cutoff, and a sensor after the window and before the cutoff starts nothing.
- A sensor is judged by when the stream-router handles it, not when it was written.
- Nothing is published when a window ends.
- `POST_RUN_SENSOR_MISSING` counts `sensorTimeout` from `completedAt`, which is why `sensorTimeout` may not exceed the window. It is checked only for the daily run date equal to the current UTC date (see step 3 of [Post-Run Monitoring](#post-run-monitoring)). It compares each post-run sensor's whole stored record with the baseline, so a change in any field, a timestamp such as `updatedAt` included, counts; when the write happened is not otherwise used. A date whose post-run sensor the stream-router saw after completion is not checked again, reruns included (step 3).

Dry-run pipelines are not bounded by the window; their late data and drift last as long as the dry-run marker (7-day TTL).

## Pushing Sensor Data

The framework reads sensor data from DynamoDB but does not write it. External processes are responsible for pushing sensor values. Write a sensor record to the control table:

| Attribute | Value |
|---|---|
| `PK` | `PIPELINE#{pipeline-id}` |
| `SK` | `SENSOR#{key}` |
| `data` | A map of the fields referenced by validation rules (e.g., `status`, `count`, `updatedAt`). Rules read only this map, not top-level attributes. |

The sensor key decides which trigger or post-run rule a write counts for: it matches a configured `key` when it is that key or the key followed by `#` and a period, such as `orders#2026-03-10`, `orders#20260310` or `orders#2026-03-10T07`. A key that only begins with it is a different sensor: `orders-count` is not the `orders` sensor. When rules are evaluated for a run, a rule reads the sensor `<key>`, or `<key>#<period>` for the run's own date, which the write's `data` names in `date` (and `hour` for an hourly run). Don't put `#` in a configured key.

`age_lt` and the rerun circuit breaker read a timestamp in `data`, such as `updatedAt`, as RFC3339 (`2026-03-10T09:00:00Z`) or as epoch seconds or milliseconds, given as a number or a string of digits. A value below 10^12 is read as seconds. `age_lt` fails on a value in none of these forms. The rerun circuit breaker, which checks manual requests only, compares each sensor's `updatedAt` with the last successful run and ignores one it cannot read; when no sensor has a readable `updatedAt`, it lets the rerun through.

### Run Date

A trigger or post-run sensor write names its run date in `data`:

| Field | Form | Example |
|---|---|---|
| `date` | string, `YYYY-MM-DD` or `YYYYMMDD` | `"2026-03-10"`, `"20260310"` |
| `hour` | string, two digits from `00` to `23`; optional | `"07"` |

With `date` alone the run date is daily (`2026-03-10`); with `date` and `hour` it is hourly (`2026-03-10T07`). A bad value is not reported. A missing `date`, a `date` in another form, or one stored as a number gives the current UTC date. An `hour` in another form (`"7"`, `"24"`) or stored as a number is dropped, so the run date is the daily one. The period in a per-period key (`orders#2026-03-10`) is not read for this, so set `date` there too. Check the values your writer sends. In the example below, put the date your data is for in place of `2026-03-10`: a trigger sensor processed after that date's [cutoff](../schedules/#cutoff) starts nothing.

```sh
aws dynamodb put-item --table-name production-interlock-control --item '{
  "PK": {"S": "PIPELINE#gold-revenue"},
  "SK": {"S": "SENSOR#upstream-complete"},
  "data": {"M": {
    "status": {"S": "ready"},
    "date": {"S": "2026-03-10"},
    "updatedAt": {"S": "2026-03-10T07:55:00Z"}
  }}
}'
```

For an hourly run, add `"hour": {"S": "07"}` to `data`. Write numbers as `N`, such as `"count": {"N": "15432"}`. Rerun requests and expected run dates use other rows; see [Writing Sensor Data and Rerun Requests](../../guides/shared-deployment/#writing-sensor-data-and-rerun-requests).

### What a Write Starts

Each sensor write that changes the item reaches the stream-router Lambda through the DynamoDB stream (a `PutItem` that changes nothing emits no stream record). Only a write of the trigger sensor (`schedule.trigger.key`) starts a Step Functions execution, and only when all of these hold:

- the write passes the trigger condition;
- the current day in `schedule.timezone` (UTC when unset) is not [excluded](../schedules/#calendar-exclusions);
- the pipeline is not in dry run;
- the run date's [cutoff](../schedules/#cutoff) has not passed;
- the run date has no trigger row yet, so no run holds it.

A trigger sensor write for a date that already has a trigger row starts nothing; for a date whose last run succeeded it can be [late data](#watch-window). A write of a post-run sensor goes to [post-run monitoring](#post-run-monitoring). A write of a sensor that only validation rules read starts nothing: the running evaluation reads it at its next poll. The watchdog also checks the stored trigger sensors on each pass and starts a run for one that passes under the same conditions (the run date must also have no finished job). So a stored trigger sensor that passes can start its date's run on a later pass, or start it again after a [released evaluation](../schedules/#cutoff).
