---
title: Schedules & SLA
weight: 2
description: Cron schedules, timezone handling, evaluation windows, trigger conditions, and SLA deadlines.
---

## Schedule Configuration

The `schedule` block controls when and how the pipeline evaluates readiness. Schedules use cron expressions and support evaluation windows with configurable polling intervals.

```yaml
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
```

### Schedule Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `cron` | string | no | Five-field cron expression (minute, hour, day of month, month, day of week), evaluated in `timezone` |
| `timezone` | string | no | IANA time zone name (for example `America/New_York`) for cron and deadline interpretation. Empty means `UTC`. An unknown name, or `Local`, rejects the config as `CONFIG_INVALID` |
| `trigger` | object | no | Condition that activates the evaluation loop (see below) |
| `evaluation` | object | no | Window and interval settings for the evaluation loop |
| `exclude` | object | no | Dates the pipeline does not run (see [Calendar Exclusions](#calendar-exclusions)) |
| `include` | object | no | `dates` (`YYYY-MM-DD` list) the pipeline is expected to run on; the watchdog publishes `IRREGULAR_SCHEDULE_MISSED` for a date with no trigger row once that date has begun in `timezone` (UTC when unset). It does not stop runs on other dates. Not allowed with `cron` |
| `time` | string | no | `HH:MM` in `timezone`: on an `include` date, the missed-date check waits until this time. Without it the check fires at local midnight of the date, so set `time` to avoid an alert before the run is due. Not allowed with `cron` |

### Trigger Condition

The `trigger` inside `schedule` defines the condition that activates the pipeline evaluation loop. This is separate from the `validation` rules that determine readiness.

```yaml
schedule:
  trigger:
    key: upstream-complete
    check: equals
    field: status
    value: ready
```

When the trigger condition is met (e.g., an upstream pipeline signals completion), the evaluation loop begins. The loop then evaluates the full set of `validation` rules at the configured interval.

| Field | Type | Description |
|---|---|---|
| `key` | string | Sensor key whose writes are checked (`SENSOR#<key>`) |
| `check` | string | Operator, as in validation rules |
| `field` | string | Field in the sensor's `data` map (not needed for `exists`) |
| `value` | any | Value to compare against (not needed for `exists`) |
| `cutoff`, `cutoffDayOffset` | | See [Cutoff](#cutoff) |

The trigger condition uses the same check syntax as validation rules: `exists`, `equals`, `gt`, `gte`, `lt`, `lte`, `age_lt`.

### Cutoff

The cutoff is the latest time a run date may start. Until then Interlock keeps waiting for readiness, and the start-by check (`schedule.cron`) and the SLA warning and breach are what alert. A run that starts one minute before the cutoff made it.

```yaml
schedule:
  timezone: America/New_York
  trigger:
    key: upstream-complete
    check: equals
    field: status
    value: ready
    cutoff: "02:00"
    cutoffDayOffset: 1
```

| Field | Type | Default | Description |
|---|---|---|---|
| `cutoff` | string | none | `HH:MM` for daily run dates, `:MM` for hourly ones |
| `cutoffDayOffset` | int | `0` | Days after the run date a daily cutoff falls on, 0 to 7 |

A daily cutoff falls on the run date plus `cutoffDayOffset` days, at that wall-clock time in `schedule.timezone` (or `sla.timezone` when that is not set, else UTC). Above, the run for 2026-03-10 must start by 2026-03-11 02:00 New York time. An hourly `:MM` cutoff falls at minute MM of the hour after the run's hour: run `2026-03-10T10` with `cutoff: ":15"` must start by 11:15. A cutoff is never earlier than its wall time: one inside a spring-forward gap falls at the end of the gap (02:30 becomes 03:00), and a time repeated at fall-back falls at its first occurrence. A non-zero offset is rejected with an hourly cutoff and without a cutoff.

The cutoff applies to a run date whose key matches its cadence (`YYYY-MM-DD` for `HH:MM`, `YYYY-MM-DDThh` for `:MM`). A key of the other cadence, such as a date-only sensor on a pipeline with an hourly `:MM` cutoff, has no cutoff, and its sensors start runs as before.

A sensor processed at or after the cutoff starts nothing (the stream-router goes by the time it handles the record), for any date it names, whether or not the date has a `RUN#` row. If the date has no trigger row yet, the stream-router closes it: it writes a `FAILED_FINAL` trigger row and publishes `SENSOR_DEADLINE_EXPIRED` once, with `cutoff` and `cutoffAt` in its detail and `source: stream-router`. The watchdog's sensor-trigger reconciliation does the same for a ready sensor it finds (`source: reconciliation`). An excluded date, or one whose job ran (its latest job row is `success`, `fail` or `timeout`), is left alone. A date whose evaluation was released never ran, so it is closed at the cutoff too, and a `RERUN_REQUEST` can then run it. A date whose launch ran out of retries already ended `FAILED_FINAL` and is left alone. Without a sensor, the watchdog closes only run dates with a `RUN#` row (see [Expected Run Dates](#expected-run-dates)), on its next pass after the cutoff (`source: watchdog`). If the publish fails, the row is removed and the close is tried again: the stream record is redelivered up to three times, after which reconciliation closes the date while the sensor stays ready, and the watchdog retries on its next pass.

A run whose evaluation began before the cutoff and has not passed readiness by then ends at its next evaluation poll, so the alert can come up to one `evaluation.interval` after the cutoff. Its trigger row becomes `FAILED_FINAL` and it publishes one `SENSOR_DEADLINE_EXPIRED` with `source: orchestrator`; the job is not launched. A run that passed readiness before the cutoff still launches. Requested reruns and failure retries are not checked: a date with a `RERUN#` row, written before any rerun or retry starts, has had a rerun accepted. If publishing the alert keeps failing through the step's retries, the execution fails with its row still `RUNNING`, and once its stale deadline (`staleAt`, `sfn_timeout_seconds` plus 30 minutes from the latest attempt) passes the watchdog closes it with `SFN_TIMEOUT` instead.

An evaluation window that ends before the cutoff releases the date: the orchestrator writes an `evaluation-released` job row, deletes the trigger row and publishes `EVALUATION_RELEASED`, which is not an alert. Evaluation starts again on the next trigger sensor write, or on the next watchdog pass while the trigger sensor still passes, until the cutoff; the date then closes with one `SENSOR_DEADLINE_EXPIRED`. Each release, restart and exhaustion cycle writes one `evaluation-released` row and publishes one `EVALUATION_RELEASED` and one `JOB_TRIGGERED`, so a pipeline whose sensor keeps passing while a rule fails produces that set on every sensor write and every watchdog pass until the cutoff. Normally one execution runs at a time; in a rare case a Task retry after a lost Lambda response can delete the lock a new evaluation has just taken, letting a second one start. A cron pipeline restarts the same way, on a sensor write or a watchdog pass: its cron time is only the start-by time. A `RERUN_REQUEST` for a released date before its cutoff is refused with an `INFRA_FAILURE` alert ("lock reset failed"), since there is no trigger row to reset: write the sensor again instead. A released date without a `RUN#` row whose sensor stops passing never gets a trigger row, so it gets no final alert and cannot be rerun until a passing trigger sensor, written or found by the reconciler after the cutoff, closes it. A launch that runs out of retries is not released: the date ends `FAILED_FINAL` with `RETRY_EXHAUSTED`, and only a `RERUN_REQUEST` runs it again. If publishing `RETRY_EXHAUSTED` keeps failing through the step's retries, the row stays `RUNNING` and the execution still finishes through SLA cleanup; a `RERUN_REQUEST` is rejected as a run in progress until the watchdog closes the date with `SFN_TIMEOUT` once its stale deadline (`staleAt`) passes. A status write that fails after a successful publish is retried and can send `RETRY_EXHAUSTED` again. Without a cutoff for the date (none set, or a key of the other cadence), or for a rerun, an exhausted window ends the date: `VALIDATION_EXHAUSTED` and a `FAILED_FINAL` trigger row, and only a `RERUN_REQUEST` runs it again. If publishing `VALIDATION_EXHAUSTED` keeps failing through the step's retries, the execution fails with the row still `RUNNING`, and once its stale deadline (`staleAt`) passes the watchdog closes it with `SFN_TIMEOUT`, as on the cutoff path. A retry after a publish that succeeded but whose status write failed sends the alert twice.

Once the date is closed, nothing more starts for it: investigate, then submit a `RERUN_REQUEST` to run it manually. The cutoff limits only the first start of a run date. A `RERUN_REQUEST`, manual or the late-data one written when a sensor changes after a `COMPLETED` run, still runs after the cutoff. The late-data one is written and run only while the date's [post-run watch window](../pipelines/#watch-window) is open.

### Evaluation Window

The `evaluation` block controls how long and how often the pipeline checks its validation rules after the schedule activates.

```yaml
schedule:
  evaluation:
    window: 1h
    interval: 5m
```

| Field | Type | Default | Description |
|---|---|---|---|
| `window` | duration | `1h` | Maximum time to keep evaluating after activation |
| `interval` | duration | `5m` | Time between evaluation attempts within the window |

If the validation rules are not satisfied within the evaluation window, the execution ends without launching the job: before the date's cutoff the date is released (`EVALUATION_RELEASED`) and can start again, and otherwise it ends with `VALIDATION_EXHAUSTED` (see [Cutoff](#cutoff)).

### Event-Driven Schedules

Pipelines can omit the `cron` field entirely and rely solely on sensor writes to trigger evaluation. When a sensor record is written to DynamoDB, the stream-router Lambda starts the Step Functions execution.

```yaml
schedule:
  trigger:
    key: orders-landed
    check: exists
  evaluation:
    window: 30m
    interval: 2m
```

This pattern is useful for event-driven pipelines that should evaluate immediately when upstream data arrives.

### Execution Granularity

Pipelines run at daily granularity by default. For sub-daily (hourly) execution, sensor data must include both `date` and `hour` fields. The framework builds a composite execution date (e.g., `2026-03-03T10`) that scopes the entire execution — trigger lock, joblog entries, SLA, and retries — to that specific hour.

Every job learns the date, derived from the composite date: `glue` and `emr` jobs receive `--par_day` (YYYYMMDD) and `--par_hour` (HH) arguments, `emr-serverless` jobs the same flags as entry point arguments, a `step-function` job gets `par_day` and `par_hour` in its execution input, a `databricks` job gets them as job parameters, and `http`, `lambda` and `airflow` jobs get the run identity, including `par_day` and `par_hour` when no body or payload is configured (see [Triggers](../triggers/)). `par_hour` is set for hourly dates only.

The granularity is determined by the sensor data, not the pipeline config. A single pipeline can process both daily and hourly executions depending on what its sensors provide.

### Cron-Based Schedules

A cron expression never starts a run: the sensor trigger does, as for any other pipeline. The cron time is the start-by time for each run date that has a `RUN#` row (see the SLA section for the row format). If the job has not started by then, the watchdog publishes `SCHEDULE_MISSED` once for that date, with the start-by time in its detail. A pipeline with no `RUN#` rows gets no start-by check. A run counts as started when the orchestrator launches its job, not when evaluation begins; a run that already finished (`COMPLETED` or `FAILED_FINAL`) is not reported.

The watchdog's reconciler covers cron pipelines as it does other sensor-triggered pipelines. A trigger sensor with no `date` field takes today's UTC date as its run date, so one that keeps passing makes the watchdog start each new day's run on its first pass after UTC midnight. Put the `date` in the sensor to control which run date it starts.

```yaml
schedule:
  cron: "0 8 * * *"        # start by 08:00 New York time on each RUN# date
  timezone: America/New_York
  trigger:
    key: upstream-complete
    check: equals
    field: status
    value: ready
  evaluation:
    window: 2h
    interval: 10m
```

For a daily run date the start-by time is the cron's first fire on that local day; for an hourly run date (`YYYY-MM-DDThh`), its first fire in that local hour. A date the cron does not fire on (a Saturday for `0 8 * * 1-5`) and an excluded date get no check. `schedule.time` is for inclusion calendars only; together with `schedule.cron` it rejects the config.

The expression is validated when the config loads. It must be standard five-field cron (minute, hour, day of month, month, day of week), evaluated in `schedule.timezone`. Each field takes `*`, numbers, ranges (`1-5`), steps (`*/15`, `0-30/10`), lists (`1,15`) and, for month and day of week, names (`JAN`, `MON-FRI`). Day of week runs 0 to 6 with 0 as Sunday; `7` is not accepted. `?` means the same as `*` in the day fields. `L`, `W` and `#` are not supported. A seconds or year field, a descriptor such as `@daily` or `@every 1h`, a `TZ=` prefix, or an out-of-range value like `61 8 * * *` or `0 8 * * 1,9` rejects the config, which is reported as `CONFIG_INVALID`. When both day of month and day of week are restricted, either one matching is enough, as in standard cron. On a DST change, a cron time in the skipped local hour (spring forward) does not occur that day, so that day has no start-by time, and for a daily run date a cron time in the repeated hour (fall back) is taken at its first occurrence.

Common cron patterns:

| Cron | Description |
|---|---|
| `0 8 * * *` | Daily at 08:00 |
| `0 */2 * * *` | Every 2 hours |
| `0 8 * * 1-5` | Weekdays at 08:00 |
| `30 6 1 * *` | First of month at 06:30 |

## SLA Configuration

SLAs enforce time-based constraints. Breaching an SLA emits an EventBridge event but does not cancel the pipeline.

```yaml
sla:
  deadline: "10:00"
  expectedDuration: 30m
```

### SLA Fields

| Field | Type | Description |
|---|---|---|
| `deadline` | string | `HH:MM` for daily execution dates, `:MM` for hourly ones -- all validation rules must pass and the job must complete by this time |
| `deadlineDayOffset` | int | Days after the execution date on which a daily `HH:MM` deadline falls, 0 to 7. Default 0 (the same day) |
| `expectedDuration` | duration | Expected time for the job to complete; the warning fires this long before the breach. With `maxDuration` it must be positive and below `maxDuration` |
| `maxDuration` | duration | Relative SLA: the time budget from the first sensor arrival, at most `24h`. Needs `schedule.trigger` |
| `timezone` | string | IANA time zone name (for example `America/New_York`) for `deadline`. Empty means `UTC`. An unknown name, or `Local`, rejects the config as `CONFIG_INVALID` |
| `critical` | bool | Accepted and passed into the state machine input; nothing reads it |

### Deadline Day

A daily deadline falls on the execution date plus `deadlineDayOffset` days, for cron and sensor-triggered pipelines alike. Interlock never moves it on its own. A pipeline that processes date D's data on D+1 says so:

```yaml
sla:
  deadline: "10:00"
  deadlineDayOffset: 1   # the run for 2026-03-10 is due 2026-03-11 10:00
```

With the default of 0, the run for 2026-03-10 is due 2026-03-10 10:00, so a run that finishes after 10:00 that day is an `SLA_BREACH`. A non-zero offset is rejected with an hourly `:MM` deadline (its date already names the hour) and without a `deadline`.

The deadline is placed on the execution date, a UTC date, at `deadline` in `sla.timezone`, so a zone east of UTC with an early deadline (for example `Asia/Tokyo` and `08:00`) needs `deadlineDayOffset: 1`, or the deadline is already past when the day starts.

### Expected Run Dates

The watchdog schedules SLA alerts only for run dates the pipeline says it expects. You write one row per expected run to the control table (`<environment>-interlock-control`); Interlock never works out a run date from the clock. A pipeline with no rows gets no proactive SLA alerts, start-by checks or watchdog cutoff checks (a sensor processed after the cutoff still starts nothing).

| `PK` | `SK` | Attributes |
|---|---|---|
| `PIPELINE#<id>` | `RUN#<runKey>` | `ttl` (optional): epoch seconds after which DynamoDB deletes the row |

`<runKey>` is the execution date: `YYYY-MM-DD` for a daily `HH:MM` deadline, `YYYY-MM-DDThh` (one row per hour) for an hourly `:MM` deadline. A key of the other form is ignored, a malformed key is skipped with a warning, and a key on an excluded date is skipped. Generate a calendar of rows ahead of time, or add each day's row before its deadline:

```sh
aws dynamodb put-item --table-name production-interlock-control --item '{
  "PK": {"S": "PIPELINE#gold-revenue"},
  "SK": {"S": "RUN#2026-03-10"},
  "ttl": {"N": "1775692800"}
}'
```

The control table's TTL attribute is `ttl`; setting it (above, 2026-04-09, the run date plus 30 days) keeps old rows from piling up. Each pass the watchdog reads the rows from 8 days before to 2 days after the current UTC date and schedules alerts for every row whose breach is still ahead and whose run has not finished. That window covers any timezone and `deadlineDayOffset` up to 7, the most validation allows.

### Deadline Evaluation

The watchdog creates two one-time EventBridge Scheduler entries for each expected run date (a `RUN#` row) of a pipeline with an SLA:

- **Warning**: fires at `deadline - expectedDuration`
- **Breach**: fires at `deadline`

Each Scheduler entry invokes the sla-monitor Lambda at the exact timestamp to publish the corresponding event. Entries auto-delete after firing.

When the pipeline job completes (or all retries are exhausted), the state machine cancels any unfired Scheduler entries. `SLA_MET` is published only when the run ends before the breach time and the date's trigger row is `COMPLETED`. A run that did not complete (it ended `FAILED_FINAL`, its final status could not be recorded, or its status could not be read) gets no `SLA_MET`; `SLA_BREACH` is still published when the breach time has passed. A date that fails before its breach time gets its failure alert (`RETRY_EXHAUSTED`, `JOB_FAILED` and the like) and neither `SLA_MET` nor `SLA_BREACH`, because the cancel step removes the breach schedule.

The deadline is interpreted in `sla.timezone` (or UTC if not set).

### Relative SLA

With `maxDuration`, the SLA counts from the date's first sensor arrival instead of a wall-clock deadline. The watchdog checks it on each pass while the run has not finished:

- **Warning**: `RELATIVE_SLA_WARNING` once at first sensor arrival + `maxDuration` - `expectedDuration` (75% of `maxDuration` without `expectedDuration`)
- **Breach**: `RELATIVE_SLA_BREACH` once at first sensor arrival + `maxDuration`

A pass that first sees a date after its breach time publishes only the breach. `expectedDuration` must be below `maxDuration`, or the config is rejected.

The watchdog checks only daily run dates for today and yesterday (UTC), so hourly run dates get no relative SLA alerts, and a date whose first sensor arrives a day or more late may be missed. Keep `expectedDuration` above the watchdog interval (5 minutes by default), or a pass may land after the breach and send only `RELATIVE_SLA_BREACH`.

The relative SLA creates no Scheduler entries: with both `deadline` and `maxDuration` (and a `RUN#` row), a run that has not finished gets both `SLA_WARNING`/`SLA_BREACH` from its deadline schedules and `RELATIVE_SLA_WARNING`/`RELATIVE_SLA_BREACH` from the watchdog. For any run with `maxDuration` and a first sensor arrival, the `SLA_MET` or `SLA_BREACH` verdict at the end of the run is judged against the relative breach time, so a relative-only run that finishes late gets both `RELATIVE_SLA_BREACH` and a final `SLA_BREACH`.

### EventBridge SLA Events

SLA events are published to the custom EventBridge bus. You can create rules to route these events to SNS, Lambda, or any other EventBridge target.

| Event | When |
|---|---|
| `SLA_WARNING` | Warning deadline reached (job still running) |
| `SLA_BREACH` | Final deadline reached (job still running) |
| `SLA_MET` | Run ended before the breach time and its trigger row is `COMPLETED` |

Example event:

```json
{
  "source": "interlock",
  "detail-type": "SLA_BREACH",
  "detail": {
    "pipelineId": "gold-revenue",
    "scheduleId": "daily",
    "date": "2026-03-01",
    "message": "Pipeline gold-revenue: SLA_BREACH"
  }
}
```

## Calendar Exclusions

Pipelines can be excluded from running on weekends or specific dates with `schedule.exclude`.

```yaml
schedule:
  timezone: America/New_York
  exclude:
    weekends: true
    dates:
      - "2026-12-25"
      - "2026-01-01"
```

| Field | Type | Description |
|---|---|---|
| `weekends` | bool | Exclude Saturdays and Sundays |
| `dates` | list | Excluded dates, `YYYY-MM-DD` |
| `holidays` | string | Accepted but not read |

A trigger sensor write, and the watchdog's sensor-trigger reconciliation, start nothing while the current day in `schedule.timezone` is excluded. Reruns, failure retries, the post-run drift rerun, and the SLA, start-by and cutoff checks look at the execution date instead (its date part for an hourly `YYYY-MM-DDThh` date); a skipped rerun or retry publishes `PIPELINE_EXCLUDED`. The Terraform `calendars_path` variable is accepted but not read: named calendar files are not loaded, so list the dates in `exclude.dates`.
