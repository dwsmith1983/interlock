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
| `timezone` | string | no | IANA timezone for cron and deadline interpretation (defaults to `UTC`) |
| `trigger` | object | no | Condition that activates the evaluation loop (see below) |
| `evaluation` | object | no | Window and interval settings for the evaluation loop |

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

The trigger condition uses the same check syntax as validation rules: `exists`, `equals`, `gt`, `gte`, `lt`, `lte`, `age_lt`, `age_gt`.

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
| `window` | duration | — | Maximum time to keep evaluating after activation |
| `interval` | duration | — | Time between evaluation attempts within the window |

If the validation rules are not satisfied within the evaluation window, the Step Functions execution transitions to a timeout state and emits an EventBridge event.

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

Glue triggers automatically receive `--par_day` (YYYYMMDD) and `--par_hour` (HH) arguments derived from the composite date.

The granularity is determined by the sensor data, not the pipeline config. A single pipeline can process both daily and hourly executions depending on what its sensors provide.

### Cron-Based Schedules

A cron expression never starts a run: the sensor trigger does, as for any other pipeline. The cron time is the start-by time for each run date that has a `RUN#` row (see the SLA section for the row format). If the job has not started by then, the watchdog publishes `SCHEDULE_MISSED` once for that date, with the start-by time in its detail. A pipeline with no `RUN#` rows gets no start-by check. A run counts as started when the orchestrator launches its job, not when evaluation begins; a run that already finished (`COMPLETED` or `FAILED_FINAL`) is not reported.

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
| `deadline` | string | `HH:MM` -- all validation rules must pass and the job must complete by this time |
| `deadlineDayOffset` | int | Days after the execution date on which a daily `HH:MM` deadline falls, 0 to 7. Default 0 (the same day) |
| `expectedDuration` | duration | Expected time for the job to complete (used for SLA breach detection) |

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

The watchdog schedules SLA alerts only for run dates the pipeline says it expects. You write one row per expected run to the control table (`<environment>-interlock-control`); Interlock never works out a run date from the clock. A pipeline with an SLA and no rows gets no proactive SLA alerts (and, in later releases, no start-by or cutoff checks).

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

When the pipeline job completes (or all retries are exhausted), the state machine cancels any unfired Scheduler entries. If the run ended before the breach time, a `SLA_MET` event is published.

The deadline is interpreted in `sla.timezone` (or UTC if not set).

### EventBridge SLA Events

SLA events are published to the custom EventBridge bus. You can create rules to route these events to SNS, Lambda, or any other EventBridge target.

| Event | When |
|---|---|
| `SLA_WARNING` | Warning deadline reached (job still running) |
| `SLA_BREACH` | Final deadline reached (job still running) |
| `SLA_MET` | Job completed before the warning deadline |

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

Pipelines can be excluded from running on specific days of the week or calendar dates. When excluded, the pipeline is dormant -- no evaluation, no SLA monitoring.

### Inline Exclusions

```yaml
exclusions:
  days:
    - saturday
    - sunday
  dates:
    - "2026-12-25"
    - "2026-01-01"
```

### Named Calendar Reference

Define reusable calendars in YAML files and reference them from pipeline configs:

```yaml
# calendars/holidays.yaml
name: holidays
days:
  - saturday
  - sunday
dates:
  - "2026-12-25"
  - "2026-01-01"
  - "2026-07-04"
```

Reference from a pipeline:

```yaml
exclusions:
  calendar: holidays
```

### Combining Calendar and Inline Exclusions

Inline `days` and `dates` are merged with the named calendar. A date or day matching any source triggers exclusion.

```yaml
exclusions:
  calendar: holidays
  days:
    - friday
  dates:
    - "2026-03-15"
```

Calendar YAML files are loaded from the `calendars_path` directory configured in the Terraform module.
