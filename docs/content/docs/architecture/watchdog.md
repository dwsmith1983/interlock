---
title: Watchdog
weight: 3
description: Nine scheduled checks for stale runs, missed cron schedules, missed inclusion-calendar dates, missed triggers, SLA schedules, cutoffs, post-run sensors, relative SLAs and invalid configs.
---

The watchdog is one of six Lambda functions in the Interlock framework. It runs independently on an EventBridge schedule (default: every 5 minutes) and runs nine checks in a table-driven loop to detect silent failures:

1. **Stale triggers** -- a Step Function execution started but never completed (timeout, infrastructure failure)
2. **Missed schedules** -- a cron pipeline's job has not started by the cron start-by time for a `RUN#` date
3. **Missed inclusion schedules** -- a pipeline with an inclusion calendar has no trigger on a scheduled date. The check starts when the date begins in `schedule.timezone` (UTC when unset), or at `schedule.time` on that date when it is set; without `schedule.time` it fires at local midnight of the date, so set `time` to avoid an alert before the run is due. A date whose evaluation was released before its cutoff has no trigger row either, so `IRREGULAR_SCHEDULE_MISSED` fires once for it, as for a date that never started. The once-per-date marker (`SENSOR#irregular-missed-check#<date>`) is written only after the publish succeeds, so a failed publish is retried on the next pass. Each pass checks only inclusion dates up to 8 days before the current date in `schedule.timezone` (that date included), at most the 3 most recent of them; a missed date older than that is not reported
4. **Sensor-trigger reconciliation** -- a sensor-triggered pipeline's conditions (with or without `schedule.cron`) are met but no trigger exists (self-heals missed triggers); past the date's `schedule.trigger.cutoff` it closes the date with `FAILED_FINAL` and one `SENSOR_DEADLINE_EXPIRED` instead of starting it. A run it starts records the date's first sensor arrival at that start, unless one is already recorded, so its SLA verdict counts `maxDuration` from it
5. **SLA scheduling** -- proactively ensures EventBridge Scheduler entries exist for each expected run date (`RUN#` row) of a pipeline with an `sla.deadline`, once its warning is at most 6 hours away (see [Deadline Evaluation](../../configuration/schedules/#deadline-evaluation))
6. **Cutoffs** -- an expected run date (`RUN#` row) passed its `schedule.trigger.cutoff` without starting; it is closed with `FAILED_FINAL` and one `SENSOR_DEADLINE_EXPIRED`
7. **Missing post-run sensors** -- a pipeline completed but the expected post-run sensor never arrived
8. **Relative SLA** -- a pipeline with `maxDuration` SLA whose run has not finished: `RELATIVE_SLA_WARNING` once at first sensor arrival + `maxDuration` - `expectedDuration` (75% of `maxDuration` without `expectedDuration`), then `RELATIVE_SLA_BREACH` once at arrival + `maxDuration`. Run dates with a first sensor arrival, daily and hourly, are checked from 8 days before to 2 days after today (UTC). Nothing is sent once a breach time is more than 24 hours past, so an alert the watchdog could not send by then (an outage, say) is lost. For a run the watchdog started itself, the arrival is the time it started it
9. **Invalid configs** -- a pipeline's `CONFIG` row was rejected (malformed or failing validation), so that pipeline is skipped while the others run; reported as `CONFIG_INVALID` once per broken config version and day

A dry-run pipeline is skipped by every check except stale triggers and invalid configs.

In STAMP terms, these are safety constraint violations caused by _what didn't happen_ rather than what went wrong.

## Problem

Interlock's event-driven architecture (sensor write -> DynamoDB Stream -> Step Function) only acts when data arrives. Two failure modes escape this detection:

**Stale triggers**: A Step Function execution starts but gets stuck or times out silently. The trigger lock remains in `RUNNING` status indefinitely.

**Missed schedules**: Upstream ingestion fails silently for a cron-scheduled pipeline. No sensor data arrives, no trigger fires, no SLA check ever runs. The pipeline is silently skipped with zero alerts.

This is a classic STAMP gap: the control structure assumes the controlled process always produces feedback. When it doesn't, the controller never acts.

## Stale Trigger Detection

The watchdog scans the control table for `TRIGGER#` records with `RUNNING` status and checks whether their stale deadline (`staleAt`) has passed.

### Algorithm

```
Scan all TRIGGER# records with status=RUNNING

For each trigger:
  If staleAt > 0 and now > staleAt → stale
  Otherwise → not stale (skip)

  Parse pipeline ID, schedule, date from PK/SK
  Publish SFN_TIMEOUT event to EventBridge
  If the publish failed → skip (still RUNNING, retried next pass)
  Set trigger status to FAILED_FINAL
```

### Stale Threshold

A running trigger row carries its stale deadline as `staleAt`: `sfn_timeout_seconds` plus 30 minutes (4h30m by default) from the run's latest attempt, since each retry refreshes it. The row has no `ttl`, so DynamoDB never deletes it: a run's row is closed by its own execution, and one its execution left `RUNNING` only by the watchdog. An execution that fails in Evaluate, ValidationExhausted or CheckJob closes the row itself through `RecordInfraFailure` (`INFRA_FAILURE`, then `FAILED_FINAL`), so this check is the backstop, for example for an execution that timed out or was stopped, one that ended on `States.Runtime` or `States.DataLimitExceeded` (which a `States.ALL` Catch does not catch), a `RecordInfraFailure`, `CompleteTrigger` or `TriggerRetryExhausted` that failed through its retries, or a released evaluation whose lock delete kept failing. Triggers past their `staleAt` are considered timed-out Step Function executions. The watchdog transitions them to `FAILED_FINAL` status. It does not retry the job: retries start only from a run's CompleteTrigger, and a stale trigger has no run left to start one. With the watchdog stopped, a dead run stays `RUNNING`.

The status is set only after `SFN_TIMEOUT` is published. A failed publish leaves the row `RUNNING` with its `staleAt`, so the next pass sends the alert and closes the row. If the status write fails after a successful publish, the next pass sends `SFN_TIMEOUT` again. While a failed publish keeps a stale run `RUNNING`, a `RERUN_REQUEST` for that date is still refused as in progress, and the date is closed once a later pass publishes.

A dry-run pipeline never takes a trigger lock, so a `RUNNING` row for one is left from before the pipeline switched to dry-run; the watchdog closes it the same way.

### Event Format

```json
{
  "source": "interlock",
  "detail-type": "SFN_TIMEOUT",
  "detail": {
    "pipelineId": "silver-orders",
    "scheduleId": "stream",
    "date": "2026-03-01",
    "message": "step function timed out for silver-orders/stream/2026-03-01",
    "timestamp": "2026-03-01T12:30:00Z",
    "detail": {
      "source": "watchdog",
      "actionHint": "step function passed its stale deadline: check SFN execution history",
      "staleAt": "2026-03-01T12:25:00Z"
    }
  }
}
```

`staleAt` is the row's stale deadline in RFC3339 UTC.

## Missed Schedule Detection

For a cron pipeline the cron time is a start-by checkpoint, not a trigger. The watchdog checks each expected run date (a `RUN#` row written by the user) and alerts when the job has not started by then.

### Algorithm

```
Load all pipeline configs (via config cache)

For each pipeline with schedule.cron that is not a dry run:
  List RUN# keys from 8 days before to 2 days after today (UTC)
  No keys → skip the pipeline

  For each key:
    Skip a malformed key (warning) or an excluded date
    Start-by = the cron's first fire in the key's local day (or hour)
    No fire in that period, or now before start-by → skip

    Read TRIGGER#cron#<key>
    startedAt set, or status COMPLETED or FAILED_FINAL → started (skip)

    Marker SENSOR#schedule-missed-check#<key> present → already alerted (skip)
    Publish SCHEDULE_MISSED; on success, write the marker
```

### What Counts as Started

The orchestrator writes `startedAt` on the trigger row just before it launches the job, since a `lambda` job runs inside the launch call. A `RUNNING` row without `startedAt` is a run still evaluating its rules, so it has not started. `FAILED_FINAL`, including a launch whose retries ran out (`RETRY_EXHAUSTED`), already sent its own final alert, so it is not reported again. The marker makes the alert once per run date: later passes skip that date. It is written only after the publish succeeds, so a failed publish is retried on the next pass. It expires 31 days after it is written; the check reads `RUN#` keys from at most 8 days back, so an expired marker's key is no longer checked.

### Event Format

```json
{
  "source": "interlock",
  "detail-type": "SCHEDULE_MISSED",
  "detail": {
    "pipelineId": "gold-revenue",
    "scheduleId": "cron",
    "date": "2026-03-01",
    "message": "job for gold-revenue on 2026-03-01 has not started by 2026-03-01T09:00:00Z",
    "timestamp": "2026-03-01T09:10:00Z",
    "detail": {
      "source": "watchdog",
      "cron": "0 9 * * *",
      "startBy": "2026-03-01T09:00:00Z",
      "actionHint": "start-by time passed and the job has not started; the SLA is at risk"
    }
  }
}
```

## Missing Post-Run Sensor Detection

For pipelines with `postRun` rules, the watchdog checks every completed run date, daily (`YYYY-MM-DD`) or hourly (`YYYY-MM-DDThh`), for a post-run sensor within a configurable grace period after job completion.

### Algorithm

```
Load all pipeline configs (via config cache)

For each pipeline with postRun rules that is not a dry run:
  Query its TRIGGER#<schedule>#<date> rows from 8 days before today's UTC date
  through 2 days after it, hourly keys included
  For each row:
    Key not a valid execution date → skip (logged)
    Status not COMPLETED, or the date excluded by the calendar → skip
    No parseable completedAt → skip
    deadline = completedAt + sensorTimeout
    Before the deadline, or more than 24h after it → skip
    Marker SENSOR#postrun-check#<date> present → a post-run sensor arrived, or already alerted (skip)
    Publish POST_RUN_SENSOR_MISSING for the date
    On success, write the marker SENSOR#postrun-check#<date> with alerted
```

Arrival is the marker alone: the watchdog reads no sensors. The stream-router writes it when it handles a post-run rule's sensor for a `COMPLETED` date, so a rerun of that date (a drift rerun that resets `completedAt` included) gets no `POST_RUN_SENSOR_MISSING`. The date is the one the write's data names (`date`, and `hour` for an hourly run), the current UTC date without `date`: a post-run sensor for any other run date (T+1, hourly, or a schedule timezone that puts the run date on another day) must carry them, plain rule key included, or the run date gets no marker and alerts. A write while the trigger row is `RUNNING` records nothing, so a sensor written only before completion alerts.

What is not checked: a date more than 8 days before or 2 days after the current UTC date, so neither a `sensorTimeout` long enough to put the deadline past that lookback nor a rerun or backfill of a date older than 8 days is checked (raise an issue if you need one); a date whose deadline passed more than 24 hours ago, so after a watchdog outage, dates whose deadline is more than 24 hours old at the first pass after recovery are skipped; trigger rows under the other schedule ID (a pipeline that changed between `cron` and `stream`); calendar-excluded dates and dry-run pipelines. The watch window does not suppress the alert: it can come after the window has ended.

The marker makes the alert once per date. The watchdog writes it with `alerted` only after the publish succeeds, so a failed publish is retried on the next pass. The stream-router writes the same marker with `arrived` when a post-run rule's sensor is written for a `COMPLETED` date (inside the watch window or after it) under the rule's own key or its key with that date or hour, unless a marker is already there; a date with either marker is skipped. It expires 31 days after it is written, long after its date stops being checked.

### Configuration

The `sensorTimeout` field on `postRun` controls the grace period, counted from the trigger row's `completedAt`. Defaults to `"2h"` (2 hours).

```yaml
postRun:
  sensorTimeout: "2h"
  rules:
    - key: output-row-count
      check: gte
      field: count
      value: 1000
  driftField: count
```

### Event Format

```json
{
  "source": "interlock",
  "detail-type": "POST_RUN_SENSOR_MISSING",
  "detail": {
    "pipelineId": "silver-orders",
    "scheduleId": "stream",
    "date": "2026-03-01",
    "message": "post-run sensor missing for silver-orders on 2026-03-01",
    "timestamp": "2026-03-01T14:30:00Z",
    "detail": {
      "source": "watchdog",
      "sensorTimeout": "2h",
      "ruleKeys": "output-row-count",
      "completedAt": "2026-03-01T12:20:00Z",
      "actionHint": "post-run sensor data has not arrived within the expected timeout"
    }
  }
}
```

`sensorTimeout` is the configured value, empty when it is unset (the 2-hour default applies). `ruleKeys` lists the `postRun.rules` keys, separated by a comma and a space. `completedAt` is the trigger row's completion the timeout was counted from. The event's `date` is the trigger key, `YYYY-MM-DDThh` for an hourly run.

## Deployment

The watchdog Lambda is invoked by an EventBridge scheduled rule on the default event bus:

```
EventBridge rule (rate) → watchdog Lambda → DynamoDB (read configs, triggers)
                                          → EventBridge custom bus (publish events)
```

The watchdog reads from the control and joblog tables and writes only to the control table (trigger status, dedup markers, released locks). It also starts executions during sensor-trigger reconciliation and creates the SLA Scheduler entries.

### IAM Permissions

| Action | Scope |
|---|---|
| DynamoDB read (GetItem, Query, Scan, etc.) | Control and joblog tables + indexes |
| DynamoDB write (PutItem, UpdateItem, DeleteItem) | Control table only |
| Step Functions StartExecution | Pipeline state machine |
| EventBridge Scheduler CreateSchedule | SLA schedule group |
| EventBridge PutEvents | Custom event bus |

### Terraform Configuration

The watchdog schedule is configurable via the `watchdog_schedule` Terraform variable:

```hcl
variable "watchdog_schedule" {
  description = "EventBridge schedule expression for watchdog invocations"
  type        = string
  default     = "rate(5 minutes)"
}
```

Keep the interval well under 6 hours. SLA schedules are created at most 6 hours before a run date's warning, so passes further apart than that can miss `SLA_WARNING` and `SLA_BREACH`; nothing checks the value against that limit (see [Deadline Evaluation](../../configuration/schedules/#deadline-evaluation)).

If the watchdog stops running (its rule is disabled or stalls, or invocations stop for any other reason), the `{env}-interlock-watchdog-heartbeat` alarm fires after about `watchdog_heartbeat_minutes` (default 15) with no invocation, and up to about 10 minutes more because of how CloudWatch evaluates missing data. Its `INFRA_ALARM` goes to Slack. With a `rate()` schedule the heartbeat must be at least twice the interval, or the plan fails; with `cron()`, set it above the longest gap between runs. See [CloudWatch Alarms](../../reference/alerting/#cloudwatch-alarms).

## Error Handling

All nine checks run independently in sequence. An error in any check does not prevent the remaining checks from running. Errors are collected into an aggregate error and returned to the Lambda runtime.

When one or more checks fail, the watchdog publishes a `WATCHDOG_DEGRADED` event to EventBridge whose message lists the failed check names. It has no inner `detail`, and `pipelineId` is empty, so the event-sink stores it under the PK `PIPELINE#`. The event-sink records it, but it is not on the module's alert rule, so it does not reach Slack; add a rule on the bus to route it.

```json
{
  "source": "interlock",
  "detail-type": "WATCHDOG_DEGRADED",
  "detail": {
    "pipelineId": "",
    "message": "watchdog checks failed: stale-triggers, sla-scheduling",
    "timestamp": "2026-03-01T12:30:00Z"
  }
}
```

## Relationship to Step Functions

The watchdog is **not** part of the Step Functions state machine. It runs on its own schedule precisely to detect cases where the Step Function _didn't_ start (missed schedules) or where it started but got stuck (stale triggers). This independence is fundamental to its role as a safety net.
