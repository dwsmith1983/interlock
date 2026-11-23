---
title: Watchdog
weight: 3
description: Detects stale triggers, missed schedules, SLA gaps, passed cutoffs, and missing post-run sensors.
---

The watchdog is one of six Lambda functions in the Interlock framework. It runs independently on an EventBridge schedule (default: every 5 minutes) and runs nine checks in a table-driven loop to detect silent failures:

1. **Stale triggers** -- a Step Function execution started but never completed (timeout, infrastructure failure)
2. **Missed schedules** -- a cron pipeline's job has not started by the cron start-by time for a `RUN#` date
3. **Missed inclusion schedules** -- a pipeline with an inclusion calendar has no trigger on a scheduled date. The check starts when the date begins in `schedule.timezone` (UTC when unset), or at `schedule.time` on that date when it is set; without `schedule.time` it fires at local midnight of the date, so set `time` to avoid an alert before the run is due. A date whose evaluation was released before its cutoff has no trigger row either, so `IRREGULAR_SCHEDULE_MISSED` fires once for it, as for a date that never started. The once-per-date marker (`SENSOR#irregular-missed-check#<date>`) is written only after the publish succeeds, so a failed publish is retried on the next pass
4. **Sensor-trigger reconciliation** -- a sensor-triggered pipeline's conditions (with or without `schedule.cron`) are met but no trigger exists (self-heals missed triggers); past the date's `schedule.trigger.cutoff` it closes the date with `FAILED_FINAL` and one `SENSOR_DEADLINE_EXPIRED` instead of starting it
5. **SLA scheduling** -- proactively ensures EventBridge Scheduler entries exist for each expected run date (`RUN#` row) of a pipeline with an SLA config
6. **Cutoffs** -- an expected run date (`RUN#` row) passed its `schedule.trigger.cutoff` without starting; it is closed with `FAILED_FINAL` and one `SENSOR_DEADLINE_EXPIRED`
7. **Missing post-run sensors** -- a pipeline completed but the expected post-run sensor never arrived
8. **Relative SLA** -- a pipeline with `maxDuration` SLA whose run has not finished: `RELATIVE_SLA_WARNING` once at first sensor arrival + `maxDuration` - `expectedDuration` (75% of `maxDuration` without `expectedDuration`), then `RELATIVE_SLA_BREACH` once at arrival + `maxDuration`
9. **Invalid configs** -- a pipeline's `CONFIG` row was rejected (malformed or failing validation), so that pipeline is skipped while the others run; reported as `CONFIG_INVALID` once per broken config version and day

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

A running trigger row carries its stale deadline as `staleAt`: `sfn_timeout_seconds` plus 30 minutes (4h30m by default) from the run's latest attempt, since each retry refreshes it. The row has no `ttl`, so DynamoDB never deletes it: a live run's row is closed by its own execution, and a dead run's only by the watchdog. Triggers past their `staleAt` are considered timed-out Step Function executions. The watchdog transitions them to `FAILED_FINAL` status. It does not retry the job: retries start only from a run's CompleteTrigger, and a stale trigger has no run left to start one. With the watchdog stopped, a dead run stays `RUNNING`.

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

The orchestrator writes `startedAt` on the trigger row just before it launches the job, since a `lambda` job runs inside the launch call. If the launch fails and its retries run out, the trigger row is released, so the date can still be reported. A `RUNNING` row without `startedAt` is a run still evaluating its rules, so it has not started. `FAILED_FINAL` already sent its own final alert, so it is not reported again. The marker makes the alert once per run date: later passes skip that date. It is written only after the publish succeeds, so a failed publish is retried on the next pass.

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

For pipelines with `postRun` config, the watchdog checks whether post-run sensors have arrived within a configurable grace period after job completion.

### Algorithm

```
Load all pipeline configs (via config cache)

For each pipeline with postRun config:
  Find TRIGGER# record for today with status=COMPLETED
  If no completed trigger, or it has no completedAt → skip
  Marker SENSOR#postrun-check#<date> present → already alerted (skip)

  Calculate elapsed time since completedAt
  If elapsed < sensorTimeout → skip (still within grace period)

  Read the baseline SENSOR#postrun-baseline#<date> (none if no rule's sensor existed at completion and none has been seeded inside the watch window since)
  For each postRun rule key:
    Look up SENSOR#{key}, or SENSOR#{key}#YYYY-MM-DD or SENSOR#{key}#YYYYMMDD for the date
    If it exists and its value differs from its baseline entry, or it has no entry → arrived (skip)

  Publish POST_RUN_SENSOR_MISSING event to EventBridge
  On success, write the marker SENSOR#postrun-check#<date>
```

A post-run sensor has arrived when it exists and either differs from what it was when the run completed or did not exist then. The whole stored record is compared, so a change in any of its fields, a timestamp such as `updatedAt` included, counts as a change; when the write happened is not otherwise used. A sensor rewritten with identical values has not arrived. A sensor present at completion whose baseline capture failed (`BASELINE_CAPTURE_FAILED`) has no entry, so it counts as arrived. A post-run sensor written between the run completing and the baseline capture counts as present at completion. An entry the stream-router seeded from a first write after completion carries `_seededAfterCompletion`, so it differs from the sensor and counts as arrived.

The marker makes the alert once per date. It is written only after the publish succeeds, so a failed publish is retried on the next pass.

### Configuration

The `sensorTimeout` field on `postRun` controls the grace period, counted from the trigger row's `completedAt`. Defaults to `"2h"` (2 hours). Only today's UTC daily trigger key is checked, so hourly keys and dates past UTC midnight get no `POST_RUN_SENSOR_MISSING`.

```yaml
postRun:
  sensorTimeout: "2h"
  rules:
    - key: output-row-count
      check: gte
      field: count
      value: 1000
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
    "message": "post-run sensor not received within 2h of completion",
    "timestamp": "2026-03-01T14:30:00Z"
  }
}
```

## Deployment

The watchdog Lambda is invoked by an EventBridge scheduled rule on the default event bus:

```
EventBridge rule (rate) → watchdog Lambda → DynamoDB (read configs, triggers)
                                          → EventBridge custom bus (publish events)
```

The watchdog reads from the control, joblog and rerun tables and writes only to the control table (trigger status, dedup markers, released locks). It also starts executions during sensor-trigger reconciliation and creates the SLA Scheduler entries.

### IAM Permissions

| Action | Scope |
|---|---|
| DynamoDB read (GetItem, Query, Scan, etc.) | All 3 tables + indexes |
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

## Error Handling

All nine checks run independently in sequence. An error in any check does not prevent the remaining checks from running. Errors are collected into an aggregate error and returned to the Lambda runtime.

When one or more checks fail, the watchdog publishes a `WATCHDOG_DEGRADED` event to EventBridge listing the failed check names. The event-sink records it, but it is not on the module's alert rule, so it does not reach Slack; add a rule on the bus to route it.

```json
{
  "source": "interlock",
  "detail-type": "WATCHDOG_DEGRADED",
  "detail": {
    "message": "watchdog checks failed: stale-triggers, sla-scheduling",
    "timestamp": "2026-03-01T12:30:00Z"
  }
}
```

## Relationship to Step Functions

The watchdog is **not** part of the Step Functions state machine. It runs on its own schedule precisely to detect cases where the Step Function _didn't_ start (missed schedules) or where it started but got stuck (stale triggers). This independence is fundamental to its role as a safety net.
