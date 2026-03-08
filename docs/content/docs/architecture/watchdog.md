---
title: Watchdog
weight: 3
description: Detects stale trigger executions, missed cron schedules, and missing post-run sensors.
---

The watchdog is one of four Lambda functions in the Interlock framework. It runs independently on an EventBridge schedule (default: every 5 minutes) and detects three classes of silent failures:

1. **Stale triggers** -- a Step Function execution started but never completed (timeout, infrastructure failure)
2. **Missed schedules** -- a cron-scheduled pipeline's expected start time passed with no trigger record
3. **Missing post-run sensors** -- a pipeline completed but the expected post-run sensor never arrived

In STAMP terms, these are safety constraint violations caused by _what didn't happen_ rather than what went wrong.

## Problem

Interlock's event-driven architecture (sensor write -> DynamoDB Stream -> Step Function) only acts when data arrives. Two failure modes escape this detection:

**Stale triggers**: A Step Function execution starts but gets stuck or times out silently. The trigger lock remains in `RUNNING` status indefinitely.

**Missed schedules**: Upstream ingestion fails silently for a cron-scheduled pipeline. No sensor data arrives, no trigger fires, no SLA check ever runs. The pipeline is silently skipped with zero alerts.

This is a classic STAMP gap: the control structure assumes the controlled process always produces feedback. When it doesn't, the controller never acts.

## Stale Trigger Detection

The watchdog scans the control table for `TRIGGER#` records with `RUNNING` status and checks whether their TTL has expired.

### Algorithm

```
Scan all TRIGGER# records with status=RUNNING

For each trigger:
  If TTL > 0 and now > TTL → stale
  Otherwise → not stale (skip)

  Parse pipeline ID, schedule, date from PK/SK
  Publish SFN_TIMEOUT event to EventBridge
  Set trigger status to FAILED_FINAL
```

### Stale Threshold

The default stale trigger threshold is 24 hours. Triggers whose TTL has expired are considered timed-out Step Function executions. The watchdog transitions them to `FAILED_FINAL` status, which prevents the stream-router from attempting further retries.

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
    "timestamp": "2026-03-01T12:30:00Z"
  }
}
```

## Missed Schedule Detection

The watchdog loads all pipeline configs and checks cron-scheduled pipelines for missing trigger records.

### Algorithm

```
Load all pipeline configs (via config cache)

For each pipeline with a cron schedule:
  Skip if excluded by calendar (weekends, specific dates)

  Resolve schedule ID ("cron" for cron-scheduled pipelines)
  Check if a TRIGGER# record exists for today's date

  If trigger exists → not missed (skip)

  If schedule.time is configured:
    Resolve timezone (UTC if not specified)
    If current time < expected start time → skip (not yet due)

  Publish SCHEDULE_MISSED event to EventBridge
```

### Deadline Resolution

The watchdog determines whether a schedule is missed by checking:

1. **Trigger record existence** -- if a `TRIGGER#{schedule}#{date}` record exists in the control table, the pipeline has already been triggered today
2. **Expected start time** -- if the pipeline config includes a `schedule.time`, the watchdog only alerts after that time has passed in the configured timezone

If no `schedule.time` is configured, the watchdog alerts as soon as it detects a missing trigger record for today's date.

### Event Format

```json
{
  "source": "interlock",
  "detail-type": "SCHEDULE_MISSED",
  "detail": {
    "pipelineId": "gold-revenue",
    "scheduleId": "cron",
    "date": "2026-03-01",
    "message": "missed schedule for gold-revenue on 2026-03-01",
    "timestamp": "2026-03-01T09:10:00Z"
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
  If no completed trigger → skip

  Calculate elapsed time since trigger completion
  If elapsed < sensorTimeout → skip (still within grace period)

  For each postRun rule key:
    Check if SENSOR#{key} exists with data newer than trigger completion
    If sensor exists → skip

  Publish POST_RUN_SENSOR_MISSING event to EventBridge
```

### Configuration

The `sensorTimeout` field on `postRun` controls the grace period. Defaults to `"2h"` (2 hours).

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

The watchdog reads from all three DynamoDB tables (configs from control, job events from joblog, reruns from rerun) but only writes to the control table (to update stale trigger status).

### IAM Permissions

| Action | Scope |
|---|---|
| DynamoDB read (GetItem, Query, Scan, etc.) | All 3 tables + indexes |
| DynamoDB write (PutItem, UpdateItem) | Control table only |
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

Both detection scans run independently. An error in stale trigger detection does not prevent missed schedule detection from running. Errors are logged but do not cause the Lambda invocation to fail, which prevents EventBridge from retrying with potentially stale state.

## Relationship to Step Functions

The watchdog is **not** part of the Step Functions state machine. It runs on its own schedule precisely to detect cases where the Step Function _didn't_ start (missed schedules) or where it started but got stuck (stale triggers). This independence is fundamental to its role as a safety net.
