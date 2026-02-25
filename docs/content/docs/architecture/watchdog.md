---
title: Watchdog
weight: 4
description: Absence detection for silently missed pipeline schedules.
---

The watchdog detects **missed pipeline schedules** — the absence of an expected control action. In STAMP terms, this is a safety constraint violation caused by _what didn't happen_ rather than what went wrong.

## Problem

Interlock's event-driven architecture (MARKER → Step Function / watcher loop) only acts when data arrives. If upstream ingestion fails silently:

1. No MARKER is written
2. No execution starts
3. No SLA check ever runs
4. The pipeline is silently skipped — zero alerts

This is a classic STAMP gap: the control structure assumes the controlled process always produces feedback. When it doesn't, the controller never acts.

## Algorithm

The watchdog runs `CheckMissedSchedules` on a regular interval:

```
For each registered pipeline:
  Skip if Watch.Enabled == false
  Skip if Trigger == nil
  Skip if excluded today (calendar/inline)

  For each schedule:
    Resolve deadline: schedule Deadline > SLA.EvaluationDeadline > skip
    If deadline hasn't passed → skip
    If RunLog entry exists (any status) → skip
    Acquire dedup lock → if already held → skip
    Fire SCHEDULE_MISSED alert
    Append SCHEDULE_MISSED event
```

### Deadline Resolution

The watchdog checks two deadline sources in priority order:

| Priority | Source | Field | Meaning |
|---|---|---|---|
| 1 | Schedule | `deadline` | Per-schedule evaluation deadline |
| 2 | Pipeline SLA | `evaluationDeadline` | Pipeline-wide evaluation deadline |

`evaluationDeadline` is the correct fallback because it marks when evaluation should have _started_. `completionDeadline` implies a run already exists, which is the opposite of the watchdog's concern.

If neither deadline is configured, the watchdog skips that schedule — there's no expected time to compare against.

### Dedup

One alert per pipeline per schedule per day, enforced via distributed lock:

- **Key**: `watchdog:{pipelineID}:{scheduleID}:{YYYY-MM-DD}`
- **TTL**: 24 hours
- **Mechanism**: `Provider.AcquireLock()` — the same lock interface used by evaluation locks

Daily key rotation means locks self-clean via TTL. No new provider methods were needed.

### Alert Format

```json
{
  "level": "error",
  "pipelineId": "gharchive-silver",
  "message": "Pipeline gharchive-silver schedule h14 missed: no evaluation started by deadline 15:30 on 2026-02-24",
  "details": {
    "scheduleId": "h14",
    "date": "2026-02-24",
    "deadline": "15:30",
    "type": "schedule_missed"
  },
  "timestamp": "2026-02-24T15:35:00Z"
}
```

Alert level is `error` — a missed schedule means the upstream data flow is silently broken, not just late.

## Deployment Modes

### Local (polling)

The `Watchdog` struct wraps `CheckMissedSchedules` in a polling loop:

```go
wd := watchdog.New(provider, calendarReg, alertFn, logger, 5*time.Minute)
wd.Start(ctx)
defer wd.Stop(ctx)
```

Follows the archiver pattern: goroutine with ticker, `sync.WaitGroup` for clean shutdown, runs a scan immediately on start then on each interval.

Enable in `interlock.yaml`:

```yaml
watchdog:
  enabled: true
  interval: 5m
```

### AWS Lambda

A dedicated Lambda function invoked by an EventBridge scheduled rule (default: every 5 minutes). The handler calls `CheckMissedSchedules` once per invocation — no polling loop needed.

```
EventBridge rule (rate) → watchdog Lambda → DynamoDB (read pipelines, run logs, locks)
                                          → SNS (alerts)
```

The watchdog Lambda is **not** part of the Step Function state machine. It runs independently on its own schedule, which is exactly the point — it detects when the Step Function _didn't_ start.

### GCP Cloud Function

An HTTP Cloud Function triggered by Cloud Scheduler on a cron schedule (`*/5 * * * *`). Same pattern as the Lambda handler: calls `CheckMissedSchedules` once per invocation.

## Package Structure

```
internal/watchdog/
├── watchdog.go       # CheckMissedSchedules + Watchdog polling wrapper
└── watchdog_test.go  # 11 unit tests
```

### Exports

| Export | Description |
|---|---|
| `CheckMissedSchedules(ctx, opts)` | Pure function, backend-agnostic scan |
| `CheckOptions` | Provider, CalendarReg, AlertFn, Logger, Now |
| `MissedSchedule` | Result struct: PipelineID, ScheduleID, Date, Deadline |
| `Watchdog` | Polling wrapper for local mode |
| `New(provider, calReg, alertFn, logger, interval)` | Constructor |
| `Start(ctx)` / `Stop(ctx)` | Lifecycle methods |

The core `CheckMissedSchedules` function is a pure function with injectable dependencies (`CheckOptions.Now` for time, `CheckOptions.Provider` for storage). This makes it testable and reusable across all deployment modes.
