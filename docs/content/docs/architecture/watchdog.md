---
title: Watchdog
weight: 4
description: Absence detection for missed schedules and stuck pipeline runs.
---

The watchdog detects two classes of silent failures:

1. **Missed schedules** — upstream ingestion failed silently, no MARKER arrived, no evaluation started by the deadline
2. **Stuck runs** — a run started but has been in PENDING/TRIGGERING/RUNNING longer than the threshold

In STAMP terms, these are safety constraint violations caused by _what didn't happen_ rather than what went wrong.

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
  "alertType": "schedule_missed",
  "pipelineId": "earthquake-silver",
  "message": "Pipeline earthquake-silver schedule daily missed: no evaluation started by deadline 09:20 on 2026-02-25",
  "details": {
    "scheduleId": "daily",
    "date": "2026-02-25",
    "deadline": "09:20",
    "type": "schedule_missed"
  },
  "timestamp": "2026-02-25T09:25:00Z"
}
```

Alert level is `error` — a missed schedule means the upstream data flow is silently broken, not just late.

## Stuck-Run Detection

The watchdog also runs `CheckStuckRuns` to detect runs that have been in a non-terminal state too long.

### Algorithm

```
For each registered pipeline:
  Skip if Watch.Enabled == false
  Skip if Trigger == nil

  For each schedule:
    Get RunLog entry for today
    If no entry → skip (no run started)
    If terminal status (COMPLETED/FAILED/CANCELLED) → skip
    If age < StuckRunThreshold → skip
    Acquire dedup lock → if already held → skip
    Fire RUN_STUCK alert
    Append RUN_STUCK event
```

### Configuration

```yaml
watchdog:
  enabled: true
  interval: 5m
  stuckRunThreshold: 30m    # default: 30 minutes
```

The threshold is configurable via `WatchdogConfig.StuckRunThreshold`. Runs in PENDING, TRIGGERING, or RUNNING state for longer than this threshold are flagged.

### Dedup

One stuck-run alert per pipeline per schedule per day, using a separate lock namespace:

- **Key**: `watchdog:stuck:{pipelineID}:{scheduleID}:{YYYY-MM-DD}`
- **TTL**: 24 hours

### Alert Format

```json
{
  "level": "error",
  "alertType": "stuck_run",
  "pipelineId": "earthquake-silver",
  "message": "Pipeline earthquake-silver schedule daily run stuck in RUNNING for 45m0s on 2026-02-25",
  "details": {
    "scheduleId": "daily",
    "date": "2026-02-25",
    "status": "RUNNING",
    "duration": "45m0s",
    "runId": "abc-123"
  },
  "timestamp": "2026-02-25T12:45:00Z"
}
```

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
├── watchdog.go       # CheckMissedSchedules + CheckStuckRuns + Watchdog polling wrapper
└── watchdog_test.go  # Unit tests covering both detection modes
```

### Exports

| Export | Description |
|---|---|
| `CheckMissedSchedules(ctx, opts)` | Pure function, scans for missed schedule deadlines |
| `CheckStuckRuns(ctx, opts)` | Pure function, scans for runs stuck in non-terminal states |
| `CheckOptions` | Provider, CalendarReg, AlertFn, Logger, Now, StuckRunThreshold |
| `MissedSchedule` | Result struct: PipelineID, ScheduleID, Date, Deadline |
| `StuckRun` | Result struct: PipelineID, ScheduleID, Date, Status, Duration |
| `Watchdog` | Polling wrapper for local mode (runs both checks) |
| `New(provider, calReg, alertFn, logger, interval)` | Constructor |
| `Start(ctx)` / `Stop(ctx)` | Lifecycle methods |

Both core functions are pure functions with injectable dependencies (`CheckOptions.Now` for time, `CheckOptions.Provider` for storage). This makes them testable and reusable across all deployment modes. The `Watchdog` polling wrapper calls both `CheckMissedSchedules` and `CheckStuckRuns` on each scan.
