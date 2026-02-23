---
title: Schedules & Exclusions
weight: 2
description: Multi-schedule windows, SLA deadlines, calendar exclusions, and timezone handling.
---

## Multi-Schedule Support

Pipelines can define multiple schedule windows per day. Each schedule has its own evaluation cycle, lock, run log entry, and SLA deadline.

```yaml
schedules:
  - name: morning
    after: "06:00"
    deadline: "10:00"
    timezone: America/New_York
  - name: afternoon
    after: "14:00"
    deadline: "18:00"
    timezone: America/New_York
```

### ScheduleConfig Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Unique name within the pipeline (used as `scheduleID`) |
| `after` | string | no | `HH:MM` — schedule becomes active after this time |
| `deadline` | string | no | `HH:MM` — SLA deadline for this window |
| `timezone` | string | no | IANA timezone (e.g. `America/New_York`); falls back to pipeline SLA timezone, then UTC |

### Default Schedule

Pipelines without an explicit `schedules` list get an implicit default:

```yaml
schedules:
  - name: daily
```

The `daily` schedule has no `after` time (always active) and no per-schedule deadline (uses pipeline-level SLA if set).

### How Schedules Work

During each watcher tick (or Step Function execution):

1. `ResolveSchedules()` returns the configured schedules (or `[{Name: "daily"}]` if empty)
2. For each schedule, `IsScheduleActive()` checks if `now >= After` in the schedule's timezone
3. A separate lock is acquired per schedule: `eval:{pipelineID}:{scheduleID}`
4. Run logs are keyed by `(pipelineID, date, scheduleID)` — each schedule tracks independently
5. SLA deadlines prefer the schedule's `deadline` over the pipeline's `sla.evaluationDeadline`

### Hourly Schedules

For pipelines that run every hour, define 24 schedules:

```yaml
schedules:
  - name: h00
    after: "00:00"
    deadline: "01:00"
  - name: h01
    after: "01:00"
    deadline: "02:00"
  # ... through h23
```

The dedup key becomes `{pipeline}:{date}:{scheduleID}` (e.g. `my-pipeline:2026-02-23:h14`).

## SLA Configuration

SLAs enforce time-based constraints. Breaching an SLA fires an alert but does not cancel the pipeline.

```yaml
sla:
  evaluationDeadline: "10:00"
  completionDeadline: "12:00"
  timezone: America/New_York
  validationTimeout: "+45m"
```

### SLAConfig Fields

| Field | Type | Description |
|---|---|---|
| `evaluationDeadline` | string | `HH:MM` — all traits must reach READY by this time |
| `completionDeadline` | string | `HH:MM` — triggered job must finish by this time |
| `timezone` | string | IANA timezone for deadline interpretation |
| `validationTimeout` | string | Duration offset (e.g. `+45m`) — hard stop for evaluation |

### Deadline Resolution

When a schedule defines its own `deadline`, that takes priority:

1. **Schedule deadline** — `sched.Deadline` (if set)
2. **Pipeline evaluation SLA** — `sla.evaluationDeadline` (fallback)

The `ParseSLADeadline()` function converts an `HH:MM` string to an absolute time using the configured timezone and reference date.

### Validation Timeout

Unlike SLA deadlines (which alert but continue), `validationTimeout` is a hard stop. If evaluation hasn't completed within this offset from the schedule activation time, the Step Function transitions to a timeout state.

## Calendar Exclusions

Pipelines can be excluded from running on specific days of the week or calendar dates. When excluded, the pipeline is completely dormant — no locks, no evaluation, no SLA alerts.

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

Define reusable calendars in YAML files:

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

### Combining Calendar + Inline

Inline `days` and `dates` are merged with the named calendar. A date or day matching any source triggers exclusion.

```yaml
exclusions:
  calendar: holidays      # base exclusions
  days:
    - friday              # additional inline exclusion
  dates:
    - "2026-03-15"        # additional specific date
```

### Calendar Configuration

Calendar YAML files are loaded from directories listed in `calendarDirs`:

```yaml
calendarDirs:
  - ./calendars
```

### Exclusion Evaluation

`IsExcluded()` checks in this order:

1. Load the named calendar (if specified) from the registry
2. Merge calendar `days` + `dates` with inline `days` + `dates`
3. Resolve timezone from `pipeline.SLA.Timezone` (or UTC)
4. Compare current day-of-week (case-insensitive) against merged days
5. Compare current date (`YYYY-MM-DD`) against merged dates
6. If any match → pipeline is excluded for today

Active runs from a previous day are picked up on the next non-excluded day or via callback API.
