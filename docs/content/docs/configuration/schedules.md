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
| `cron` | string | no | Cron expression for schedule activation (5-field format) |
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

### Cron-Based Schedules

For time-based schedules, specify a cron expression. The pipeline activates at the cron time, then evaluates until the window expires or validation passes.

```yaml
schedule:
  cron: "0 8 * * *"
  timezone: America/New_York
  evaluation:
    window: 2h
    interval: 10m
```

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
| `expectedDuration` | duration | Expected time for the job to complete (used for SLA breach detection) |

### Deadline Evaluation

The SLA monitor Lambda runs as a parallel branch in the Step Functions state machine. It checks the current time against the configured deadline at regular intervals. If the deadline passes without the pipeline reaching a terminal state, the monitor emits an `sla.breached` event to EventBridge.

The deadline is interpreted in the schedule's configured `timezone` (or UTC if not set).

### EventBridge SLA Events

SLA breaches are published to the custom EventBridge bus. You can create rules to route these events to SNS, Lambda, or any other EventBridge target.

Example event:

```json
{
  "source": "interlock",
  "detail-type": "sla.breached",
  "detail": {
    "pipelineId": "gold-revenue",
    "deadline": "10:00",
    "timezone": "UTC"
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
