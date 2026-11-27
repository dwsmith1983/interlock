---
title: "Dry-Run / Shadow Mode"
weight: 20
---

# Dry-Run / Shadow Mode

Dry-run mode lets you evaluate a pipeline against real sensor data without triggering any jobs or starting Step Function executions. Interlock records what it *would* do as EventBridge events, giving you a complete picture of trigger timing, SLA projections, late data arrivals, and drift detection — all without side effects.

## When to Use Dry-Run

- **Onboarding a new pipeline.** Validate that sensor data, validation rules, and scheduling work correctly before committing compute resources.
- **Testing config changes.** Modify thresholds, rules, or SLA deadlines on a dry-run copy and compare its decisions against the live pipeline.
- **Comparing against an existing orchestrator.** Run Interlock in shadow mode alongside Airflow, Dagster, or Prefect to verify it makes the same triggering decisions.

## Setup

Add `dryRun: true` to your pipeline config. The pipeline must have `job.type` configured.

```yaml
pipeline:
  id: bronze-hourly-dryrun
dryRun: true

schedule:
  timezone: "UTC"
  trigger:
    key: files-landed
    check: gte
    field: files_processed
    value: 4
    cutoff: "08:00"

validation:
  trigger: "ALL"
  rules:
    - key: files-landed
      check: gte
      field: files_processed
      value: 4

job:
  type: glue
  config:
    jobName: bronze-etl

sla:
  deadline: "06:00"
  expectedDuration: "45m"

postRun:
  rules:
    - key: output-count
      check: gte
      field: count
      value: 1000
  driftField: "count"
```

Deploy the config the same way as any live pipeline: put the file in the module's `pipelines_path` directory, named after its `pipeline.id` (here `bronze-hourly-dryrun.yaml`). The module writes it to the control table as the pipeline's `CONFIG` row.

## What Happens

When the pipeline's trigger sensor is written, the stream-router Lambda checks it with the same trigger condition and validation rules as a live pipeline. Instead of starting a Step Function execution, it writes a `DRY_RUN#{schedule}#{date}` marker to the control table and publishes EventBridge events.

```
Trigger sensor write → stream-router
  → Trigger condition fails: no action
  → Current day excluded by the calendar: publish PIPELINE_EXCLUDED
  → DRY_RUN# marker for the date exists: publish DRY_RUN_LATE_DATA
  → Otherwise evaluate the validation rules once
     → Rules fail: no action (the next trigger sensor write tries again)
     → Rules pass:
        → Write the DRY_RUN# marker (7-day TTL)
        → Capture the post-run baseline (if postRun.rules are set)
        → Publish DRY_RUN_WOULD_TRIGGER
        → Publish DRY_RUN_SLA_PROJECTION (if sla.expectedDuration is set)
        → Publish DRY_RUN_COMPLETED
```

`DRY_RUN_COMPLETED` is always the last event of a would-trigger. After the marker exists, a write of a `postRun.rules` sensor for the date is compared with the baseline (see [Drift Detection](#drift-detection)) and a `JOB#` row for the date reports what a retry would do. A `RERUN_REQUEST` reports what a rerun would do, marker or not.

`DRY_RUN#` markers have a 7-day TTL and are automatically cleaned up by DynamoDB.

## Differences from a Live Pipeline

A dry-run pipeline reads the same config as a live one, but some of it does not apply:

- **No cutoff.** `schedule.trigger.cutoff` is still required, but it is not applied: a trigger sensor processed after the cutoff can still record a would-trigger, and no `SENSOR_DEADLINE_EXPIRED` is published.
- **No evaluation window.** The validation rules are evaluated once, on each trigger sensor write. A live pipeline re-evaluates every `schedule.evaluation.interval` until the window closes; a dry-run pipeline records nothing until a trigger sensor write finds the rules passing.
- **No watchdog checks.** The watchdog skips dry-run pipelines in every check except stale triggers and invalid configs. So there are no SLA schedules (no `SLA_WARNING` or `SLA_BREACH`), no `SCHEDULE_MISSED` or `IRREGULAR_SCHEDULE_MISSED`, no reconciliation of missed triggers, no `POST_RUN_SENSOR_MISSING` and no `RELATIVE_SLA_WARNING` or `RELATIVE_SLA_BREACH`.
- **No watch window.** Late data and drift are reported for as long as the date's marker exists (7 days), not for `postRun.watchWindow`.
- **Nothing runs.** No trigger row, no Step Functions execution, no job and no rerun request.

## Monitoring Events

Dry-run pipelines emit these event types to EventBridge:

| Event | Meaning |
|---|---|
| `DRY_RUN_WOULD_TRIGGER` | All validation rules passed — Interlock would have triggered the job |
| `DRY_RUN_SLA_PROJECTION` | Estimated completion time vs. deadline, with status `met`, `breach`, or `n/a` |
| `DRY_RUN_COMPLETED` | Last event of a would-trigger, with the SLA verdict (`met`, `breach`, or `n/a`) |
| `DRY_RUN_LATE_DATA` | Sensor data arrived after the trigger point was already recorded |
| `DRY_RUN_DRIFT` | Post-run sensor data changed from its baseline |
| `DRY_RUN_WOULD_RERUN` / `DRY_RUN_RERUN_REJECTED` | What a `RERUN_REQUEST` for the pipeline would do |
| `DRY_RUN_WOULD_RETRY` / `DRY_RUN_RETRY_EXHAUSTED` | What CompleteTrigger would decide for a `fail` or `timeout` job row |

Create an EventBridge rule to capture these events:

```json
{
  "source": ["interlock"],
  "detail-type": [
    { "prefix": "DRY_RUN_" }
  ]
}
```

Route events to CloudWatch Logs, SNS, or an SQS queue for analysis. The `DRY_RUN_*` events and `PIPELINE_EXCLUDED` are also on the module's alert rule, so with Slack configured they are posted to Slack. See the [Alerting reference](../../reference/alerting/#dry-run-events) for full event payload details.

## SLA Projection

When `sla.expectedDuration` is set, Interlock publishes a projection at the moment the validation rules pass. With `sla.deadline` it compares the projected completion with the deadline, using the same SLA calculation as production monitoring, including `sla.deadlineDayOffset`. A relative SLA (`sla.maxDuration`) is not projected: with `expectedDuration` and no `deadline` the status is `met`, with no `deadline` or `marginSeconds`. Without `expectedDuration` no projection is published, and `DRY_RUN_COMPLETED` has the status `n/a`.

The `DRY_RUN_SLA_PROJECTION` event includes:

| Field | Description |
|---|---|
| `status` | `"met"`, `"breach"`, or `"n/a"` for a run date of the other form than the deadline (a daily date under `:MM`, or an hourly one under `HH:MM`), which has no `deadline` or `marginSeconds` |
| `estimatedCompletion` | Trigger time + `expectedDuration` |
| `deadline` | Resolved breach deadline |
| `marginSeconds` | Seconds between estimated completion and deadline (negative = breach) |

This lets you tune `expectedDuration` before going live, avoiding false SLA alerts on day one.

## Drift Detection

If `postRun.rules` are set, Interlock captures a baseline of the post-run rule sensors at the moment validation passes. When the post-run sensor updates later, it compares the new value against the baseline. A post-run sensor missing from that snapshot takes its first later write as its baseline, as on a live pipeline.

If the delta exceeds `driftThreshold`, a `DRY_RUN_DRIFT` event is published with the previous count, current count, delta, and the sensor key that changed. This tells you whether your production pipeline would have triggered a drift re-run.

## Going Live

To promote a dry-run pipeline to live:

1. Remove `dryRun: true` from the pipeline config (or set it to `false`).
2. Deploy the updated config.

No cleanup is needed. Existing `DRY_RUN#` markers expire naturally via their 7-day TTL and do not interfere with `TRIGGER#` rows that live pipelines use.

## Side-by-Side Deployment

You can run a dry-run pipeline alongside a live pipeline that reads the same data. Use different pipeline IDs:

```yaml
# Live pipeline
pipeline:
  id: bronze-hourly
---
# Dry-run shadow
pipeline:
  id: bronze-hourly-dryrun
dryRun: true
```

Sensors belong to one pipeline: a sensor row's key is `PIPELINE#<id>`, so each pipeline reads only the sensors written under its own ID. The shadow pipeline needs its own sensor writes, under `PIPELINE#bronze-hourly-dryrun`: have each sensor source write every sensor twice, once under each pipeline's key. The live pipeline then triggers jobs, and the dry-run pipeline records observations. This is useful for validating config changes before applying them to the live pipeline.

## Troubleshooting

**No events appearing.** Verify that `schedule.trigger` names the sensor key being written, and that the sensor is written under the dry-run pipeline's own key (`PIPELINE#<dry-run id>`). Every pipeline needs a trigger with a cutoff; a config without one is rejected.

**Stale markers.** `DRY_RUN#` markers have a 7-day TTL. If you need to re-evaluate the same date, wait for the marker to expire or delete it manually from the control table.

**Calendar exclusions blocking evaluation.** Calendar exclusions apply to dry-run pipelines the same way as live pipelines. On an excluded day a passing trigger sensor write publishes `PIPELINE_EXCLUDED` instead of the dry-run events. Check your calendar config if events stop appearing on expected dates.

**SLA projection shows `met` but you expected `breach`.** The projection uses the same deadline resolution as production, including timezone handling and `sla.deadlineDayOffset`. Verify that `sla.deadline`, `sla.deadlineDayOffset` and `sla.timezone` match your expectations. A pipeline with `sla.expectedDuration` and `sla.maxDuration` but no `sla.deadline` always shows `met`, since a relative SLA is not projected.
