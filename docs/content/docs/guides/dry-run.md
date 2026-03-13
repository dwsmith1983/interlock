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

Add `dryRun: true` to your pipeline config. The pipeline must have `schedule.trigger` (sensor-driven evaluation) and `job.type` configured.

```yaml
id: bronze-hourly-dryrun
dryRun: true

schedule:
  trigger: "sensor"
  timezone: "UTC"

validation:
  trigger: all_pass
  rules:
    - field: "files_processed"
      operator: ">="
      value: 4

job:
  type: glue
  name: bronze-etl

sla:
  deadline: "06:00"
  expectedDuration: "45m"

postRun:
  rules:
    - field: "count"
      operator: ">="
      value: 1000
  driftField: "count"
```

Deploy the config the same way as any live pipeline — via `jsonencode(yamldecode(...))` in Terraform or direct DynamoDB writes.

## What Happens

When sensor data arrives, the stream-router Lambda evaluates the dry-run pipeline through the same validation path as a live pipeline. Instead of starting a Step Function execution, it writes a `DRY_RUN#` marker to the control table and publishes EventBridge events.

```
Sensor arrives → Stream-router
  → Check calendar exclusions (still enforced)
  → Check for existing DRY_RUN# marker
     → Marker exists: publish DRY_RUN_LATE_DATA event
     → No marker: evaluate validation rules
        → Rules fail: no action
        → Rules pass:
           → Write DRY_RUN# marker (7-day TTL)
           → Capture post-run baseline (if PostRun configured)
           → Publish DRY_RUN_WOULD_TRIGGER event
           → Compute and publish DRY_RUN_SLA_PROJECTION (if SLA configured)
```

`DRY_RUN#` markers have a 7-day TTL and are automatically cleaned up by DynamoDB.

## Monitoring Events

Dry-run pipelines emit five event types to EventBridge:

| Event | Meaning |
|---|---|
| `DRY_RUN_WOULD_TRIGGER` | All validation rules passed — Interlock would have triggered the job |
| `DRY_RUN_SLA_PROJECTION` | Estimated completion time vs. deadline — `met` or `breach` status |
| `DRY_RUN_COMPLETED` | Observation loop closed — carries the SLA verdict (`met`, `breach`, or `n/a`) |
| `DRY_RUN_LATE_DATA` | Sensor data arrived after the trigger point was already recorded |
| `DRY_RUN_DRIFT` | Post-run sensor data changed from the baseline captured at trigger time |

Create an EventBridge rule to capture these events:

```json
{
  "source": ["interlock"],
  "detail-type": [
    { "prefix": "DRY_RUN_" }
  ]
}
```

Route events to CloudWatch Logs, SNS, or an SQS queue for analysis. See the [Alerting reference](../../reference/alerting/) for full event payload details.

## SLA Projection

When `sla.expectedDuration` and `sla.deadline` are configured, Interlock computes a projected completion time at the moment validation rules pass. The projection reuses the same SLA calculation logic as production monitoring (including hourly pipeline T+1 adjustment).

The `DRY_RUN_SLA_PROJECTION` event includes:

| Field | Description |
|---|---|
| `status` | `"met"` or `"breach"` |
| `estimatedCompletion` | Trigger time + `expectedDuration` |
| `deadline` | Resolved breach deadline |
| `marginSeconds` | Seconds between estimated completion and deadline (negative = breach) |

This lets you tune `expectedDuration` before going live, avoiding false SLA alerts on day one.

## Drift Detection

If `postRun` is configured, Interlock captures a baseline snapshot of the drift field at the moment validation passes. When the post-run sensor updates later, it compares the new value against the baseline.

If the delta exceeds `driftThreshold`, a `DRY_RUN_DRIFT` event is published with the previous count, current count, delta, and the sensor key that changed. This tells you whether your production pipeline would have triggered a drift re-run.

## Going Live

To promote a dry-run pipeline to live:

1. Remove `dryRun: true` from the pipeline config (or set it to `false`).
2. Deploy the updated config.

No cleanup is needed. Existing `DRY_RUN#` markers expire naturally via their 7-day TTL and do not interfere with `TRIGGER#` rows that live pipelines use.

## Side-by-Side Deployment

You can run a dry-run pipeline alongside a live pipeline watching the same sensors. Use different pipeline IDs:

```yaml
# Live pipeline
id: bronze-hourly

# Dry-run shadow
id: bronze-hourly-dryrun
dryRun: true
```

Both pipelines receive the same sensor events. The live pipeline triggers jobs; the dry-run pipeline records observations. This is useful for validating config changes before applying them to the live pipeline.

## Troubleshooting

**No events appearing.** Verify that `schedule.trigger` is set to `"sensor"`. Dry-run requires sensor-driven evaluation — cron-only pipelines won't produce events.

**Stale markers.** `DRY_RUN#` markers have a 7-day TTL. If you need to re-evaluate the same date, wait for the marker to expire or delete it manually from the control table.

**Calendar exclusions blocking evaluation.** Calendar exclusions apply to dry-run pipelines the same way as live pipelines. Check your calendar config if events stop appearing on expected dates.

**SLA projection shows `met` but you expected `breach`.** The projection uses the same deadline resolution as production, including timezone handling and T+1 adjustment for hourly pipelines. Verify that `sla.deadline` and `sla.timezone` match your expectations.
