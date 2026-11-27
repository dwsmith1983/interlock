---
title: Alerting
weight: 1
description: EventBridge-based event publishing, detail types, and consumer patterns.
---

Interlock publishes all lifecycle and alert events to a custom EventBridge event bus. The framework includes two built-in consumers: an **event-sink** Lambda that logs all events to a DynamoDB events table, and an **alert-dispatcher** Lambda that delivers Slack notifications with message threading. You can also create custom rules to route events to SNS, SQS, Lambda, CloudWatch Logs, or any other EventBridge target.

## EventBridge Event Bus

The Terraform module creates a custom event bus named `{environment}-interlock-events`. Four of the six Lambda functions publish events to this bus using `events:PutEvents`: stream-router, orchestrator, sla-monitor and watchdog.

The bus name is available as a Terraform output:

```hcl
output "event_bus_name" {
  value = module.interlock.event_bus_name
}

output "event_bus_arn" {
  value = module.interlock.event_bus_arn
}
```

## Detail Types

Each event is published with a `detail-type` field that classifies the event. Use these values in EventBridge rule patterns for filtering and routing.

The event-sink records every event in the events table. The module's alert rule sends every type below to the alert queue (Slack) except `VALIDATION_PASSED`, `EVALUATION_RELEASED`, `JOB_TRIGGERED`, `POST_RUN_PASSED`, `POST_RUN_BASELINE_CAPTURED`, `MANUAL_RERUN_STILL_REQUIRED` and `WATCHDOG_DEGRADED`, which are recorded only. `INFRA_ALARM` reaches both through its own rules (see [CloudWatch Alarms](#cloudwatch-alarms)).

### SLA Events

Published by the **sla-monitor** Lambda via EventBridge Scheduler callbacks and during SLA cleanup in the Step Functions state machine.

| Detail Type | Meaning | When |
|---|---|---|
| `SLA_WARNING` | SLA warning threshold reached | The run has not finished by the warning timestamp: its trigger row is not `COMPLETED` or `FAILED_FINAL` (without a trigger row, its job log has no final result) |
| `SLA_BREACH` | SLA deadline exceeded | The run has not finished by the breach timestamp, by the same rule. A date that fails after its breach time, including a launch that runs out of retries, also gets one from the cancel step, which can follow the scheduled `SLA_BREACH` |
| `SLA_MET` | Run completed before its SLA breach time | Published when the run ends before the breach time, only if the date's trigger row is `COMPLETED`; a run that did not complete (it ended `FAILED_FINAL`, its final status could not be recorded, or its status could not be read) gets none. A run date of the other form than `sla.deadline` (a daily date under `:MM`, or an hourly one under `HH:MM`) gets neither `SLA_MET` nor `SLA_BREACH`, unless it has a relative SLA. A run whose final status could not be recorded keeps its SLA alerts while its row is `RUNNING`, besides the cancel step's verdict. A date that fails before its breach time gets its failure alert (`RETRY_EXHAUSTED`, `JOB_FAILED` and the like) and neither `SLA_MET` nor `SLA_BREACH`, because the cancel step removes the breach schedule |

### Lifecycle Events

Published by the **orchestrator** and **stream-router** Lambdas during the pipeline lifecycle. The **watchdog** publishes some of them too: `SFN_TIMEOUT`, `POST_RUN_SENSOR_MISSING`, `SENSOR_DEADLINE_EXPIRED`, and `JOB_TRIGGERED` when its reconciliation restarts a released date.

| Detail Type | Meaning | When |
|---|---|---|
| `VALIDATION_PASSED` | All validation rules passed | Readiness evaluation succeeds, before trigger |
| `VALIDATION_EXHAUSTED` | Evaluation window closed without passing, and the date ends | The window closed for a rerun. The trigger row becomes `FAILED_FINAL`, so this is the date's final alert; only a `RERUN_REQUEST` runs it again |
| `EVALUATION_RELEASED` | Evaluation window closed before the cutoff, and the date can start again | A first run's window closed before its cutoff: the trigger row is deleted and an `evaluation-released` job row written, so the next trigger sensor write or watchdog pass starts evaluation again. Detail carries `cutoff`, `cutoffAt` and `source: orchestrator`. Not an alert: it is recorded in the events table but is not on the alert rule |
| `JOB_TRIGGERED` | Pipeline job was triggered | Trigger fired successfully: the stream-router started an execution, the orchestrator launched the job, or the watchdog's reconciliation restarted a date whose evaluation was released (`source: reconciliation`) |
| `JOB_COMPLETED` | Triggered job completed successfully | The stream-router sees a `success` job log row (written by the job, by CheckJob after polling, or by the orchestrator for `lambda`), with the same gates as `JOB_FAILED` |
| `JOB_FAILED` | Triggered job failed | The stream-router sees a `fail` or `timeout` job log row: one the job wrote, one CheckJob wrote after polling a failure, the `fail` row the orchestrator writes when a `lambda` function returns an error, or the `timeout` row written when the job poll window closes. Only new rows for a date with a trigger row are reported. Detail carries `event`, `error` (cut to 2 KB), `runId` and, when set, `category` |
| `RETRY_EXHAUSTED` | All retry attempts consumed | Job failed `maxRetries` (or `maxCodeRetries` for permanent failures) times without success, or the job could not be launched within the Trigger state's `trigger_max_attempts` retries, or the orchestrator stayed throttled through that state's throttle retries (the message carries the last error, cut to 1 KB). The trigger row becomes `FAILED_FINAL`, so this is the date's final alert; nothing starts the date again on its own, and a `RERUN_REQUEST` runs it. If the step that publishes it after a failed launch (`TriggerRetryExhausted`) is itself throttled through its retries, or its publish keeps failing through them, no alert is sent, the row stays `RUNNING` and the execution still finishes through SLA cleanup; a `RERUN_REQUEST` is rejected as a run in progress until the watchdog closes the date with `SFN_TIMEOUT` once its stale deadline (`staleAt`) passes. A status write that fails after a successful publish is retried and can send `RETRY_EXHAUSTED` again. |
| `INFRA_FAILURE` | Unrecoverable infrastructure error | An execution failed in Evaluate, ValidationExhausted or CheckJob (a crash, throttling that outlasted the state's retries, or a ValidationExhausted store or publish error that did): the trigger row becomes `FAILED_FINAL`, so this is the date's final alert, and only a `RERUN_REQUEST` runs it again. The message ends in the Step Functions error and its cause (cut to 1 KB; for a failed Lambda call a JSON object with the `errorMessage`). The cause can name AWS identifiers (account ID, role, table or key ARNs), so whoever can read the alerts sees them; it holds no credentials. Detail carries `source: orchestrator`, the same `error` and `cause`, and an `actionHint`: a job that had been launched may still be running, so check it before requesting the rerun. If the event cannot be published or the status cannot be written through the retries of `RecordInfraFailure`, the row stays `RUNNING` until the watchdog's `SFN_TIMEOUT`; a status write that fails after a successful publish is retried and can send `INFRA_FAILURE` again. Also published when a failure retry's execution cannot start (`FAILED_FINAL`, detail `execution` and `error`) and when a `RERUN_REQUEST` finds no trigger row to reset |
| `SFN_TIMEOUT` | Step Functions execution timed out | Global `TimeoutSeconds` exceeded (configurable via `sfn_timeout_seconds` Terraform variable); the watchdog publishes it once the trigger row's `staleAt` passes, with `staleAt` in the detail, and closes the row `FAILED_FINAL` only after the publish succeeds, so a failed publish is retried on the next pass |
| `DATA_DRIFT` | Reserved | Defined and on the alert rule, but nothing publishes it; post-run drift is `POST_RUN_DRIFT` |
| `POST_RUN_DRIFT` | Post-run sensor changed after completion | Sensor value drifted from baseline after job completed, inside the [watch window](../../configuration/pipelines/#watch-window), while a rerun can follow: until `job.maxAutoRerunAge` (default `120h`) has passed since completion, with a `data-drift` rerun request. Past the age drift publishes `MANUAL_RERUN_REQUIRED` instead, since with no rerun the baseline stays as it is and every later write would repeat it |
| `POST_RUN_DRIFT_INFLIGHT` | Post-run sensor changed while job running | Informational: a post-run sensor drifted from the date's baseline while the trigger row is `RUNNING`. It fires only while a baseline from an earlier completion is still present: a first run and its retries have not completed, and an accepted rerun deletes the baseline when it starts, so none of them has one |
| `POST_RUN_FAILED` | Post-run rules failed | A post-run sensor arrived inside the watch window, did not drift, and the `postRun.rules` failed |
| `POST_RUN_PASSED` | Post-run rules passed | As `POST_RUN_FAILED`, when the rules pass. Not an alert |
| `POST_RUN_BASELINE_CAPTURED` | Post-run baseline written | A successful run's sensor values were captured as the drift baseline. Not an alert |
| `POST_RUN_SENSOR_MISSING` | No post-run sensor data received | On the first watchdog pass after the trigger row's `completedAt` plus `sensorTimeout`, the watchdog found no post-run sensor that exists and either differs from its completion-time baseline or was absent then. Only the daily run date equal to the current UTC date is checked, so an earlier date, an hourly run or a timeout that ends after UTC midnight gets none. Once per date, retried on the next pass if the publish fails; detail carries `source: watchdog`, `sensorTimeout`, `ruleKeys` and `actionHint` |
| `BASELINE_CAPTURE_FAILED` | Baseline capture error | Error occurred while capturing the post-run baseline at trigger completion |
| `SENSOR_DEADLINE_EXPIRED` | A run date passed its cutoff without starting | The run date is closed (`FAILED_FINAL`) by the stream-router or the watchdog's reconciliation when a sensor is processed after the cutoff, by the watchdog on its next pass for a date with a `RUN#` row, or by the orchestrator when a run is still evaluating at the cutoff, and from then nothing more starts for it; `source` says which; detail has `cutoff` and `cutoffAt`; run it manually with `RERUN_REQUEST` |
| `PIPELINE_EXCLUDED` | Pipeline excluded by calendar | Sensor, rerun, job-failure, or post-run drift skipped due to calendar exclusion. For a rerun request, detail carries the request's `reason` and `why` (`excluded by calendar`), and no job row is written |

### Rerun Events

Published by the **stream-router** when processing rerun requests and late data arrivals.

| Detail Type | Meaning | When |
|---|---|---|
| `LATE_DATA_ARRIVAL` | Sensor updated after job completed | The trigger sensor was written again for a `COMPLETED` date whose last run succeeded, inside the [watch window](../../configuration/pipelines/#watch-window). A `late-data` rerun request follows only until `job.maxAutoRerunAge` (default `120h`) has passed since completion. Writes no job row; a `rerun-accepted` job row does not count as a run. Once the rerun budget is spent, each further late trigger-sensor write inside the window publishes this again, followed by `RERUN_REJECTED` until the age; past it no request is written, so no `RERUN_REJECTED` follows, and `MANUAL_RERUN_REQUIRED` (or `MANUAL_RERUN_STILL_REQUIRED`) asks for a manual rerun instead |
| `RERUN_REJECTED` | Rerun request rejected | The date's run is still `RUNNING` (any reason; no budget is spent), the rerun budget is spent, or the circuit breaker refused a manual request: the date's last run succeeded (a `rerun-accepted` job row does not count as a run), at least one sensor has a readable `updatedAt` ([accepted forms](../../configuration/pipelines/#pushing-sensor-data)), and none is later than that run. Late-data and drift requests skip the circuit breaker. Writes no job row; detail carries `reason` (the request's: `manual`, `late-data`, `data-drift` or the value written) and `why` (the rejection, as in the message) |
| `RERUN_ACCEPTED` | Rerun request accepted | Rerun passed the budget check, the date's run was not in progress (`RUNNING`), a manual request also passed the circuit breaker, and the trigger lock was reset |
| `MANUAL_RERUN_REQUIRED` | A completed date needs a manual rerun | The date's last run succeeded and its automatic reruns have ended, but late data arrived inside the watch window past `job.maxAutoRerunAge` (`source: sensor`, published after `LATE_DATA_ARRIVAL`) or after the window (alone), a post-run sensor drifted from its baseline past the age or after the window (`source: sensor`, in place of `POST_RUN_DRIFT`; after the window only while the date's baseline row exists, which expires 31 days after it is written), or an automatic rerun request was skipped when the stream-router handled it, past the age or the window (`source: rerun-request`). Since the last run must be known to have succeeded, nothing is published once the run's job log row has expired (30 days after it is written). Published once per date, reason and completion: a marker row `manual-rerun-alert-<reason>#<date>` keeps the `completedAt` it was published for, written after the publish succeeds, so a rerun that completes later gets another. A date with no `completedAt`, or an execution date the calendar excludes (a manual rerun of it is refused with `PIPELINE_EXCLUDED`), gets none. Detail carries `reason` (`late-data` or `data-drift`), `blockedBy` (`job.maxAutoRerunAge` or `postRun.watchWindow`), `source`, `jobType` and `job` (the job's identifiers only, such as an `http` URL without user info, query or fragment and its method, a Glue `jobName` or a Lambda `functionName`; never headers, bodies, payloads or arguments), `lastRunId`, `startedAt`, `completedAt`, `finishedAgo`, `maxAutoRerunAge`, `watchWindow`, for a sensor write `sensorKey`, `sensorData` (left out with `sensorDataOmitted: true` above 32 KB) and `arrivedAt`, for drift also `driftField`, `previousCount`, `currentCount`, `delta`, `driftThreshold` and `baseline` (the baseline entry it was compared with, left out with `baselineOmitted: true` above 32 KB), for a request its `requestedAt`, then `rerunRequest` (the `table`, `PK`, `SK` and `reason: manual` of the row to write) and an `actionHint`. A key whose value is unknown is left out. Sent to Slack, also shown in the channel (see [Alert Dispatcher](#alert-dispatcher-slack)), as the reason, job, last run, finish time, arrival or request time, sensor key, for drift a line `Drift: <driftField> <previousCount> to <currentCount> (change <delta>, threshold <driftThreshold>)`, and action hint; never the sensor data, the baseline or the request row |
| `MANUAL_RERUN_STILL_REQUIRED` | The same manual rerun is still needed | Each later arrival for a date, reason and completion that already had `MANUAL_RERUN_REQUIRED`, with the same detail. Recorded in the events table only, not sent to Slack |

The manual-rerun events publish an `http` or `airflow` job's URL and a `databricks` workspace URL with their path (the query, fragment and user info are removed), so keep credentials in `headers`. They also copy the late sensor's data into the event unchanged (left out above 32 KB), so sensor rows should not hold secrets.

### Dry-Run Events

Published by the **stream-router** Lambda for pipelines with `dryRun: true`. These events record what Interlock *would* do without executing any jobs or starting Step Functions.

| Detail Type | Meaning | When |
|---|---|---|
| `DRY_RUN_WOULD_TRIGGER` | All validation rules passed | Interlock would have triggered the pipeline job at this time; detail carries `triggeredAt` and `rulesEvaluated` |
| `DRY_RUN_LATE_DATA` | Sensor updated after trigger point | A trigger sensor write that passes the trigger condition arrived after the date's dry-run marker was written; detail carries `triggeredAt` and `lateBy` |
| `DRY_RUN_SLA_PROJECTION` | Estimated completion vs. deadline | Published after `DRY_RUN_WOULD_TRIGGER` whenever `sla.expectedDuration` is set; projects whether `sla.deadline` would be met |
| `DRY_RUN_DRIFT` | Post-run sensor data changed | Sensor value changed from its baseline; it would have triggered a drift re-run |
| `DRY_RUN_COMPLETED` | The dry-run trigger was recorded | Always the last event of a would-trigger, published right after `DRY_RUN_WOULD_TRIGGER` and any projection; carries the SLA verdict (`met`, `breach`, or `n/a`) |
| `DRY_RUN_WOULD_RERUN` | A rerun request would run | A `RERUN_REQUEST` for a dry-run pipeline passed the exclusion check, its rerun budget, the run-in-progress check (the date is not `RUNNING`) and the circuit breaker; detail carries `reason` and `circuitBreaker` (`passed`, `skipped (no job history)`, or `skipped (automatic request)` for a `late-data` or `data-drift` request) |
| `DRY_RUN_RERUN_REJECTED` | A rerun request would be refused | The date is excluded, the budget is spent, the date's run is still `RUNNING`, or the circuit breaker found no new sensor data for a manual request; detail carries `reason`, the kind of rejection (`excluded by calendar`, `limit exceeded`, `run in progress` or `circuit breaker`), not the request's reason as in `RERUN_REJECTED` |
| `DRY_RUN_WOULD_RETRY` | A job failure would be retried | A `fail` or `timeout` row for a dry-run pipeline with retry budget left (`retries: 0`) |
| `DRY_RUN_RETRY_EXHAUSTED` | A job failure would end the date | As `DRY_RUN_WOULD_RETRY`, when the budget is zero or the date is excluded |

The `DRY_RUN_SLA_PROJECTION` detail includes `status` (`"met"`, `"breach"`, or `"n/a"`), `triggeredAt`, `expectedDuration`, `estimatedCompletion` (trigger time plus `expectedDuration`), `deadline`, and `marginSeconds` fields. In dry-run events `deadline` is the breach time (RFC 3339, UTC), not the configured deadline. A run date of the other form than `sla.deadline` has the status `"n/a"` and no `deadline` or `marginSeconds`. A relative SLA (`sla.maxDuration`) is not projected: with `expectedDuration` and no `deadline` the status is `"met"`, with no `deadline` or `marginSeconds`.

The `DRY_RUN_COMPLETED` detail includes `triggeredAt` and `slaStatus` (`"met"`, `"breach"`, or `"n/a"`). With a projection it also has `estimatedCompletion`, and `deadline` when the projection computed one; without `sla.expectedDuration` the status is `"n/a"`.

### Watchdog Events

Published by the **watchdog** Lambda, invoked on an EventBridge schedule (default: every 5 minutes).

| Detail Type | Meaning | When |
|---|---|---|
| `RELATIVE_SLA_WARNING` | A relative SLA (`sla.maxDuration`) is at risk | Once per date (daily or hourly, from 8 days before to 2 days after the current UTC date) whose run has not finished (as for `SLA_WARNING`), on the first watchdog pass at or after first sensor arrival (for a run the watchdog started, the time it started it) + `maxDuration` - `expectedDuration` (75% of `maxDuration` without `expectedDuration`) and before the breach; detail carries `maxDuration`, `expectedDuration`, `sensorArrivalAt`, `warningAt` and `breachAt` |
| `RELATIVE_SLA_BREACH` | A relative SLA (`sla.maxDuration`) was exceeded | Once per date (daily or hourly, from 8 days before to 2 days after the current UTC date) whose run has not finished (as for `SLA_WARNING`), on the first watchdog pass at or after first sensor arrival (for a run the watchdog started, the time it started it) + `maxDuration`, up to 24 hours after that time. A breach not sent within those 24 hours (a longer watchdog outage, say) is never sent, and a rerun started later gets none. Detail carries `maxDuration`, `sensorArrivalAt` and `breachAt` |
| `IRREGULAR_SCHEDULE_MISSED` | An inclusion-calendar date has no run | Once per `schedule.include` date with no trigger row, once the date has begun in `schedule.timezone` (UTC when unset) and, when set, `schedule.time` on that date has passed; without `schedule.time` it fires at local midnight of the date. Only dates up to 8 days before the current date in `schedule.timezone` are checked, at most the 3 most recent. Retried on the next pass if the publish fails |
| `SCHEDULE_MISSED` | The job for a run date has not started by the cron's start-by time, so the SLA is at risk | Once per `RUN#` date of a cron pipeline, on the first watchdog pass after the start-by time; detail carries `cron` and `startBy`. A date that already ended `COMPLETED` or `FAILED_FINAL` (for example after `RETRY_EXHAUSTED`) is not reported |
| `TRIGGER_RECOVERED` | Sensor trigger condition met but no trigger existed | Watchdog re-evaluated sensor data and self-healed a trigger the stream-router missed. The recovered run records its first sensor arrival at this start, unless one is already recorded, so its SLA verdict counts `maxDuration` from it. A restart of a date whose evaluation was released publishes `JOB_TRIGGERED` instead |
| `WATCHDOG_DEGRADED` | One or more watchdog checks failed | The message lists the failed checks; there is no inner `detail`, and `pipelineId` is empty (the event-sink row's PK is `PIPELINE#`). Not an alert |
| `CONFIG_INVALID` | A pipeline's `CONFIG` row was rejected, so that pipeline does not run | At the end of a watchdog pass, for a row whose PK is not `PIPELINE#<id>`, whose `config` is missing, not a string or not valid JSON for the config, or that fails validation (for example no `schedule.trigger` or no `schedule.trigger.cutoff`, an `sla` block with neither `deadline` nor `maxDuration`, a `deadline` that is not `HH:MM` or `:MM` or has no positive `expectedDuration`, a cutoff earlier than the `sla.deadline` breach, a trigger condition or `validation`/`postRun` rule that can never pass (an unknown `check`, a missing `key` or `field`, `equals` without a `value`, a threshold that is not a number, an `age_lt` that is not a positive Go duration), a rule `key` with `#` or equal to one of Interlock's own sensor row names, a `validation.trigger` other than `ALL` or `ANY` or `ANY` with no rules, or a `schedule.evaluation` window or interval that is not whole seconds of at least `1s`, an interval longer than the window, a window that allows more than 1000 evaluations, or a window plus `job.jobPollWindowSeconds` plus the longest [retry delay](../../configuration/pipelines/#retry-budgets) longer than `sfn_timeout_seconds`, a sum that is only a floor for the execution's length, so leave headroom). Once per broken config version and day: a marker row `CONFIG_INVALID#<hash>` with a 24 h TTL holds it, so a changed but still broken config reports at once, while a config fixed and broken again with the same content within 24 h does not report again. At most 20 reports per pass; the rest follow on later passes. `pipelineId` is the parsed ID, or the raw PK; detail carries `reason` (cut to 1 KB) and `hash` (first 16 hex characters of the SHA-256 of the `config` string, absent when there is none) |

## Event Payload Structure

Events follow the standard EventBridge envelope format:

```json
{
  "version": "0",
  "id": "abc123",
  "source": "interlock",
  "detail-type": "SLA_WARNING",
  "time": "2026-03-01T10:00:00Z",
  "region": "us-east-1",
  "resources": [],
  "detail": {
    "pipelineId": "gold-revenue",
    "scheduleId": "stream",
    "date": "2026-03-01",
    "message": "pipeline gold-revenue: SLA_WARNING",
    "timestamp": "2026-03-01T10:00:00.482913Z",
    "detail": {
      "status": "RUNNING",
      "source": "schedule",
      "actionHint": "pipeline running \u2014 may complete before breach",
      "deadline": "10:30",
      "timezone": "UTC",
      "breachAt": "2026-03-01T10:30:00Z"
    }
  }
}
```

The outer `detail` object always includes `pipelineId`. `scheduleId` is `cron` for a pipeline with `schedule.cron` and `stream` otherwise. The inner `detail` map holds the fields of that event type; an event without any has none.

## Creating EventBridge Rules

Subscribe to events by creating EventBridge rules that match on `detail-type`. Route matched events to any supported EventBridge target.

### Route SLA Alerts to SNS

```hcl
resource "aws_cloudwatch_event_rule" "sla_alerts" {
  name           = "interlock-sla-alerts"
  event_bus_name = module.interlock.event_bus_name

  event_pattern = jsonencode({
    "detail-type" = ["SLA_WARNING", "SLA_BREACH"]
  })
}

resource "aws_cloudwatch_event_target" "sla_to_sns" {
  rule           = aws_cloudwatch_event_rule.sla_alerts.name
  event_bus_name = module.interlock.event_bus_name
  target_id      = "sla-to-sns"
  arn            = aws_sns_topic.alerts.arn
}
```

### Route All Events to CloudWatch Logs

```hcl
resource "aws_cloudwatch_event_rule" "all_events" {
  name           = "interlock-all-events"
  event_bus_name = module.interlock.event_bus_name

  event_pattern = jsonencode({
    source = ["interlock"]
  })
}

resource "aws_cloudwatch_event_target" "to_cloudwatch" {
  rule           = aws_cloudwatch_event_rule.all_events.name
  event_bus_name = module.interlock.event_bus_name
  target_id      = "to-cloudwatch"
  arn            = aws_cloudwatch_log_group.interlock_events.arn
}
```

### Route Job Failures to a Lambda for Custom Handling

```hcl
resource "aws_cloudwatch_event_rule" "job_failures" {
  name           = "interlock-job-failures"
  event_bus_name = module.interlock.event_bus_name

  event_pattern = jsonencode({
    "detail-type" = ["JOB_FAILED", "RETRY_EXHAUSTED"]
  })
}

resource "aws_cloudwatch_event_target" "failure_handler" {
  rule           = aws_cloudwatch_event_rule.job_failures.name
  event_bus_name = module.interlock.event_bus_name
  target_id      = "failure-handler"
  arn            = aws_lambda_function.failure_handler.arn
}
```

### Filter by Pipeline ID

```hcl
resource "aws_cloudwatch_event_rule" "gold_pipeline_events" {
  name           = "interlock-gold-pipeline"
  event_bus_name = module.interlock.event_bus_name

  event_pattern = jsonencode({
    source      = ["interlock"]
    detail = {
      pipelineId = ["gold-revenue"]
    }
  })
}
```

## Built-In Observability

### Event Sink

The Terraform module deploys an `event-sink` Lambda that captures all EventBridge events to a DynamoDB events table. This provides a queryable audit log without any additional configuration.

The events table uses:
- **PK**: `PIPELINE#{pipelineId}` — all events for a pipeline
- **SK**: `{tsMillis}#{eventType}#{hash}`, sorted by the event's time in epoch milliseconds. `hash` is the first 16 hex characters of the SHA-256 of the event's content. Two events of one type in the same millisecond each keep their row. An event delivered more than once is one row.
- **GSI1**: `eventType` → `timestamp` — query all events of a given type

The table is named `{env}-interlock-events`. Each event row also holds `eventType`, `timestamp` (epoch milliseconds, a number), `pipelineId`, `scheduleId`, `date` (the run date, empty for events without one), `message` and `ttl`. An event with details has them in the map `detail`, whose keys differ per event type. A `JOB_FAILED` row as plain JSON, without `PK`, `SK`, `eventType`, `timestamp` and `ttl`:

```json
{
  "pipelineId": "gold-revenue",
  "scheduleId": "stream",
  "date": "2026-03-04",
  "message": "job fail for gold-revenue: table not found",
  "detail": {
    "event": "fail",
    "error": "table not found",
    "runId": "jr_0123456789",
    "category": "PERMANENT"
  }
}
```

The details of a scheduled `SLA_WARNING` or `SLA_BREACH` row are `status` (the trigger row's, or `not started`), `source` (`schedule`), `actionHint`, `deadline`, `timezone` and `breachAt`. Those of `SLA_MET` and of the `SLA_BREACH` sent when the run ends are:

- `source` (`cancel`), `breachAt` and `status` (the trigger row's). An `SLA_BREACH` whose row could not be read has no `status`.
- `deadline` and `timezone` when the verdict is against `sla.deadline`.
- `maxDuration` and `sensorArrivalAt` instead when it is against a relative SLA. With both a `deadline` and a `maxDuration`, the relative SLA decides once the first sensor arrival is known.

`deadline` is the configured `HH:MM` or `:MM`. `timezone` is `sla.timezone`, or `UTC` when unset. `breachAt` is the breach time in UTC (RFC 3339). SLA schedules created by v0.11.0 or earlier keep their old payload after an upgrade. Their `SLA_WARNING` has `breachAt` but no `deadline` or `timezone`; their `SLA_BREACH` has none of the three.

`INFRA_ALARM` rows are under `PIPELINE#INFRASTRUCTURE` and carry the time of the alarm's state change.

Records expire after a configurable TTL (default: 90 days via the `events_table_ttl_days` variable).

An event the event-sink could not write is kept for 14 days in the event-sink DLQ, which has its own [alarm](#cloudwatch-alarms). See [Unwritten Events](../../deployment/terraform/#unwritten-events) for what a message holds and how to write the event later.

#### Querying Events

One pipeline in a time range. `SK` is a string and starts with the milliseconds, so `:from` is the first millisecond to return and `:to` the one after the last:

```bash
aws dynamodb query --table-name dev-interlock-events \
  --key-condition-expression 'PK = :pk AND SK BETWEEN :from AND :to' \
  --expression-attribute-values '{
    ":pk": {"S": "PIPELINE#gold-revenue"},
    ":from": {"S": "1772582400000"},
    ":to": {"S": "1772668800000"}
  }'
```

One event type across all pipelines. `timestamp` is a number and a DynamoDB reserved word, and both ends of the range are included:

```bash
aws dynamodb query --table-name dev-interlock-events --index-name GSI1 \
  --key-condition-expression 'eventType = :type AND #ts BETWEEN :from AND :to' \
  --expression-attribute-names '{"#ts": "timestamp"}' \
  --expression-attribute-values '{
    ":type": {"S": "JOB_FAILED"},
    ":from": {"N": "1772582400000"},
    ":to": {"N": "1772668799999"}
  }'
```

One run date of a pipeline. `date` is a reserved word too. The filter runs after the rows are read, so this query reads every event row of the pipeline. To read less, replace `SK < :letters` with a range of milliseconds as in the first query. Make it wide enough: events for a run date are stamped when they happen, which can be days later.

```bash
aws dynamodb query --table-name dev-interlock-events \
  --key-condition-expression 'PK = :pk AND SK < :letters' \
  --filter-expression '#d = :date' \
  --expression-attribute-names '{"#d": "date"}' \
  --expression-attribute-values '{
    ":pk": {"S": "PIPELINE#gold-revenue"},
    ":letters": {"S": "A"},
    ":date": {"S": "2026-03-04"}
  }'
```

The alert-dispatcher keeps its Slack thread records in the same table under the same `PK`, with an `SK` that starts with `THREAD`. A query on `PK` alone returns them too; `SK < "A"` leaves them out. The table does not record whether an alert was posted to Slack: an alert that could not be posted ends in the alert DLQ, which has its own [alarm](#cloudwatch-alarms).

### Alert Dispatcher (Slack)

The module deploys an `alert-dispatcher` Lambda that reads from an SQS alert queue and posts formatted notifications to Slack using the Bot API (`chat.postMessage`).

**Configuration:**

| Variable | Description |
|---|---|
| `slack_secret_arn` | ARN of a Secrets Manager secret whose value is the bot token as a plain string (not JSON), with `chat:write` scope. Read at the dispatcher's cold start |
| `slack_channel_id` | Channel ID to post alerts to |
| `slack_bot_token` | Deprecated: the token as a plain Terraform value (sensitive). Setting it without `slack_secret_arn` makes the plan print a deprecation warning. When both are set, the secret wins |

With `slack_bot_token`, `slack_secret_arn` and `slack_channel_id` all empty, the dispatcher runs in logging-only mode and writes each alert to its log; once a token or secret is set, `slack_channel_id` is required.

**Message text**: each message starts with an icon. A red circle marks failures, timeouts, SLA breaches, missed schedules and cutoffs, post-run drift, `CONFIG_INVALID`, `INFRA_ALARM` and `MANUAL_RERUN_REQUIRED`; a yellow circle marks `SLA_WARNING` and `RELATIVE_SLA_WARNING`; a check mark marks `SLA_MET`. Every other event gets an info icon, among them `POST_RUN_SENSOR_MISSING`, `POST_RUN_DRIFT_INFLIGHT`, `RERUN_REJECTED` and the dry-run events. In event values, control characters (newlines and tabs too), line and paragraph separators and bidirectional controls become spaces, and `&`, `<` and `>` are escaped. An SLA alert with a `deadline` shows it as `Deadline 06:00 America/New_York (2026-03-01T11:00:00Z)`: the configured deadline, its time zone and the breach time in UTC.

**Message threading**: alerts with a run date share one Slack thread per pipeline, schedule and date. Alerts without one (`CONFIG_INVALID` and `INFRA_ALARM`) share one thread per pipeline and UTC day. The day is the one on which the alert was queued for Slack, not the one on which it was delivered. Every `INFRA_ALARM` has the pipeline `INFRASTRUCTURE`, so a day's alarms share a thread. The first alert starts the thread and later ones reply in it. Replies in a day thread also show in the channel; replies in a run date's thread do not, except `MANUAL_RERUN_REQUIRED`, which can come days after the date's other alerts. A thread belongs to its channel: after `slack_channel_id` changes, the next alert of each thread starts it again in the new channel. Thread records are stored in the events table and expire with the same TTL.

**Delivery retries**: an alert that could not be posted is tried again after 60, 120, 240 and 480 seconds, then every 900. A random extra of up to half the wait is added, so alerts that failed together do not come back together. A `Retry-After` header (whole seconds) longer than the wait replaces it, up to 3600 seconds, with the same extra. That makes 10 tries over about 90 to 135 minutes, or up to about 10 hours when Slack keeps asking for an hour. One wait after the last try, SQS moves the alert to the alert DLQ, which keeps it for 14 days from when it was first queued. A message that cannot be read takes the same path. If the dispatcher could not set a wait (it timed out or did not start, or SQS refused), the alert comes back after the queue's 180 second visibility timeout; 10 tries that all end that way take about 27 minutes.

- A post that timed out may already have been delivered, so a retry can show the same alert twice.
- A retried alert can arrive after later alerts of the same date, and the first one posted starts the thread.
- A permanent error, such as a revoked token or an unknown channel, is retried like a passing one: 10 tries over about 90 to 135 minutes, then the DLQ 15 to 23 minutes later. The alert is not posted. The DLQ depth alarm fires, but its own Slack message fails the same way, and the Lambda error alarm does not count a failed post. Set `sns_alarm_topic_arn` to hear about it. Once the cause is fixed, alerts still in the queue are delivered at their next try (a new secret value is read at the dispatcher's next cold start). Those in the DLQ stay until you redrive them to the alert queue.

**SLA warning suppression**: when an SLA breach has already occurred, the `fire-alert` handler suppresses the corresponding `SLA_WARNING` to prevent duplicate notifications.

## Consumer Patterns

Since EventBridge supports many target types, you can build any alert delivery pattern:

| Pattern | EventBridge Target | Use Case |
|---|---|---|
| PagerDuty / Slack | SNS topic with subscription | On-call alerting for SLA breaches |
| Audit log | CloudWatch Logs log group | Compliance and debugging |
| Custom processing | Lambda function | Enrich, deduplicate, or aggregate events |
| Queue for batch processing | SQS queue | Downstream systems that process events in batches |
| Cross-account delivery | EventBridge in another account | Centralized observability |

## CloudWatch Alarms

The Terraform module creates CloudWatch alarms that monitor infrastructure health independently of pipeline events. Alarm state changes are reshaped into `INFRA_ALARM` events via EventBridge input transformers. Every state change of an Interlock alarm (to `ALARM`, `OK` or `INSUFFICIENT_DATA`) goes to event-sink; only a change to `ALARM` goes to the alert queue and Slack.

### Alarm Categories

| Category | Count | Metric | Threshold |
|---|---|---|---|
| Lambda errors | 6 (one per function) | `Errors` | `>= 1` per 5-minute period |
| Step Functions failures | 1 | `ExecutionsFailed` | `>= 1` per 5-minute period |
| DLQ depth | 4 (control, joblog, alert, event-sink) | `ApproximateNumberOfMessagesVisible` | `>= 1` |
| Stream iterator age | 1 (stream-router, covers the control and joblog streams) | `IteratorAge` | `>= 300,000ms` (5 minutes) |
| Alert rule delivery | 2 (the `alert_events` and `cw_alarm_alert` rules, which send alerts to the alert queue) | `FailedInvocations` | `>= 1` per 5-minute period |

### How It Works

1. CloudWatch changes an alarm's state, for example to `ALARM` when a metric crosses its threshold
2. The alarm state change publishes to the default EventBridge bus
3. Two EventBridge rules match the module's alarms (names starting `{environment}-interlock-`), each with an **input transformer** that reshapes the change into an `INFRA_ALARM` event with the standard Interlock event structure
4. One rule sends every state change to event-sink (→ events table); the other sends only changes to `ALARM` to the SQS alert queue (→ alert-dispatcher → Slack)

### SNS Integration

Optionally route alarm notifications to an SNS topic for external consumers (PagerDuty, email, etc.):

```hcl
module "interlock" {
  source = "path/to/interlock/deploy/terraform"

  sns_alarm_topic_arn = aws_sns_topic.ops_alerts.arn
  # ...
}
```

When `sns_alarm_topic_arn` is set, every alarm notifies the topic both on entering `ALARM` and on returning to `OK` (alarm and OK actions), alongside the EventBridge route.
