---
title: AWS Architecture
weight: 2
description: DynamoDB single-table design, Step Functions state machine, and Lambda handlers.
---

The AWS variant replaces the local watcher loop with an event-driven architecture using DynamoDB Streams, Step Functions, and Lambda. A separate watchdog Lambda runs on an EventBridge schedule to detect silently missed pipelines.

## High-Level Flow

```
DynamoDB Stream → stream-router Lambda → Step Function execution
                                              │
                        ┌─────────────────────┼─────────────────────┐
                        ↓                     ↓                     ↓
                  orchestrator           evaluator              trigger
                   (Lambda)              (Lambda)              (Lambda)
                        │                     │                     │
                        ↓                     ↓                     ↓
                  DynamoDB table      DynamoDB table         Glue / EMR /
                  (state, locks,      (trait results)       Step Functions /
                   run logs)                                 Databricks
                                                                │
                                                                ↓
                                                           run-checker
                                                            (Lambda)
```

## DynamoDB Single-Table Design

All data lives in one DynamoDB table with a composite primary key (`PK`, `SK`) and one global secondary index (`GSI1PK`, `GSI1SK`).

### Key Schema

| Entity | PK | SK | GSI1PK | GSI1SK |
|---|---|---|---|---|
| Pipeline config | `PIPELINE#{id}` | `CONFIG` | `PIPELINES` | `{id}` |
| Trait result | `PIPELINE#{id}` | `TRAIT#{type}` | — | — |
| Run state | `RUN#{runID}` | `STATE` | — | — |
| Run (list copy) | `PIPELINE#{id}` | `RUN#{runID}` | — | — |
| Run log | `PIPELINE#{id}` | `RUNLOG#{date}#{scheduleID}` | — | — |
| Event | `PIPELINE#{id}` | `EVENT#{timestamp}#{uuid}` | — | — |
| Lock | `LOCK#{key}` | `LOCK` | — | — |
| Rerun | `RERUN#{rerunID}` | `STATE` | `RERUNS` | `{rerunID}` |
| Rerun (list copy) | `PIPELINE#{id}` | `RERUN#{rerunID}` | — | — |
| Readiness cache | `PIPELINE#{id}` | `READINESS` | — | — |
| MARKER (stream trigger) | `MARKER#{pipeline}` | `{date}#{scheduleID}` | — | — |

### Design Decisions

- **Dual-write pattern**: Runs and reruns are written to both a truth item (direct lookup + CAS) and a list copy (pipeline-scoped queries)
- **TTL-on-read**: DynamoDB TTL deletes are lazy (up to 48h delay), so all reads filter expired items client-side
- **CAS via ConditionExpression**: `UpdateItem` with `#version = :expected`; `ConditionalCheckFailedException` maps to `(false, nil)`
- **Locks via conditional PutItem**: `attribute_not_exists(PK) OR #ttl < :now`

## Step Function State Machine

The state machine orchestrates the full pipeline lifecycle in 47 states:

### Main Flow

1. **InitDefaults** — merge default values for optional fields
2. **CheckExclusion** — skip if pipeline is excluded today (calendar/day/date)
3. **AcquireLock** — distributed lock (`eval:{pipeline}:{schedule}`)
4. **CheckRunLog** — skip if already completed for this date+schedule
5. **ResolvePipeline** — load config, resolve archetype, generate UUID run ID
6. **EvaluateTraits** — parallel Map state evaluating all traits via evaluator Lambda
7. **CheckEvaluationSLA** — alert if evaluation deadline breached
8. **CheckValidationTimeout** — hard stop if validation timeout exceeded
9. **CheckReadiness** — evaluate combined trait results
10. **TriggerPipeline** — execute the configured trigger
11. **PollRunStatus** — 30-second Wait → CheckRunStatus loop
12. **CheckCompletionSLA** — alert if completion deadline breached
13. **LogResult** — write run log entry
14. **ReleaseLock** — release distributed lock
15. **CheckDrift** — optional post-completion monitoring

### Error Handling

Every Task state has a `Catch` block routing to `AlertError`, which sends an alert via the orchestrator's `alertError` action before transitioning to `End`.

Lambda invocations use built-in retry: `IntervalSeconds: 2`, `MaxAttempts: 3`, `BackoffRate: 2`.

### ARN Substitution

The ASL template at `deploy/statemachine.asl.json` uses four substitution variables, resolved by Terraform's `templatefile()`:

- `${OrchestratorFunctionArn}`
- `${EvaluatorFunctionArn}`
- `${TriggerFunctionArn}`
- `${RunCheckerFunctionArn}`

## Lambda Handlers

### stream-router

Processes DynamoDB Stream events and handles two record types:

**MARKER# records** — Extracts `pipelineID`, `date`, and `scheduleID` from the record and starts a Step Function execution with a deterministic name for dedup. The `date` is read from the record's `date` attribute when present, falling back to `time.Now().UTC()` for backward compatibility. This ensures correct date resolution at midnight rollover (e.g., an h23 completion marker carries the previous day's date).

**RUNLOG# records** — Publishes lifecycle events to an SNS topic when a run reaches a terminal status (COMPLETED or FAILED). This enables downstream consumers (monitoring dashboards, notification systems, active recovery) to react to pipeline completion without polling DynamoDB.

Lifecycle event publishing requires the `LIFECYCLE_TOPIC_ARN` environment variable. It is best-effort — errors are logged, not propagated. When the variable is not set, lifecycle publishing is silently disabled.

### orchestrator

Multi-action dispatcher invoked by 10+ Step Function states. Each call specifies an `action` field:

| Action | Purpose |
|---|---|
| `checkExclusion` | Calendar and day-of-week exclusion check |
| `acquireLock` | Distributed lock acquisition |
| `checkRunLog` | Existing run log lookup |
| `resolvePipeline` | Load config + archetype, generate run ID |
| `checkReadiness` | Evaluate combined trait results |
| `checkEvaluationSLA` | Evaluation deadline check |
| `checkCompletionSLA` | Completion deadline check |
| `checkValidationTimeout` | Hard validation timeout |
| `logResult` | Write run log entry |
| `releaseLock` | Release distributed lock |
| `checkDrift` | Post-completion trait drift detection |
| `alertError` | Send error alert |

### evaluator

Evaluates a single trait using the HTTP evaluator runner (`HTTPRunner`). Receives trait type, evaluator path, config, timeout, and TTL. Stores the result in DynamoDB and returns the status.

### trigger

Executes the pipeline trigger with CAS state transitions:
1. CAS `PENDING → TRIGGERING`
2. Execute trigger (Glue, EMR, HTTP, etc.)
3. CAS `TRIGGERING → RUNNING` (success) or `TRIGGERING → FAILED` (error)

### run-checker

Delegates to `trigger.Runner.CheckStatus()` to poll the running job. Returns `running`, `succeeded`, or `failed` state.

### watchdog

Invoked by an EventBridge scheduled rule (default: every 5 minutes). Performs two scans:

1. **`CheckMissedSchedules()`** — scans all pipelines for schedules whose evaluation deadline has passed without a RunLog entry. Fires `SCHEDULE_MISSED` alerts.
2. **`CheckStuckRuns()`** — scans all pipelines for runs stuck in non-terminal states (PENDING, TRIGGERING, RUNNING) beyond a configurable threshold (default: 30 minutes). Fires `RUN_STUCK` alerts.

Both checks fire alerts via SNS and append audit events to DynamoDB. Dedup locks prevent duplicate alerts per pipeline/schedule/day.

The watchdog is **not** part of the Step Function state machine — it runs independently to detect when the Step Function _didn't_ start or when it started but got stuck. See [Watchdog](../watchdog) for the full algorithm.
