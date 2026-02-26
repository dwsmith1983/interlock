---
title: "Retry Loop ASL Pattern"
weight: 10
---

# Step Function Retry & Readiness Polling Pattern

This guide shows the recommended ASL patterns for implementing retry loops and readiness polling in your Step Function state machine.

## Prerequisites

These patterns require interlock v0.2.2+ which adds:

- `failureCategory` in run-checker responses (classifies failures as `TRANSIENT`, `TIMEOUT`, or `PERMANENT`)
- `retryable` and `retryBackoffSeconds` in orchestrator `logResult` responses
- `not_ready` result with `pollAdvised` from orchestrator `checkReadiness`

## Failure Retry Loop

When a job fails, the orchestrator's `logResult` action returns retry metadata. The ASL can use this to loop back and retry.

```json
{
  "LogRunFailed": {
    "Type": "Task",
    "Resource": "${OrchestratorArn}",
    "Parameters": {
      "action": "logResult",
      "pipelineID.$": "$.pipelineID",
      "scheduleID.$": "$.scheduleID",
      "payload": {
        "status": "FAILED",
        "runID.$": "$.runID",
        "message.$": "$.failureMessage",
        "failureCategory.$": "$.failureCategory"
      }
    },
    "ResultPath": "$.logResult",
    "Next": "IsRetryable"
  },

  "IsRetryable": {
    "Type": "Choice",
    "Choices": [
      {
        "Variable": "$.logResult.payload.retryable",
        "BooleanEquals": true,
        "Next": "WaitRetryBackoff"
      }
    ],
    "Default": "ReleaseLockFailed"
  },

  "WaitRetryBackoff": {
    "Type": "Wait",
    "SecondsPath": "$.logResult.payload.retryBackoffSeconds",
    "Next": "AcquireLock"
  }
}
```

### How it works

1. `LogRunFailed` calls the orchestrator with `failureCategory` from the run-checker
2. The orchestrator computes `retryable` (based on category + attempt count + max attempts) and `retryBackoffSeconds`
3. `IsRetryable` branches: if retryable, wait and loop back to `AcquireLock`; otherwise, proceed to final cleanup
4. `WaitRetryBackoff` uses `SecondsPath` for dynamic exponential backoff

### Backward compatibility

If the ASL does not pass `failureCategory`, the orchestrator defaults it to `TRANSIENT`, making the failure retryable. This ensures existing deployments get retry behavior without ASL changes.

## Readiness Polling

When traits fail (data not ready), the orchestrator returns `not_ready` with poll metadata. The ASL can use this to wait and re-evaluate.

```json
{
  "CheckReadiness": {
    "Type": "Task",
    "Resource": "${OrchestratorArn}",
    "Parameters": {
      "action": "checkReadiness",
      "pipelineID.$": "$.pipelineID",
      "payload": {
        "traitResults.$": "$.traitResults"
      }
    },
    "ResultPath": "$.readiness",
    "Next": "IsReady"
  },

  "IsReady": {
    "Type": "Choice",
    "Choices": [
      {
        "Variable": "$.readiness.result",
        "StringEquals": "proceed",
        "Next": "TriggerJob"
      },
      {
        "Variable": "$.readiness.result",
        "StringEquals": "not_ready",
        "Next": "WaitReadiness"
      }
    ],
    "Default": "HandleEvaluatorError"
  },

  "WaitReadiness": {
    "Type": "Wait",
    "Seconds": 60,
    "Next": "AcquireLock"
  }
}
```

### How it works

1. `CheckReadiness` evaluates trait results and returns `proceed`, `not_ready`, or `error`
2. `IsReady` branches on the result:
   - `proceed`: all required traits pass, trigger the job
   - `not_ready`: data not ready yet, wait and re-evaluate (loops back to `AcquireLock`)
   - `error`: evaluator infrastructure failure, handle separately
3. `WaitReadiness` pauses before re-evaluation (use a fixed interval or compute dynamically)

### Backward compatibility

The previous `skip` result is replaced by `not_ready`. Existing ASL templates that check `result == "proceed"` with a default fallback will treat `not_ready` the same as `skip` — both hit the default path. No ASL changes are required for existing deployments to continue working.

## Complete Flow

The recommended state machine flow combining both patterns:

```
AcquireLock → CheckRunLog → ResolvePipeline → EvaluateTraits → CheckReadiness
                                                                    │
                                                        ┌──────────┼──────────┐
                                                        │          │          │
                                                    proceed    not_ready    error
                                                        │          │          │
                                                   TriggerJob  Wait(60s)  Alert+Skip
                                                        │          │
                                                   PollStatus  → AcquireLock
                                                        │
                                                  ┌─────┼─────┐
                                                  │           │
                                              succeeded    failed
                                                  │           │
                                             LogCompleted  LogFailed
                                                              │
                                                        ┌─────┼─────┐
                                                        │           │
                                                    retryable  non-retryable
                                                        │           │
                                                   Wait(backoff)  Cleanup
                                                        │
                                                   AcquireLock
```
