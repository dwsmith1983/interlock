---
title: "Step Functions State Machine"
weight: 10
---

# Step Functions State Machine

This guide explains the ASL patterns used in Interlock's Step Functions state machine. The state machine uses 31 states to orchestrate pipeline evaluation, job triggering, and SLA monitoring via EventBridge Scheduler.

## Architecture Overview

The state machine is defined in `deploy/statemachine.asl.json`. The Terraform module provisions it automatically with Lambda ARN substitution.

```
CheckRetryBackoff
  → (retry) RetryBackoff → InitEvalLoop
  → (any other run) InitEvalLoop

InitEvalLoop → Evaluate → IsReady
  → (passed) Trigger → HasTriggerResult
      → (no trigger result) FailValidationExhausted
      → InitJobPollLoop → WaitForJob → CheckJob → IsJobDone
          → (terminal) CompleteTrigger → IsRetrying
              → (retrying) Done
              → CheckCancelSLA → CancelSLASchedules → Done
          → (running) IncrementJobPollElapsed → CheckJobPollExhausted
              → (window remaining) WaitForJob (loop)
              → (exhausted) JobPollExhausted → InjectTimeoutEvent → CompleteTrigger
  → (not ready) WaitInterval → IncrementElapsed → CheckWindowExhausted
      → (window remaining) Evaluate (loop)
      → (window exhausted) ValidationExhausted (release, end or cutoff) → Done
  → (cutoff) ValidationExhausted → Done

Trigger fails after its retries
  → TriggerRetryExhausted → CheckCancelSLA → CancelSLASchedules → Done

Evaluate, ValidationExhausted or CheckJob fails after its retries
  → RecordInfraFailure → InfraFailure
```

`FailValidationExhausted` is a guard: a Trigger state that succeeds always writes `$.triggerResult`, so it is not reached in normal runs.

SLA monitoring uses EventBridge Scheduler instead of a parallel branch. The watchdog creates one-time Scheduler entries for each run date with a `RUN#` row, at most 6 hours before its warning; at their exact timestamps they invoke the sla-monitor Lambda, which fires the alert. When the job completes, unfired entries are cancelled.

## Retry Backoff

A retry after a job failure runs as its own execution and waits before anything else:

```json
{
  "CheckRetryBackoff": {
    "Type": "Choice",
    "Choices": [
      {
        "Variable": "$.retryDelaySeconds",
        "NumericGreaterThan": 0,
        "Next": "RetryBackoff"
      }
    ],
    "Default": "InitEvalLoop"
  },
  "RetryBackoff": {
    "Type": "Wait",
    "SecondsPath": "$.retryDelaySeconds",
    "Next": "InitEvalLoop"
  }
}
```

The execution's input carries `retryDelaySeconds`, set when the execution starts from the `-r<N>` suffix of its name: 60 seconds doubling per retry, at most 900 (60, 120, 240, 480, 900, 900, ...). A first run, a requested rerun and a watchdog start have no suffix and get 0, so they go straight to the evaluation loop.

The wait comes before `InitEvalLoop` sets the evaluation counter, so it takes nothing from the evaluation window. It does count against the execution's own timeout, which is why the config check adds the longest delay the pipeline can reach. Each retry is a new execution, so the delays do not add up there. The delays are not randomised: failed jobs end at different moments, and a random delay could be close to zero.

## Evaluation Loop

The evaluation loop calls the **orchestrator** Lambda with `mode=evaluate` to check validation rules against sensor data.

### Evaluate and Poll

```json
{
  "Evaluate": {
    "Type": "Task",
    "Resource": "${orchestrator_arn}",
    "Parameters": {
      "mode": "evaluate",
      "pipelineId.$": "$.pipelineId",
      "scheduleId.$": "$.scheduleId",
      "date.$": "$.date"
    },
    "ResultPath": "$.evaluateResult",
    "Next": "IsReady"
  },
  "IsReady": {
    "Type": "Choice",
    "Choices": [
      {
        "Variable": "$.evaluateResult.status",
        "StringEquals": "passed",
        "Next": "Trigger"
      },
      {
        "Variable": "$.evaluateResult.status",
        "StringEquals": "cutoff",
        "Next": "ValidationExhausted"
      }
    ],
    "Default": "WaitInterval"
  },
  "WaitInterval": {
    "Type": "Wait",
    "SecondsPath": "$.config.evaluationIntervalSeconds",
    "Next": "IncrementElapsed"
  }
}
```

### How it works

1. **Evaluate** calls the orchestrator with `mode: evaluate`. The orchestrator reads sensor data from DynamoDB and evaluates all validation rules defined in the pipeline config.
2. **IsReady** branches on the result: if `passed`, proceed to trigger; if `cutoff` (the run date's cutoff passed and it is not a rerun), go to **ValidationExhausted**, which sets `FAILED_FINAL`, publishes `SENSOR_DEADLINE_EXPIRED` and ends the execution; otherwise, wait and re-evaluate.
3. **WaitInterval** uses `SecondsPath` for configurable poll intervals (`evaluationIntervalSeconds`, from `schedule.evaluation.interval` in the pipeline config, default `5m`).
4. **IncrementElapsed** tracks total elapsed time using `States.MathAdd`.
5. **CheckWindowExhausted** compares elapsed time against `evaluationWindowSeconds` (from `schedule.evaluation.window`, default `1h`). If the window is exhausted, **ValidationExhausted** ends the execution, skipping the SLA cleanup since no job ran. Before the run date's cutoff, a first run is released: the orchestrator writes an `evaluation-released` job row, deletes the trigger row and publishes `EVALUATION_RELEASED` (not an alert), so the next trigger sensor write or watchdog pass can start evaluation again until the cutoff, and the date's SLA schedules still fire. For a rerun it publishes `VALIDATION_EXHAUSTED` and sets `FAILED_FINAL`, and the run's unfired SLA schedules fire and are suppressed because the run is finished. At or after the cutoff, a first run publishes `SENSOR_DEADLINE_EXPIRED` and then sets `FAILED_FINAL`.

## Trigger and Job Polling

```json
{
  "Trigger": {
    "Type": "Task",
    "Resource": "${orchestrator_arn}",
    "Parameters": {
      "mode": "trigger",
      "pipelineId.$": "$.pipelineId",
      "scheduleId.$": "$.scheduleId",
      "date.$": "$.date",
      "executionName.$": "$$.Execution.Name"
    },
    "ResultPath": "$.triggerResult",
    "Next": "HasTriggerResult",
    "Retry": [
      {
        "ErrorEquals": ["Lambda.TooManyRequestsException"],
        "IntervalSeconds": 2,
        "MaxAttempts": 7,
        "BackoffRate": 2,
        "MaxDelaySeconds": 60
      },
      {
        "ErrorEquals": [
          "Lambda.ServiceException",
          "Lambda.AWSLambdaException",
          "States.TaskFailed"
        ],
        "IntervalSeconds": 30,
        "MaxAttempts": ${trigger_max_attempts},
        "BackoffRate": 2
      }
    ],
    "Catch": [
      {
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.errorInfo",
        "Next": "TriggerRetryExhausted"
      }
    ]
  },
  "CheckJob": {
    "Type": "Task",
    "Resource": "${orchestrator_arn}",
    "Parameters": {
      "mode": "check-job",
      "pipelineId.$": "$.pipelineId",
      "scheduleId.$": "$.scheduleId",
      "date.$": "$.date",
      "runId.$": "$.triggerResult.runId",
      "metadata.$": "$.triggerResult.metadata"
    },
    "ResultPath": "$.checkJobResult",
    "Next": "IsJobDone"
  },
  "IsJobDone": {
    "Type": "Choice",
    "Choices": [
      {
        "Not": {
          "Variable": "$.checkJobResult.event",
          "IsPresent": true
        },
        "Next": "IncrementJobPollElapsed"
      },
      {
        "Variable": "$.checkJobResult.event",
        "StringEquals": "success",
        "Next": "CompleteTrigger"
      },
      {
        "Variable": "$.checkJobResult.event",
        "StringEquals": "fail",
        "Next": "CompleteTrigger"
      },
      {
        "Variable": "$.checkJobResult.event",
        "StringEquals": "timeout",
        "Next": "CompleteTrigger"
      }
    ],
    "Default": "IncrementJobPollElapsed"
  }
}
```

The Trigger state has a dedicated retry policy for infrastructure failures. When the orchestrator cannot start the job (e.g., Glue `ConcurrentRunsExceededException`), it returns a Lambda error. Step Functions retries up to `trigger_max_attempts` times (default 3, so four attempts in all), waiting 30s, 60s and 120s. A throttled call to the orchestrator itself uses none of these attempts (see [Error Handling](#error-handling)); a `lambda` job whose own function is throttled is a launch failure and does. If all retries exhaust, the Catch routes to `TriggerRetryExhausted`, which records the failure, publishes `RETRY_EXHAUSTED` and sets the trigger row `FAILED_FINAL`, then to `CheckCancelSLA` for SLA cleanup instead of crashing. Nothing starts the date again on its own; submit a `RERUN_REQUEST` to run it.

Once triggered, the state machine polls job status via `check-job` mode. The `IsJobDone` Choice state first checks whether the `event` field is present (missing means still running), then routes terminal events (`success`, `fail`, `timeout`) to **CompleteTrigger**, which sets `COMPLETED`, starts the next retry attempt (`status: retrying`, which skips the SLA cleanup so the retry keeps the schedules) or sets `FAILED_FINAL`. When `jobPollWindowSeconds` passes first, **JobPollExhausted** writes a `timeout` row and the run goes to CompleteTrigger as a timeout.

## SLA Cleanup

SLA alerts are scheduled outside the state machine: the watchdog creates the warning and breach Scheduler entries for each `RUN#` date at most 6 hours before its warning, and each entry invokes the sla-monitor Lambda with `mode=fire-alert` at its time. The state machine only cleans up when the run ends.

```json
{
  "CheckCancelSLA": {
    "Type": "Choice",
    "Choices": [
      {
        "Variable": "$.config.sla",
        "IsPresent": true,
        "Next": "CancelSLASchedules"
      }
    ],
    "Default": "Done"
  },
  "CancelSLASchedules": {
    "Type": "Task",
    "Resource": "${sla_monitor_arn}",
    "Parameters": {
      "mode": "cancel",
      "pipelineId.$": "$.pipelineId",
      "scheduleId.$": "$.scheduleId",
      "date.$": "$.date",
      "deadline.$": "$.config.sla.deadline",
      "expectedDuration.$": "$.config.sla.expectedDuration",
      "maxDuration.$": "$.config.sla.maxDuration",
      "timezone.$": "$.config.sla.timezone",
      "sensorArrivalAt.$": "$.sensorArrivalAt"
    },
    "ResultPath": "$.slaResult",
    "Next": "Done"
  }
}
```

### How it works

1. **CheckCancelSLA** skips the cleanup when the pipeline has no `sla` config.
2. **CancelSLASchedules** deletes any unfired Scheduler entries and publishes `SLA_MET` if the run completed before the breach time, and `SLA_BREACH` once the breach time has passed.
3. If CompleteTrigger itself fails after its retries, **CheckSLAForCompleteTriggerFailure** and **CancelSLAOnCompleteTriggerFailure** cancel the schedules before the execution ends in **CompleteTriggerFailed**. This path publishes no `SLA_MET` unless the trigger row was already `COMPLETED` (the failure came after the status write). While the row stays `RUNNING`, the watchdog schedules the warning and breach that are still ahead again, until it closes the stale row.

## Error Handling

All Task states include Retry and Catch blocks. Evaluate, ValidationExhausted and CheckJob use:

```json
{
  "Retry": [
    {
      "ErrorEquals": ["Lambda.TooManyRequestsException"],
      "IntervalSeconds": 2,
      "MaxAttempts": 7,
      "BackoffRate": 2,
      "MaxDelaySeconds": 60
    },
    {
      "ErrorEquals": [
        "Lambda.ServiceException",
        "Lambda.AWSLambdaException",
        "States.TaskFailed"
      ],
      "IntervalSeconds": 2,
      "MaxAttempts": 3,
      "BackoffRate": 2
    }
  ],
  "Catch": [
    {
      "ErrorEquals": ["States.ALL"],
      "ResultPath": "$.errorInfo",
      "Next": "RecordInfraFailure"
    }
  ]
}
```

Step Functions uses the first retrier that matches an error, and each retrier counts its own attempts, starting again on every visit to the state. `States.TaskFailed` matches a throttle too, so the throttle retrier comes first.

The first retrier handles a call refused because the function has no free instance (`Lambda.TooManyRequestsException`): seven retries, after 2, 4, 8, 16, 32, 60 and 60 seconds, on every Task state. Those 182 seconds are longer than the orchestrator's 120-second timeout, so an instance held by another call, such as a synchronous `lambda` job, is free before they run out. A throttle that outlasts them goes to the Catch; the second retrier does not retry it.

The delays are not randomised (no `JitterStrategy`), so executions throttled at the same moment retry together. A random delay is drawn from zero up to the full one, so the total could fall below the orchestrator's timeout and the retries could run out while the instance is still held.

The evaluation and poll loops count only their own wait intervals, not throttle retries, so a heavily throttled execution can reach `sfn_timeout_seconds` before its windows end. The run's stale deadline (`staleAt`, that timeout plus 30 minutes) still falls after the execution's own timeout.

The second retrier handles transient Lambda errors with exponential backoff (2s, 4s, 8s). The **Trigger** state overrides it with a longer retry (`trigger_max_attempts`, default 3: waits of 30s, 60s and 120s) because infrastructure failures like Glue concurrency limits benefit from more time between attempts.

Catch routing varies by state:
- **Evaluate, ValidationExhausted, CheckJob**: Catch routes to `RecordInfraFailure`, then `InfraFailure` (Fail state, terminates the execution)
- **RecordInfraFailure**: Catch routes to `InfraFailure`
- **Trigger**: Catch routes to `TriggerRetryExhausted`, which ends the date `FAILED_FINAL`, then `CheckCancelSLA` (graceful termination with SLA cleanup)
- **TriggerRetryExhausted**: Catch routes to `CheckCancelSLA`
- **JobPollExhausted**: Catch routes to `InjectTimeoutEvent` (the `timeout` row is best effort)
- **CompleteTrigger**: Catch routes to `CheckSLAForCompleteTriggerFailure` (SLA cleanup, then `CompleteTriggerFailed`)
- **CancelSLAOnCompleteTriggerFailure**: Catch routes to `CompleteTriggerFailed`
- **CancelSLASchedules**: Catch routes to `Done` (cleanup failure is non-fatal)

`RecordInfraFailure` invokes the orchestrator with `mode=infra-failure`: it publishes `INFRA_FAILURE`, whose message carries the caught error and its cause, then sets the date's `RUNNING` trigger row `FAILED_FINAL`, so a `RERUN_REQUEST` can run it at once. A job that had been launched may still be running, so check it first. There is no SLA cleanup on this path; the unfired schedules are suppressed because the run is finished. If the state fails through its own retries, the execution still ends in `InfraFailure` with the row `RUNNING`, and the watchdog closes it with `SFN_TIMEOUT`.

The state also passes `$$.Execution.StartTime`. When ValidationExhausted released the lock but its response was lost, a sensor write or the watchdog may already have started another execution, and the `RUNNING` row is that one's. An `evaluation-released` job row written after this execution started, as the date's newest outcome, marks that case, and the row is left alone. The same row is there when the release failed between writing it and deleting the lock; that date stays `RUNNING` until the watchdog's `SFN_TIMEOUT`. If the other execution writes its own outcome row before this state runs, the release is no longer the newest outcome and that execution's row is closed with `INFRA_FAILURE`; both rows have to fall within the failing execution's last retries.

## Customization

The ASL is a template file (`deploy/statemachine.asl.json`) processed by Terraform's `templatefile` function. The substitution variables are:

| Variable | Terraform Source |
|---|---|
| `${orchestrator_arn}` | `aws_lambda_function.orchestrator.arn` |
| `${sla_monitor_arn}` | `aws_lambda_function.sla_monitor.arn` |
| `${sfn_timeout_seconds}` | `var.sfn_timeout_seconds` |
| `${trigger_max_attempts}` | `var.trigger_max_attempts` |

The evaluation interval, the evaluation window and the job poll window come from the pipeline config (`schedule.evaluation.interval`, `schedule.evaluation.window` and `job.jobPollWindowSeconds`), passed in the Step Functions input (`$.config`) rather than written into the ASL. The job check interval travels the same way (`$.config.jobCheckIntervalSeconds`, read by **WaitForJob**), but it is fixed at 60 seconds: no pipeline field or module variable sets it.
