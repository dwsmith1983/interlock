---
title: "Step Functions State Machine"
weight: 10
---

# Step Functions State Machine

This guide explains the ASL patterns used in Interlock's Step Functions state machine. The state machine uses 28 states to orchestrate pipeline evaluation, job triggering, and SLA monitoring via EventBridge Scheduler.

## Architecture Overview

The state machine is defined in `deploy/statemachine.asl.json`. The Terraform module provisions it automatically with Lambda ARN substitution.

```
InitEvalLoop → Evaluate → IsReady
  → (passed) Trigger → HasTriggerResult
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
```

SLA monitoring uses EventBridge Scheduler instead of a parallel branch. The watchdog creates one-time Scheduler entries for each run date with a `RUN#` row; at their exact timestamps they invoke the sla-monitor Lambda, which fires the alert. When the job completes, unfired entries are cancelled.

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
3. **WaitInterval** uses `SecondsPath` for configurable poll intervals (set via `evaluation.interval` in the pipeline config).
4. **IncrementElapsed** tracks total elapsed time using `States.MathAdd`.
5. **CheckWindowExhausted** compares elapsed time against `evaluationWindowSeconds`. If the window is exhausted, **ValidationExhausted** ends the execution, skipping the SLA cleanup since no job ran. Before the run date's cutoff, a first run is released: the orchestrator writes an `evaluation-released` job row, deletes the trigger row and publishes `EVALUATION_RELEASED` (not an alert), so the next trigger sensor write or watchdog pass can start evaluation again until the cutoff, and the date's SLA schedules still fire. With no cutoff for the date, or for a rerun, it publishes `VALIDATION_EXHAUSTED` and sets `FAILED_FINAL`, and the run's unfired SLA schedules fire and are suppressed because the run is finished. At or after the cutoff, a first run publishes `SENSOR_DEADLINE_EXPIRED` and then sets `FAILED_FINAL`.

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
        "ErrorEquals": [
          "Lambda.ServiceException",
          "Lambda.AWSLambdaException",
          "Lambda.TooManyRequestsException",
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

The Trigger state has a dedicated retry policy for infrastructure failures. When the orchestrator cannot start the job (e.g., Glue `ConcurrentRunsExceededException`), it returns a Lambda error. Step Functions retries up to `trigger_max_attempts` times (default 3, so four attempts in all), waiting 30s, 60s and 120s. If all retries exhaust, the Catch routes to `TriggerRetryExhausted`, which records the failure, publishes `RETRY_EXHAUSTED` and sets the trigger row `FAILED_FINAL`, then to `CheckCancelSLA` for SLA cleanup instead of crashing. Nothing starts the date again on its own; submit a `RERUN_REQUEST` to run it.

Once triggered, the state machine polls job status via `check-job` mode. The `IsJobDone` Choice state first checks whether the `event` field is present (missing means still running), then routes terminal events (`success`, `fail`, `timeout`) to **CompleteTrigger**, which sets `COMPLETED`, starts the next retry attempt (`status: retrying`, which skips the SLA cleanup so the retry keeps the schedules) or sets `FAILED_FINAL`. When `jobPollWindowSeconds` passes first, **JobPollExhausted** writes a `timeout` row and the run goes to CompleteTrigger as a timeout.

## SLA Cleanup

SLA alerts are scheduled outside the state machine: the watchdog creates the warning and breach Scheduler entries for each `RUN#` date, and each entry invokes the sla-monitor Lambda with `mode=fire-alert` at its time. The state machine only cleans up when the run ends.

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
3. If CompleteTrigger itself fails after its retries, **CheckSLAForCompleteTriggerFailure** and **CancelSLAOnCompleteTriggerFailure** cancel the schedules before the execution ends in **CompleteTriggerFailed**. This path publishes no `SLA_MET` unless the trigger row was already `COMPLETED` (the failure came after the status write).

## Error Handling

All Task states include Retry and Catch blocks:

```json
{
  "Retry": [
    {
      "ErrorEquals": [
        "Lambda.ServiceException",
        "Lambda.AWSLambdaException",
        "Lambda.TooManyRequestsException",
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
      "Next": "InfraFailure"
    }
  ]
}
```

The default retry policy handles transient Lambda errors with exponential backoff (2s, 4s, 8s). The **Trigger** state overrides this with a longer retry (`trigger_max_attempts`, default 3: waits of 30s, 60s and 120s) because infrastructure failures like Glue concurrency limits benefit from more time between attempts.

Catch routing varies by state:
- **Most states**: Catch routes to `InfraFailure` (Fail state — terminates execution)
- **Trigger**: Catch routes to `TriggerRetryExhausted`, which ends the date `FAILED_FINAL`, then `CheckCancelSLA` (graceful termination with SLA cleanup)
- **CompleteTrigger**: Catch routes to `CheckSLAForCompleteTriggerFailure` (SLA cleanup, then `CompleteTriggerFailed`)
- **CancelSLASchedules**: Catch routes to `Done` (cleanup failure is non-fatal)

## Customization

The ASL is a template file (`deploy/statemachine.asl.json`) processed by Terraform's `templatefile` function. The substitution variables are:

| Variable | Terraform Source |
|---|---|
| `${orchestrator_arn}` | `aws_lambda_function.orchestrator.arn` |
| `${sla_monitor_arn}` | `aws_lambda_function.sla_monitor.arn` |
| `${sfn_timeout_seconds}` | `var.sfn_timeout_seconds` |
| `${trigger_max_attempts}` | `var.trigger_max_attempts` |

The evaluation interval, window duration, and job check interval are driven by pipeline config values passed in the Step Functions input (`$.config`), not hardcoded in the ASL.
