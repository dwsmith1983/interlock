---
title: "Step Functions State Machine"
weight: 10
---

# Step Functions State Machine

This guide explains the ASL patterns used in Interlock's Step Functions state machine. The state machine uses 18 sequential states to orchestrate pipeline evaluation, job triggering, and SLA monitoring via EventBridge Scheduler.

## Architecture Overview

The state machine is defined in `deploy/statemachine.asl.json`. The Terraform module provisions it automatically with Lambda ARN substitution.

```
InitEvalLoop → Evaluate → IsReady
  → (passed) Trigger → HasTriggerResult
      → WaitForJob → CheckJob → IsJobDone
          → (terminal) CheckCancelSLA → CancelSLASchedules → Done
          → (running) WaitForJob (loop)
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
      "date.$": "$.date"
    },
    "ResultPath": "$.triggerResult",
    "Next": "CheckSLAConfig",
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
        "Next": "CheckCancelSLA"
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
        "Next": "WaitForJob"
      },
      {
        "Variable": "$.checkJobResult.event",
        "StringEquals": "success",
        "Next": "CheckCancelSLA"
      },
      {
        "Variable": "$.checkJobResult.event",
        "StringEquals": "fail",
        "Next": "CheckCancelSLA"
      },
      {
        "Variable": "$.checkJobResult.event",
        "StringEquals": "timeout",
        "Next": "CheckCancelSLA"
      }
    ],
    "Default": "WaitForJob"
  }
}
```

The Trigger state has a dedicated retry policy for infrastructure failures. When the orchestrator cannot start the job (e.g., Glue `ConcurrentRunsExceededException`), it returns a Lambda error. Step Functions retries up to `trigger_max_attempts` times (default 3, so four attempts in all), waiting 30s, 60s and 120s. If all retries exhaust, the Catch routes to `CheckCancelSLA` for graceful SLA cleanup instead of crashing.

Once triggered, the state machine polls job status via `check-job` mode. The `IsJobDone` Choice state first checks whether the `event` field is present (missing means still running), then routes terminal events (`success`, `fail`, `timeout`) to SLA cleanup.

## SLA Scheduling

```json
{
  "CheckSLAConfig": {
    "Type": "Choice",
    "Choices": [
      {
        "Variable": "$.config.sla",
        "IsPresent": true,
        "Next": "ScheduleSLAAlerts"
      }
    ],
    "Default": "HasTriggerResult"
  },
  "ScheduleSLAAlerts": {
    "Type": "Task",
    "Resource": "${sla_monitor_arn}",
    "Parameters": {
      "mode": "schedule",
      "pipelineId.$": "$.pipelineId",
      "scheduleId.$": "$.scheduleId",
      "date.$": "$.date",
      "deadline.$": "$.config.sla.deadline",
      "expectedDuration.$": "$.config.sla.expectedDuration"
    },
    "ResultPath": "$.slaSchedule",
    "Next": "HasTriggerResult"
  },
  "CancelSLASchedules": {
    "Type": "Task",
    "Resource": "${sla_monitor_arn}",
    "Parameters": {
      "mode": "cancel",
      "pipelineId.$": "$.pipelineId",
      "scheduleId.$": "$.scheduleId",
      "date.$": "$.date",
      "warningAt.$": "$.slaSchedule.warningAt",
      "breachAt.$": "$.slaSchedule.breachAt"
    },
    "ResultPath": "$.slaResult",
    "Next": "Done"
  }
}
```

### How it works

1. **CheckSLAConfig** skips SLA scheduling if no `sla` config is present in the pipeline config.
2. **ScheduleSLAAlerts** creates two one-time EventBridge Scheduler entries — one for the warning timestamp and one for the breach timestamp. Each entry invokes the sla-monitor Lambda with `mode=fire-alert` at the exact time. Entries auto-delete after execution.
3. After the job reaches a terminal state, **CheckCancelSLA** checks if SLA was configured (by checking for `$.slaSchedule`).
4. **CancelSLASchedules** deletes any unfired Scheduler entries and publishes `SLA_MET` if the job completed before the warning deadline.

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
- **Trigger**: Catch routes to `CheckCancelSLA` (graceful termination with SLA cleanup)
- **ScheduleSLAAlerts**: Catch routes to `HasTriggerResult` (SLA failure is non-fatal)
- **CancelSLASchedules**: Catch routes to `Done` (cleanup failure is non-fatal)

## Customization

The ASL is a template file (`deploy/statemachine.asl.json`) processed by Terraform's `templatefile` function. The two substitution variables are:

| Variable | Terraform Resource |
|---|---|
| `${orchestrator_arn}` | `aws_lambda_function.orchestrator.arn` |
| `${sla_monitor_arn}` | `aws_lambda_function.sla_monitor.arn` |

The evaluation interval, window duration, and job check interval are driven by pipeline config values passed in the Step Functions input (`$.config`), not hardcoded in the ASL.
