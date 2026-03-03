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
  → (passed) Trigger → CheckSLAConfig → ScheduleSLAAlerts → HasTriggerResult
      → WaitForJob → CheckJob → IsJobDone
          → (terminal) CheckCancelSLA → CancelSLASchedules → Done
          → (running) WaitForJob (loop)
  → (not ready) WaitInterval → IncrementElapsed → CheckWindowExhausted
      → (window remaining) Evaluate (loop)
      → (window exhausted) ValidationExhausted → CheckSLAConfig → ... → Done
```

SLA monitoring uses EventBridge Scheduler instead of a parallel branch. The sla-monitor Lambda creates one-time Scheduler entries that fire alerts at exact timestamps. When the job completes, unfired entries are cancelled.

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
2. **IsReady** branches on the result: if `passed`, proceed to trigger; otherwise, wait and re-evaluate.
3. **WaitInterval** uses `SecondsPath` for configurable poll intervals (set via `evaluation.interval` in the pipeline config).
4. **IncrementElapsed** tracks total elapsed time using `States.MathAdd`.
5. **CheckWindowExhausted** compares elapsed time against `evaluationWindowSeconds`. If the window is exhausted, the orchestrator publishes a `VALIDATION_EXHAUSTED` event.

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
        "MaxAttempts": 4,
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

The Trigger state has a dedicated retry policy for infrastructure failures. When the orchestrator's trigger execution fails (e.g., Glue `ConcurrentRunsExceededException`), it returns a Lambda error. Step Functions retries 4 times with exponential backoff (30s, 60s, 120s, 240s). If all retries exhaust, the Catch routes to `CheckCancelSLA` for graceful SLA cleanup instead of crashing.

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

The default retry policy handles transient Lambda errors with exponential backoff (2s, 4s, 8s). The **Trigger** state overrides this with a longer retry (30s, 60s, 120s, 240s) because infrastructure failures like Glue concurrency limits benefit from more time between attempts.

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
