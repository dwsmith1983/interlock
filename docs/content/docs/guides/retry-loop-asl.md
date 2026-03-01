---
title: "Step Functions ASL Pattern"
weight: 10
---

# Step Functions Evaluation and SLA Monitoring Pattern

This guide explains the ASL patterns used in Interlock's Step Functions state machine. The state machine uses a Parallel state with two concurrent branches: an evaluation loop and an SLA monitoring branch.

## Architecture Overview

The state machine is defined in `deploy/statemachine.asl.json` and contains approximately 26 states across two parallel branches. The Terraform module provisions it automatically with Lambda ARN substitution.

```
Parallel
  Branch 1: Evaluation Loop
    InitEvalLoop -> Evaluate -> IsReady
      -> (passed) Trigger -> WaitForJob -> CheckJob -> IsJobDone
      -> (not ready) WaitInterval -> IncrementElapsed -> CheckWindowExhausted
          -> (window remaining) Evaluate (loop)
          -> (window exhausted) ValidationExhausted -> EvalDone

  Branch 2: SLA Monitoring
    CheckSLAConfig
      -> (no SLA) SLASkipped
      -> (has SLA) CalcDeadlines -> WaitForWarning -> FireSLAWarning
                                 -> WaitForBreach -> FireSLABreach -> SLADone
```

Both branches run concurrently. The evaluation branch may complete before the SLA branch fires, or the SLA branch may fire warnings while the evaluation loop is still polling.

## Evaluation Loop

The evaluation branch is the core pipeline lifecycle. It calls the **orchestrator** Lambda in different modes to evaluate validation rules, trigger jobs, and poll job status.

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

1. **Evaluate** calls the orchestrator with `mode: evaluate`. The orchestrator reads sensor data from DynamoDB and evaluates all validation rules defined in the pipeline YAML config.
2. **IsReady** branches on the result: if `passed`, proceed to trigger; otherwise, wait and re-evaluate.
3. **WaitInterval** uses `SecondsPath` for configurable poll intervals (set via `evaluation.interval` in the pipeline YAML).
4. **IncrementElapsed** tracks total elapsed time using `States.MathAdd`.
5. **CheckWindowExhausted** compares elapsed time against `evaluationWindowSeconds`. If the window is exhausted, the orchestrator fires a `VALIDATION_EXHAUSTED` event.

### Trigger and Job Polling

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
    "Next": "WaitForJob"
  },

  "CheckJob": {
    "Type": "Task",
    "Resource": "${orchestrator_arn}",
    "Parameters": {
      "mode": "check-job",
      "pipelineId.$": "$.pipelineId",
      "scheduleId.$": "$.scheduleId",
      "runId.$": "$.triggerResult.runId"
    },
    "ResultPath": "$.checkJobResult",
    "Next": "IsJobDone"
  },

  "IsJobDone": {
    "Type": "Choice",
    "Choices": [
      {
        "Variable": "$.checkJobResult.event",
        "StringEquals": "success",
        "Next": "EvalDone"
      },
      {
        "Variable": "$.checkJobResult.event",
        "StringEquals": "fail",
        "Next": "EvalDone"
      },
      {
        "Variable": "$.checkJobResult.event",
        "StringEquals": "timeout",
        "Next": "EvalDone"
      }
    ],
    "Default": "WaitForJob"
  }
}
```

Once all validation rules pass, the orchestrator triggers the configured job (Glue, HTTP, EMR, Step Functions, etc.). The state machine then polls job status via `check-job` mode until it receives a terminal event (`success`, `fail`, or `timeout`).

## SLA Monitoring Branch

The SLA branch runs in parallel with the evaluation loop. It uses the **sla-monitor** Lambda to calculate deadlines and fire alerts.

```json
{
  "CheckSLAConfig": {
    "Type": "Choice",
    "Choices": [
      {
        "Variable": "$.config.sla",
        "IsPresent": true,
        "Next": "CalcDeadlines"
      }
    ],
    "Default": "SLASkipped"
  },

  "CalcDeadlines": {
    "Type": "Task",
    "Resource": "${sla_monitor_arn}",
    "Parameters": {
      "mode": "calculate",
      "pipelineId.$": "$.pipelineId",
      "scheduleId.$": "$.scheduleId",
      "date.$": "$.date",
      "sla.$": "$.config.sla"
    },
    "ResultPath": "$.slaDeadlines",
    "Next": "WaitForWarning"
  },

  "WaitForWarning": {
    "Type": "Wait",
    "TimestampPath": "$.slaDeadlines.warningAt",
    "Next": "FireSLAWarning"
  },

  "FireSLAWarning": {
    "Type": "Task",
    "Resource": "${sla_monitor_arn}",
    "Parameters": {
      "mode": "fire-alert",
      "pipelineId.$": "$.pipelineId",
      "scheduleId.$": "$.scheduleId",
      "date.$": "$.date",
      "alertType": "SLA_WARNING"
    },
    "ResultPath": "$.slaWarningResult",
    "Next": "WaitForBreach"
  }
}
```

### How it works

1. **CheckSLAConfig** skips the branch if no `sla` config is present in the pipeline YAML.
2. **CalcDeadlines** computes `warningAt` and `breachAt` timestamps from the SLA config.
3. **WaitForWarning** uses `TimestampPath` to sleep until the warning time.
4. **FireSLAWarning** publishes an `SLA_WARNING` event to EventBridge.
5. **WaitForBreach** then sleeps until the breach time.
6. **FireSLABreach** publishes an `SLA_BREACH` event to EventBridge.

If the evaluation branch completes before either deadline, the Parallel state completes and the SLA waits are cancelled.

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
      "Next": "EvalBranchFailed"
    }
  ]
}
```

Transient Lambda errors are retried with exponential backoff (2s, 4s, 8s). Unrecoverable errors route to a Fail state that terminates the branch and propagates up through the Parallel state's top-level Catch.

## Customization

The ASL is a template file (`deploy/statemachine.asl.json`) processed by Terraform's `templatefile` function. The two substitution variables are:

| Variable | Terraform Resource |
|---|---|
| `${orchestrator_arn}` | `aws_lambda_function.orchestrator.arn` |
| `${sla_monitor_arn}` | `aws_lambda_function.sla_monitor.arn` |

The evaluation interval, window duration, and job check interval are driven by pipeline config values passed in the Step Functions input (`$.config`), not hardcoded in the ASL.
