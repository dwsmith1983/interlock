---
title: Triggers
weight: 3
description: All 8 job types with configuration examples and status polling.
---

When a pipeline's validation rules pass, the orchestrator Lambda executes the configured job to start the downstream workload. The `job` block in the pipeline YAML defines the job type and its configuration.

## Job Configuration

```yaml
job:
  type: glue
  config:
    jobName: gold-revenue-etl
  maxRetries: 2
```

### Common Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | string | -- | Job type (required, see types below) |
| `config` | map | -- | Type-specific configuration |
| `maxRetries` | int | 0 | Maximum retry attempts on failure |

## Job Types

### `http`

Sends an HTTP request to an arbitrary endpoint.

```yaml
job:
  type: http
  config:
    method: POST
    url: https://api.example.com/jobs/start
    headers:
      Authorization: "Bearer ${TOKEN}"
      Content-Type: application/json
    body: '{"pipeline": "${PIPELINE}", "date": "${DATE}"}'
```

| Field | Type | Description |
|---|---|---|
| `method` | string | HTTP method (`GET`, `POST`, `PUT`) |
| `url` | string | Target URL |
| `headers` | map | HTTP headers |
| `body` | string | Request body (supports template variables) |

### `command`

Executes a shell command. Intended for use in development and testing scenarios.

```yaml
job:
  type: command
  config:
    command: /usr/local/bin/run-etl --pipeline my-pipeline
    timeout: 300
```

| Field | Type | Description |
|---|---|---|
| `command` | string | Shell command to execute |
| `timeout` | int | Execution timeout in seconds |

### `airflow`

Triggers an Apache Airflow DAG run and polls for completion.

```yaml
job:
  type: airflow
  config:
    url: https://airflow.example.com
    dagID: my_dag
    headers:
      Authorization: "Bearer ${AIRFLOW_TOKEN}"
    pollInterval: 1m
```

| Field | Type | Description |
|---|---|---|
| `url` | string | Airflow webserver URL |
| `dagID` | string | DAG identifier |
| `headers` | map | Authentication headers |
| `pollInterval` | string | Status poll interval (e.g. `1m`) |

### `glue`

Starts an AWS Glue job run.

```yaml
job:
  type: glue
  config:
    jobName: my-glue-etl
    arguments:
      "--pipeline": my-pipeline
      "--date": "${DATE}"
```

| Field | Type | Description |
|---|---|---|
| `jobName` | string | Glue job name |
| `arguments` | map | Job arguments (keys prefixed with `--`) |

**Returned metadata**: `jobRunId` -- used by the orchestrator to poll `GetJobRun`.

### `emr`

Submits a step to an existing EMR cluster.

```yaml
job:
  type: emr
  config:
    clusterID: j-1234567890ABC
    arguments:
      "--class": com.example.ETLJob
      "--jar": s3://bucket/etl.jar
```

| Field | Type | Description |
|---|---|---|
| `clusterID` | string | EMR cluster ID |
| `arguments` | map | Step arguments |

**Returned metadata**: `clusterID`, `stepId` -- used by the orchestrator to poll `DescribeStep`.

### `emr-serverless`

Starts a job run on EMR Serverless.

```yaml
job:
  type: emr-serverless
  config:
    applicationID: "00f00abcde12345"
    arguments:
      "--class": com.example.ETLJob
      "--s3-input": s3://bucket/input/
```

| Field | Type | Description |
|---|---|---|
| `applicationID` | string | EMR Serverless application ID |
| `arguments` | map | Job run arguments |

**Returned metadata**: `applicationId`, `jobRunId` -- used by the orchestrator to poll `GetJobRun`.

### `step-function`

Starts an AWS Step Functions execution.

```yaml
job:
  type: step-function
  config:
    stateMachineARN: arn:aws:states:us-east-1:123456789:stateMachine:my-workflow
    input: '{"key": "value"}'
```

| Field | Type | Description |
|---|---|---|
| `stateMachineARN` | string | Step Function state machine ARN |
| `input` | string | JSON input to the execution |

**Returned metadata**: `executionArn` -- used by the orchestrator to poll `DescribeExecution`.

### `databricks`

Submits a job run to Databricks via REST API 2.1.

```yaml
job:
  type: databricks
  config:
    workspaceURL: https://my-workspace.cloud.databricks.com
    headers:
      Authorization: "Bearer ${DATABRICKS_TOKEN}"
    body: '{"job_id": 12345, "notebook_params": {"date": "${DATE}"}}'
```

| Field | Type | Description |
|---|---|---|
| `workspaceURL` | string | Databricks workspace URL |
| `headers` | map | Authentication headers |
| `body` | string | JSON request body (Jobs API 2.1 format) |

**Returned metadata**: `runId` -- used by the orchestrator to poll via `GET /api/2.1/jobs/runs/get`.

## Template Variables

The `body`, `input`, and `arguments` fields support template substitution. Variables are resolved at trigger time:

| Variable | Value |
|---|---|
| `${DATE}` | Current date (`YYYY-MM-DD`) |
| `${PIPELINE}` | Pipeline ID |
| `${SCHEDULE}` | Schedule ID |
| `${RUN_ID}` | Run ID (UUID v4) |

## Status Polling

After a job executes, the orchestrator Lambda polls for completion using the returned metadata. Each job type implements `CheckStatus()`:

| Type | SDK Call | Status Mapping |
|---|---|---|
| `glue` | `GetJobRun` + CloudWatch RCA verification | RUNNING -> running, SUCCEEDED -> succeeded (verified), FAILED/STOPPED -> failed |
| `emr` | `DescribeStep` | RUNNING/PENDING -> running, COMPLETED -> succeeded, FAILED/CANCELLED -> failed |
| `emr-serverless` | `GetJobRun` | RUNNING/SUBMITTED -> running, SUCCESS -> succeeded, FAILED/CANCELLED -> failed |
| `step-function` | `DescribeExecution` | RUNNING -> running, SUCCEEDED -> succeeded, FAILED/TIMED_OUT/ABORTED -> failed |
| `databricks` | `GET /runs/get` | RUNNING/PENDING -> running, TERMINATED(SUCCESS) -> succeeded, others -> failed |
| `airflow` | `GET /dags/{id}/dagRuns` | running -> running, success -> succeeded, failed -> failed |

The `http` and `command` types are fire-and-forget -- they do not support status polling.

### Glue RCA Verification

AWS Glue can report `SUCCEEDED` via the `GetJobRun` API even when the Spark job failed internally (the driver process catches the exception and exits cleanly). When a Glue job reports `SUCCEEDED`, the orchestrator cross-checks the CloudWatch RCA (root cause analysis) log stream for `GlueExceptionAnalysisJobFailed` events. If the RCA log confirms a failure, the job is marked as failed with the actual failure reason.

This check requires `logs:FilterLogEvents` permission on the Glue log group (granted automatically when `enable_glue_trigger = true`). If the permission is missing or the RCA log stream does not exist, the check degrades gracefully and trusts the Glue API response.

## IAM Permissions

The Terraform module provides opt-in variables to grant the orchestrator Lambda permission to invoke each AWS job type:

| Variable | Default | Grants |
|---|---|---|
| `enable_glue_trigger` | `false` | `glue:StartJobRun`, `glue:GetJobRun`, `logs:FilterLogEvents` (scoped to `/aws-glue/jobs/logs-v2`) |
| `enable_emr_trigger` | `false` | `elasticmapreduce:AddJobFlowSteps`, `elasticmapreduce:DescribeStep` |
| `enable_emr_serverless_trigger` | `false` | `emr-serverless:StartJobRun`, `emr-serverless:GetJobRun` |
| `enable_sfn_trigger` | `false` | `states:StartExecution`, `states:DescribeExecution` |

Enable only the trigger types your pipelines use to maintain least-privilege IAM policies.
