---
title: Triggers
weight: 3
description: All 8 trigger types with TriggerConfig examples and metadata keys.
---

When a pipeline reaches `READY` status, Interlock executes its configured trigger to start the downstream job. The `trigger.Runner` dispatches to type-specific handlers and returns metadata for status polling.

## TriggerConfig

```yaml
trigger:
  type: http           # required: trigger type
  method: POST         # type-specific fields below
  url: https://...
  timeout: 60
```

### Common Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | string | — | Trigger type (required) |
| `timeout` | int | 30 | Execution timeout in seconds |

## Trigger Types

### `http`

Sends an HTTP request to an arbitrary endpoint.

```yaml
trigger:
  type: http
  method: POST
  url: https://api.example.com/jobs/start
  headers:
    Authorization: "Bearer ${TOKEN}"
    Content-Type: application/json
  body: '{"pipeline": "my-pipeline", "date": "${DATE}"}'
  timeout: 30
```

| Field | Type | Description |
|---|---|---|
| `method` | string | HTTP method (`GET`, `POST`, `PUT`) |
| `url` | string | Target URL |
| `headers` | map | HTTP headers |
| `body` | string | Request body (supports template variables) |

### `command`

Executes a shell command on the local machine. Only available in local (watcher) mode.

```yaml
trigger:
  type: command
  command: /usr/local/bin/run-etl --pipeline my-pipeline
  timeout: 300
```

| Field | Type | Description |
|---|---|---|
| `command` | string | Shell command to execute |

### `airflow`

Triggers an Apache Airflow DAG run and polls for completion.

```yaml
trigger:
  type: airflow
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
trigger:
  type: glue
  jobName: my-glue-etl
  arguments:
    "--pipeline": my-pipeline
    "--date": "${DATE}"
```

| Field | Type | Description |
|---|---|---|
| `jobName` | string | Glue job name |
| `arguments` | map | Job arguments (keys prefixed with `--`) |

**Returned metadata**: `jobRunId` — used by `run-checker` to poll `GetJobRun`.

### `emr`

Submits a step to an existing EMR cluster.

```yaml
trigger:
  type: emr
  clusterID: j-1234567890ABC
  arguments:
    "--class": com.example.ETLJob
    "--jar": s3://bucket/etl.jar
```

| Field | Type | Description |
|---|---|---|
| `clusterID` | string | EMR cluster ID |
| `arguments` | map | Step arguments |

**Returned metadata**: `clusterID`, `stepId` — used by `run-checker` to poll `DescribeStep`.

### `emr-serverless`

Starts a job run on EMR Serverless.

```yaml
trigger:
  type: emr-serverless
  applicationID: "00f00abcde12345"
  arguments:
    "--class": com.example.ETLJob
    "--s3-input": s3://bucket/input/
```

| Field | Type | Description |
|---|---|---|
| `applicationID` | string | EMR Serverless application ID |
| `arguments` | map | Job run arguments |

**Returned metadata**: `applicationId`, `jobRunId` — used by `run-checker` to poll `GetJobRun`.

### `step-function`

Starts an AWS Step Functions execution.

```yaml
trigger:
  type: step-function
  stateMachineARN: arn:aws:states:us-east-1:123456789:stateMachine:my-workflow
  body: '{"input": "value"}'
```

| Field | Type | Description |
|---|---|---|
| `stateMachineARN` | string | Step Function state machine ARN |
| `body` | string | JSON input to the execution |

**Returned metadata**: `executionArn` — used by `run-checker` to poll `DescribeExecution`.

### `databricks`

Submits a job run to Databricks via REST API 2.1.

```yaml
trigger:
  type: databricks
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

**Returned metadata**: `runId` — used by `run-checker` to poll via `GET /api/2.1/jobs/runs/get`.

## Template Variables

The `body` and `arguments` fields support template substitution. Variables are resolved at trigger time:

| Variable | Value |
|---|---|
| `${DATE}` | Current date (`YYYY-MM-DD`) |
| `${PIPELINE}` | Pipeline ID |
| `${SCHEDULE}` | Schedule ID |
| `${RUN_ID}` | Run ID (UUID v4) |

## Status Polling

After a trigger executes, the `run-checker` Lambda (or watcher loop) polls for completion using the returned metadata. Each trigger type implements `CheckStatus()`:

| Type | SDK Call | Status Mapping |
|---|---|---|
| `glue` | `GetJobRun` | RUNNING→running, SUCCEEDED→succeeded, FAILED/STOPPED→failed |
| `emr` | `DescribeStep` | RUNNING/PENDING→running, COMPLETED→succeeded, FAILED/CANCELLED→failed |
| `emr-serverless` | `GetJobRun` | RUNNING/SUBMITTED→running, SUCCESS→succeeded, FAILED/CANCELLED→failed |
| `step-function` | `DescribeExecution` | RUNNING→running, SUCCEEDED→succeeded, FAILED/TIMED_OUT/ABORTED→failed |
| `databricks` | `GET /runs/get` | RUNNING/PENDING→running, TERMINATED(SUCCESS)→succeeded, others→failed |
| `airflow` | `GET /dags/{id}/dagRuns` | running→running, success→succeeded, failed→failed |

The `http` and `command` types are fire-and-forget — they do not support status polling.
