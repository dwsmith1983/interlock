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
      Authorization: "Bearer ${INTERLOCK_API_TOKEN}"
    body: '{"pipeline": "${INTERLOCK_PIPELINE_ID}", "date": "${INTERLOCK_DATE}", "runId": "${INTERLOCK_RUN_ID}"}'
```

| Field | Type | Description |
|---|---|---|
| `method` | string | HTTP method (`GET`, `POST`, `PUT`) |
| `url` | string | Target URL |
| `headers` | map | HTTP headers |
| `body` | string | Request body (supports [template variables](#template-variables)) |

#### Run identity

Every request carries the run's identity in four headers, set after the
configured `headers` so config cannot override them. The same values expand in
`body` and header values:

| Header | Variable | Value |
|---|---|---|
| `X-Interlock-Pipeline-Id` | `${INTERLOCK_PIPELINE_ID}` | pipeline ID |
| `X-Interlock-Schedule-Id` | `${INTERLOCK_SCHEDULE_ID}` | schedule ID: `stream` for sensor-triggered pipelines, `cron` for cron ones |
| `X-Interlock-Date` | `${INTERLOCK_DATE}` | execution date: `2026-03-01`, or `2026-03-01T05` for an hourly run |
| `X-Interlock-Run-Id` | `${INTERLOCK_RUN_ID}` | run ID (the Step Functions execution name) |

In `body` the identity variables are inserted JSON-escaped (whatever the
pipeline ID holds, the body stays valid JSON), so use them inside JSON strings.
Header values are inserted as they are. When IDs are not plain ASCII, read them
from the body fields rather than the headers: net/http trims leading and
trailing whitespace and sends raw bytes in headers. A pipeline ID containing a
control character other than tab cannot be sent in a header at all, so its http
launches fail.

With no `body`, Interlock sends `par_day` (`20260301`), `par_hour` (`05`, hourly
dates only), `pipelineId`, `scheduleId`, `date` and `runId` as a JSON object.

The run ID is an identifier, not a secret: anything that can write to the joblog
table can report a result for any run.

#### Reporting the result

A 2xx response only means the job was accepted. The run stays running until the
job writes its result to the joblog table (Terraform output `joblog_table_name`,
named `{environment}-interlock-joblog`):

| Attribute | Value |
|---|---|
| `PK` | `PIPELINE#` followed by `X-Interlock-Pipeline-Id` |
| `SK` | `JOB#<X-Interlock-Schedule-Id>#<X-Interlock-Date>#<unix millis, now>` |
| `event` | `success`, `fail` or `timeout` |
| `runId` | `X-Interlock-Run-Id` |
| `ttl` | recommended: expiry in unix seconds (Interlock's own rows use now + 30 days) |
| `error` | optional failure message |
| `category` | optional failure category: `TRANSIENT`, `PERMANENT` or `TIMEOUT` |

```sh
aws dynamodb put-item --table-name prod-interlock-joblog --item '{
  "PK": {"S": "PIPELINE#my-pipeline"},
  "SK": {"S": "JOB#stream#2026-03-01#1772366400000"},
  "event": {"S": "success"},
  "runId": {"S": "<X-Interlock-Run-Id>"},
  "ttl": {"N": "1774958400"}
}'
```

The job needs `dynamodb:PutItem` on the joblog table. Scope it to the
pipeline's own partition, and keep the statement to item-level actions:
a `Scan` names no partition key, so `ForAllValues` on `dynamodb:LeadingKeys`
does not restrict it.

```json
{
  "Effect": "Allow",
  "Action": "dynamodb:PutItem",
  "Resource": "arn:aws:dynamodb:<region>:<account>:table/<environment>-interlock-joblog",
  "Condition": {
    "ForAllValues:StringEquals": {"dynamodb:LeadingKeys": ["PIPELINE#my-pipeline"]}
  }
}
```

What a row does:

- The run's job check reads only rows whose `runId` is its own run ID. The
  newest `success`, `fail` or `timeout` row ends the run. `success` sets the
  trigger to `COMPLETED`. On `fail` or `timeout` the run's CompleteTrigger
  starts the next attempt, a new execution with a new run ID, until
  `maxRetries` retries have run (`maxCodeRetries`, default 1, when the run's own
  row says `PERMANENT`); after that it publishes `RETRY_EXHAUSTED` and sets the
  trigger to `FAILED_FINAL`. An excluded date is never retried: it publishes
  `PIPELINE_EXCLUDED` and sets `FAILED_FINAL`. The count comes from the
  execution name (`-r<N>` is attempt N), so a requested rerun starts a new
  chain with a fresh budget.
- The stream-router publishes `JOB_COMPLETED` for every `success` row,
  whatever its `runId`, and starts nothing for `fail` or `timeout` rows.

A job that never reports: when `job.jobPollWindowSeconds` (default 3600) passes
with no result row for the run, the orchestrator writes a `job-poll-exhausted`
row and publishes `JOB_POLL_EXHAUSTED`, and CompleteTrigger treats the closed
window like any other timeout, retrying while the budget lasts. Set the window
to cover the job's runtime. A `fail` or `timeout` that arrives after its
execution has ended, or for a run ID that is not the current attempt's,
changes nothing; a lock it leaves stale is the watchdog's to clear.

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
      Authorization: "Bearer ${INTERLOCK_AIRFLOW_TOKEN}"
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
```

| Field | Type | Description |
|---|---|---|
| `jobName` | string | Glue job name |
| `arguments` | map | Job arguments (keys prefixed with `--`); Interlock adds `--par_day` and, for hourly dates, `--par_hour` |

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
      Authorization: "Bearer ${INTERLOCK_DATABRICKS_TOKEN}"
    body: '{"job_id": 12345}'
```

| Field | Type | Description |
|---|---|---|
| `workspaceURL` | string | Databricks workspace URL |
| `headers` | map | Authentication headers |
| `body` | string | JSON request body (Jobs API 2.1 format) |

**Returned metadata**: `runId` -- used by the orchestrator to poll via `GET /api/2.1/jobs/runs/get`.

## Template Variables

Variables are expanded when the job is triggered. Any other `${...}` expands to
an empty string.

| Variable | Expands in | Value |
|---|---|---|
| `${INTERLOCK_<NAME>}` | `http` body and headers, `airflow` body and headers, `databricks` headers, `lambda` payload | the orchestrator Lambda's environment variable `INTERLOCK_<NAME>` |
| `${INTERLOCK_PIPELINE_ID}`, `${INTERLOCK_SCHEDULE_ID}`, `${INTERLOCK_DATE}`, `${INTERLOCK_RUN_ID}` | `http` body and headers | the [run identity](#run-identity), which takes precedence over an environment variable of the same name |

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

The `http`, `command` and `lambda` types have no status API. An `http` job reports its own result (see [`http`](#http)); `command` and `lambda` are recorded as succeeded once the call returns.

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
