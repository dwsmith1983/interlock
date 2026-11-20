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
- The stream-router reports every new `JOB#` row, whatever its `runId`:
  `success` publishes `JOB_COMPLETED`, `fail` and `timeout` publish
  `JOB_FAILED` (error cut to 2 KB). It starts nothing. A row that overwrites an
  earlier one, has an invalid date, or belongs to a schedule and date with no
  trigger row (no dry-run marker, for a dry-run pipeline) is not reported.

A job that never reports: when `job.jobPollWindowSeconds` (default 3600) passes
with no result row for the run, the orchestrator writes a `timeout` row for the
run (reported as `JOB_FAILED`) and CompleteTrigger treats it like any other
timeout, retrying while the budget lasts. Set the window to cover the job's
runtime. A `fail` or `timeout` that arrives after its execution has ended, or
for a run ID that is not the current attempt's, publishes `JOB_FAILED` and is
otherwise ignored; a lock it leaves stale is the watchdog's to clear.

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
| `body` | string | JSON sent as the DAG run's `conf` (optional) |
| `pollInterval` | string | Status poll interval (e.g. `1m`) |

The DAG run's `conf` carries the run. With no `body` it is the same JSON object
the `http` default body is (`par_day`, `par_hour` for hourly dates,
`pipelineId`, `scheduleId`, `date`, `runId`). A configured `body` expands the
[run identity](#run-identity) variables JSON-escaped, so use them inside JSON
strings. The headers expand only `INTERLOCK_` environment variables: the status
poll sends them again without a run identity.

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
| `arguments` | map | Step arguments, sent as `key=value` in key order; Interlock adds `--par_day` (`--par_day=20260301`) and, for hourly dates, `--par_hour`, replacing a configured value of the same key |

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
    arguments:
      env: prod
```

| Field | Type | Description |
|---|---|---|
| `stateMachineARN` | string | Step Function state machine ARN |
| `arguments` | map | Top-level string fields of the execution input; Interlock adds `par_day` (`20260301`) and, for hourly dates, `par_hour` (`05`), replacing a configured value of the same key |

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

### `lambda`

Invokes an AWS Lambda function synchronously (`RequestResponse`) and waits for it to return.

```yaml
job:
  type: lambda
  config:
    functionName: my-audit
    payload: '{"source": "orders"}'
```

| Field | Type | Description |
|---|---|---|
| `functionName` | string | Function name or ARN |
| `payload` | string | JSON event passed to the function (optional) |

The event carries the run. With no `payload` it is the same JSON object the
`http` default body is (`par_day`, `par_hour` for hourly dates, `pipelineId`,
`scheduleId`, `date`, `runId`). A configured `payload` expands the
[run identity](#run-identity) variables JSON-escaped, so use them inside JSON
strings. `INTERLOCK_<NAME>` values are inserted as they are, and the expanded
`payload` must be valid JSON: otherwise the job fails before the function is
invoked, with an error that does not quote the payload.

The call's outcome is the job's result, written to the joblog under the run ID:

- The function returns normally: a `success` row, and the trigger becomes `COMPLETED`.
- The function returns an error (a `FunctionError`: an unhandled exception, and also a function timeout or
  running out of memory): a `fail` row with category `PERMANENT`. Up to 200 bytes of the function's error
  text go into the row's `error` and so into the `JOB_FAILED` alert. CompleteTrigger retries it under
  `maxCodeRetries` (default 1), not `maxRetries`, like any `PERMANENT` failure.
- The function could not be invoked (throttling, a missing function, no permission): the function never
  ran, so the orchestrator writes `infra-trigger-failure` and the Trigger state's own retry calls it again
  (see [Error Handling](../../architecture/aws/#error-handling)).

## Template Variables

Variables are expanded when the job is triggered. Any other `${...}` expands to
an empty string.

| Variable | Expands in | Value |
|---|---|---|
| `${INTERLOCK_<NAME>}` | `http` body and headers, `airflow` body and headers, `databricks` headers, `lambda` payload | the orchestrator Lambda's environment variable `INTERLOCK_<NAME>` |
| `${INTERLOCK_PIPELINE_ID}`, `${INTERLOCK_SCHEDULE_ID}`, `${INTERLOCK_DATE}`, `${INTERLOCK_RUN_ID}` | `http` body and headers, `lambda` payload, `airflow` body | the [run identity](#run-identity), which takes precedence over an environment variable of the same name |

`emr-serverless` and `databricks` jobs do not receive the run date yet.

### Setting `INTERLOCK_` values

The Terraform module sets the orchestrator's `INTERLOCK_` variables in two ways:

- `orchestrator_env`, a map of plain Lambda environment variables. Its values
  are stored in Terraform state and shown in plans and the Lambda console, so
  keep it for non-secret values.
- `trigger_secrets_arn`, the full ARN of a Secrets Manager secret, including
  its random 6-character suffix, in the same region as the deployment. Its
  value is a JSON object of strings, for tokens:

  ```json
  {"INTERLOCK_API_TOKEN": "...", "INTERLOCK_AIRFLOW_TOKEN": "..."}
  ```

  The module grants the orchestrator `secretsmanager:GetSecretValue` on that
  secret only. A secret under the default `aws/secretsmanager` key needs
  nothing more. A secret under a customer managed key needs
  `kms:Decrypt` on that key for the orchestrator role
  (`<environment>-interlock-orchestrator`), and the key policy must allow it.
  `kms_key_arn` covers this only when it is the same key; a key dedicated to
  the secret keeps the other Lambda roles, which hold `kms:Decrypt` on
  `kms_key_arn`, out of it.

For example, create the secret and look up its full ARN, suffix included:

```sh
aws secretsmanager create-secret --name <env>-interlock-triggers \
  --secret-string '{"INTERLOCK_AIRFLOW_TOKEN":"..."}'
aws secretsmanager describe-secret --secret-id <env>-interlock-triggers \
  --query ARN --output text
```

then set that ARN in your tfvars:

```hcl
trigger_secrets_arn = "<that ARN>"
```

The secret must exist before `terraform apply`, or the orchestrator's cold
start fails.

Both take keys matching `^INTERLOCK_[A-Z0-9_]+$`. A key named like a run
identity variable (`INTERLOCK_PIPELINE_ID`, `INTERLOCK_SCHEDULE_ID`,
`INTERLOCK_DATE`, `INTERLOCK_RUN_ID`) is accepted, but the run identity wins
where the table above says so. The orchestrator reads the secret at cold start
and sets each pair in its environment, so a secret key wins over an
`orchestrator_env` entry of the same name. A secret that cannot be read, is not
a JSON object of strings, has another key, or has a value containing a control
character (a trailing newline, for one) stops the cold start.

Because values load at cold start, running containers keep the old value after
a rotation. A code or configuration update to the function (a new build, or a
changed `orchestrator_env` value) starts new containers. After a leak, revoke
the old token at its issuer rather than relying on rotation.

The error of a failed `http`, `airflow` or `databricks` call includes up to 512
bytes of the response body, and a `lambda` job's error keeps up to 200 bytes of
the function's error payload. Both can reach the job log, the orchestrator's
CloudWatch logs, the Step Functions execution history, events and alerts. So
that an endpoint or function echoing the request does not leak a token, every
`INTERLOCK_<NAME>` value `${...}` put into that request is replaced with
`[REDACTED]` in the request's error text before it is cut to those limits. The
forms matched are the value as is, JSON-escaped (with or without `<`, `>` and
`&` as `\u003c`, `\u003e` and `\u0026`, and with or without `/` as `\/`), and
URL-encoded for a query or a path (percent-encoding with uppercase hex, as Go's
query and path escaping writes it). An echo that escapes the value twice is not
matched. Values shorter than 4 characters are not replaced, since that would
garble unrelated text. A status poll redacts the values its headers expand to
the same way.

This guards against an endpoint or function that echoes the request by
accident. It does not cover one that rewrites the token on purpose (it already
holds the token), or other secrets an endpoint or function holds, so keep
trigger endpoints from echoing credentials and functions from putting their
event in error messages.

Trigger calls follow a redirect only to the same host as written in the URL,
including any explicit port, keeping the scheme or going from `http` to `https`,
and keeping the method. An `http` to `https` upgrade is followed when the URL
has no port (`http://a.example` to `https://a.example`); with an explicit port
the port changes too, and the redirect is refused. A redirect to another host or
port, from `https` to `http`, or one that changes the method (a 301 or 302 turns
a POST into a GET and drops its body) is not followed, so configured headers
never travel elsewhere and a request is never sent without its body. The call
fails with an error naming the redirect target (without userinfo or query);
point the trigger URL at the final endpoint.

An `http` job whose POST (or other non-GET request) is answered with 303 See
Other counts as triggered, as for a 2xx: the endpoint processed the request, and
the job reports its own result. The redirect is not followed. A GET follows a
same-origin 303 like any other redirect. `airflow` and `databricks` calls fail
on any 3xx, since they need the run ID from a 2xx body.

`http`, `airflow` and `databricks` responses are read up to 1 MiB. A larger one
fails with `response exceeds 1 MiB`.

Anyone with write access to the control table, where pipeline configs live, can
send any `INTERLOCK_` value to a URL of their choice, for example through an
`http` trigger's headers or body. Write access to the control table therefore
amounts to read access on these tokens.

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

The `http` and `lambda` types have no status API. An `http` job reports its own result (see [`http`](#http)); a `lambda` job's result is the call's outcome (see [`lambda`](#lambda)).

### Glue RCA Verification

AWS Glue can report `SUCCEEDED` via the `GetJobRun` API even when the Spark job failed internally (the driver process catches the exception and exits cleanly). When a Glue job reports `SUCCEEDED`, the orchestrator cross-checks the CloudWatch RCA (root cause analysis) log stream for `GlueExceptionAnalysisJobFailed` events. If the RCA log confirms a failure, the job is marked as failed with the actual failure reason.

This check requires `logs:FilterLogEvents` permission on the Glue log group (granted automatically when `enable_glue_trigger = true`). If the permission is missing or the RCA log stream does not exist, the check degrades gracefully and trusts the Glue API response.

## IAM Permissions

The Terraform module provides opt-in variables to grant the orchestrator Lambda permission to invoke each AWS job type:

| Variable | Default | Grants |
|---|---|---|
| `enable_glue_trigger` | `false` | `glue:StartJobRun`, `glue:GetJobRun`, `logs:FilterLogEvents` (scoped to `/aws-glue/jobs/logs-v2`) |
| `enable_emr_trigger` | `false` | `elasticmapreduce:AddJobFlowSteps`, `elasticmapreduce:DescribeStep` |
| `enable_emr_serverless_trigger` | `false` | `emr-serverless:StartJobRun` on `emr_serverless_app_arns`, `emr-serverless:GetJobRun` on their job runs |
| `enable_sfn_trigger` | `false` | `states:StartExecution` on `sfn_trigger_arns`, `states:DescribeExecution` on their executions |
| `enable_lambda_trigger` | `false` | `lambda:InvokeFunction` on `lambda_trigger_arns` (required when enabled) |

Enable only the trigger types your pipelines use to maintain least-privilege IAM policies.
