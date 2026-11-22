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

The `config` keys are the names in each type's table, such as `dagId` and
`clusterId` (not `dag_id`); they match without regard to case. Validation rejects a config with a key its type does
not have or without a field marked required, so the pipeline is skipped at load
rather than failing when it triggers.

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
| `method` | string | HTTP method, upper case: `GET`, `HEAD`, `POST` (default), `PUT`, `PATCH` or `DELETE` |
| `url` | string | Target URL (required) |
| `headers` | map | HTTP headers |
| `body` | string | Request body (supports [template variables](#template-variables)) |
| `timeout` | int | Request timeout in seconds; unset or 0 uses 30 |

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
otherwise ignored; a row it leaves `RUNNING` is closed by the watchdog with
`SFN_TIMEOUT` once its stale deadline (`staleAt`, `sfn_timeout_seconds` plus 30
minutes from the latest attempt) passes.

### `airflow`

Triggers an Apache Airflow DAG run and polls for completion.

```yaml
job:
  type: airflow
  config:
    url: https://airflow.example.com
    dagId: my_dag
    headers:
      Authorization: "Bearer ${INTERLOCK_AIRFLOW_TOKEN}"
```

| Field | Type | Description |
|---|---|---|
| `url` | string | Airflow webserver URL (required) |
| `dagId` | string | DAG identifier (required) |
| `headers` | map | Authentication headers |
| `body` | string | JSON sent as the DAG run's `conf` (optional) |
| `timeout` | int | Timeout of the request that starts the DAG run, in seconds; unset or 0 uses 30 |
| `apiVersion` | int | Airflow major version: `2` (default) or `3` |

The DAG run's `conf` carries the run. With no `body` it is the same JSON object
the `http` default body is (`par_day`, `par_hour` for hourly dates,
`pipelineId`, `scheduleId`, `date`, `runId`). A configured `body` expands the
[run identity](#run-identity) variables JSON-escaped, so use them inside JSON
strings. The headers expand only `INTERLOCK_` environment variables: the status
poll sends them again without a run identity.

`apiVersion: 3` targets the Airflow 3 REST API: the run starts with
`POST /api/v2/dags/{dagId}/dagRuns` and is polled at
`/api/v2/dags/{dagId}/dagRuns/{runID}`. The request body carries
`"logical_date": null`, which Airflow 3 requires, so Airflow picks the run's
date as Airflow 2 does; the run date still reaches the DAG through `conf`.
Airflow 2 (`apiVersion` unset or `2`) uses `/api/v1` and sends only `conf`.
Airflow 3 authenticates with a JWT bearer token: keep it in the
[trigger secret](#setting-interlock_-values) like any other token and send it as
`Authorization: "Bearer ${INTERLOCK_AIRFLOW_TOKEN}"` in `headers`. Airflow 3
access tokens expire after the lifetime set by Airflow's
`[api_auth] jwt_expiration_time`, and trigger values load at cold start, so a
renewed token reaches the orchestrator only once the secret is updated and new
containers start (see [Setting `INTERLOCK_` values](#setting-interlock_-values)).

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
| `jobName` | string | Glue job name (required) |
| `arguments` | map | Job arguments (keys prefixed with `--`); Interlock adds `--par_day` and, for hourly dates, `--par_hour` |

**Returned metadata**: `jobRunId` -- used by the orchestrator to poll `GetJobRun`.

### `emr`

Submits a step to an existing EMR cluster.

```yaml
job:
  type: emr
  config:
    clusterId: j-1234567890ABC
    stepName: daily-etl
    command: s3://bucket/etl.jar
    arguments:
      "--class": com.example.ETLJob
```

| Field | Type | Description |
|---|---|---|
| `clusterId` | string | EMR cluster ID (required) |
| `stepName` | string | Name of the step (required) |
| `command` | string | JAR the step runs, such as an S3 URI (required) |
| `arguments` | map | Step arguments, sent as `key=value` in key order; Interlock adds `--par_day` (`--par_day=20260301`) and, for hourly dates, `--par_hour`, replacing a configured value of the same key |

**Returned metadata**: `clusterID`, `stepId` -- used by the orchestrator to poll `DescribeStep`.

### `emr-serverless`

Starts a Spark job run on an EMR Serverless application.

```yaml
job:
  type: emr-serverless
  config:
    applicationId: "00f00abcde12345"
    jobName: daily-etl
    executionRoleArn: arn:aws:iam::123456789012:role/emr-serverless-job
    entryPoint: s3://bucket/jobs/etl.py
    entryPointArguments: ["--input", "s3://bucket/input/"]
    sparkSubmitParameters: "--conf spark.executor.cores=2"
```

| Field | Type | Description |
|---|---|---|
| `applicationId` | string | EMR Serverless application ID (required) |
| `jobName` | string | Job run name (required) |
| `executionRoleArn` | string | IAM runtime role the job run executes under (required); it must be in the Terraform module's `emr_serverless_execution_role_arns` |
| `entryPoint` | string | Script or JAR that `spark-submit` runs, such as an S3 URI (required) |
| `entryPointArguments` | list | Arguments passed to the entry point (optional); Interlock adds `--par_day` `20260301` and, for hourly dates, `--par_hour` `05`, each flag followed by its value. A flag already in the list keeps its place and takes the run's value, so validation rejects one with no value after it (last in the list, or followed by another `--` flag). Only the two-argument form (`--par_day`, `20260301`) is recognised: a `--par_day=20260301` element is left as is and the run's flag is appended |
| `sparkSubmitParameters` | string | Extra `spark-submit` parameters (optional) |

The job run's client token is derived from the run ID, so when the Trigger
state retries a start whose response was lost, the retry reuses the token and
EMR Serverless does not start a second job run. A failure retry or a rerun has
a new run ID and starts a new job run.

Config values are stored in plaintext and are visible in `GetJobRun`, so a job
that needs a credential should read its own secret under its runtime role
rather than take it as an argument.

**Returned metadata**: `applicationId`, `jobRunId` -- used by the orchestrator to poll `GetJobRun`.

### `step-function`

Starts an AWS Step Functions execution.

```yaml
job:
  type: step-function
  config:
    stateMachineArn: arn:aws:states:us-east-1:123456789:stateMachine:my-workflow
    arguments:
      env: prod
```

| Field | Type | Description |
|---|---|---|
| `stateMachineArn` | string | Step Function state machine ARN (required) |
| `arguments` | map | Top-level string fields of the execution input; Interlock adds `par_day` (`20260301`) and, for hourly dates, `par_hour` (`05`), replacing a configured value of the same key |

**Returned metadata**: `executionArn` -- used by the orchestrator to poll `DescribeExecution`.

### `databricks`

Starts a run of an existing Databricks job with `POST /api/2.1/jobs/run-now`
and polls it to completion.

```yaml
job:
  type: databricks
  config:
    workspaceUrl: https://my-workspace.cloud.databricks.com
    jobId: "123456789012345"
    headers:
      Authorization: "Bearer ${INTERLOCK_DATABRICKS_TOKEN}"
    arguments:
      env: prod
```

| Field | Type | Description |
|---|---|---|
| `workspaceUrl` | string | Databricks workspace URL (required) |
| `jobId` | string | Numeric ID of the job to start, quoted so an ID of any size stays exact (required) |
| `headers` | map | Authentication headers |
| `arguments` | map | Job parameters of the run; Interlock adds `par_day` (`20260301`) and, for hourly dates, `par_hour` (`05`), replacing a configured value of the same key |

The request body is `{"job_id": <jobId>, "idempotency_token": <token>,
"job_parameters": <arguments>}`, so the job must define job-level parameters
named `par_day` and `par_hour` (and any other key in `arguments`) for the run
to accept them. Validation rejects a
`jobId` that is missing, unquoted (a number loses precision above 2^53) or not
a positive base-10 integer, and the trigger checks it again before any request.

The run-now `idempotency_token` is derived from the run ID (its hex SHA-256,
64 characters, the most Databricks accepts), so when the Trigger state retries
a start whose response was lost, the retry sends the same token and Databricks
returns the run it already started instead of starting a second one. A redrive
that runs the Trigger state again keeps the run ID and so also gets the earlier
run back. A failure retry, a rerun or a run the watchdog starts has a new run
ID and starts a new run. If the earlier run was deleted, Databricks rejects the
token and the retry fails rather than starting another run.

**Returned metadata**: `databricks_workspace_url`, `databricks_run_id` -- used by the orchestrator to poll `GET /api/2.1/jobs/runs/get`.

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
| `functionName` | string | Function name or ARN (required) |
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
- The function is still running when the orchestrator's wait runs out: the orchestrator writes a `fail`
  row with category `OUTCOME_UNKNOWN`, and CompleteTrigger sets `FAILED_FINAL` without a retry under
  either budget, since a second call could run the job twice. The row's `JOB_FAILED` alert carries the
  message; no `RETRY_EXHAUSTED` is published.

A `lambda` job must finish within the orchestrator's wait: the orchestrator Lambda's remaining time less
15 seconds, about 100 seconds with the module's 120-second timeout. Longer work belongs in a
[`step-function`](#step-function) job, which the orchestrator polls instead of waiting on. When the wait
runs out, the orchestrator stops waiting but the function keeps running to its own timeout, and its result
is not recorded.

## Template Variables

Variables are expanded when the job is triggered. Any other `${...}` expands to
an empty string.

| Variable | Expands in | Value |
|---|---|---|
| `${INTERLOCK_<NAME>}` | `http` body and headers, `airflow` body and headers, `databricks` headers, `lambda` payload | the orchestrator Lambda's environment variable `INTERLOCK_<NAME>` |
| `${INTERLOCK_PIPELINE_ID}`, `${INTERLOCK_SCHEDULE_ID}`, `${INTERLOCK_DATE}`, `${INTERLOCK_RUN_ID}` | `http` body and headers, `lambda` payload, `airflow` body | the [run identity](#run-identity), which takes precedence over an environment variable of the same name |

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

Both take keys matching `^INTERLOCK_[A-Z0-9_]+$`, except the run identity
variables (`INTERLOCK_PIPELINE_ID`, `INTERLOCK_SCHEDULE_ID`, `INTERLOCK_DATE`,
`INTERLOCK_RUN_ID`), which each run sets itself: Terraform rejects them in
`orchestrator_env`. The orchestrator reads the secret at cold start
and sets each pair in its environment, so a secret key wins over an
`orchestrator_env` entry of the same name. A secret that cannot be read, is not
a JSON object of strings, has another key or a run identity key, or has a value
containing a control character (a trailing newline, for one) stops the cold
start.

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
| `airflow` | `GET /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}` (`/api/v2/...` with `apiVersion: 3`) | running -> running, success -> succeeded, failed -> failed |

The `airflow` and `databricks` polls send the trigger's configured headers, expanded like the trigger's own. A poll runs only when the run's URL, kept from the trigger call, has the scheme and host of the trigger config, so the headers never go elsewhere: changing a trigger's URL while a run is polling stops the polls for that run until the state machine times out.

The `http` and `lambda` types have no status API. An `http` job reports its own result (see [`http`](#http)); a `lambda` job's result is the call's outcome (see [`lambda`](#lambda)).

### Glue RCA Verification

AWS Glue can report `SUCCEEDED` via the `GetJobRun` API even when the Spark job failed internally (the driver process catches the exception and exits cleanly). When a Glue job reports `SUCCEEDED`, the orchestrator reads the run's job insights exception analysis stream, `<runId>-job-insights-rca-driver`, for a `GlueExceptionAnalysisJobFailed` event. If it finds one, the job is marked as failed, and the event's failure reason, with control characters other than newline and tab removed and cut to 512 bytes, goes into the failure message and the alerts that carry it.

Up to Glue 4.0, for a job with job run insights on (`--enable-job-insights`), Glue writes that stream when a Spark job in the run fails, including a run whose script catches the failure and exits cleanly. A job without job insights gets no false-success check, and its `SUCCEEDED` is trusted as reported.

Only `/aws-glue/jobs/logs-v2` is read, always. A job with a security configuration or a custom `--continuous-log-logGroup` writes its job insights streams elsewhere, so it gets no false-success check. Glue 5.0 writes no job insights stream, even for a run that fails with job insights on, so a Glue 5.0 job gets no false-success check, and its `SUCCEEDED` is taken as reported. AWS's Glue 5.0 migration guide says the stream moved to `/aws-glue/jobs/error`, but Glue 5.0 runs write none there either. On Glue 5.0, a script that catches an error must exit non-zero (re-raise it, or call `sys.exit(1)`) for the run to fail.

This check requires `logs:FilterLogEvents` permission on `/aws-glue/jobs/logs-v2` (granted automatically when `enable_glue_trigger = true`). A missing stream or log group logs nothing, since CloudWatch returns `ResourceNotFoundException` for both and a missing stream is the normal result for a clean success. Any other failed query, such as a missing permission or CloudWatch Logs being unreachable, trusts the Glue response and logs one warning naming the log group, the stream and the AWS error code. If the orchestrator cannot build a CloudWatch Logs client, the check is skipped with a warning naming the log group, the run and the error code.

## IAM Permissions

The Terraform module provides opt-in variables to grant the orchestrator Lambda permission to invoke each AWS job type:

| Variable | Default | Grants |
|---|---|---|
| `enable_glue_trigger` | `false` | `glue:StartJobRun`, `glue:GetJobRun`, `logs:FilterLogEvents` (scoped to `/aws-glue/jobs/logs-v2`) |
| `enable_emr_trigger` | `false` | `elasticmapreduce:AddJobFlowSteps`, `elasticmapreduce:DescribeStep` |
| `enable_emr_serverless_trigger` | `false` | `emr-serverless:StartJobRun` on `emr_serverless_app_arns`, `emr-serverless:GetJobRun` on their job runs, `iam:PassRole` on `emr_serverless_execution_role_arns` (passed to `emr-serverless.amazonaws.com` only) |
| `enable_sfn_trigger` | `false` | `states:StartExecution` on `sfn_trigger_arns`, `states:DescribeExecution` on their executions |
| `enable_lambda_trigger` | `false` | `lambda:InvokeFunction` on `lambda_trigger_arns` (required when enabled) |

Enable only the trigger types your pipelines use to maintain least-privilege IAM policies.
