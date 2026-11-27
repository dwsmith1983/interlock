---
title: Quickstart
weight: 2
description: Define pipelines, deploy to AWS, and push sensor data.
---

This guide walks through one pipeline from start to finish: write its YAML config, deploy the Terraform module, push sensor data to DynamoDB, and check that the job ran. It assumes the prerequisites in [Installation](../installation/), plus an existing AWS Glue job that succeeds when run and its ARN: Interlock starts that job, it does not create one.

## 1. Write Pipeline YAML Configs

Create a `pipelines/` directory next to your Terraform configuration and add one YAML file per pipeline. Name each file after its pipeline ID: `pipelines/gold-revenue.yaml` for `pipeline.id: gold-revenue`. `terraform plan` fails when the two differ. The module reads only the `*.yaml` and `*.yml` files directly in the directory, not subdirectories.

This guide runs `gold-revenue`, which starts an AWS Glue job once two sensors say the day's data is ready:

```yaml
# pipelines/gold-revenue.yaml
pipeline:
  id: gold-revenue
  owner: analytics-team
  description: Gold-tier revenue aggregation pipeline

schedule:
  timezone: UTC
  trigger:
    key: upstream-complete
    check: equals
    field: status
    value: ready
    cutoff: "23:00"
  evaluation:
    window: 1h
    interval: 5m

validation:
  trigger: "ALL"
  rules:
    - key: upstream-complete
      check: equals
      field: status
      value: ready
    - key: upstream-complete
      check: age_lt
      field: updatedAt
      value: 2h
    - key: row-count
      check: gte
      field: count
      value: 1000

job:
  type: glue
  config:
    jobName: gold-revenue-etl
  maxRetries: 2
```

What it does:

- `schedule.trigger`: a write to the `upstream-complete` sensor with `status: ready` starts a run for the date the sensor names. `cutoff` is the last time (23:00 in `schedule.timezone`) a sensor can start a run for that date. Every pipeline needs a trigger with a cutoff.
- `schedule.evaluation`: once started, the rules are checked every 5 minutes for up to 1 hour.
- `validation`: with `ALL`, every rule must pass. A rule reads the sensor named by its own `key`, so each key a rule uses must be written (step 3).
- `job`: when the rules pass, Interlock starts the Glue job `gold-revenue-etl` and polls it until it finishes. Use the name of an existing Glue job in your account that succeeds when run. Interlock adds the argument `--par_day` with the run date as `YYYYMMDD` (for example `--par_day 20261127`); the job can read it or ignore it.

### Other Job Types

The same pipeline can start other jobs. This one posts to an HTTP endpoint instead:

```yaml
# pipelines/silver-orders.yaml
pipeline:
  id: silver-orders
  owner: data-platform
  description: Silver-tier orders ingestion pipeline

schedule:
  trigger:
    key: orders-landed
    check: exists
    cutoff: "23:00"

validation:
  trigger: "ANY"
  rules:
    - key: orders-landed
      check: exists
    - key: orders-count
      check: gt
      field: count
      value: 0

job:
  type: http
  config:
    url: https://example.com/trigger
    method: POST
  maxRetries: 1
```

A 2xx response only means the job was accepted. An `http` job must then write its result to the joblog table, or the run waits until its poll window ends (1 hour by default) and fails as a timeout; see [Reporting the result](../../configuration/triggers/#reporting-the-result). For a first run, a `glue`, `emr`, `emr-serverless`, `step-function` or `lambda` job is simpler: Interlock gets the result from AWS itself.

### Pipeline Config Reference

| Section | Purpose |
|---|---|
| `pipeline` | Identity: `id`, `owner`, `description` |
| `schedule` | When to evaluate: `trigger` condition with its `cutoff` (required), `cron`, `timezone`, evaluation `window` and `interval` |
| `sla` | SLA deadlines: `deadline`, `deadlineDayOffset`, `expectedDuration`, `maxDuration` |
| `validation` | Readiness rules: `trigger` mode (`ALL` or `ANY`), list of `rules` |
| `job` | What to trigger: `type` (http, airflow, glue, emr, emr-serverless, step-function, databricks, lambda), `config`, `maxRetries` |

### Validation Rule Checks

| Check | Description |
|---|---|
| `exists` | The sensor row exists |
| `equals` | Field equals expected value |
| `gt` / `gte` | Field is greater than (or equal to) value |
| `lt` / `lte` | Field is less than (or equal to) value |
| `age_lt` | Field timestamp (RFC 3339, or epoch seconds or milliseconds) is younger than the given duration |

## 2. Deploy the Terraform Module

Build the Lambda binaries from a clone of the repository, checked out at the release you want to run (see [Installation](../installation/#1-build-lambda-binaries)):

```bash
cd interlock
git checkout <release-tag>
./deploy/build.sh
```

Reference the module from the same clone, so the module and the binaries match. The module declares no provider, so configure the AWS provider in your own configuration:

```hcl
provider "aws" {
  region = "us-east-1"
}

module "interlock" {
  source = "path/to/interlock/deploy/terraform"

  environment    = "production"
  dist_path      = "path/to/interlock/deploy/dist"  # output of ./deploy/build.sh
  pipelines_path = "${path.module}/pipelines"

  # gold-revenue starts a Glue job
  enable_glue_trigger = true
  glue_job_arns       = ["arn:aws:glue:us-east-1:123456789012:job/gold-revenue-etl"]

  tags = {
    Project = "my-data-platform"
  }
}
```

Replace the region, account ID and job name with your own. Each AWS job type needs its `enable_*_trigger` flag and its ARN list: `glue_job_arns`, `emr_cluster_arns`, `emr_serverless_app_arns` with `emr_serverless_execution_role_arns`, `sfn_trigger_arns` or `lambda_trigger_arns`. `terraform plan` fails when a flag is on and its list is empty. Without the flag, the orchestrator is not allowed to start the job: the date ends with `RETRY_EXHAUSTED` and is closed, and step 5 says how to run it again. The `http`, `airflow` and `databricks` types need no flag. See [Module Variables](../../deployment/terraform/#module-variables).

Deploy:

```bash
cd your-project
terraform init
terraform apply
```

The module writes each pipeline file as a `CONFIG` row in the control table and provisions the Lambda functions, the Step Functions state machine, the DynamoDB tables, the EventBridge bus and the IAM roles.

The names used below are module outputs. To read them with `terraform output`, pass them through in your root configuration:

```hcl
output "control_table_name" { value = module.interlock.control_table_name }
output "events_table_name"  { value = module.interlock.events_table_name }
output "sfn_name"           { value = module.interlock.sfn_name }
```

With `environment = "production"` they are `production-interlock-control`, `production-interlock-events` and `production-interlock-pipeline`.

## 3. Push Sensor Data to DynamoDB

Your own processes (ETL jobs, monitoring scripts, data producers) write sensor rows to the control table (`<environment>-interlock-control`, module output `control_table_name`). Each write is a DynamoDB `PutItem`, with the fields the rules read in a `data` map. Interlock only reads DynamoDB; it never checks your data itself.

Write `row-count` first and the trigger sensor `upstream-complete` last. The trigger write starts the run, and a rule whose sensor is missing fails until the next evaluation, 5 minutes later.

```python
from datetime import datetime, timezone

import boto3

table = boto3.resource("dynamodb").Table("production-interlock-control")

now = datetime.now(timezone.utc)
run_date = now.strftime("%Y-%m-%d")  # today's UTC date

# The day's row count
table.put_item(Item={
    "PK": "PIPELINE#gold-revenue",
    "SK": "SENSOR#row-count",
    "data": {"count": 15432},
})

# Upstream data has landed: this write starts the run for run_date
table.put_item(Item={
    "PK": "PIPELINE#gold-revenue",
    "SK": "SENSOR#upstream-complete",
    "data": {
        "status": "ready",
        "date": run_date,  # the run date; without it the stream-router uses the current UTC date
        "updatedAt": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    },
})
```

The trigger sensor must arrive before the cutoff of the date it names, 23:00 UTC here. A sensor processed after the cutoff starts nothing: if the date has no run yet, Interlock closes it and publishes `SENSOR_DEADLINE_EXPIRED`. So build the date when you write the sensor, as above, rather than copying a fixed one.

## 4. The Pipeline Runs

The trigger write sets off the rest:

1. The **DynamoDB stream** invokes the **stream-router** Lambda, which checks the trigger condition and the cutoff.
2. The stream-router starts a **Step Functions execution** for the pipeline and the run date.
3. The state machine calls the **orchestrator** Lambda to evaluate the validation rules against the sensor rows. If they do not pass, it waits `evaluation.interval` and tries again, until `evaluation.window` ends.
4. When the rules pass (all or any, by `validation.trigger`), the orchestrator starts the job.
5. The state machine polls the job status until the job finishes, then records the result.

## 5. Check That It Ran

**Step Functions console**: open the state machine `production-interlock-pipeline` (module output `sfn_name`). The run has an execution named `gold-revenue-stream-<run date>-<unix time>`, and its graph shows each step. It ends `Succeeded` when it reaches the end of the state machine, which a run whose job failed can do too, so read the job's own result from the events.

**Events table**: every event Interlock publishes is written to `production-interlock-events` (module output `events_table_name`), keyed by pipeline:

```bash
aws dynamodb query --table-name production-interlock-events \
  --key-condition-expression "PK = :pk" \
  --expression-attribute-values '{":pk": {"S": "PIPELINE#gold-revenue"}}' \
  --query 'Items[].[eventType.S, date.S, message.S]' \
  --output table
```

A successful run shows, for the run date, `JOB_TRIGGERED` (from the stream-router), `VALIDATION_PASSED`, `JOB_TRIGGERED` again (from the orchestrator, when it starts the job) and `JOB_COMPLETED`.

Rough timings: the first `JOB_TRIGGERED` and `VALIDATION_PASSED` come within seconds of the trigger write when the rules pass. `JOB_COMPLETED` follows once the Glue job has finished and the next status check has seen it; the state machine checks every 60 seconds. If something went wrong, look for:

| Event | Meaning |
|---|---|
| `SENSOR_DEADLINE_EXPIRED` | The trigger sensor arrived after the date's cutoff, so nothing started |
| `EVALUATION_RELEASED` | The rules did not all pass within `evaluation.window`. Evaluation starts again on the next trigger sensor write, or on a watchdog pass while the trigger sensor still passes, until the cutoff |
| `RETRY_EXHAUSTED` | The job could not be started (for example without its `enable_*_trigger` permission), or it failed and its retries are spent. A launch is tried 4 times with waits of 30, 60 and 120 seconds (the default `trigger_max_attempts = 3`), so this comes about 3.5 minutes after the first attempt |
| `JOB_FAILED` | The job started and failed. Interlock retries it up to `job.maxRetries` times (2 here), waiting 1 minute before the first retry and 2 before the second; when they are spent, `RETRY_EXHAUSTED` follows |
| `CONFIG_INVALID` | The pipeline config was rejected; its message says why |

After `RETRY_EXHAUSTED` the date is closed (`FAILED_FINAL`). Writing the sensor again for the same date starts nothing, because the date already has its trigger row. To run it again, fix the cause and write a `RERUN_REQUEST` row for the date (see [Retry and Rerun Policy](../../configuration/pipelines/#retry-and-rerun-policy) and the request format in [Writing Sensor Data and Rerun Requests](../../guides/shared-deployment/#writing-sensor-data-and-rerun-requests)), or try the next run date.

Push the sensors from step 3 again first: that refreshes `updatedAt`, which the `age_lt` 2h rule checks when the rerun evaluates, and starts nothing on its own. Then write the request:

```bash
aws dynamodb put-item --table-name production-interlock-control --item \
  "{\"PK\": {\"S\": \"PIPELINE#gold-revenue\"}, \"SK\": {\"S\": \"RERUN_REQUEST#stream#$(date -u +%Y-%m-%d)\"}, \"reason\": {\"S\": \"manual\"}, \"requestedAt\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}"
```

A manual rerun spends the `job.maxManualReruns` budget (1 by default).

**Slack**: alert events go to Slack only when `slack_secret_arn` and `slack_channel_id` are set (see [Secrets Manager](../../deployment/terraform/#secrets-manager)). Without them the alert-dispatcher only logs them.

## 6. Add an SLA (Optional)

An `sla` block sets a deadline for the run to finish:

```yaml
sla:
  deadline: "22:00"
  expectedDuration: 30m
```

With this block in `gold-revenue.yaml`, `SLA_WARNING` fires at 21:30 UTC (`deadline` minus `expectedDuration`) and `SLA_BREACH` at 22:00 if the run has not finished. The cutoff must not be earlier than the deadline, or the config is rejected; 23:00 is later than 22:00.

Run `terraform apply` to load the change. The Lambdas cache configs for up to 5 minutes, so wait that long before the next step.

The SLA applies to a run that starts after the change, and its alerts are for a date whose run has not finished. Today's date already ran in step 3, so use a date that has not, such as tomorrow's. The watchdog sets up alerts only for dates the pipeline expects, so first write a `RUN#` row for that date:

```bash
run_date=$(date -u -d tomorrow +%Y-%m-%d)   # GNU date; on macOS: date -u -v+1d +%Y-%m-%d
aws dynamodb put-item --table-name production-interlock-control --item \
  "{\"PK\": {\"S\": \"PIPELINE#gold-revenue\"}, \"SK\": {\"S\": \"RUN#$run_date\"}}"
```

Then push the sensors as in step 3, with `"date"` set to that date (in the Python, `run_date = (now + timedelta(days=1)).strftime("%Y-%m-%d")`, importing `timedelta`). When the run finishes before its 22:00 deadline, the events table shows `SLA_MET` for the date. A run still going at 21:30 or 22:00 gets `SLA_WARNING` or `SLA_BREACH` instead.

Without a `RUN#` row the date gets no `SLA_WARNING` and no `SLA_BREACH` ahead of time; when its run ends, it still gets `SLA_MET` if it completed before the deadline, or `SLA_BREACH` if the deadline had passed. See [Expected Run Dates](../../configuration/schedules/#expected-run-dates).

## 7. Subscribe to Events

All lifecycle events are published to the custom EventBridge bus. The module already writes every event to the events table and, when `slack_secret_arn` and `slack_channel_id` are set, sends alert events to Slack. Add EventBridge rules on the bus for other targets (SNS, SQS, Lambda, CloudWatch Logs, etc.):

```hcl
resource "aws_cloudwatch_event_rule" "sla_alerts" {
  name           = "interlock-sla-alerts"
  event_bus_name = module.interlock.event_bus_name

  event_pattern = jsonencode({
    "detail-type" = ["SLA_WARNING", "SLA_BREACH"]
  })
}

resource "aws_cloudwatch_event_target" "sla_to_sns" {
  rule           = aws_cloudwatch_event_rule.sla_alerts.name
  event_bus_name = module.interlock.event_bus_name
  target_id      = "sla-to-sns"
  arn            = aws_sns_topic.alerts.arn
}
```

## Next Steps

- [Architecture Overview](../../architecture/overview/): the STAMP model and the state machine design
- [Pipeline Configuration](../../configuration/pipelines/): the full pipeline configuration reference
- [Alerting](../../reference/alerting/): EventBridge event types and consumer patterns
