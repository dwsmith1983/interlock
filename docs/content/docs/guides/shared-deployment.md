---
title: "Shared Deployment"
weight: 30
---

# Adding a Pipeline to a Shared Deployment

Run one Interlock deployment per AWS account. Each data engineering team in that account adds its own pipelines to it. A team in another account deploys its own copy of the module.

## What Is Shared

Every pipeline in a deployment uses the same:

- four DynamoDB tables (control, joblog, rerun, events)
- Step Functions state machine and six Lambda functions
- EventBridge bus and Slack channel (`slack_channel_id`)

What keeps pipelines apart is the pipeline ID in every key: `PK = PIPELINE#<id>`. A pipeline's config, sensor rows, trigger locks, job results, reruns and events all sit under that partition key.

## Pipeline IDs

An ID is 1 to 31 characters of `A-Z`, `a-z`, `0-9`, `_` and `-`, and must equal the ID in its `PIPELINE#<id>` key. IDs are global within a deployment, so start each one with the team's name: `sales-orders-daily`, `risk-exposure-hourly`. See [Pipelines](../../configuration/pipelines/#pipeline) for where the limit comes from.

## Adding a Pipeline

### In the central directory

Add the YAML file to the directory the deployment passes as `pipelines_path`, named after the ID: `sales-orders-daily.yaml` for `pipeline.id: sales-orders-daily`. The module writes it as a `CONFIG` row and fails `terraform plan` when the name and ID differ. Every new pipeline is then a change to the central repository.

### From the team's own Terraform

A team can write the same `CONFIG` row from its own Terraform. Take the table name from the core deployment's `control_table_name` output, either with `terraform_remote_state` (the core root module has to re-export the module's output) or as a variable:

```hcl
data "terraform_remote_state" "interlock" {
  backend = "s3"
  config = {
    bucket = "platform-terraform-state"
    key    = "interlock/terraform.tfstate"
    region = "eu-west-1"
  }
}

locals {
  sales_orders = yamldecode(file("${path.module}/pipelines/sales-orders-daily.yaml"))
}

resource "aws_dynamodb_table_item" "sales_orders_daily" {
  table_name = data.terraform_remote_state.interlock.outputs.control_table_name
  hash_key   = "PK"
  range_key  = "SK"

  item = jsonencode({
    PK     = { S = "PIPELINE#${local.sales_orders.pipeline.id}" }
    SK     = { S = "CONFIG" }
    config = { S = jsonencode(local.sales_orders) }
  })
}
```

Building the key from `pipeline.id` keeps the two equal. The module creates nothing else per pipeline, so this row is all a new pipeline needs (plus job permissions, see [Limits](#limits)).

Interlock checks the config when it loads it, not at `terraform apply`. A config that fails validation is skipped, logged and reported by the watchdog as `CONFIG_INVALID`, and the other pipelines keep running.

## Writing Sensor Data and Rerun Requests

The team's jobs write to the control table (`<environment>-interlock-control`), under their pipeline's key:

| Row | `PK` | `SK` | Attributes |
|---|---|---|---|
| Sensor | `PIPELINE#<id>` | `SENSOR#<key>` | `data`: a map of the fields the rules read (`status`, `count`, ...); `ttl` (optional, epoch seconds) |
| Rerun request | `PIPELINE#<id>` | `RERUN_REQUEST#<schedule>#<date>` | `reason` (optional, default `manual`), `requestedAt` (the request time) and `ttl` (optional, epoch seconds) |
| Expected run date | `PIPELINE#<id>` | `RUN#<runKey>` | `ttl` (optional, epoch seconds); see [Expected Run Dates](../../configuration/schedules/#expected-run-dates) |

- Put sensor fields in `data`: rule evaluation reads only that map.
- The trigger sensor, whose key is `schedule.trigger.key` or that key followed by `#` and a period (`orders#2026-03-10`), sets the execution date from its `date` (`YYYY-MM-DD` or `YYYYMMDD`) and `hour` (`00` to `23`) fields. Both must be strings. A missing or invalid `date` gives the current UTC date.
- In a rerun request, `<schedule>` is `cron` when the pipeline has `schedule.cron` and `stream` otherwise, and `<date>` is the execution date, `YYYY-MM-DD` or `YYYY-MM-DDThh`. A request with any other schedule or date is skipped. Requests with the same `reason` share one rerun budget per date, `job.maxManualReruns` (default 1); `data-drift` and `late-data` share `job.maxDriftReruns`.
- A request for a date whose run is still `RUNNING` is rejected with `RERUN_REJECTED` (`a run is in progress for <date>`) and spends no budget. A run whose execution ended without closing its row (it timed out or was stopped, or the step that would have closed the row failed) stays `RUNNING` until the watchdog's [stale-trigger check](../../architecture/watchdog/#stale-trigger-detection) publishes `SFN_TIMEOUT` and sets it to `FAILED_FINAL`, once the row's stale deadline (`staleAt`) has passed. The deadline counts from the run's latest attempt (each retry refreshes it) and is `sfn_timeout_seconds` plus 30 minutes, 4h30m by default; add up to one `watchdog_schedule` interval. Nothing shortens the wait. Write the request again, with a new `requestedAt`, once the run ends or after the `SFN_TIMEOUT`.
- Interlock acts on the DynamoDB stream, and a `PutItem` that changes nothing in the item emits no stream record. Interlock does not delete a request row once handled, so a repeated request needs a new `requestedAt`, and a sensor rewritten with the same `data` triggers nothing.
- Interlock never deletes or expires the rows a team writes. Its own `SENSOR#` bookkeeping rows and the `late-data` and `data-drift` rerun requests it writes expire 31 days after they are written (see [Control Table](../../architecture/aws/#control-table)). A per-period sensor (`orders#2026-03-10`) and a manual rerun request are written once per date, so they pile up under the pipeline's key; give them a `ttl` to have DynamoDB delete them. Set it past the date's last use: its cutoff, any rerun of the date you still expect to request, and for a post-run sensor the date's post-run watch window. A key without a period is rewritten in place and needs none.
- An automatic `late-data` or `data-drift` request for the same schedule and date replaces a manual request row and gives it Interlock's `ttl`.

```sh
aws dynamodb put-item --table-name production-interlock-control --item '{
  "PK": {"S": "PIPELINE#sales-orders-daily"},
  "SK": {"S": "SENSOR#orders-landed"},
  "data": {"M": {"status": {"S": "ready"}, "date": {"S": "2026-11-15"}}}
}'

aws dynamodb put-item --table-name production-interlock-control --item '{
  "PK": {"S": "PIPELINE#sales-orders-daily"},
  "SK": {"S": "RERUN_REQUEST#stream#2026-11-15"},
  "reason": {"S": "manual"},
  "requestedAt": {"S": "2026-11-16T09:30:00Z"}
}'
```

A job started by an `http` trigger also reports its result to the joblog table, under the same key (see [Reporting the result](../../configuration/triggers/#reporting-the-result)).

## Keeping Teams Apart with IAM

IAM keeps one team's rows apart from another's. It does not limit what a config makes the shared orchestrator do: a `CONFIG` row can name any job ARN the deployment allows (another team's job included, with its own arguments), and a `type: http` job calls any public URL. An `emr-serverless` config can run any code under any role in `emr_serverless_execution_role_arns`: the role list is deployment-wide, and the config writer chooses both the role and the entry point, so list only roles every config writer may use, or deploy separately. Review configs like code. The central `pipelines_path` route gets that review in the central repository; route a team's own Terraform through the same review.

Without a guard, two teams that pick the same ID share one `CONFIG` row and can change each other's pipeline. Give each team's roles access only to keys that start with its prefix, one statement for its jobs and one for its Terraform:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SalesJobs",
      "Effect": "Allow",
      "Action": "dynamodb:PutItem",
      "Resource": [
        "arn:aws:dynamodb:<region>:<account>:table/<environment>-interlock-control",
        "arn:aws:dynamodb:<region>:<account>:table/<environment>-interlock-joblog"
      ],
      "Condition": {
        "ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["PIPELINE#sales-*"]},
        "Null": {"dynamodb:LeadingKeys": "false"}
      }
    },
    {
      "Sid": "SalesTerraform",
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem"
      ],
      "Resource": "arn:aws:dynamodb:<region>:<account>:table/<environment>-interlock-control",
      "Condition": {
        "ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["PIPELINE#sales-*"]},
        "Null": {"dynamodb:LeadingKeys": "false"}
      }
    }
  ]
}
```

Attach the first statement to the job role and the second to the Terraform role. The joblog resource is only for jobs started by an `http` trigger.

- `StringLike` matches the partition key by prefix; for one pipeline, use `ForAllValues:StringEquals` with `PIPELINE#sales-orders-daily`.
- `ForAllValues` passes a request that names no partition key, such as a `Scan`, so keep the `Null` condition and leave `Scan` out.
- IAM cannot restrict by sort key, so the job role can still write any row under its prefix, `CONFIG` included.
- The same `LeadingKeys` condition covers a team's `RUN#` rows: whichever role writes them needs only `PutItem` under its prefix.
- An Allow confines a role only if it is that role's only access to these tables. Enforce that with a permissions boundary or an SCP.
- Pick prefixes that don't overlap (`sales-` covers `sales-eu-`), and keep each ID in one place: a file in `pipelines_path` and a team's resource for the same ID write the same row and fight over it.

## Limits

- **Alerts:** all pipelines alert to the one Slack channel and event bus. Pipeline events carry `detail.pipelineId`, so a team can add its own rule on the bus (the `event_bus_name` output, re-exported like `control_table_name`) with the pattern `{"source": ["interlock"], "detail": {"pipelineId": [{"prefix": "sales-"}]}}`. That rule misses deployment-level failures: `WATCHDOG_DEGRADED` has an empty `pipelineId`, and CloudWatch alarms go to the default bus.
- **Job permissions:** the orchestrator role is shared, so a team's job ARNs go in the core deployment's `glue_job_arns`, `emr_cluster_arns`, `emr_serverless_app_arns` (with the job runtime roles in `emr_serverless_execution_role_arns`), `lambda_trigger_arns` or `sfn_trigger_arns`, with the matching `enable_*_trigger` flag. Any `emr-serverless` config can use any of those runtime roles, so list only roles every config writer may use. `emr-serverless` client tokens come from the execution name, so two deployments that share one EMR Serverless application can collide only if the same pipeline, schedule and date start in the same second; give each deployment its own application.
- **Deployment settings:** `sfn_timeout_seconds`, `trigger_max_attempts` and `watchdog_schedule` apply to every pipeline.
