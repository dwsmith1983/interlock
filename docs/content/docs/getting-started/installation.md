---
title: Installation
weight: 1
description: Prerequisites and deploying the Interlock Terraform module.
---

Interlock is deployed as infrastructure, not installed as a binary. You deploy the reusable Terraform module into your AWS account, and it provisions all the resources needed to run pipeline safety checks.

## Prerequisites

| Dependency | Version | Purpose |
|---|---|---|
| Terraform | 1.5+ | Deploy the Interlock module |
| AWS CLI v2 | Latest | AWS credential configuration |
| AWS provider | 5.0+ | Required by the module |
| Go | 1.26+ | Build Lambda handler binaries |
| `zip` | Any | Packages each handler in `deploy/build.sh` |
| AWS Account | N/A | DynamoDB, Lambda, Step Functions, EventBridge |
| A job to start | N/A | Interlock starts your existing jobs and does not create them. The [Quickstart](../quickstart/) needs an AWS Glue job that succeeds when run, and its ARN |

## Deploy the Terraform Module

### 1. Build Lambda Binaries

Clone the repository, check out the release you want to run (see [Releases](https://github.com/dwsmith1983/interlock/releases)), and build the Lambda handler zip files:

```bash
git clone https://github.com/dwsmith1983/interlock.git
cd interlock
git checkout <release-tag>
./deploy/build.sh
```

This produces one zip file per Lambda function (six in total) in the `deploy/dist/` directory.

### 2. Reference the Module

In your Terraform configuration, reference the Interlock module from the same clone, so the module and the binaries come from the same release. The module declares no provider, so configure the AWS provider yourself:

```hcl
provider "aws" {
  region = "us-east-1"
}

module "interlock" {
  source = "path/to/interlock/deploy/terraform"

  environment    = "production"
  dist_path      = "path/to/interlock/deploy/dist"  # build output from step 1
  pipelines_path = "${path.module}/pipelines"

  tags = {
    Project = "interlock"
  }
}
```

`pipelines_path` is a directory of pipeline YAML files (see [Quickstart](../quickstart/)). Name each file after its pipeline ID: `pipelines/silver-orders.yaml` for `pipeline.id: silver-orders`. `terraform plan` fails when they differ.

A pipeline whose job runs on AWS (Glue, EMR, EMR Serverless, Step Functions or Lambda) also needs that type's `enable_*_trigger` flag and ARN list, for example `enable_glue_trigger = true` with `glue_job_arns`. See [Module Variables](../../deployment/terraform/#module-variables).

### 3. Apply

```bash
terraform init
terraform plan
terraform apply
```

This provisions:
- 4 DynamoDB tables (control, joblog, rerun, events)
- 6 Lambda functions (stream-router, orchestrator, sla-monitor, watchdog, event-sink, alert-dispatcher)
- 1 Step Functions state machine
- 1 EventBridge custom event bus
- EventBridge schedule for the watchdog
- IAM roles with least-privilege policies
- SQS dead-letter queues for stream processing, undelivered alerts and unwritten events
- CloudWatch log groups

## What Gets Deployed

### Lambda Functions

| Function | Purpose |
|---|---|
| stream-router | Processes DynamoDB Stream events, starts Step Function executions |
| orchestrator | Evaluates validation rules, triggers jobs, checks job status |
| sla-monitor | Fires SLA_WARNING and SLA_BREACH alerts, and cancels SLA schedules when a run ends, recording SLA_MET or SLA_BREACH |
| watchdog | Runs the safety checks on an EventBridge timer: stale triggers, start-by times, cutoffs, SLA schedules, post-run sensors, invalid configs |
| event-sink | Writes every event to the events table |
| alert-dispatcher | Sends alert events to Slack, or logs them when Slack is not configured |

### DynamoDB Tables

| Table | Purpose |
|---|---|
| control | Pipeline configs, sensor data, evaluation state |
| joblog | Run history and job tracking |
| rerun | Rerun request tracking |
| events | Event log and Slack thread records |

### Module Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `environment` | yes | -- | Environment name (e.g., staging, production) |
| `dist_path` | yes | -- | Path to Lambda zip files |
| `pipelines_path` | yes | -- | Path to pipeline YAML configs |
| `calendars_path` | no | `""` | Accepted but not read |
| `lambda_memory_size` | no | `128` | Memory (MB) for stream-router, orchestrator, sla-monitor and watchdog; event-sink and alert-dispatcher stay at 128 |
| `log_retention_days` | no | `30` | CloudWatch log retention |
| `watchdog_schedule` | no | `rate(5 minutes)` | EventBridge watchdog schedule |
| `sfn_timeout_seconds` | no | `14400` | Step Functions execution timeout (4h default) |

This table lists the main variables. The trigger permissions, Slack, encryption and the rest are in [Module Variables](../../deployment/terraform/#module-variables).

### Module Outputs

| Output | Description |
|---|---|
| `control_table_name` | Name of the control DynamoDB table |
| `control_table_arn` | ARN of the control DynamoDB table |
| `joblog_table_name` | Name of the job log DynamoDB table |
| `rerun_table_name` | Name of the rerun DynamoDB table |
| `events_table_name` | Name of the events DynamoDB table |
| `events_table_arn` | ARN of the events DynamoDB table |
| `event_bus_name` | Name of the EventBridge event bus |
| `event_bus_arn` | ARN of the EventBridge event bus |
| `sfn_arn` | ARN of the pipeline state machine |
| `sfn_name` | Name of the pipeline state machine |
| `scheduler_group_name` | Name of the EventBridge Scheduler group for SLA alerts |
| `sla_monitor_arn` | ARN of the SLA monitor Lambda function |
