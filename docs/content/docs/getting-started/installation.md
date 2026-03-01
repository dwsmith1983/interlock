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
| Go | 1.24+ | Build Lambda handler binaries |
| AWS Account | N/A | DynamoDB, Lambda, Step Functions, EventBridge |

## Deploy the Terraform Module

### 1. Build Lambda Binaries

Clone the repository and build the Lambda handler zip files:

```bash
git clone https://github.com/dwsmith1983/interlock.git
cd interlock
./deploy/build.sh
```

This produces zip files in the `dist/` directory for all four Lambda functions.

### 2. Reference the Module

In your Terraform configuration, reference the Interlock module:

```hcl
module "interlock" {
  source = "github.com/dwsmith1983/interlock//deploy/terraform"

  environment    = "production"
  dist_path      = "${path.module}/dist"
  pipelines_path = "${path.module}/pipelines"
  calendars_path = "${path.module}/calendars"  # optional

  tags = {
    Project = "interlock"
  }
}
```

### 3. Apply

```bash
terraform init
terraform plan
terraform apply
```

This provisions:
- 3 DynamoDB tables (control, joblog, rerun)
- 4 Lambda functions (stream-router, orchestrator, sla-monitor, watchdog)
- 1 Step Functions state machine
- 1 EventBridge custom event bus
- EventBridge schedule for the watchdog
- IAM roles with least-privilege policies
- SQS dead-letter queues for stream processing
- CloudWatch log groups

## What Gets Deployed

### Lambda Functions

| Function | Purpose |
|---|---|
| stream-router | Processes DynamoDB Stream events, starts Step Function executions |
| orchestrator | Evaluates validation rules, triggers jobs, checks job status |
| sla-monitor | Calculates SLA deadlines, fires SLA_WARNING and SLA_BREACH alerts |
| watchdog | Scans for missed schedules on an EventBridge timer |

### DynamoDB Tables

| Table | Purpose |
|---|---|
| control | Pipeline configs, sensor data, evaluation state |
| joblog | Run history and job tracking |
| rerun | Rerun request tracking |

### Module Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `environment` | yes | -- | Environment name (e.g., staging, production) |
| `dist_path` | yes | -- | Path to Lambda zip files |
| `pipelines_path` | yes | -- | Path to pipeline YAML configs |
| `calendars_path` | no | `""` | Path to calendar YAML files |
| `lambda_memory_size` | no | `128` | Lambda memory in MB |
| `log_retention_days` | no | `30` | CloudWatch log retention |
| `watchdog_schedule` | no | `rate(5 minutes)` | EventBridge watchdog schedule |
| `sfn_timeout_seconds` | no | `43200` | Step Functions execution timeout (12h default) |

### Module Outputs

| Output | Description |
|---|---|
| `control_table_name` | Name of the control DynamoDB table |
| `control_table_arn` | ARN of the control DynamoDB table |
| `joblog_table_name` | Name of the job log DynamoDB table |
| `rerun_table_name` | Name of the rerun DynamoDB table |
| `event_bus_name` | Name of the EventBridge event bus |
| `event_bus_arn` | ARN of the EventBridge event bus |
| `sfn_arn` | ARN of the pipeline state machine |
| `sfn_name` | Name of the pipeline state machine |
