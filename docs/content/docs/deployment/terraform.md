---
title: Terraform (AWS)
weight: 1
description: Bootstrap state backend, configure variables, and deploy the full AWS stack.
---

The AWS deployment is managed by Terraform files in `deploy/terraform/`. The stack includes DynamoDB, 6 Lambda functions, a Step Function state machine, SNS for alerts, and EventBridge for scheduling.

## Prerequisites

- Terraform 1.5+
- AWS CLI v2 configured with appropriate credentials
- Go 1.24+ (to build Lambda binaries)

## Bootstrap State Backend

Before the first deploy, create the S3 bucket and DynamoDB table for Terraform state:

```bash
cd deploy/terraform/bootstrap
terraform init
terraform apply
```

This creates:
- S3 bucket for `.tfstate` files
- DynamoDB table for state locking

## Build Lambda Binaries

```bash
make build-lambda
```

This runs `deploy/build.sh`, which cross-compiles all 6 Lambda handlers for `linux/arm64` and packages them as zip archives.

## Configure Variables

Create a `terraform.tfvars` file (or use `-var` flags):

```hcl
project_name = "my-interlock"
aws_region   = "us-east-1"

# DynamoDB
table_name    = "my-interlock"
readiness_ttl = "1h"
retention_ttl = "168h"

# SNS
sns_topic_name = "my-interlock-alerts"

# Lambda
lambda_memory  = 256
lambda_timeout = 60

# Evaluator API (for HTTP evaluator runner)
evaluator_base_url = "https://api.example.com"

# Archetype directory (bundled in Lambda zip)
archetype_dir = "/var/task/archetypes"

# S3 data bucket (for trigger artifacts)
data_bucket_name = "my-interlock-data"
```

### Key Variables

| Variable | Type | Default | Description |
|---|---|---|---|
| `project_name` | string | — | Prefix for all resource names |
| `aws_region` | string | — | AWS region |
| `table_name` | string | — | DynamoDB table name |
| `readiness_ttl` | string | `1h` | Trait readiness cache TTL |
| `retention_ttl` | string | `168h` | Data retention TTL |
| `sns_topic_name` | string | — | SNS topic for alerts |
| `lambda_memory` | number | `256` | Lambda memory (MB) |
| `lambda_timeout` | number | `60` | Lambda timeout (seconds) |
| `evaluator_base_url` | string | `""` | HTTP evaluator base URL |
| `archetype_dir` | string | `/var/task/archetypes` | Archetype YAML directory in Lambda |
| `watchdog_interval` | string | `rate(5 minutes)` | EventBridge schedule for watchdog Lambda |

## Deploy

```bash
cd deploy/terraform
terraform init
terraform plan
terraform apply
```

### What Gets Created

| Resource | Description |
|---|---|
| DynamoDB table | Single-table with TTL on `expiresAt`, GSI1 for list queries |
| DynamoDB Stream | Triggers `stream-router` Lambda on `MARKER#` writes |
| 6 Lambda functions | stream-router, evaluator, orchestrator, trigger, run-checker, watchdog |
| Lambda layers | Shared dependencies (Python `requests` for Python Lambdas) |
| Step Function | 47-state machine orchestrating the full pipeline lifecycle |
| SNS topic | Alert delivery |
| EventBridge rules | Scheduled MARKER writes to trigger pipeline evaluation |
| IAM roles | Least-privilege policies for each Lambda |
| CloudWatch Log Groups | Per-Lambda log groups |

### Resource Architecture

The Terraform uses `for_each` for the core Lambda functions (evaluator, orchestrator, trigger, run-checker), keeping the configuration DRY. The `stream-router` is defined separately because it has a unique DynamoDB Stream event source mapping. The `watchdog` is also separate — it has its own EventBridge schedule trigger and is not part of the Step Function workflow.

The Step Function ASL template uses `templatefile()` to substitute Lambda ARNs:

```hcl
resource "aws_sfn_state_machine" "pipeline" {
  definition = templatefile("${path.module}/../statemachine.asl.json", {
    OrchestratorFunctionArn = aws_lambda_function.go["orchestrator"].arn
    EvaluatorFunctionArn    = aws_lambda_function.go["evaluator"].arn
    TriggerFunctionArn      = aws_lambda_function.go["trigger"].arn
    RunCheckerFunctionArn   = aws_lambda_function.go["run-checker"].arn
  })
}
```

### IAM

All IAM roles and policies are centralized in `iam.tf`:

- Each Lambda gets its own execution role
- DynamoDB access is scoped to the specific table
- SNS publish is scoped to the alert topic
- Step Functions has permission to invoke all 4 downstream Lambdas
- `stream-router` has permission to start Step Function executions

## Tear Down

```bash
cd deploy/terraform
terraform destroy
```

To also remove the state backend:

```bash
cd deploy/terraform/bootstrap
terraform destroy
```
