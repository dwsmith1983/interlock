---
title: Quickstart
weight: 2
description: Define pipelines, deploy to AWS, and push sensor data.
---

This guide walks through the end-to-end workflow: define pipeline YAML configs, deploy the Terraform module, push sensor data to DynamoDB, and watch your pipeline trigger automatically when all validation rules pass.

## 1. Write Pipeline YAML Configs

Create a `pipelines/` directory and add a YAML file for each pipeline. Here is a minimal example:

```yaml
pipeline:
  id: silver-orders
  owner: data-platform
  description: Silver-tier orders ingestion pipeline

schedule:
  trigger:
    key: orders-landed
    check: exists

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

A more complete example with SLA deadlines and a cron schedule:

```yaml
pipeline:
  id: gold-revenue
  owner: analytics-team
  description: Gold-tier revenue aggregation pipeline

schedule:
  cron: "0 8 * * *"
  timezone: UTC
  trigger:
    key: upstream-complete
    check: equals
    field: status
    value: ready
  evaluation:
    window: 1h
    interval: 5m

sla:
  deadline: "10:00"
  expectedDuration: 30m

validation:
  trigger: "ALL"
  rules:
    - key: upstream-complete
      check: equals
      field: status
      value: ready
    - key: row-count
      check: gte
      field: count
      value: 1000
    - key: freshness
      check: age_lt
      field: updatedAt
      value: 2h

job:
  type: glue
  config:
    jobName: gold-revenue-etl
  maxRetries: 2
```

### Pipeline Config Reference

| Section | Purpose |
|---|---|
| `pipeline` | Identity: `id`, `owner`, `description` |
| `schedule` | When to evaluate: `cron`, `timezone`, trigger condition, evaluation window/interval |
| `sla` | SLA deadlines: `deadline`, `expectedDuration` |
| `validation` | Readiness rules: `trigger` mode (`ALL` or `ANY`), list of `rules` |
| `job` | What to trigger: `type` (http, glue, emr, emr-serverless, step-function), `config`, `maxRetries` |

### Validation Rule Checks

| Check | Description |
|---|---|
| `exists` | Key exists in sensor data |
| `equals` | Field equals expected value |
| `gt` / `gte` | Field is greater than (or equal to) value |
| `lt` / `lte` | Field is less than (or equal to) value |
| `age_lt` | Field timestamp is within the given duration |

## 2. Deploy the Terraform Module

Reference the Interlock module in your Terraform configuration:

```hcl
module "interlock" {
  source = "github.com/dwsmith1983/interlock//deploy/terraform"

  environment    = "production"
  dist_path      = "./dist"
  pipelines_path = "./pipelines"

  tags = {
    Project = "my-data-platform"
  }
}
```

Build and deploy:

```bash
# Build Lambda binaries
cd interlock && ./deploy/build.sh

# Deploy infrastructure
cd your-project
terraform init
terraform apply
```

The Terraform module reads your pipeline YAML files and writes them as `CONFIG` rows in the control DynamoDB table. The module provisions all Lambda functions, the Step Functions state machine, DynamoDB tables, EventBridge bus, and IAM roles.

## 3. Push Sensor Data to DynamoDB

External processes (your ETL jobs, monitoring scripts, data producers) push sensor data to the control table. Each sensor write is a DynamoDB `PutItem`:

```python
import boto3

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("production-interlock-control")

# Signal that upstream data has landed
table.put_item(Item={
    "PK": "PIPELINE#gold-revenue",
    "SK": "SENSOR#upstream-complete",
    "status": "ready",
    "updatedAt": "2026-03-01T08:05:00Z",
})

# Push a row count metric
table.put_item(Item={
    "PK": "PIPELINE#gold-revenue",
    "SK": "SENSOR#row-count",
    "count": 15432,
})
```

The framework reads DynamoDB only. Your external processes are responsible for pushing sensor data. This decouples data quality checks from the safety framework.

## 4. Pipeline Triggers Automatically

When sensor data is written to DynamoDB, the following happens automatically:

1. **DynamoDB Stream** fires, invoking the **stream-router** Lambda
2. Stream-router starts a **Step Functions execution** for the pipeline
3. The state machine calls the **orchestrator** Lambda to evaluate validation rules against sensor data
4. If rules pass (all or any, depending on `validation.trigger`), the orchestrator triggers the job
5. The state machine polls job status until completion
6. In parallel, the **sla-monitor** branch tracks SLA deadlines and fires `SLA_WARNING` / `SLA_BREACH` events to EventBridge if needed

No manual intervention required. The entire lifecycle is automated.

## 5. Subscribe to Events

All lifecycle events are published to the custom EventBridge bus. Create EventBridge rules to route events to your preferred targets (SNS, SQS, Lambda, CloudWatch Logs, etc.):

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

- [Architecture Overview](../architecture/overview) -- understand the STAMP model and state machine design
- [Configuration](../configuration/pipelines) -- full pipeline configuration reference
- [Alerting](../reference/alerting) -- EventBridge event types and consumer patterns
