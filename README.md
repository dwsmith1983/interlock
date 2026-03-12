# Interlock

STAMP-based safety framework for data pipeline reliability. Interlock prevents pipelines from executing when preconditions aren't safe — like a physical interlock mechanism.

The framework applies [Leveson's Systems-Theoretic Accident Model](https://mitpress.mit.edu/9780262016629/engineering-a-safer-world/) to data engineering: pipelines have **declarative validation rules** (feedback), **sensor data in DynamoDB** (process models), and **conditional execution** (safe control actions).

## What Interlock Is (and Isn't)

Interlock is **not** an orchestrator and **not** a scheduler. It's a safety controller — the layer that decides whether a pipeline *should* run, not just whether it's *scheduled* to run. It works with whatever you already have: schedulers like cron, Airflow, Databricks Workflows, or EventBridge, and orchestrators like Dagster, Prefect, or Step Functions.

A scheduler fires because the clock says so. An orchestrator sequences tasks once they're kicked off. Neither asks whether the data your pipeline needs is actually present, fresh, and correct before executing. **Interlock does.** You route the trigger path through Interlock: sensor data lands in DynamoDB, Interlock evaluates readiness against declarative YAML rules, and only triggers the job when preconditions are met. Your scheduler can still provide the clock signal — an EventBridge cron writing a sensor tick, for example — but Interlock decides whether that tick becomes a job run.

**After a run completes**, Interlock keeps watching. It detects post-completion drift (source data changed after your job succeeded), late data arrival, SLA breaches, and silently missed schedules — failure modes that schedulers and orchestrators don't address because they stop paying attention once a job finishes.

## How It Works

External processes push sensor data into a DynamoDB **control table**. When a trigger condition is met (cron schedule or sensor arrival), a Step Functions workflow evaluates all validation rules against the current sensor state. If all rules pass, the pipeline job is triggered. EventBridge events provide observability at every stage.

```
Sensor data → DynamoDB Stream → stream-router Lambda → Step Functions
                                                           │
                                               ┌───────────┼───────────┐
                                               ▼           ▼           ▼
                                           Evaluate     Trigger     SLA Monitor
                                           (rules)     (job)       (deadlines)
                                               │           │
                                               ▼           ▼
                                           EventBridge ──────────────────┐
                                           (all events)                  │
                                               │                         │
                                    ┌──────────┼──────────┐              │
                                    ▼                     ▼              ▼
                               event-sink          SQS alert queue  CloudWatch
                               (→ events table)    (→ alert-dispatcher → Slack)
```

### Declarative Validation Rules

Pipeline configs define validation as declarative YAML rules — no custom evaluator code needed:

```yaml
validation:
  trigger: "ALL"       # ALL rules must pass, or ANY one
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
```

Supported checks: `exists`, `equals`, `gt`, `gte`, `lt`, `lte`, `age_lt`, `age_gt`.

## Architecture

```
┌───────────────────┐     DynamoDB Stream     ┌───────────────────────────┐
│    DynamoDB       │ ────────────────────►   │     stream-router         │
│  4 tables:        │                         │  sensor → evaluate        │
│  - control        │                         │  config → cache invalidate│
│  - joblog         │                         │  job-log → rerun/success  │
│  - rerun          │                         └───────┬───────────────────┘
│  - events         │
└───────────────────┘                                 │
                                          ┌───────────▼──────────────┐
                                          │     Step Functions       │
                                          │  24 sequential states:   │
                                          │  Evaluate → Trigger →    │
                                          │  Poll → SLA → Done       │
                                          └──────────┬───────────────┘
                                                     │
                                    ┌────────────────┼────────────────┐
                                    ▼                ▼                ▼
                              orchestrator     sla-monitor       watchdog
                              (evaluate,       (schedule SLA     (stale runs,
                               trigger,         via EventBridge   missed cron,
                               check-job)       Scheduler)       post-run gaps)
```

### Lambda Functions

| Function | Purpose |
|----------|---------|
| `stream-router` | Routes DynamoDB Stream events, starts Step Function executions, evaluates post-run drift |
| `orchestrator` | Multi-mode handler: evaluate rules, trigger jobs, check status, complete triggers |
| `sla-monitor` | Schedules SLA alerts via EventBridge Scheduler; cancels on job completion |
| `watchdog` | Detects stale triggers, missed cron schedules, and missing post-run sensors |
| `event-sink` | Writes all EventBridge events to the events table for centralized logging |
| `alert-dispatcher` | Delivers Slack notifications from SQS alert queue with message threading |

### DynamoDB Tables

| Table | Purpose |
|-------|---------|
| `control` | Pipeline configs, sensor data, run state (PK/SK design) |
| `joblog` | Job execution event log (trigger, success, failure) |
| `rerun` | Rerun request tracking |
| `events` | Centralized event log with GSI for querying by type and timestamp |

## Pipeline Configuration

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

## Dry-Run / Shadow Mode

Evaluate Interlock against running pipelines without executing any jobs. Set `dryRun: true` in your pipeline config — the stream-router observes the real sensor stream and records what it *would* do as EventBridge events, while your existing orchestrator continues running as-is.

```yaml
pipeline:
  id: gold-revenue-dryrun
schedule:
  trigger:
    key: upstream-complete
    check: equals
    field: status
    value: ready
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
job:
  type: glue
  config:
    jobName: gold-revenue-etl
dryRun: true
```

Dry-run publishes four observation events:

| Event | Meaning |
|-------|---------|
| `DRY_RUN_WOULD_TRIGGER` | All validation rules passed — Interlock would have triggered the job |
| `DRY_RUN_LATE_DATA` | Sensor updated after the trigger point — would have triggered a re-run |
| `DRY_RUN_SLA_PROJECTION` | Estimated completion vs. deadline — would the SLA be met or breached? |
| `DRY_RUN_DRIFT` | Post-run sensor data changed — would have detected drift and re-run |

No Step Function executions, no job triggers, no rerun requests. Remove `dryRun: true` to switch to live mode — `DRY_RUN#` markers have a 7-day TTL and don't interfere with `TRIGGER#` rows.

## Trigger Types

| Type | SDK/Protocol | Use Case |
|------|-------------|----------|
| `command` | Subprocess | Local scripts, CLI tools |
| `http` | HTTP POST | Generic REST APIs, webhooks |
| `airflow` | HTTP (Airflow API) | Apache Airflow DAG runs |
| `glue` | AWS SDK | AWS Glue ETL jobs |
| `emr` | AWS SDK | Amazon EMR step execution |
| `emr-serverless` | AWS SDK | EMR Serverless job runs |
| `step-function` | AWS SDK | AWS Step Functions executions |
| `databricks` | HTTP (REST 2.1) | Databricks job runs |
| `lambda` | AWS SDK | Direct Lambda invocation |

## Deployment

Interlock ships as a **reusable Terraform module** — no framework code in your repo.

```hcl
module "interlock" {
  source = "github.com/dwsmith1983/interlock//deploy/terraform"

  project_name     = "my-data-platform"
  environment      = "prod"
  pipeline_configs = "s3://my-bucket/pipelines/"
}
```

The module creates all required infrastructure: DynamoDB tables, Lambda functions, Step Functions state machine, EventBridge rules, CloudWatch alarms, and IAM roles. See the [deployment docs](https://dwsmith1983.github.io/interlock/docs/deployment/terraform/) for the full variable reference.

## Example

See [interlock-aws-example](https://github.com/dwsmith1983/interlock-aws-example) for a complete telecom ETL deployment with 6 pipelines, bronze/silver medallion architecture, and a CloudFront dashboard.

## Project Structure

```
interlock/
├── cmd/lambda/
│   ├── stream-router/       # DynamoDB Stream → Step Functions
│   ├── orchestrator/        # Evaluate, trigger, check-job, post-run
│   ├── sla-monitor/         # SLA deadline calculations + alerts
│   ├── watchdog/            # Missed schedule + stale run detection
│   ├── event-sink/          # EventBridge → events table
│   └── alert-dispatcher/    # SQS → Slack (Bot API with threading)
├── pkg/types/               # Public domain types (pipeline config, events, DynamoDB keys)
├── internal/
│   ├── lambda/              # Lambda handler logic + shared types
│   ├── store/               # DynamoDB storage layer (3-table design)
│   ├── config/              # Pipeline YAML config loading
│   ├── validation/          # Declarative validation rule engine
│   ├── trigger/             # Trigger execution (8 types)
│   └── calendar/            # Calendar exclusion registry
├── deploy/
│   ├── terraform/           # Reusable Terraform module
│   ├── build.sh             # Lambda build script (linux/arm64)
│   └── statemachine.asl.json # Step Functions ASL definition
└── testdata/
    ├── pipelines/           # Sample pipeline configs
    └── calendars/           # Sample calendar exclusion files
```

## Development

```bash
make test            # Run all tests
make build-lambda    # Build 6 Lambda handlers (linux/arm64)
make lint            # go fmt + go vet
```

### Prerequisites

- Go 1.24+
- AWS CLI v2 + Terraform >= 1.5 (for deployment)
- Slack Bot token with `chat:write` scope (for alert notifications)

## License

[Elastic License 2.0](LICENSE)
