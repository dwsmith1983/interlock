# Interlock

STAMP-based safety controller for data pipeline reliability. Interlock prevents pipelines from executing when preconditions aren't safe — like a physical interlock mechanism. Sensors report readiness, a controller evaluates safety constraints, and actuators trigger jobs only when it's safe.

Built on [Leveson's Systems-Theoretic Accident Model](https://mitpress.mit.edu/9780262016629/engineering-a-safer-world/) (STAMP): pipelines have **declarative validation rules** (safety constraints), **sensor data in DynamoDB** (process models), and **conditional execution** (safe control actions).

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

### Safety Model (STAMP)

Interlock maps directly to STAMP's control-theoretic safety structure. Each component has a defined role in the feedback loop that prevents unsafe pipeline execution:

| STAMP Concept | Interlock Component | Role |
|---------------|---------------------|------|
| Controlled Process | User's pipeline or job | The workload being safeguarded (Glue, EMR, Airflow DAG, Databricks, etc.) |
| Actuator | Trigger | Fires the job via REST call, AWS SDK, or subprocess — only when the controller says go |
| Controller | orchestrator Lambda (coordinated by Step Functions) | Evaluates validation rules against sensor state; decides whether to trigger |
| Sensor | DynamoDB sensor records | External processes write readiness signals (status, counts, timestamps, lag) to the control table |
| Feedback | Post-run drift detection, job logs, SLA monitoring | Monitors completed jobs for late data, source drift, SLA breaches, and silent failures |
| Safety Constraint | Validation rules (declarative YAML) | The preconditions that must be satisfied before the actuator fires |

The safety loop: sensors report the current state of upstream dependencies → the controller evaluates declarative constraints against that state → the actuator triggers the job only when all constraints pass → feedback mechanisms monitor the completed job and detect post-completion issues (drift, late data, SLA breaches) that may require a re-run.

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

## Pipeline Patterns

The sensor model is Interlock's universal interface. Whether your pipeline is batch, streaming rollup, ad-hoc, or depends on other pipelines — the pattern is the same: write sensor data to the control table, define validation rules, and let Interlock decide when it's safe to run.

The examples below show the relevant sections. A complete config also requires `pipeline:`, `job:`, and optionally `postRun:` and `dryRun:` fields — see [Pipeline Configuration](#pipeline-configuration) for a full example.

### Batch Precondition

Wait for an upstream job to report completion and a minimum row count before triggering a downstream ETL:

```yaml
schedule:
  cron: "0 8 * * *"
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
    - key: row-count
      check: gte
      field: count
      value: 1000
```

### Streaming Rollup Safety

A Kafka consumer processes transactions throughout the day. At close-of-business, a batch rollup must only run when the stream has caught up. The consumer writes lag and record count sensors to the control table:

```yaml
# schedule.trigger starts evaluation when lag drops below threshold;
# validation.rules re-check at each interval until the window closes
schedule:
  trigger:
    key: consumer-lag
    check: lte
    field: lag_seconds
    value: 30
  evaluation:
    window: 30m
    interval: 2m
validation:
  trigger: "ALL"
  rules:
    - key: consumer-lag
      check: lte
      field: lag_seconds
      value: 30
    - key: record-count
      check: gte
      field: count
      value: 5000
    - key: cutoff-status
      check: equals
      field: status
      value: closed
```

### Cross-Pipeline Dependency

Upstream pipeline handlers write success sensors directly to the downstream pipeline's control table entry (`PK = PIPELINE#<downstream-id>`). No special cross-pipeline machinery — it's just a sensor write to the right partition key:

```yaml
# silver-daily pipeline — waits for all 24 hourly runs to complete
schedule:
  trigger:
    key: daily-status
    check: equals
    field: all_hours_complete
    value: true
validation:
  trigger: "ALL"
  rules:
    - key: daily-status
      check: equals
      field: all_hours_complete
      value: true
```

See [interlock-aws-example](https://github.com/dwsmith1983/interlock-aws-example) for the full bronze → silver-hourly → silver-daily dependency chain.

### Ad-Hoc / Irregular Schedule

For pipelines that run on specific business dates (month-end close, quarterly reporting) rather than a fixed cron. Use an inclusion calendar with a relative SLA measured from first sensor arrival:

```yaml
schedule:
  include:
    dates:
      - "2026-01-31"
      - "2026-02-28"
      - "2026-03-31"
  trigger:
    key: month-end-ready
    check: equals
    field: status
    value: ready
  evaluation:
    window: 8h
    interval: 10m
sla:
  maxDuration: 4h
```

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
├── pkg/
│   ├── types/               # Public domain types (pipeline config, events, DynamoDB keys)
│   ├── validation/          # Declarative validation rule engine
│   └── sla/                 # Pure SLA deadline calculations
├── internal/
│   ├── lambda/              # Shared types, interfaces, utilities
│   │   ├── orchestrator/    # Evaluate, trigger, check-job handlers
│   │   ├── stream/          # DynamoDB stream routing, reruns, post-run
│   │   ├── watchdog/        # Stale trigger + missed schedule detection
│   │   ├── sla/             # SLA deadline calculation + alerts
│   │   ├── alert/           # Slack notification formatting
│   │   └── sink/            # EventBridge event persistence
│   ├── aws/lambda/          # Lambda context middleware (timeout derivation)
│   ├── handler/             # Stream batch processing (ReportBatchItemFailures)
│   ├── dlq/                 # Dead-letter queue routing, error classification
│   ├── telemetry/           # OpenTelemetry + structured logging with correlation IDs
│   ├── client/              # Circuit breaker for external HTTP calls
│   ├── resilience/          # Exponential backoff retry with jitter
│   ├── concurrency/         # Bounded worker pool (errgroup + semaphore)
│   ├── store/               # DynamoDB storage layer (3-table design)
│   ├── config/              # Pipeline YAML config loading
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
make lint            # golangci-lint + go test -race
make audit           # Full quality gate (same as lint, CI blocking)
```

### Local Development with LocalStack

Run the full Interlock stack locally against [LocalStack](https://localstack.cloud/) Community — real Lambdas, real DynamoDB streams, real Step Functions, real EventBridge — without an AWS account. EventBridge Scheduler is skipped (Pro-only feature); the `sla-monitor` Lambda no-ops scheduler calls when `SKIP_SCHEDULER=true`, which the deploy sets by default. Prerequisites: Docker, Go 1.25+, Python 3.11+, and `boto3` (`pip install -r deploy/localstack/requirements.txt`).

```bash
make -f deploy/localstack/Makefile localstack-all
```

See [`deploy/localstack/README.md`](deploy/localstack/README.md) for the full resource map, teardown commands, and smoke-test details.

### Observability

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to enable OpenTelemetry trace and metric export (e.g., to Jaeger or Grafana). When unset, telemetry gracefully degrades to no-op providers with zero overhead.

Application metrics: `interlock.records.processed`, `interlock.stage.duration`, `interlock.rules.evaluated`, `interlock.dlq.routed`, `interlock.worker_pool.active`, `interlock.circuit_breaker.state`.

All structured logs include `correlation_id` for cross-service tracing when the context carries one.

### Prerequisites

- Go 1.25+
- AWS CLI v2 + Terraform >= 1.5 (for deployment)
- Slack Bot token with `chat:write` scope (for alert notifications)

## License

[Elastic License 2.0](LICENSE)
