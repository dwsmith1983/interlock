# Interlock

STAMP-based safety framework for data pipeline reliability. Interlock prevents pipelines from executing when preconditions aren't safe — like a physical interlock mechanism.

The framework applies [Leveson's Systems-Theoretic Accident Model](https://mitpress.mit.edu/9780262016629/engineering-a-safer-world/) to data engineering: pipelines have control structures with **traits** (feedback), **readiness predicates** (process models), and **conditional execution** (safe control actions).

## Quick Start

```bash
# Build from source
make build

# Initialize a project (starts a local Valkey container)
./interlock init my-project
cd my-project

# Evaluate a pipeline's readiness
interlock evaluate example

# Run a pipeline (evaluate + trigger)
interlock run example

# Check status
interlock status

# Start the HTTP API server
interlock serve
```

## How It Works

Interlock uses a three-level check system:

1. **Archetypes** define *what* to check — reusable templates of safety traits (e.g., `batch-ingestion` requires source-freshness, upstream-dependency, resource-availability).
2. **Pipeline configs** specialize *how* — override thresholds, point to specific evaluators, set TTLs.
3. **Evaluators** perform the actual checks — subprocess (any language, JSON stdin/stdout) or HTTP.

```
interlock evaluate my-pipeline
  → load pipeline config
  → resolve archetype (merge trait definitions)
  → for each required trait, IN PARALLEL:
      spawn evaluator subprocess → pipe config JSON to stdin → read result from stdout
  → apply readiness rule (all-required-pass)
  → READY or NOT_READY (with blocking trait list)
```

### Evaluator Protocol

Evaluators are executable files. Interlock pipes config as JSON to stdin and reads the result from stdout.

**Input** (stdin):
```json
{"maxLagSeconds": 300, "source": "sales_events"}
```

**Output** (stdout):
```json
{"status": "PASS", "value": {"lagSeconds": 45, "threshold": 300}}
```

Status must be `"PASS"` or `"FAIL"`. Non-zero exit code or timeout = automatic `FAIL`.

Write evaluators in any language:

```bash
#!/bin/bash
# evaluators/check-disk-space
echo '{"status":"PASS","value":{"disk_pct":42}}'
```

```python
#!/usr/bin/env python3
# evaluators/check-source-freshness
import json, sys
config = json.load(sys.stdin)
lag = 45  # query your source system here
result = {"status": "PASS" if lag <= config["maxLagSeconds"] else "FAIL",
          "value": {"lagSeconds": lag}}
json.dump(result, sys.stdout)
```

## Lifecycle Management

Interlock manages the full lifecycle of pipeline runs, not just readiness gating.

### Retry with Backoff

Failed triggers or runs are automatically retried with configurable exponential backoff:

```yaml
retry:
  maxAttempts: 3
  backoffSeconds: 30
  backoffMultiplier: 2.0
  retryableFailures: [TRANSIENT, TIMEOUT]
```

Failures are classified as `TRANSIENT` (HTTP 5xx, network errors), `PERMANENT` (HTTP 4xx), `TIMEOUT`, or `EVALUATOR_CRASH`. Only categories listed in `retryableFailures` are retried.

### Cascade (Downstream Notification)

When a pipeline completes, Interlock scans all registered pipelines for traits that reference it via `upstreamPipeline` config. Matching downstream pipelines receive cascade markers, triggering their own evaluation cycles:

```
silver completes → NotifyDownstream → writes MARKER for gold → gold evaluates → triggers
```

In AWS mode, cascade markers flow through DynamoDB Streams → stream-router → Step Function. In local mode, the watcher picks up state changes on the next tick.

### Post-Run Monitoring / Drift Detection

After a pipeline completes, Interlock can continue monitoring traits for drift — detecting when conditions that were true at trigger time have degraded:

```yaml
watch:
  interval: 30s
  monitoring:
    enabled: true
    duration: 2h
```

If drift is detected during the monitoring window, a `MONITORING_DRIFT_DETECTED` event is emitted, an alert fires, and a rerun record is created.

### Replay (Re-run Completed Pipelines)

Completed pipelines can be re-run via the API, bypassing the usual dedup check:

```bash
curl -X POST http://localhost:3000/api/pipelines/my-pipeline/rerun \
  -d '{"originalDate": "2026-02-23", "reason": "data quality fix"}'
```

Rerun records track the original date, reason, and status independently from the primary run log.

### SLA Tracking

Pipelines can define evaluation and completion deadlines. Breaches fire alerts:

```yaml
sla:
  evaluationDeadline: "09:00"
  completionDeadline: "12:00"
  validationTimeout: "10:00"
  timezone: America/New_York
```

| SLA Type | Fires When |
|----------|-----------|
| Evaluation deadline | Pipeline still NOT_READY after the deadline |
| Completion deadline | Pipeline triggered but not yet COMPLETED |
| Validation timeout | Pipeline still in evaluation phase (force-fails) |

## Architecture

Interlock runs in two modes: **local** (Redis + subprocess evaluators) and **AWS** (DynamoDB + Lambda + Step Functions).

### Local Mode

```
┌──────────────────────────────────────────────────┐
│                  interlock serve                 │
│   (HTTP API + watcher loop + status)             │
├──────────────────────────────────────────────────┤
│               InterlockEngine                    │
│   (pure STAMP logic — readiness, lifecycle, UCA) │
├──────────────┬───────────────────────────────────┤
│   Provider   │  Redis/Valkey    [implemented]    │
│   Interface  │  DynamoDB        [implemented]    │
│              │  Postgres        [archival only]  │
├──────────────┴───────────────────────────────────┤
│              Evaluator Runner                    │
│   (subprocess: JSON stdin → JSON stdout)         │
│   (any language: Python, Bash, Go, JS, etc.)     │
└──────────────────────────────────────────────────┘
```

### AWS Mode

```
┌──────────────────┐     DynamoDB Stream     ┌──────────────────┐
│    DynamoDB      │ ──────────────────────► │  stream-router   │
│  (single table)  │                         │  (MARKER# → SFN) │
└────────┬─────────┘                         └────────┬─────────┘
         │                                            │
         │                               ┌────────────▼─────────────┐
         │                               │    Step Function         │
         │                               │  (47-state lifecycle)    │
         │                               └──┬────┬────┬─────┬───────┘
         │                                  │    │    │     │
    ┌────▼─────┐  ┌──────────┐  ┌───────────┴┐  ┌┴────▼──┐ ┌┴───────────┐
    │orchestr- │  │evaluator │  │  trigger   │  │  run-  │ │    SNS     │
    │  ator    │  │(per-trait│  │(job launch)│  │checker │ │  (alerts)  │
    │(14 acts) │  │  eval)   │  │            │  │(poll)  │ │            │
    └──────────┘  └──────────┘  └────────────┘  └────────┘ └────────────┘
```

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

## Multi-Schedule Support

Pipelines can define multiple evaluation schedules, each with independent timing, deadlines, and SLA tracking:

```yaml
name: multi-window-pipeline
archetype: batch-ingestion
schedules:
  - name: morning
    after: "06:00"
    deadline: "09:00"
    timezone: America/New_York
  - name: evening
    after: "18:00"
    deadline: "21:00"
    timezone: America/New_York
```

Each schedule is evaluated independently with its own lock, run log, and SLA window. Pipelines without explicit schedules default to a single `daily` schedule.

## Calendar Exclusions

Skip pipeline evaluation on specific days or dates:

```yaml
name: weekday-only-pipeline
archetype: batch-ingestion
exclusions:
  days: [Saturday, Sunday]
  dates: ["2025-12-25", "2026-01-01"]
```

Named calendars can be defined in YAML files and referenced across pipelines:

```yaml
# calendars/us-holidays.yaml
name: us-holidays
dates: ["2025-12-25", "2026-01-01", "2026-07-04"]
```

```yaml
# pipeline config
exclusions:
  calendar: us-holidays
```

## Project Structure

```
interlock/
├── cmd/
│   ├── interlock/             # CLI binary
│   └── lambda/                # AWS Lambda handlers
│       ├── stream-router/     #   DynamoDB Stream → SFN
│       ├── orchestrator/      #   Multi-action workflow logic
│       ├── evaluator/         #   Single trait evaluation
│       ├── trigger/           #   Job execution + state machine
│       └── run-checker/       #   External job status polling
├── pkg/types/                 # Public domain types
├── internal/
│   ├── engine/                # Readiness evaluation engine
│   ├── provider/
│   │   ├── redis/             # Redis/Valkey provider
│   │   ├── dynamodb/          # DynamoDB provider (single-table)
│   │   └── postgres/          # Postgres archival store
│   ├── watcher/               # Reactive evaluation loop
│   ├── schedule/              # Schedule, SLA, retry utilities
│   ├── evaluator/             # Subprocess + HTTP evaluator runners
│   ├── trigger/               # Trigger execution (8 types)
│   ├── alert/                 # Alert dispatching (console, webhook, file, SNS)
│   ├── lambda/                # Shared Lambda init + types
│   ├── archetype/             # Archetype loading + resolution
│   ├── calendar/              # Calendar exclusion registry
│   └── config/                # YAML config loading
├── deploy/
│   ├── cdk/                   # AWS CDK stack (Go)
│   ├── build.sh               # Lambda build script
│   └── statemachine.asl.json  # Step Function definition (Amazon States Language)
└── demo/
    ├── local/                 # Local demo (Redis + Airflow + Grafana)
    └── aws/                   # AWS E2E test suite
```

## Configuration

### interlock.yaml

```yaml
provider: redis
redis:
  addr: localhost:6379
  keyPrefix: "interlock:"
server:
  addr: ":3000"
archetypeDirs:
  - ./archetypes
evaluatorDirs:
  - ./evaluators
pipelineDirs:
  - ./pipelines
alerts:
  - type: console
  - type: webhook
    url: http://localhost:8080/alerts
  - type: file
    path: ./alerts.log
```

### Archetype Definition

```yaml
name: batch-ingestion
requiredTraits:
  - type: source-freshness
    description: "Source data is recent enough"
    defaultConfig:
      maxLagSeconds: 300
    defaultTtl: 300        # seconds — Valkey EXPIRE
    defaultTimeout: 30     # seconds — kill evaluator after this
  - type: upstream-dependency
    description: "Upstream pipelines completed"
    defaultTtl: 600
    defaultTimeout: 60
optionalTraits:
  - type: schema-contract
readinessRule:
  type: all-required-pass
```

### Pipeline Configuration

```yaml
name: daily-sales-rollup
archetype: batch-ingestion
tier: 2

traits:
  source-freshness:
    evaluator: ./evaluators/check-sales-freshness
    config:
      source: sales_events
      maxLagSeconds: 60    # override archetype default
    ttl: 120
    timeout: 15

trigger:
  type: command
  command: "python scripts/run_pipeline.py"
```

## Timeout Handling

| Timeout | Prevents | Mechanism |
|---------|----------|-----------|
| **Evaluator timeout** | Hung evaluator subprocess | `context.WithTimeout` + process kill. Trait result = FAIL (EVALUATOR_TIMEOUT) |
| **Trait TTL** | Stale process model | Valkey native `EXPIRE`. Key disappears after TTL. Missing key = STALE |

## Locking

Concurrent trigger prevention via atomic compare-and-swap:

- **Redis**: Lua script for atomic CAS on run state version
- **DynamoDB**: `ConditionExpression` on version attribute

1. Create `RunState{status: PENDING, version: 1}`
2. Evaluate readiness
3. If READY: CAS to `TRIGGERING, version: 2` where `version == 1`
4. Success = you own the trigger. Failure = another process won.

One trigger wins. Others fail the CAS and back off.

## HTTP API

Start with `interlock serve` (default `:3000`).

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/health` | Health check |
| `GET` | `/api/pipelines` | List all pipelines |
| `POST` | `/api/pipelines` | Register a pipeline |
| `GET` | `/api/pipelines/{id}` | Get pipeline config |
| `DELETE` | `/api/pipelines/{id}` | Remove a pipeline |
| `POST` | `/api/pipelines/{id}/evaluate` | Evaluate readiness |
| `GET` | `/api/pipelines/{id}/readiness` | Get cached readiness |
| `GET` | `/api/pipelines/{id}/traits` | Get all trait results |
| `GET` | `/api/pipelines/{id}/traits/{type}` | Get single trait result |
| `POST` | `/api/pipelines/{id}/traits/{type}` | Push external trait result |
| `POST` | `/api/pipelines/{id}/run` | Evaluate + trigger |
| `GET` | `/api/pipelines/{id}/runs` | List recent runs |
| `GET` | `/api/runs/{runId}` | Get run state |
| `POST` | `/api/runs/{runId}/complete` | Completion callback |
| `GET` | `/api/pipelines/{id}/runlogs` | List run log entries |
| `GET` | `/api/pipelines/{id}/runlogs/{date}` | Get run log for date (query: `?schedule=`) |
| `GET` | `/api/pipelines/{id}/events` | List pipeline events |
| `POST` | `/api/pipelines/{id}/rerun` | Request replay of completed run |
| `GET` | `/api/pipelines/{id}/reruns` | List reruns for pipeline |
| `GET` | `/api/reruns` | List all reruns |
| `POST` | `/api/reruns/{rerunId}/complete` | Complete a rerun |

### Orchestrator Integration

Interlock is a safety gate, not an orchestrator. It sits in front of any job execution system:

```yaml
# Airflow (native trigger type)
trigger:
  type: airflow
  url: http://airflow:8080
  dagID: my_pipeline
  headers:
    Authorization: "Bearer ${AIRFLOW_TOKEN}"

# AWS Glue (native SDK trigger)
trigger:
  type: glue
  jobName: my-etl-job

# AWS EMR
trigger:
  type: emr
  clusterID: j-XXXXXXXXXXXXX
  arguments: ["spark-submit", "s3://bucket/job.py"]

# Databricks
trigger:
  type: databricks
  workspaceURL: https://workspace.databricks.com
  jobName: my-job
  headers:
    Authorization: "Bearer ${DATABRICKS_TOKEN}"

# HTTP (generic)
trigger:
  type: http
  method: POST
  url: https://my-service.com/run
  body: '{"pipeline": "daily-sales"}'

# Local command
trigger:
  type: command
  command: "python scripts/run_pipeline.py"
```

Completion callbacks: `POST /api/runs/{runId}/complete`
```json
{"status": "success", "metadata": {"rows_processed": 150000}}
```

## AWS Deployment

Interlock deploys to AWS as a fully serverless, event-driven system using CDK.

### Build and Deploy

```bash
# Build Lambda handlers + archetype layer
make build-lambda

# Synthesize CloudFormation template
make cdk-synth

# Deploy to AWS
make cdk-deploy

# Run CDK tests
make cdk-test
```

### Stack Resources

| Resource | Description |
|----------|-------------|
| DynamoDB table | Single-table design with streams, GSI, TTL |
| 5 Lambda functions | stream-router, orchestrator, evaluator, trigger, run-checker |
| Step Function | 47-state pipeline lifecycle (Amazon States Language) |
| SNS topic | Alert notifications |
| Lambda layer | Archetype YAML definitions |

### Configuration

Set via environment variables before `cdk deploy`:

| Variable | Default | Description |
|----------|---------|-------------|
| `INTERLOCK_DESTROY_ON_DELETE` | `false` | Allow `cdk destroy` to delete DynamoDB table |
| `EVALUATOR_BASE_URL` | — | Base URL for HTTP evaluators |

Opt-in trigger permissions are configured in `deploy/cdk/config.go`:

```go
cfg.EnableGlueTrigger = true          // Glue StartJobRun/GetJobRun
cfg.EnableEMRTrigger = true           // EMR AddJobFlowSteps/DescribeStep
cfg.EnableEMRServerlessTrigger = true // EMR Serverless StartJobRun/GetJobRun
cfg.EnableStepFunctionTrigger = true  // SFN StartExecution/DescribeExecution
```

### E2E Testing

See [`demo/aws/`](demo/aws/) for the AWS E2E test suite (11 scenarios: progressive readiness, quality drop, dedup, exclusion, not found, stream-router, cascade, retry, monitoring, replay, SLA breach).

See [`demo/local/`](demo/local/) for the local E2E test suite (11 scenarios including cascade, retry, monitoring, replay, and SLA breach).

```bash
# AWS
./demo/aws/e2e-test.sh run       # deploy + test
./demo/aws/e2e-test.sh teardown  # clean up

# Local (Docker)
make local-e2e-test              # build + test
make local-e2e-test-teardown     # clean up
```

## CLI Reference

```
interlock init <project>         Create project scaffolding + start Valkey
interlock add-pipeline           Register a new pipeline
interlock evaluate <pipeline>    Evaluate readiness
interlock run <pipeline>         Evaluate + trigger
interlock status [pipeline]      Show status and recent runs
interlock serve                  Start HTTP API server
```

## Starter Archetypes

| Archetype | Required Traits | Use Case |
|-----------|----------------|----------|
| `batch-ingestion` | source-freshness, upstream-dependency, resource-availability | ETL, scheduled batch jobs |
| `streaming-enrichment` | source-freshness, enrichment-source, resource-availability | Stream processing, real-time enrichment |
| `ml-feature-pipeline` | source-freshness, upstream-dependency, resource-availability, model-registry | ML feature engineering, model training |

## Demos

| Demo | Scenarios | Description | Location |
|------|-----------|-------------|----------|
| **Local** | 11 | Docker Compose with Redis, Airflow, Grafana — includes cascade, retry, monitoring, replay, SLA | [`demo/local/`](demo/local/) |
| **AWS** | 11 | E2E test suite with DynamoDB, Lambda, Step Functions — includes cascade, retry, monitoring, replay, SLA | [`demo/aws/`](demo/aws/) |

## Development

```bash
make build                    # Build binary
make test                     # Run all tests
make test-unit                # Run unit tests (no Redis needed)
make test-integration         # Run integration tests (requires Redis on localhost:6379)
make lint                     # Run gofmt + go vet + golangci-lint
make dist                     # Cross-compile for linux/darwin amd64/arm64
make build-lambda             # Build Lambda handlers for deployment
make cdk-synth                # Synthesize CDK CloudFormation template
make cdk-diff                 # Show pending CDK changes
make cdk-deploy               # Deploy CDK stack to AWS
make cdk-test                 # Run CDK Go tests
make e2e-test                 # Run AWS E2E test suite
make local-e2e-test           # Run local E2E test suite (Docker)
make local-e2e-test-teardown  # Tear down local E2E stack
```

### Prerequisites

- Go 1.24+
- Docker (for `interlock init` — starts Valkey container)
- Redis/Valkey on `localhost:6379` (for integration tests)
- AWS CLI v2 + CDK v2 (for AWS deployment)

## License

TBD
