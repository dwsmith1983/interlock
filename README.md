# Interlock

STAMP-based safety framework for data pipeline reliability. Interlock prevents pipelines from executing when preconditions aren't safe — like a physical interlock mechanism.

The framework applies [Leveson's Systems-Theoretic Accident Model](https://mitpress.mit.edu/9780262016629/engineering-a-safer-world/) to data engineering: pipelines have control structures with **traits** (feedback), **readiness predicates** (process models), and **conditional execution** (safe control actions).

```
go get github.com/dwsmith1983/interlock
```

## Quick Start

```bash
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
  -> load pipeline config
  -> resolve archetype (merge trait definitions)
  -> for each required trait, IN PARALLEL:
      spawn evaluator subprocess -> pipe config JSON to stdin -> read result from stdout
  -> apply readiness rule (all-required-pass)
  -> READY or NOT_READY (with blocking trait list)
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
┌──────────────────┐     DynamoDB Stream     ┌──────────────────────────────┐
│    DynamoDB      │ ──────────────────────► │       stream-router          │
│  (single table)  │                         │  MARKER# → SFN               │
└────────┬─────────┘                         │  RUNLOG# → lifecycle events  │
         │                                   └──────┬──────────┬────────────┘
         │                                          │          │
         │                             ┌────────────▼───┐   ┌──▼──────────┐
         │                             │ Step Function  │   │  SNS        │
         │                             │ (47-state      │   │ (lifecycle) │
         │                             │  lifecycle)    │   └─────────────┘
         │                             └┬───┬───┬───┬───┘
         │                              │   │   │   │
    ┌────▼─────┐  ┌──────────┐  ┌───────┴┐ ┌┴───▼──┐ ┌──────────┐ ┌──────────┐
    │orchestr- │  │evaluator │  │trigger │ │ run-  │ │   SNS    │ │ watchdog │
    │  ator    │  │(per-trait│  │(launch)│ │checker│ │ (alerts) │ │(EventBr) │
    │(14 acts) │  │  eval)   │  │        │ │(poll) │ │          │ │stuck+miss│
    └──────────┘  └──────────┘  └────────┘ └───────┘ └──────────┘ └──────────┘
```

### Cloud Support

| Cloud | Status |
|-------|--------|
| AWS (DynamoDB + Lambda + Step Functions) | Implemented |
| GCP (Firestore + Cloud Run + Workflows) | Planned |
| Azure (Cosmos DB + Functions + Durable Functions) | Planned |

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

## Lifecycle Management

### Retry with Backoff

Failed triggers are automatically retried with configurable exponential backoff:

```yaml
retry:
  maxAttempts: 3
  backoffSeconds: 30
  backoffMultiplier: 2.0
  retryableFailures: [TRANSIENT, TIMEOUT]
```

### Cascade (Downstream Notification)

When a pipeline completes, Interlock notifies downstream pipelines that depend on it via cascade markers, triggering their evaluation cycles.

### Post-Run Monitoring / Drift Detection

After completion, Interlock can monitor traits for drift — detecting when conditions that were true at trigger time have degraded. Drift triggers alerts and rerun records.

```yaml
watch:
  interval: 30s
  monitoring:
    enabled: true
    duration: 2h
```

### SLA Tracking

Pipelines define evaluation and completion deadlines. Breaches fire alerts:

```yaml
sla:
  evaluationDeadline: "09:00"
  completionDeadline: "12:00"
  timezone: America/New_York
```

### Watchdog (Absence Detection)

The watchdog runs independently to detect two classes of silent failures:

- **Missed schedules** — upstream ingestion failed silently, no MARKER arrived, no evaluation started by the deadline
- **Stuck runs** — a run started but has been in PENDING/TRIGGERING/RUNNING longer than the threshold (default: 30 minutes)

```yaml
watchdog:
  enabled: true
  interval: 5m
  stuckRunThreshold: 30m
```

On AWS, the watchdog runs as a separate Lambda on an EventBridge schedule. It is intentionally outside the Step Function — its job is to detect when the Step Function _didn't_ start.

### Lifecycle Events

When a pipeline run reaches a terminal status (COMPLETED or FAILED), the stream-router publishes an SNS event to a lifecycle topic. Downstream consumers can subscribe for active recovery, observability, or notification workflows.

### Alert Categorization

All alerts carry a machine-readable `alertType` field for filtering and routing:

| Category | Source | Meaning |
|---|---|---|
| `schedule_missed` | Watchdog | No evaluation started by deadline |
| `stuck_run` | Watchdog | Run in non-terminal state beyond threshold |
| `evaluation_sla_breach` | Orchestrator | Evaluation deadline exceeded |
| `completion_sla_breach` | Orchestrator | Completion deadline exceeded |
| `validation_timeout` | Orchestrator | Hard validation timeout hit |
| `trait_drift` | Orchestrator | Post-completion trait regression |

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

Pipelines without explicit schedules default to a single `daily` schedule.

## Calendar Exclusions

Skip pipeline evaluation on specific days or dates:

```yaml
exclusions:
  days: [Saturday, Sunday]
  dates: ["2025-12-25", "2026-01-01"]
  calendar: us-holidays    # reference a named calendar file
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
      maxLagSeconds: 60
    ttl: 120
    timeout: 15

trigger:
  type: command
  command: "python scripts/run_pipeline.py"
```

## Project Structure

```
interlock/
├── cmd/
│   ├── interlock/             # CLI binary
│   └── lambda/                # AWS Lambda handlers
│       ├── stream-router/     #   DynamoDB Stream -> SFN
│       ├── orchestrator/      #   Multi-action workflow logic
│       ├── evaluator/         #   Single trait evaluation
│       ├── trigger/           #   Job execution + state machine
│       ├── run-checker/       #   External job status polling
│       └── watchdog/          #   Missed schedule + stuck-run detection
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
│   ├── archetype/             # Archetype loading + resolution
│   ├── calendar/              # Calendar exclusion registry
│   └── config/                # YAML config loading
├── deploy/
│   ├── terraform/             # Terraform deployment (AWS infrastructure)
│   ├── build.sh               # Lambda build script
│   └── statemachine.asl.json  # Step Function definition
└── demo/
    ├── local/                 # Local demo (Redis + Docker Compose)
    └── aws/                   # AWS E2E test suite
```

## Development

```bash
make build                    # Build binary
make test                     # Run all tests
make test-unit                # Unit tests (no Redis needed)
make test-integration         # Integration tests (requires Redis)
make lint                     # gofmt + go vet + golangci-lint
make build-lambda             # Build Lambda handlers
make local-e2e-test           # Run local E2E test suite (Docker)
```

### Prerequisites

- Go 1.24+
- Docker (for `interlock init` — starts Valkey container)
- Redis/Valkey on `localhost:6379` (for integration tests)
- AWS CLI v2 + Terraform >= 1.5 (for AWS deployment)

## License

[Elastic License 2.0](LICENSE)
