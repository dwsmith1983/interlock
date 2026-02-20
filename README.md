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
3. **Evaluators** are subprocesses that perform the actual checks — any language, JSON stdin/stdout protocol.

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

## Architecture

```
┌──────────────────────────────────────────────────┐
│                  interlock serve                 │
│   (HTTP API + orchestrator callbacks + status)   │
├──────────────────────────────────────────────────┤
│               InterlockEngine                    │
│   (pure STAMP logic — readiness, lifecycle, UCA) │
├──────────────┬───────────────────────────────────┤
│   Provider   │  Redis/Valkey    [implemented]    │
│   Interface  │  DynamoDB        [planned]        │
│              │  etcd/CRDs       [planned]        │
│              │  Firestore       [planned]        │
│              │  Cosmos DB       [planned]        │
├──────────────┴───────────────────────────────────┤
│              Evaluator Runner                    │
│   (subprocess: JSON stdin → JSON stdout)         │
│   (any language: Python, Bash, Go, JS, etc.)     │
└──────────────────────────────────────────────────┘
```

## Project Structure

After `interlock init`:

```
my-project/
├── interlock.yaml              # project config
├── evaluators/                 # trait evaluator scripts/binaries
│   └── check-source-freshness  # example (any language)
├── archetypes/                 # archetype definitions
│   ├── batch-ingestion.yaml
│   ├── streaming-enrichment.yaml
│   └── ml-feature-pipeline.yaml
└── pipelines/                  # pipeline configs
    └── example.yaml
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

Concurrent trigger prevention via atomic compare-and-swap (Valkey Lua script):

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
| `POST` | `/api/pipelines/{id}/run` | Evaluate + trigger |
| `GET` | `/api/pipelines/{id}/runs` | List recent runs |
| `GET` | `/api/runs/{runId}` | Get run state |
| `POST` | `/api/runs/{runId}/complete` | Completion callback |

### Orchestrator Integration

Interlock is a safety gate, not an orchestrator. It sits in front of any job execution system. Any service with an HTTP API or CLI can be triggered:

```yaml
# Airflow
trigger:
  type: http
  method: POST
  url: http://airflow:8080/api/v1/dags/my_pipeline/dagRuns
  headers:
    Authorization: "Bearer ${AIRFLOW_TOKEN}"
  body: '{"conf":{"triggered_by":"interlock"}}'

# Databricks
trigger:
  type: http
  method: POST
  url: https://workspace.databricks.com/api/2.1/jobs/run-now
  body: '{"job_id": 12345}'

# AWS Glue
trigger:
  type: http
  method: POST
  url: https://glue.us-east-1.amazonaws.com
  headers:
    Content-Type: "application/x-amz-json-1.1"
    X-Amz-Target: "AWSGlue.StartJobRun"
  body: '{"JobName": "my-etl-job"}'

# AWS Step Functions
trigger:
  type: http
  method: POST
  url: https://states.us-east-1.amazonaws.com
  headers:
    Content-Type: "application/x-amz-json-1.0"
    X-Amz-Target: "AWSStepFunctions.StartExecution"
  body: '{"stateMachineArn": "arn:aws:states:us-east-1:123456789:stateMachine:my-pipeline"}'

# AWS Lambda (via CLI)
trigger:
  type: command
  command: "aws lambda invoke --function-name my-pipeline-fn /dev/stdout"

# Local command
trigger:
  type: command
  command: "python scripts/run_pipeline.py"
```

Completion callbacks: `POST /api/runs/{runId}/complete`
```json
{"status": "success", "metadata": {"rows_processed": 150000}}
```

For AWS services, completion callbacks can be sent from Lambda on_success/on_failure hooks, Step Functions task states, Glue job bookmarks via EventBridge rules, or CloudWatch event targets — all POSTing to the Interlock callback endpoint.

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

## Development

```bash
make build            # Build binary
make test             # Run all tests
make test-unit        # Run unit tests (no Redis needed)
make test-integration # Run integration tests (requires Redis on localhost:6379)
make lint             # Run gofmt + go vet + golangci-lint
make dist             # Cross-compile for linux/darwin amd64/arm64
```

### Prerequisites

- Go 1.23+
- Docker (for `interlock init` — starts Valkey container)
- Redis/Valkey on `localhost:6379` (for integration tests)

## License

TBD
