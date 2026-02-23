# Interlock Local Demo

Full-stack demo with Airflow, Redis, and Grafana showing Interlock's STAMP-based pipeline safety in action — including lifecycle features like cascade, retry, monitoring, replay, and SLA.

## Architecture

```
            ┌───────────┐
            │  Grafana  │ :3001
            │ Infinity  │ polls Interlock REST API
            └─────┬─────┘
                  │
            ┌─────▼──────┐          ┌────────────┐
            │ Interlock  │ :3000    │  Airflow   │ :8080
            │  serve +   ├─────────►│ standalone │
            │  watcher   │ trigger  │  (SQLite)  │
            └─────┬──────┘ + poll   └────────────┘
                  │
            ┌─────▼──────┐          ┌────────────┐
            │   Redis    │ :6379    │    Seed    │ :8888
            │   store    │          │ register + │
            └────────────┘          │ callbacks  │
                                    │  + gate    │
                                    └────────────┘
```

## Quick Start

```bash
cd demo/local
docker compose up --build
```

Wait ~2 minutes for Airflow to initialize, then open:

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana Dashboard | http://localhost:3001 | anonymous (viewer) |
| Airflow UI | http://localhost:8080 | admin / admin |
| Interlock API | http://localhost:3000/api/pipelines | — |
| Interlock Metrics | http://localhost:3000/debug/vars | — |

To shut down:

```bash
docker compose down -v
```

## Demo Scenarios

Four pipelines demonstrate different outcomes in the interactive demo:

| Pipeline | Evaluator | Trigger | Expected Lifecycle |
|---|---|---|---|
| `airflow-success` | always-pass | Airflow `demo_success` DAG | READY → RUNNING → COMPLETED |
| `airflow-failure` | always-pass | Airflow `demo_failure` DAG | READY → RUNNING → FAILED |
| `blocked-pipeline` | always-fail | (never triggers) | NOT_READY forever |
| `http-callback` | always-pass | HTTP POST to seed webhook | READY → RUNNING → COMPLETED via callback |

## What Happens

1. **Redis** starts first as the backing store
2. **Airflow** boots in standalone mode with two demo DAGs (takes ~60-120s)
3. **Interlock** connects to Redis, loads archetypes, and starts the watcher
4. **Seed** waits for Interlock + Airflow, then registers all 4 pipelines via the REST API
5. The **watcher** evaluates each pipeline every 15-20s:
   - `airflow-success`: evaluator passes → triggers `demo_success` DAG → watcher polls until COMPLETED
   - `airflow-failure`: evaluator passes → triggers `demo_failure` DAG → watcher polls until FAILED
   - `blocked-pipeline`: evaluator fails → stays NOT_READY, re-evaluates every cycle (visible as incrementing `evaluations_total`)
   - `http-callback`: evaluator passes → triggers HTTP POST to seed → seed sends completion callback after 5s
6. **Grafana** auto-refreshes every 10s, showing live pipeline state and metrics

## Grafana Dashboard Rows

| Row | Panels | Source |
|---|---|---|
| System Metrics | evaluations_total, triggers_total, triggers_failed, sla_breaches | `/debug/vars` |
| Pipelines | Table of all registered pipelines | `/api/pipelines` |
| Pipeline Readiness | Per-pipeline READY/NOT_READY status | `/api/pipelines/{id}/readiness` |
| Recent Runs | Run tables for each triggered pipeline | `/api/pipelines/{id}/runs` |
| Error Metrics | evaluation_errors, alerts_dispatched, retries_scheduled, alerts_failed | `/debug/vars` |

## Useful Commands

```bash
# Check pipeline readiness
curl -s http://localhost:3000/api/pipelines/airflow-success/readiness | jq

# List all runs for a pipeline
curl -s http://localhost:3000/api/pipelines/airflow-success/runs | jq

# Get run log for today
curl -s http://localhost:3000/api/pipelines/airflow-success/runlogs/$(date +%Y-%m-%d)?schedule=daily | jq

# Request a replay
curl -X POST http://localhost:3000/api/pipelines/airflow-success/rerun \
  -H 'Content-Type: application/json' \
  -d '{"originalDate": "'$(date +%Y-%m-%d)'", "reason": "manual replay"}'

# View pipeline events
curl -s http://localhost:3000/api/pipelines/airflow-success/events | jq

# View live metrics
curl -s http://localhost:3000/debug/vars | jq '{evaluations_total, triggers_total, triggers_failed}'

# View Interlock logs
docker compose logs -f interlock

# View seed activity
docker compose logs -f seed
```

## E2E Tests

Automated end-to-end test suite with 11 scenarios validating the full Interlock lifecycle using the local Docker stack.

### Running

```bash
# From repo root
make local-e2e-test

# Or directly
./demo/local/e2e-test.sh run
```

### Teardown

```bash
make local-e2e-test-teardown

# Or directly
./demo/local/e2e-test.sh teardown
```

Results are preserved in `demo/local/e2e-results/` after teardown. Delete manually with `rm -rf demo/local/e2e-results/`.

```
e2e-results/
├── scenario-1-round-1.json         # Progressive: all fail
├── scenario-1-round-2.json         # Progressive: partial pass (1/3)
├── scenario-1-round-3-eval.json    # Progressive: all pass (READY)
├── scenario-1-round-3-complete.json # Progressive: run completed
├── scenario-2-round-1.json         # Quality drop
├── scenario-2-round-2-eval.json    # Quality restored (READY)
├── scenario-2-round-2-complete.json # Recovery run completed
├── scenario-3-result.json          # Dedup (no new run)
├── scenario-4-result.json          # Excluded day
├── scenario-5-result.json          # Pipeline not found
├── scenario-6-result.json          # Watcher-driven completion
├── scenario-7-silver.json          # Cascade: silver completes
├── scenario-7-gold.json            # Cascade: gold triggered by silver
├── scenario-8-*.json               # Retry with backoff
├── scenario-9-*.json               # Post-run monitoring / drift detection
├── scenario-10-*.json              # Replay (re-run completed pipeline)
└── scenario-11-*.json              # Evaluation SLA breach
```

### Scenarios

| # | Scenario | What It Tests |
|---|----------|---------------|
| 1 | Progressive Readiness | Sensors fail → partial pass → all pass → watcher triggers → complete via callback |
| 2 | Re-run After Quality Drop | Reset sensor below threshold → NOT_READY → restore → re-trigger → complete |
| 3 | Already Completed (Dedup) | Watcher skips pipeline when RunLog shows COMPLETED for today |
| 4 | Excluded Day | Pipeline with today's weekday excluded — watcher skips entirely |
| 5 | Pipeline Not Found | Evaluate nonexistent pipeline — returns error |
| 6 | Watcher-Driven Evaluation | Register pipeline with passing sensors, watcher auto-triggers and runs to completion |
| 7 | Cascade (Silver → Gold) | Silver completes → gold's upstream check passes → gold triggers automatically |
| 8 | Retry with Backoff | Trigger fails (gate closed) → FAILED → gate opens → watcher retries → COMPLETED |
| 9 | Post-Run Monitoring | Pipeline completes → monitoring window → sensor drift → drift event emitted |
| 10 | Replay | POST `/api/pipelines/{id}/rerun` triggers re-evaluation of a completed pipeline |
| 11 | Evaluation SLA Breach | Pipeline with past deadline → `SLA_BREACHED` event fires |

### Archetypes

| File | Name | Required Traits | Used By |
|------|------|-----------------|---------|
| `archetypes/batch-ingestion.yaml` | batch-ingestion | source-freshness, upstream-dependency, resource-availability | Scenarios 1-6, 8-11 |
| `archetypes/medallion.yaml` | medallion | upstream-dependency | Scenario 7 (gold pipeline) |

### Evaluators

The E2E tests use four sensor-backed evaluators that read from Redis:

- `check-freshness` — compares `sensor:freshness` against `maxLagSeconds`
- `check-record-count` — compares `sensor:record-count` against `threshold`
- `check-upstream` — checks if `sensor:upstream` is `"true"`
- `check-upstream-runlog` — queries the Interlock RunLog API to check if an upstream pipeline completed today

### Seed Server Endpoints

The seed container (`scripts/seed.py`) runs a webhook server on port 8888:

| Path | Method | Description |
|------|--------|-------------|
| `/webhook` | POST | Default webhook handler (accepts trigger requests) |
| `/gate` | POST | Conditional trigger — returns 200 if gate is open, 500 if closed |
| `/gate/open` | POST | Opens the gate (for retry testing) |
| `/gate/close` | POST | Closes the gate (for retry testing) |

## Notes

- RunLog tracking prevents re-triggering completed pipelines on the same day. Each scenario executes once; the dashboard shows the final state.
- `blocked-pipeline` stays NOT_READY and re-evaluates every cycle — you'll see `evaluations_total` incrementing.
- To re-run the demo, `docker compose down -v` clears all Redis state and Airflow metadata.
- Postgres archival is verified at the end of the E2E suite — the archiver runs every 30s, copying runs and events from Redis to Postgres.
