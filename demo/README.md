# Interlock Demo

Full-stack demo with Airflow, Redis, and Grafana showing Interlock's STAMP-based pipeline safety in action.

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
                                    └────────────┘
```

## Quick Start

```bash
cd demo
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

Four pipelines demonstrate different outcomes:

| Pipeline | Evaluator | Trigger | Expected Lifecycle |
|---|---|---|---|
| `airflow-success` | always-pass | Airflow `demo_success` DAG | READY → RUNNING → COMPLETED |
| `airflow-failure` | always-pass | Airflow `demo_failure` DAG | READY → RUNNING → FAILED |
| `blocked-pipeline` | always-fail | (never triggers) | NOT_READY forever |
| `http-callback` | always-pass | HTTP POST to seed webhook | READY → RUNNING → COMPLETED via callback |

## What Happens

1. **Redis** starts first as the backing store
2. **Airflow** boots in standalone mode with two demo DAGs (takes ~60-120s)
3. **Interlock** connects to Redis, loads the `demo-simple` archetype, and starts the watcher
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

# View live metrics
curl -s http://localhost:3000/debug/vars | jq '{evaluations_total, triggers_total, triggers_failed}'

# View Interlock logs
docker compose logs -f interlock

# View seed activity
docker compose logs -f seed
```

## Notes

- RunLog tracking prevents re-triggering completed pipelines on the same day. Each scenario executes once; the dashboard shows the final state.
- `blocked-pipeline` stays NOT_READY and re-evaluates every cycle — you'll see `evaluations_total` incrementing.
- To re-run the demo, `docker compose down -v` clears all Redis state and Airflow metadata.
