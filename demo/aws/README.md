# Interlock AWS E2E Demo

End-to-end testing of Interlock's AWS deployment: DynamoDB + Lambda + Step Functions.

## Architecture

```
                                    ┌─────────────────┐
                                    │  Step Function  │
                                    │  (28-state ASL) │
                                    └───┬───┬───┬───┬─┘
                                        │   │   │   │
              ┌─────────────────────────┘   │   │   └─────────────────────────┐
              │                             │   │                             │
     ┌────────▼────────┐       ┌────────────▼───▼────────────┐       ┌────────▼────────┐
     │  orchestrator   │       │     evaluator / trigger     │       │   run-checker   │
     │  (10 actions)   │       │  (trait eval / job launch)  │       │  (status poll)  │
     └────────┬────────┘       └────────────┬────────────────┘       └────────┬────────┘
              │                             │                                 │
              └──────────────┬──────────────┘                                 │
                             │                                                │
                    ┌────────▼────────┐                              ┌────────▼────────┐
                    │    DynamoDB     │◄──── DynamoDB Stream ────────│  stream-router  │
                    │  (single table) │                              │  (MARKER# → SFN)│
                    └────────┬────────┘                              └─────────────────┘
                             │
                    ┌────────▼────────┐
                    │ test-evaluator  │  ← Fake evaluator Lambda
                    │ (Function URL)  │     with sensor-driven PASS/FAIL
                    └─────────────────┘
```

## Prerequisites

- AWS CLI v2 configured with credentials
- AWS CDK v2 (`npm install -g aws-cdk`)
- Go 1.24+
- `jq` for JSON processing

## Quick Start

```bash
# Run all E2E scenarios
./demo/aws/e2e-test.sh run

# Clean up all AWS resources
./demo/aws/e2e-test.sh teardown
```

Or via Make:

```bash
make e2e-test            # run
make e2e-test-teardown   # teardown
```

## What Gets Deployed

1. **Test-evaluator Lambda** — a standalone Lambda with Function URL that:
   - Serves as trait evaluator endpoints (`/freshness`, `/record-count`, `/upstream-check`)
   - Reads "sensor state" from DynamoDB to decide PASS/FAIL
   - Provides a fake trigger endpoint (`/trigger-endpoint`)

2. **CDK Stack** (via `deploy/cdk/`) — the full Interlock infrastructure:
   - DynamoDB table (single-table design with streams)
   - 5 Lambda functions (stream-router, orchestrator, evaluator, trigger, run-checker)
   - Step Function state machine (28 states)
   - SNS topic for alerts
   - Lambda layer with archetype definitions

## Test Scenarios

### Scenario 1: Progressive Readiness

The main event — shows the system iterating through states as sensor data improves.

| Round | Sensors | Result |
|-------|---------|--------|
| 1 | lag=300, count=500, upstream=false | All FAIL → NOT_READY |
| 2 | lag=30, count=500, upstream=false | 1/3 PASS → NOT_READY |
| 3 | lag=30, count=1500, upstream=true | All PASS → TRIGGER → COMPLETED |

### Scenario 2: Re-run After Data Quality Drop

Simulates a data quality regression and recovery.

| Round | Sensors | Result |
|-------|---------|--------|
| 1 | count drops to 800 | NOT_READY |
| 2 | count restored to 1200 | TRIGGER → COMPLETED |

### Scenario 3: Already Completed (Dedup)

With a COMPLETED runlog entry, a new execution skips evaluation entirely — verifying dedup logic.

### Scenario 4: Excluded Day

Pipeline configured with today's weekday excluded. Execution hits `CheckExclusion → IsExcluded → End`.

### Scenario 5: Pipeline Not Found

Execution for a non-existent pipeline. Verifies graceful error handling.

### Scenario 6: Stream-Router Integration

Writes a MARKER# record directly to DynamoDB and verifies the full event-driven path: DynamoDB Streams → stream-router Lambda → Step Function execution.

## Results

Results are saved to `demo/aws/e2e-results/`:

```
e2e-results/
├── stack-outputs.json         # CDK stack outputs
├── scenario-1-round-1.json    # Progressive: all fail
├── scenario-1-round-2.json    # Progressive: partial pass
├── scenario-1-round-3.json    # Progressive: all pass → trigger
├── scenario-2-round-1.json    # Quality drop
├── scenario-2-round-2.json    # Quality recovery
├── scenario-3-result.json     # Dedup
├── scenario-4-result.json     # Exclusion
├── scenario-5-result.json     # Not found
└── scenario-6-result.json     # Stream-router
```

## Test-Evaluator Lambda

Source: `demo/aws/test-evaluator/main.go`

### Endpoints

| Path | Sensor Key | Config | Logic |
|------|-----------|--------|-------|
| `/freshness` | `SENSOR#freshness` | `{"maxLagSeconds": N}` | PASS if `lag <= maxLagSeconds` |
| `/record-count` | `SENSOR#record-count` | `{"threshold": N}` | PASS if `count >= threshold` |
| `/upstream-check` | `SENSOR#upstream-check` | `{"expectComplete": true}` | PASS if `complete == true` |
| `/trigger-endpoint` | — | — | Always returns success |

### Sensor Items

The E2E script seeds DynamoDB items that the test-evaluator reads:

| PK | SK | data |
|---|---|---|
| `SENSOR#freshness` | `STATE` | `{"lag": 300}` |
| `SENSOR#record-count` | `STATE` | `{"count": 500}` |
| `SENSOR#upstream-check` | `STATE` | `{"complete": false}` |

The script updates these items between Step Function executions to control trait pass/fail.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TABLE_NAME` | `interlock` | DynamoDB table name |
| `AWS_REGION` | `us-east-1` | AWS region |

## Teardown

Destroys all AWS resources:

1. CDK stack (DynamoDB table, Lambdas, Step Function, SNS topic)
2. Test-evaluator Lambda + Function URL
3. Test-evaluator IAM role + policies
4. Local results directory
