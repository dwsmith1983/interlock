# LocalStack Deploy

Runs the full Interlock stack (Lambdas, DynamoDB, EventBridge, SQS, Step
Functions) against [LocalStack](https://localstack.cloud/) Community so the
system can be smoke-tested without an AWS account.

## Prerequisites

- Docker
- Go (matching the project's toolchain version)
- Python 3.11+
- `boto3` — `pip install -r deploy/localstack/requirements.txt`

## Quick start

From the repo root:

```
make -f deploy/localstack/Makefile localstack-all
```

This runs:

1. `localstack-up`     — start LocalStack at `http://localhost:4566`
2. `localstack-build`  — build the 6 Lambda zips under `deploy/localstack/dist/`
3. `localstack-deploy` — create all infra via boto3
4. `localstack-smoke`  — sanity-check the deploy

Tear everything down:

```
make -f deploy/localstack/Makefile localstack-teardown
make -f deploy/localstack/Makefile localstack-down
```

## What gets deployed

| Kind | Names |
|---|---|
| Lambda | `local-interlock-{stream-router,orchestrator,sla-monitor,watchdog,event-sink,alert-dispatcher}` |
| DynamoDB | `local-interlock-{control,joblog,events,rerun}` (streams on control + joblog) |
| EventBridge bus | `local-interlock-events` |
| EventBridge rule | `local-interlock-events-to-sink` → event-sink Lambda |
| EventBridge rule | `local-interlock-events-to-alerts` → SQS alerts queue |
| SQS | `local-interlock-alerts` |
| Step Functions | `local-interlock-pipeline` (from `deploy/statemachine.asl.json`) |
| Event source maps | control stream → stream-router, joblog stream → stream-router, SQS → alert-dispatcher |
| IAM | `lambda-role`, `sfn-role` (dummy LocalStack roles) |

All Lambdas receive `AWS_ENDPOINT_URL=http://localstack:4566` so the Go SDK
inside them calls LocalStack instead of real AWS.

## What's skipped vs production

- **EventBridge Scheduler** — Pro-only. `sla-monitor` gets `SKIP_SCHEDULER=true`
  so its schedule-creation paths no-op.
- **CloudWatch alarms** and alarm-driven EventBridge rules.
- **KMS-encrypted SQS** — the LocalStack stubbed KMS is not representative.
- **Reserved concurrency, DLQs, secrets** — not meaningful for local smoke.

## Verifying

```
python deploy/localstack/deploy.py smoke
```

The smoke check ensures:

1. All 6 Interlock Lambdas are present.
2. All 4 DynamoDB tables are present.
3. `stream-router` has `AWS_ENDPOINT_URL` set in its environment.

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `INTERLOCK_ENV`         | `local`                   | Prefix for all resource names |
| `AWS_ENDPOINT_URL`      | `http://localhost:4566`   | LocalStack URL the deploy script talks to |
| `LAMBDA_AWS_ENDPOINT_URL` | `http://localstack:4566` | The URL Lambdas use from inside their container |
