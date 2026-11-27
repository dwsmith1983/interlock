# LocalStack Deploy

Runs the full Interlock stack (Lambdas, DynamoDB, EventBridge, SQS, Step
Functions) against [LocalStack](https://localstack.cloud/) Community so the
system can be smoke-tested without an AWS account.

## Prerequisites

- Docker. `localstack-up` mounts the Docker socket (`/var/run/docker.sock`)
  into LocalStack, which starts each Lambda in its own container.
- Go (matching the project's toolchain version)
- `zip` and `curl`
- Python 3.11+
- `boto3` — `pip install -r deploy/localstack/requirements.txt`

## Quick start

From the repo root:

```
make -f deploy/localstack/Makefile localstack-all
```

This runs:

1. `localstack-up`: start LocalStack at `http://localhost:4566`, as
   container `interlock-localstack` with the alias `localstack` on the Docker
   network `interlock-net`
2. `localstack-build`: build the 6 Lambda zips under `deploy/localstack/dist/`
3. `localstack-deploy`: create all infra via boto3
4. `localstack-smoke`: check the deploy and invoke the watchdog

Tear everything down (`localstack-down` also removes every container on
`interlock-net`, which includes the Lambda containers LocalStack started):

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
inside them calls LocalStack instead of real AWS. LocalStack starts the Lambda
containers on `interlock-net`, where `localstack` is its network alias.

## What's skipped vs production

- **EventBridge Scheduler**: Pro-only. `sla-monitor` and `watchdog` get
  `SKIP_SCHEDULER=true` so their schedule-creation paths no-op. Scheduled
  `SLA_WARNING` and `SLA_BREACH` alerts therefore do not fire locally; the
  `SLA_MET` or `SLA_BREACH` published when a run ends still does.
- **CloudWatch alarms** and alarm-driven EventBridge rules.
- **KMS-encrypted SQS** — the LocalStack stubbed KMS is not representative.
- **Reserved concurrency, DLQs, secrets** — not meaningful for local smoke.

## e2e mode

`deploy.py deploy --e2e` wires the stack for the end-to-end harness. It
expects Lambdas built with `EXTRA_TAGS=e2e bash deploy/localstack/build.sh`
(tags `lambda.norpc,e2e`). Compared with a normal deploy it:

- creates `local-interlock-schedules` (hash key `name`), where the e2e builds
  of `sla-monitor` and `watchdog` record schedules instead of calling
  EventBridge Scheduler, and drops `SKIP_SCHEDULER`;
- sets `E2E_CLOCK_URL` (`$E2E_MOCK_URL/clock`, default
  `http://mock:8080/clock`), `E2E_SCHEDULE_TABLE`,
  `JOB_CHECK_INTERVAL_SECONDS=5` and `RETRY_DELAY_MAX_SECONDS=5` (which can
  only shorten the wait before a job-failure retry) on every Lambda;
- skips the watchdog schedule rule and the SQS to alert-dispatcher mapping,
  since the harness invokes the watchdog and reads the alerts queue itself.

Tear down first when switching an existing stack between modes: a redeploy
does not remove the watchdog rule or the SQS mapping a normal deploy created.

## Verifying

```
python deploy/localstack/deploy.py smoke
```

The smoke check ensures:

1. All 6 Interlock Lambdas are present.
2. All 4 DynamoDB tables are present (the e2e schedules table is ignored).
3. `stream-router` has `AWS_ENDPOINT_URL` set in its environment.
4. The watchdog runs: it is invoked synchronously with an empty scheduled
   event and must return without a function error. This fails when LocalStack
   cannot start Lambda containers (no Docker socket) or a Lambda cannot reach
   LocalStack at `AWS_ENDPOINT_URL`. The first invoke starts a container, so it
   can take a minute.

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `INTERLOCK_ENV`         | `local`                   | Prefix for all resource names |
| `AWS_ENDPOINT_URL`      | `http://localhost:4566`   | LocalStack URL the deploy script talks to |
| `LAMBDA_AWS_ENDPOINT_URL` | `http://localstack:4566` | The URL Lambdas use from inside their container |
| `E2E_MOCK_URL`          | `http://mock:8080`        | Mock clock and job server, as Lambdas reach it (`--e2e` only) |
