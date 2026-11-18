# End-to-end tests

The suite deploys every Lambda, the state machine and the tables to LocalStack
and drives them through real DynamoDB streams, Step Functions and EventBridge.
A mock server (`mock/`) serves the simulated clock every Lambda reads and the
HTTP job the pipelines launch.

## Running

```sh
make e2e
```

This builds the images, starts LocalStack, the mock and the harness with
`docker-compose.yml`, builds the Lambdas with `EXTRA_TAGS=e2e`, deploys them
with `deploy.py deploy --e2e`, runs `go test -tags e2e ./e2e/...`, streams the
compose services' logs as it goes and tears the stack down. It needs Docker and
nothing else. The Lambdas run in containers LocalStack starts, outside compose,
so when a test fails the harness prints the last 200 lines of each Lambda's
CloudWatch log group.

From the builder container, pass the Docker socket so compose can start
sibling containers:

```sh
~/code/interlock-docker/ibuild.sh -C ~/code/interlock ~/code/interlock --docker-socket -- 'make e2e'
```

`make e2e` tears down on Ctrl-C too. If a run was killed harder than that,
remove the Lambda containers LocalStack started (they are not compose members),
then the stack:

```sh
docker rm -f $(docker ps -aq --filter network=interlock-e2e)
docker compose -f e2e/docker-compose.yml down -v --remove-orphans
```

CI runs the suite in the `E2E (LocalStack)` job of `ci.yml` when code changes,
and every night in `e2e-nightly.yml`. A failed nightly run emails only whoever
last changed that workflow, and GitHub pauses scheduled workflows after 60 days
without repository activity.

## Harness rules

- One stack and one clock for the whole run. The clock only moves forward.
- Each scenario uses its own pipeline ID and its own simulated day
  (`startDay` in `scenarios_test.go` moves to the next one).
- Scenarios run serially, never with `t.Parallel`, and must pass in any order
  (`-shuffle=on`): read only your own pipeline's events, alerts and schedules.
- Assert the order of events only when one causes the other. Otherwise
  assert the set and the exact counts: events stamped in the same simulated
  millisecond sort by type.
- Write a pipeline's config before the first invocation that reads it.
- The watchdog scans every pipeline, so a watchdog call also acts on the
  other scenarios' pipelines. `fireDueSchedules(t, pipeline)` fires only the
  given pipeline's schedules.

## Adding a scenario

Add a top-level test to `scenarios_test.go`:

1. `startDay(t)` for a fresh day, and a pipeline ID no other test uses.
2. `putConfig` (start from `scenarioConfig`), queue mock outcomes with
   `POST /jobs/<id>/outcome` if the launch should be rejected, then write the
   sensor.
3. Report the job's result. A 2xx from the mock only means the launch was
   accepted; the run stays RUNNING until its JOB# row appears. `waitLaunch`
   returns the run ID a launch carried (`X-Interlock-Run-Id`) and `reportJob`
   writes the row the job would write (`success`, `fail` or `timeout`).
4. Wait for the effects with `waitCompleted`, `waitEvents` and `waitAlerts`,
   then check `assertLaunches` and `assertTriggerStatus`.

Use `advanceTo` to move the clock, `invokeWatchdog` for the watchdog's
EventBridge rule and `fireDueSchedules` for EventBridge Scheduler. Read the
code path first and assert the exact values it produces.

## What it cannot catch

- IAM. LocalStack Community does not enforce it: the missing watchdog
  `DeleteItem` permission fixed in #92 would have passed.
- The real EventBridge Scheduler. A DynamoDB table stands in for it and only
  records the schedules.
- Glue, EMR and Databricks request shapes. Only the HTTP, `lambda` and
  `step-function` triggers run; the `lambda` job is interlock's own
  orchestrator called with a mode it rejects, so the function error path runs
  but a successful function does not, and the `step-function` job is a
  one-state Pass machine the scenario creates.
- Terraform. `deploy.py` deploys the stack.
- Terraform's `filter_criteria` drifting from deploy.py's copy. The e2e deploy
  sets the stream mapping filters from that copy, so the filters themselves run.
- DynamoDB TTL. It is off, and would run on the real clock rather than the
  simulated one.
- Part of `job.maxRetries`. `TestScenarioRerunIgnoresPreviousFailure` covers
  one CompleteTrigger retry (a reported `fail`, `maxRetries: 1`, a `-r1`
  retry that then succeeds) and `TestScenarioRetriesExhausted` the
  `RETRY_EXHAUSTED` path when both attempts fail.
  `TestScenarioSLAWarningDuringRetry` fires the SLA warning while the
  `-r1` retry polls (trigger row `RUNNING`, newest job row the first
  attempt's `fail`) and expects `SLA_WARNING`, then `SLA_MET` once the
  retry succeeds. `TestScenarioLambdaFunctionError` covers a `PERMANENT`
  failure: a `lambda` function error writes a `PERMANENT` `fail` row, the
  Trigger state does not retry it, and `maxCodeRetries: 0` (with
  `maxRetries: 1`) ends the run with `RETRY_EXHAUSTED`. Not run:
  - a `PERMANENT` failure that is retried under `maxCodeRetries`;
  - a retry after the job poll window closes (the `timeout` row): the window
    is at least 60 s and the e2e job check interval is 5 s, so one exhausted
    attempt and its retry take over two minutes;
  - a late `fail` row, written after its execution ended (reported as
    `JOB_FAILED`, starts nothing).

  The retry scenario covers the Step Functions Trigger retry.
- Slack delivery. The dispatcher's queue mapping is off in e2e.
