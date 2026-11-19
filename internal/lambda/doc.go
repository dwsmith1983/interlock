// Package lambda holds the shared types, interfaces and helpers used by the
// six handler sub-packages: stream, orchestrator, sla, watchdog, sink and
// alert. Each sub-package implements one deployed Lambda:
//
//   - stream-router (stream): routes DynamoDB stream records by sort key:
//     sensor updates, job log entries, rerun requests and config changes
//   - orchestrator (orchestrator): runs the Step Functions task modes:
//     evaluates validation rules, executes triggers, polls job status, and
//     manages the trigger lifecycle
//   - sla-monitor (sla): fires SLA warning and breach alerts and cancels SLA
//     schedules when a run ends
//   - watchdog (watchdog): runs periodic checks for stale triggers, missed
//     cron and inclusion schedules, missed sensor-triggered runs, SLA
//     scheduling, trigger cutoffs, missing post-run sensors, relative SLA
//     breaches, and rejected pipeline configs
//   - event-sink (sink): writes EventBridge events to the events table for
//     the audit trail
//   - alert-dispatcher (alert): sends alert events from SQS to Slack
//
// The handlers take their dependencies through [Deps], with the AWS clients
// behind small interfaces ([SFNAPI], [EventBridgeAPI], [SchedulerAPI], etc.)
// so tests can substitute fakes.
package lambda
