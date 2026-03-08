// Package lambda implements the core business logic for the six AWS Lambda
// handlers that power the Interlock pipeline safety framework:
//
//   - stream-router: processes DynamoDB stream events, evaluates trigger
//     conditions, manages reruns, and detects post-run data drift
//   - orchestrator: evaluates validation rules, executes triggers, polls
//     job status, and manages trigger lifecycle
//   - sla-monitor: calculates SLA deadlines, schedules/cancels EventBridge
//     Scheduler entries, and fires warning/breach alerts
//   - watchdog: detects silently missed pipeline schedules by scanning for
//     pipelines that should have run but have no trigger record
//   - event-sink: persists EventBridge events to DynamoDB for audit trail
//   - alert-dispatcher: routes SQS alert messages to Slack
//
// All handlers share a common [Deps] struct for dependency injection,
// making the package fully testable with mock implementations of AWS SDK
// interfaces ([SFNAPI], [EventBridgeAPI], [SchedulerAPI], etc.).
package lambda
