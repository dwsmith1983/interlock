package lambda

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// StreamEvent is the DynamoDB stream event input.
type StreamEvent = events.DynamoDBEvent

// SFNAPI is the subset of the Step Functions client used by stream-router.
type SFNAPI interface {
	StartExecution(ctx context.Context, input *sfn.StartExecutionInput, opts ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error)
}

// EventBridgeAPI is the subset of the EventBridge client used for publishing events.
type EventBridgeAPI interface {
	PutEvents(ctx context.Context, input *eventbridge.PutEventsInput, opts ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error)
}

// TriggerExecutor abstracts trigger execution for testing.
type TriggerExecutor interface {
	Execute(ctx context.Context, cfg *types.TriggerConfig) (map[string]interface{}, error)
}

// StatusChecker abstracts job status polling for testing.
type StatusChecker interface {
	CheckStatus(ctx context.Context, triggerType types.TriggerType, metadata map[string]interface{}, headers map[string]string) (StatusResult, error)
}

// SchedulerAPI is the subset of the EventBridge Scheduler client used for SLA timers.
type SchedulerAPI interface {
	CreateSchedule(ctx context.Context, input *scheduler.CreateScheduleInput, opts ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error)
	DeleteSchedule(ctx context.Context, input *scheduler.DeleteScheduleInput, opts ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error)
}

// StatusResult is a normalized job status from the trigger runner.
type StatusResult struct {
	State   string // "succeeded", "failed", "running"
	Message string
}

// OrchestratorInput is the input to the orchestrator Lambda from Step Functions.
type OrchestratorInput struct {
	Mode       string                 `json:"mode"` // evaluate, trigger, check-job, post-run, validation-exhausted
	PipelineID string                 `json:"pipelineId"`
	ScheduleID string                 `json:"scheduleId"`
	Date       string                 `json:"date"`
	RunID      string                 `json:"runId,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`  // trigger metadata for status polling
	ErrorInfo  map[string]interface{} `json:"errorInfo,omitempty"` // error details from SFN Catch
}

// OrchestratorOutput is the output of the orchestrator Lambda.
type OrchestratorOutput struct {
	Mode     string                 `json:"mode"`
	Status   string                 `json:"status,omitempty"` // "passed" or "not_ready"
	Results  interface{}            `json:"results,omitempty"`
	RunID    string                 `json:"runId,omitempty"`
	JobType  string                 `json:"jobType,omitempty"`
	Event    string                 `json:"event,omitempty"` // success, fail, timeout
	Error    string                 `json:"error,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"` // trigger metadata passed through for status polling
}

// SLAMonitorInput is the input to the SLA monitor Lambda.
type SLAMonitorInput struct {
	Mode             string `json:"mode"` // calculate, fire-alert, schedule, cancel, reconcile
	PipelineID       string `json:"pipelineId"`
	ScheduleID       string `json:"scheduleId"`
	Date             string `json:"date"`
	AlertType        string `json:"alertType,omitempty"`        // SLA_WARNING, SLA_BREACH (fire-alert)
	Deadline         string `json:"deadline,omitempty"`         // "HH:MM" or ":MM"
	ExpectedDuration string `json:"expectedDuration,omitempty"` // e.g. "30m"
	Timezone         string `json:"timezone,omitempty"`
	Critical         bool   `json:"critical,omitempty"`
	WarningAt        string `json:"warningAt,omitempty"` // RFC3339, passed to cancel mode
	BreachAt         string `json:"breachAt,omitempty"`  // RFC3339, passed to cancel mode
}

// SLAMonitorOutput is the output of the SLA monitor Lambda.
type SLAMonitorOutput struct {
	WarningAt string `json:"warningAt,omitempty"`
	BreachAt  string `json:"breachAt,omitempty"`
	AlertType string `json:"alertType,omitempty"`
	FiredAt   string `json:"firedAt,omitempty"`
}

// EventBridgeInput represents the envelope of an EventBridge event.
type EventBridgeInput struct {
	Source     string          `json:"source"`
	DetailType string          `json:"detail-type"`
	Detail     json.RawMessage `json:"detail"`
}
