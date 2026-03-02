package lambda

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
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

// OrchestratorInput is the input to the orchestrator Lambda from Step Functions.
type OrchestratorInput struct {
	Mode       string `json:"mode"` // evaluate, trigger, check-job, post-run
	PipelineID string `json:"pipelineId"`
	ScheduleID string `json:"scheduleId"`
	Date       string `json:"date"`
	RunID      string `json:"runId,omitempty"`
}

// OrchestratorOutput is the output of the orchestrator Lambda.
type OrchestratorOutput struct {
	Mode    string      `json:"mode"`
	Status  string      `json:"status,omitempty"` // "passed" or "not_ready"
	Results interface{} `json:"results,omitempty"`
	RunID   string      `json:"runId,omitempty"`
	JobType string      `json:"jobType,omitempty"`
	Event   string      `json:"event,omitempty"` // success, fail, timeout
	Error   string      `json:"error,omitempty"`
}

// SLAMonitorInput is the input to the SLA monitor Lambda.
type SLAMonitorInput struct {
	Mode             string `json:"mode"` // calculate, fire-alert
	PipelineID       string `json:"pipelineId"`
	ScheduleID       string `json:"scheduleId"`
	Date             string `json:"date"`
	AlertType        string `json:"alertType,omitempty"` // SLA_WARNING, SLA_BREACH
	Deadline         string `json:"deadline,omitempty"`
	ExpectedDuration string `json:"expectedDuration,omitempty"`
	Timezone         string `json:"timezone,omitempty"`
	Critical         bool   `json:"critical,omitempty"`
}

// SLAMonitorOutput is the output of the SLA monitor Lambda.
type SLAMonitorOutput struct {
	WarningAt string `json:"warningAt,omitempty"`
	BreachAt  string `json:"breachAt,omitempty"`
	AlertType string `json:"alertType,omitempty"`
	FiredAt   string `json:"firedAt,omitempty"`
}
