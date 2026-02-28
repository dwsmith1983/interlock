// Package lambda provides shared types and initialization for Lambda handlers.
package lambda

import (
	"context"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// ObservabilityEvent is published to the observability SNS topic for all
// eligible DynamoDB stream records. Consumers (e.g., event-exporter) persist
// these to S3/Delta for queryable historical analysis.
type ObservabilityEvent struct {
	EventID    string                 `json:"eventId"`
	RecordType string                 `json:"recordType"`
	EventType  string                 `json:"eventType"`
	PipelineID string                 `json:"pipelineId"`
	ScheduleID string                 `json:"scheduleId,omitempty"`
	RunID      string                 `json:"runId,omitempty"`
	Date       string                 `json:"date,omitempty"`
	Status     string                 `json:"status,omitempty"`
	Message    string                 `json:"message,omitempty"`
	Detail     map[string]interface{} `json:"detail,omitempty"`
	Timestamp  time.Time              `json:"timestamp"`
}

// SNSAPI is the subset of the SNS client used for publishing lifecycle events.
type SNSAPI interface {
	Publish(ctx context.Context, input *sns.PublishInput, opts ...func(*sns.Options)) (*sns.PublishOutput, error)
}

// LifecycleEvent is published to SNS when a pipeline run reaches a terminal state.
type LifecycleEvent struct {
	EventType  types.EventKind `json:"eventType"`
	PipelineID string          `json:"pipelineId"`
	ScheduleID string          `json:"scheduleId,omitempty"`
	Date       string          `json:"date,omitempty"`
	RunID      string          `json:"runId,omitempty"`
	Status     string          `json:"status"`
	Timestamp  time.Time       `json:"timestamp"`
}

// StreamEvent is the input to the stream-router Lambda.
type StreamEvent = events.DynamoDBEvent

// OrchestratorRequest is the input to the orchestrator Lambda.
//
// JSON tags use "pipelineID"/"scheduleID" (capital D) to match the Step
// Functions ASL, which injects these fields. API/storage types in pkg/types
// use standard camelCase "pipelineId"/"scheduleId".
type OrchestratorRequest struct {
	Action     string                 `json:"action"`
	PipelineID string                 `json:"pipelineID"`
	ScheduleID string                 `json:"scheduleID"`
	Date       string                 `json:"date,omitempty"`
	Payload    map[string]interface{} `json:"payload,omitempty"`
}

// OrchestratorResponse is the output of the orchestrator Lambda.
type OrchestratorResponse struct {
	Action  string                 `json:"action"`
	Result  string                 `json:"result"` // "proceed", "skip", "error"
	Payload map[string]interface{} `json:"payload,omitempty"`
}

// EvaluatorRequest is the input to the evaluator Lambda (one per trait).
type EvaluatorRequest struct {
	PipelineID string                 `json:"pipelineID"`
	TraitType  string                 `json:"traitType"`
	Evaluator  string                 `json:"evaluator"`
	Config     map[string]interface{} `json:"config,omitempty"`
	Timeout    int                    `json:"timeout,omitempty"`
	TTL        int                    `json:"ttl,omitempty"`
	Required   bool                   `json:"required"`
}

// EvaluatorResponse is the output of the evaluator Lambda.
type EvaluatorResponse struct {
	TraitType       string                `json:"traitType"`
	Status          types.TraitStatus     `json:"status"`
	Value           interface{}           `json:"value,omitempty"`
	Reason          string                `json:"reason,omitempty"`
	Required        bool                  `json:"required"`
	FailureCategory types.FailureCategory `json:"failureCategory,omitempty"`
}

// TriggerRequest is the input to the trigger Lambda.
type TriggerRequest struct {
	PipelineID string               `json:"pipelineID"`
	ScheduleID string               `json:"scheduleID"`
	Trigger    *types.TriggerConfig `json:"trigger"`
	RunID      string               `json:"runID"`
}

// TriggerResponse is the output of the trigger Lambda.
type TriggerResponse struct {
	RunID    string                 `json:"runID"`
	Status   string                 `json:"status"` // "running", "completed", "failed"
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	Error    string                 `json:"error,omitempty"`
}

// RunCheckRequest is the input to the run-checker Lambda.
type RunCheckRequest struct {
	PipelineID  string                 `json:"pipelineID"`
	RunID       string                 `json:"runID"`
	TriggerType types.TriggerType      `json:"triggerType"`
	Metadata    map[string]interface{} `json:"metadata"`
	Headers     map[string]string      `json:"headers,omitempty"`
}

// RunCheckResponse is the output of the run-checker Lambda.
type RunCheckResponse struct {
	State           trigger.RunCheckState `json:"state"`
	Message         string                `json:"message"`
	FailureCategory types.FailureCategory `json:"failureCategory,omitempty"`
}
