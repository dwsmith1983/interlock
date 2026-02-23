// Package lambda provides shared types and initialization for Lambda handlers.
package lambda

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// StreamEvent is the input to the stream-router Lambda.
type StreamEvent = events.DynamoDBEvent

// OrchestratorRequest is the input to the orchestrator Lambda.
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
	TraitType string            `json:"traitType"`
	Status    types.TraitStatus `json:"status"`
	Value     interface{}       `json:"value,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	Required  bool              `json:"required"`
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
	State   trigger.RunCheckState `json:"state"`
	Message string                `json:"message"`
}
