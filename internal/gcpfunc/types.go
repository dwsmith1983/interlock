package gcpfunc

import (
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// OrchestratorRequest is the input to the orchestrator Cloud Function.
type OrchestratorRequest struct {
	Action     string                 `json:"action"`
	PipelineID string                 `json:"pipelineID"`
	ScheduleID string                 `json:"scheduleID"`
	Date       string                 `json:"date,omitempty"`
	Payload    map[string]interface{} `json:"payload,omitempty"`
}

// OrchestratorResponse is the output of the orchestrator Cloud Function.
type OrchestratorResponse struct {
	Action  string                 `json:"action"`
	Result  string                 `json:"result"` // "proceed", "skip", "error"
	Payload map[string]interface{} `json:"payload,omitempty"`
}

// EvaluatorRequest is the input to the evaluator Cloud Function (one per trait).
type EvaluatorRequest struct {
	PipelineID string                 `json:"pipelineID"`
	TraitType  string                 `json:"traitType"`
	Evaluator  string                 `json:"evaluator"`
	Config     map[string]interface{} `json:"config,omitempty"`
	Timeout    int                    `json:"timeout,omitempty"`
	TTL        int                    `json:"ttl,omitempty"`
	Required   bool                   `json:"required"`
}

// EvaluatorResponse is the output of the evaluator Cloud Function.
type EvaluatorResponse struct {
	TraitType string            `json:"traitType"`
	Status    types.TraitStatus `json:"status"`
	Value     interface{}       `json:"value,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	Required  bool              `json:"required"`
}

// TriggerRequest is the input to the trigger Cloud Function.
type TriggerRequest struct {
	PipelineID string               `json:"pipelineID"`
	ScheduleID string               `json:"scheduleID"`
	Trigger    *types.TriggerConfig `json:"trigger"`
	RunID      string               `json:"runID"`
}

// TriggerResponse is the output of the trigger Cloud Function.
type TriggerResponse struct {
	RunID    string                 `json:"runID"`
	Status   string                 `json:"status"` // "running", "completed", "failed"
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	Error    string                 `json:"error,omitempty"`
}

// RunCheckRequest is the input to the run-checker Cloud Function.
type RunCheckRequest struct {
	PipelineID  string                 `json:"pipelineID"`
	RunID       string                 `json:"runID"`
	TriggerType types.TriggerType      `json:"triggerType"`
	Metadata    map[string]interface{} `json:"metadata"`
	Headers     map[string]string      `json:"headers,omitempty"`
}

// RunCheckResponse is the output of the run-checker Cloud Function.
type RunCheckResponse struct {
	State   trigger.RunCheckState `json:"state"`
	Message string                `json:"message"`
}

// StreamEvent represents a Firestore document change event from Eventarc.
type StreamEvent struct {
	OldValue *FirestoreValue `json:"oldValue,omitempty"`
	Value    *FirestoreValue `json:"value,omitempty"`
	// UpdateMask lists field paths that changed.
	UpdateMask struct {
		FieldPaths []string `json:"fieldPaths"`
	} `json:"updateMask"`
}

// FirestoreValue is a simplified representation of a Firestore document in an event.
type FirestoreValue struct {
	// Name is the full resource path: projects/{p}/databases/{d}/documents/{collection}/{docID}
	Name   string                 `json:"name"`
	Fields map[string]interface{} `json:"fields"`
}
