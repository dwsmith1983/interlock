package dlq

import (
	"crypto/rand"
	"encoding/json"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// ErrorType classifies the category of error that caused a record to enter the DLQ.
type ErrorType string

const (
	ErrorTypeValidation ErrorType = "VALIDATION"
	ErrorTypeEvaluation ErrorType = "EVALUATION"
	ErrorTypeTrigger    ErrorType = "TRIGGER"
	ErrorTypeProvider   ErrorType = "PROVIDER"
	ErrorTypeUnknown    ErrorType = "UNKNOWN"
)

// RecordState represents the lifecycle state of a DLQ record.
type RecordState string

const (
	StatePending  RecordState = "PENDING"
	StateAcked    RecordState = "ACKED"
	StateRejected RecordState = "REJECTED"
)

// Record holds a failed message and its metadata for dead-letter processing.
type Record struct {
	ID             ulid.ULID       `json:"id"`
	OriginalRecord json.RawMessage `json:"original_record,omitempty"`
	PipelineID     string          `json:"pipeline_id"`
	StageID        string          `json:"stage_id"`
	ErrorMessage   string          `json:"error_message"`
	ErrorType      ErrorType       `json:"error_type"`
	STAMPComponent string          `json:"stamp_component"`
	AttemptCount   int             `json:"attempt_count"`
	Timestamp      time.Time       `json:"timestamp"`
	CorrelationID  string          `json:"correlation_id"`
}

// entropyPool provides a thread-safe monotonic entropy source for ULID generation.
var entropyPool = sync.Pool{
	New: func() any {
		return ulid.Monotonic(rand.Reader, 0)
	},
}

// GenerateID returns a new ULID that is unique and lexicographically sortable by time.
func GenerateID() ulid.ULID {
	entropy := entropyPool.Get().(*ulid.MonotonicEntropy)
	id, err := ulid.New(ulid.Timestamp(time.Now()), entropy)
	if err != nil {
		// Fallback: create a fresh entropy source if the pooled one overflows.
		return ulid.MustNew(ulid.Timestamp(time.Now()), ulid.Monotonic(rand.Reader, 0))
	}
	entropyPool.Put(entropy)
	return id
}
