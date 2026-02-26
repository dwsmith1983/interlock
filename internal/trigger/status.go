package trigger

import "github.com/dwsmith1983/interlock/pkg/types"

// RunCheckState represents the normalized outcome of a run status check.
type RunCheckState string

const (
	RunCheckRunning   RunCheckState = "running"
	RunCheckSucceeded RunCheckState = "succeeded"
	RunCheckFailed    RunCheckState = "failed"
)

// StatusResult is the normalized result from checking a trigger's run status.
type StatusResult struct {
	State           RunCheckState
	Message         string                // original provider state for logging
	FailureCategory types.FailureCategory // classification for retry decisions
}
