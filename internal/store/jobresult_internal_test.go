package store

import (
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestIsTerminalJobEvent pins the terminal set from both sides: every job
// event constant, true only for success, fail and timeout.
func TestIsTerminalJobEvent(t *testing.T) {
	tests := map[string]bool{
		types.JobEventSuccess:               true,
		types.JobEventFail:                  true,
		types.JobEventTimeout:               true,
		types.JobEventInfraTriggerFailure:   false,
		types.JobEventInfraTriggerExhausted: false,
		types.JobEventLateDataArrival:       false,
		types.JobEventRerunAccepted:         false,
		types.JobEventRerunRejected:         false,
		types.JobEventValidationExhausted:   false,
		"":                                  false,
	}
	for event, want := range tests {
		if got := isTerminalJobEvent(event); got != want {
			t.Errorf("isTerminalJobEvent(%q) = %v, want %v", event, got, want)
		}
	}
}
