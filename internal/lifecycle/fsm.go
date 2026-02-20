// Package lifecycle implements the pipeline run state machine.
package lifecycle

import (
	"fmt"

	"github.com/interlock-systems/interlock/pkg/types"
)

// Transition table: from -> allowed tos
var validTransitions = map[types.RunStatus][]types.RunStatus{
	types.RunPending:             {types.RunTriggering, types.RunCancelled},
	types.RunTriggering:          {types.RunRunning, types.RunFailed, types.RunCancelled},
	types.RunRunning:             {types.RunCompleted, types.RunCompletedMonitoring, types.RunFailed, types.RunCancelled},
	types.RunCompleted:           {},
	types.RunCompletedMonitoring: {types.RunCompleted, types.RunFailed},
	types.RunFailed:              {},
	types.RunCancelled:           {},
}

// CanTransition checks if transitioning from one run status to another is valid.
func CanTransition(from, to types.RunStatus) bool {
	allowed, ok := validTransitions[from]
	if !ok {
		return false
	}
	for _, s := range allowed {
		if s == to {
			return true
		}
	}
	return false
}

// Transition validates and returns the new status, or an error if the transition is invalid.
func Transition(from, to types.RunStatus) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("invalid transition from %s to %s", from, to)
	}
	return nil
}

// IsTerminal returns true if the status is a terminal (final) state.
func IsTerminal(status types.RunStatus) bool {
	return status == types.RunCompleted || status == types.RunFailed || status == types.RunCancelled
}
