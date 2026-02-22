package lifecycle

import (
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestValidTransitions(t *testing.T) {
	tests := []struct {
		from  types.RunStatus
		to    types.RunStatus
		valid bool
	}{
		{types.RunPending, types.RunTriggering, true},
		{types.RunPending, types.RunCancelled, true},
		{types.RunPending, types.RunCompleted, false},
		{types.RunTriggering, types.RunRunning, true},
		{types.RunTriggering, types.RunFailed, true},
		{types.RunTriggering, types.RunCompleted, false},
		{types.RunRunning, types.RunCompleted, true},
		{types.RunRunning, types.RunCompletedMonitoring, true},
		{types.RunRunning, types.RunFailed, true},
		{types.RunRunning, types.RunPending, false},
		{types.RunCompleted, types.RunFailed, false},
		{types.RunCompleted, types.RunRunning, false},
		{types.RunFailed, types.RunPending, false},
		{types.RunCompletedMonitoring, types.RunCompleted, true},
		{types.RunCompletedMonitoring, types.RunFailed, true},
		{types.RunCancelled, types.RunPending, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.from)+"->"+string(tt.to), func(t *testing.T) {
			assert.Equal(t, tt.valid, CanTransition(tt.from, tt.to))
			err := Transition(tt.from, tt.to)
			if tt.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestIsTerminal(t *testing.T) {
	assert.True(t, IsTerminal(types.RunCompleted))
	assert.True(t, IsTerminal(types.RunFailed))
	assert.True(t, IsTerminal(types.RunCancelled))
	assert.False(t, IsTerminal(types.RunPending))
	assert.False(t, IsTerminal(types.RunRunning))
	assert.False(t, IsTerminal(types.RunTriggering))
	assert.False(t, IsTerminal(types.RunCompletedMonitoring))
}
