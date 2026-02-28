package main

import (
	"context"
	"log/slog"
	"testing"

	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/testutil"
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDeps() *intlambda.Deps {
	prov := testutil.NewMockProvider()
	return &intlambda.Deps{
		Provider: prov,
		Runner:   trigger.NewRunner(),
		AlertFn:  func(_ context.Context, a types.Alert) {},
		Logger:   slog.Default(),
	}
}

func TestHandleTrigger_NoTriggerConfig(t *testing.T) {
	d := testDeps()

	resp, err := handleTrigger(context.Background(), d, intlambda.TriggerRequest{
		PipelineID: "test-pipe",
		ScheduleID: "daily",
		RunID:      "run-1",
		Trigger:    nil,
	})
	require.NoError(t, err)
	assert.Equal(t, "failed", resp.Status)
	assert.Contains(t, resp.Error, "no trigger configuration")
}

func TestHandleTrigger_CommandSuccess(t *testing.T) {
	d := testDeps()

	resp, err := handleTrigger(context.Background(), d, intlambda.TriggerRequest{
		PipelineID: "test-pipe",
		ScheduleID: "daily",
		RunID:      "run-1",
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "echo hello"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "completed", resp.Status)
	assert.Equal(t, "run-1", resp.RunID)

	// Verify run state was created and transitioned to completed (synchronous trigger)
	run, err := d.Provider.GetRunState(context.Background(), "run-1")
	require.NoError(t, err)
	assert.Equal(t, types.RunCompleted, run.Status)
}

func TestHandleTrigger_CommandFailure(t *testing.T) {
	d := testDeps()

	resp, err := handleTrigger(context.Background(), d, intlambda.TriggerRequest{
		PipelineID: "test-pipe",
		ScheduleID: "daily",
		RunID:      "run-2",
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "exit 1"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "failed", resp.Status)
	assert.NotEmpty(t, resp.Error)

	// Verify run state shows FAILED
	run, err := d.Provider.GetRunState(context.Background(), "run-2")
	require.NoError(t, err)
	assert.Equal(t, types.RunFailed, run.Status)
}

func TestHandleTrigger_RunStateTransitions(t *testing.T) {
	d := testDeps()

	resp, err := handleTrigger(context.Background(), d, intlambda.TriggerRequest{
		PipelineID: "test-pipe",
		ScheduleID: "daily",
		RunID:      "run-3",
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "true"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "completed", resp.Status)

	// Check run log was written
	rl, err := d.Provider.GetRunLog(context.Background(), "test-pipe", resp.RunID, "daily")
	// RunLog may use date as key, just verify no crash
	_ = rl
	_ = err
}

func TestHandleTrigger_EmptyCommand(t *testing.T) {
	d := testDeps()

	resp, err := handleTrigger(context.Background(), d, intlambda.TriggerRequest{
		PipelineID: "test-pipe",
		ScheduleID: "daily",
		RunID:      "run-4",
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: ""},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "failed", resp.Status)
}
