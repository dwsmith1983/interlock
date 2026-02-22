package providertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestRunLogCRUD verifies put, get, multi-schedule, list, not-found, and default scheduleID.
func TestRunLogCRUD(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	entry := types.RunLogEntry{
		PipelineID:    "ct-runlog",
		Date:          "2026-02-21",
		ScheduleID:    "daily",
		Status:        types.RunCompleted,
		AttemptNumber: 1,
		RunID:         "ct-rl-run-1",
		StartedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	err := prov.PutRunLog(ctx, entry)
	require.NoError(t, err)

	// Get
	got, err := prov.GetRunLog(ctx, "ct-runlog", "2026-02-21", "daily")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "ct-rl-run-1", got.RunID)
	assert.Equal(t, "daily", got.ScheduleID)

	// Multi-schedule
	entry2 := types.RunLogEntry{
		PipelineID:    "ct-runlog",
		Date:          "2026-02-21",
		ScheduleID:    "morning",
		Status:        types.RunRunning,
		AttemptNumber: 1,
		RunID:         "ct-rl-run-2",
		StartedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	err = prov.PutRunLog(ctx, entry2)
	require.NoError(t, err)

	// List
	entries, err := prov.ListRunLogs(ctx, "ct-runlog", 10)
	require.NoError(t, err)
	assert.Len(t, entries, 2)

	// Not found
	got, err = prov.GetRunLog(ctx, "ct-runlog", "2026-01-01", "daily")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Default schedule — empty ScheduleID defaults to "daily"
	entry3 := types.RunLogEntry{
		PipelineID:    "ct-runlog",
		Date:          "2026-02-22",
		Status:        types.RunPending,
		AttemptNumber: 1,
		RunID:         "ct-rl-run-3",
		StartedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	err = prov.PutRunLog(ctx, entry3)
	require.NoError(t, err)

	got, err = prov.GetRunLog(ctx, "ct-runlog", "2026-02-22", "daily")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "daily", got.ScheduleID)
}
