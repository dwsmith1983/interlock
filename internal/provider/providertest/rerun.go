package providertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/interlock-systems/interlock/internal/provider"
	"github.com/interlock-systems/interlock/pkg/types"
)

// TestRerunCRUD verifies put, get, list, cross-pipeline list-all, and not-found.
func TestRerunCRUD(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	record := types.RerunRecord{
		RerunID:       "ct-rerun-1",
		PipelineID:    "ct-rerun-pipe",
		OriginalDate:  "2026-02-20",
		OriginalRunID: "ct-orig-run-1",
		Reason:        "late data",
		Status:        types.RunPending,
		RequestedAt:   time.Now(),
	}
	err := prov.PutRerun(ctx, record)
	require.NoError(t, err)

	// Get
	got, err := prov.GetRerun(ctx, "ct-rerun-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "ct-rerun-1", got.RerunID)
	assert.Equal(t, "ct-rerun-pipe", got.PipelineID)
	assert.Equal(t, "late data", got.Reason)

	// Second rerun
	record2 := types.RerunRecord{
		RerunID:      "ct-rerun-2",
		PipelineID:   "ct-rerun-pipe",
		OriginalDate: "2026-02-19",
		Reason:       "correction",
		Status:       types.RunCompleted,
		RequestedAt:  time.Now().Add(time.Second),
	}
	err = prov.PutRerun(ctx, record2)
	require.NoError(t, err)

	// ListReruns
	reruns, err := prov.ListReruns(ctx, "ct-rerun-pipe", 10)
	require.NoError(t, err)
	assert.Len(t, reruns, 2)

	// Cross-pipeline ListAllReruns
	record3 := types.RerunRecord{
		RerunID:      "ct-rerun-3",
		PipelineID:   "ct-rerun-pipe2",
		OriginalDate: "2026-02-18",
		Reason:       "test",
		Status:       types.RunPending,
		RequestedAt:  time.Now().Add(2 * time.Second),
	}
	err = prov.PutRerun(ctx, record3)
	require.NoError(t, err)

	allReruns, err := prov.ListAllReruns(ctx, 50)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(allReruns), 3)

	// Not found
	got, err = prov.GetRerun(ctx, "ct-nonexistent-rerun")
	require.NoError(t, err)
	assert.Nil(t, got)
}
