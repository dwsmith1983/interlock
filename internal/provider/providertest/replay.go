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

// TestReplayCRUD verifies PutReplay, GetReplay, and ListReplays.
func TestReplayCRUD(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	// Initially nil
	r, err := prov.GetReplay(ctx, "pipeline-rp", "2026-02-23", "daily")
	require.NoError(t, err)
	assert.Nil(t, r)

	// Put a replay
	replay := types.ReplayRequest{
		PipelineID:  "pipeline-rp",
		Date:        "2026-02-23",
		ScheduleID:  "daily",
		Reason:      "data correction",
		RequestedBy: "user@example.com",
		Status:      "pending",
		CreatedAt:   time.Now(),
	}
	require.NoError(t, prov.PutReplay(ctx, replay))

	// Get returns it
	got, err := prov.GetReplay(ctx, "pipeline-rp", "2026-02-23", "daily")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "pipeline-rp", got.PipelineID)
	assert.Equal(t, "data correction", got.Reason)
	assert.Equal(t, "pending", got.Status)

	// ListReplays returns it
	list, err := prov.ListReplays(ctx, 10)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(list), 1)

	found := false
	for _, rp := range list {
		if rp.PipelineID == "pipeline-rp" && rp.Date == "2026-02-23" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected replay not found in ListReplays")
}
