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

// TestLateArrivalPutList verifies PutLateArrival and ListLateArrivals.
func TestLateArrivalPutList(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	// Initially empty
	arrivals, err := prov.ListLateArrivals(ctx, "pipeline-la", "2026-02-23", "daily")
	require.NoError(t, err)
	assert.Empty(t, arrivals)

	// Put two entries
	now := time.Now()
	la1 := types.LateArrival{
		PipelineID:  "pipeline-la",
		Date:        "2026-02-23",
		ScheduleID:  "daily",
		DetectedAt:  now,
		RecordDelta: 100,
		TraitType:   "freshness",
	}
	la2 := types.LateArrival{
		PipelineID:  "pipeline-la",
		Date:        "2026-02-23",
		ScheduleID:  "daily",
		DetectedAt:  now.Add(time.Second),
		RecordDelta: 50,
		TraitType:   "record-count",
	}

	require.NoError(t, prov.PutLateArrival(ctx, la1))
	require.NoError(t, prov.PutLateArrival(ctx, la2))

	// List returns both
	arrivals, err = prov.ListLateArrivals(ctx, "pipeline-la", "2026-02-23", "daily")
	require.NoError(t, err)
	assert.Len(t, arrivals, 2)

	// Different schedule returns empty
	arrivals, err = prov.ListLateArrivals(ctx, "pipeline-la", "2026-02-23", "h14")
	require.NoError(t, err)
	assert.Empty(t, arrivals)
}
