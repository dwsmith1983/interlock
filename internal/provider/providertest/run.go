package providertest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/interlock-systems/interlock/internal/provider"
	"github.com/interlock-systems/interlock/pkg/types"
)

// TestRunStatePutGet verifies put, get, and not-found behavior.
func TestRunStatePutGet(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	run := types.RunState{
		RunID:      "ct-run-pg",
		PipelineID: "ct-pipe",
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	err := prov.PutRunState(ctx, run)
	require.NoError(t, err)

	got, err := prov.GetRunState(ctx, "ct-run-pg")
	require.NoError(t, err)
	assert.Equal(t, "ct-run-pg", got.RunID)
	assert.Equal(t, types.RunPending, got.Status)

	// Not found
	_, err = prov.GetRunState(ctx, "ct-nonexistent-run")
	assert.Error(t, err)
}

// TestRunStateList verifies listing runs with limit and newest-first ordering.
func TestRunStateList(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	base := time.Now()
	for i := 0; i < 5; i++ {
		run := types.RunState{
			RunID:      fmt.Sprintf("ct-list-%d", i),
			PipelineID: "ct-list-pipe",
			Status:     types.RunCompleted,
			Version:    1,
			CreatedAt:  base.Add(time.Duration(i) * time.Second),
			UpdatedAt:  base.Add(time.Duration(i) * time.Second),
		}
		require.NoError(t, prov.PutRunState(ctx, run))
	}

	runs, err := prov.ListRuns(ctx, "ct-list-pipe", 3)
	require.NoError(t, err)
	assert.Len(t, runs, 3)
	// Newest first
	assert.Equal(t, "ct-list-4", runs[0].RunID)
	assert.Equal(t, "ct-list-3", runs[1].RunID)
	assert.Equal(t, "ct-list-2", runs[2].RunID)
}

// TestCompareAndSwap verifies CAS with correct and stale versions.
func TestCompareAndSwap(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	run := types.RunState{
		RunID:      "ct-cas",
		PipelineID: "ct-pipe",
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	err := prov.PutRunState(ctx, run)
	require.NoError(t, err)

	// Correct version succeeds
	run2 := run
	run2.Status = types.RunTriggering
	run2.Version = 2
	run2.UpdatedAt = time.Now()
	ok, err := prov.CompareAndSwapRunState(ctx, "ct-cas", 1, run2)
	require.NoError(t, err)
	assert.True(t, ok)

	// Stale version fails
	run3 := run2
	run3.Status = types.RunRunning
	run3.Version = 3
	ok, err = prov.CompareAndSwapRunState(ctx, "ct-cas", 1, run3) // version 1 is stale
	require.NoError(t, err)
	assert.False(t, ok)

	// Verify state is still at version 2
	got, err := prov.GetRunState(ctx, "ct-cas")
	require.NoError(t, err)
	assert.Equal(t, types.RunTriggering, got.Status)
	assert.Equal(t, 2, got.Version)
}

// TestCASRaceCondition verifies exactly 1 goroutine wins a concurrent CAS.
func TestCASRaceCondition(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	run := types.RunState{
		RunID:      "ct-race",
		PipelineID: "ct-pipe",
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	require.NoError(t, prov.PutRunState(ctx, run))

	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			newRun := types.RunState{
				RunID:      "ct-race",
				PipelineID: "ct-pipe",
				Status:     types.RunTriggering,
				Version:    2,
				UpdatedAt:  time.Now(),
				Metadata:   map[string]interface{}{"winner": id},
			}
			ok, err := prov.CompareAndSwapRunState(ctx, "ct-race", 1, newRun)
			if err == nil && ok {
				winners.Add(1)
			}
		}(i)
	}
	wg.Wait()
	assert.Equal(t, int32(1), winners.Load(), "exactly 1 goroutine should win the CAS")
}

// TestGetRunStateDirectLookup verifies lookup by runID only (no pipeline context needed).
func TestGetRunStateDirectLookup(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	run := types.RunState{
		RunID:      "ct-direct",
		PipelineID: "ct-pipe",
		Status:     types.RunRunning,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	err := prov.PutRunState(ctx, run)
	require.NoError(t, err)

	got, err := prov.GetRunState(ctx, "ct-direct")
	require.NoError(t, err)
	assert.Equal(t, "ct-direct", got.RunID)
	assert.Equal(t, "ct-pipe", got.PipelineID)
	assert.Equal(t, types.RunRunning, got.Status)

	// Not found
	_, err = prov.GetRunState(ctx, "ct-nonexistent-direct")
	assert.Error(t, err)
}
