//go:build integration

package redis

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/interlock-systems/interlock/pkg/types"
)

func setupTestProvider(t *testing.T) *RedisProvider {
	t.Helper()
	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	ctx := context.Background()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	prefix := fmt.Sprintf("interlock-test-%d:", time.Now().UnixNano())
	prov := NewFromClient(client, prefix)

	t.Cleanup(func() {
		// Clean up test keys
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, prefix+"*", 100).Result()
			if err != nil {
				break
			}
			if len(keys) > 0 {
				client.Del(ctx, keys...)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		client.Close()
	})

	return prov
}

func TestPipelineCRUD(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	pipeline := types.PipelineConfig{
		Name:      "test-pipeline",
		Archetype: "batch-ingestion",
		Tier:      2,
	}

	// Register
	err := prov.RegisterPipeline(ctx, pipeline)
	require.NoError(t, err)

	// Get
	got, err := prov.GetPipeline(ctx, "test-pipeline")
	require.NoError(t, err)
	assert.Equal(t, "test-pipeline", got.Name)
	assert.Equal(t, "batch-ingestion", got.Archetype)

	// List
	list, err := prov.ListPipelines(ctx)
	require.NoError(t, err)
	assert.Len(t, list, 1)

	// Delete
	err = prov.DeletePipeline(ctx, "test-pipeline")
	require.NoError(t, err)

	_, err = prov.GetPipeline(ctx, "test-pipeline")
	assert.Error(t, err)
}

func TestTraitWithTTL(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	trait := types.TraitEvaluation{
		PipelineID:  "ttl-test",
		TraitType:   "freshness",
		Status:      types.TraitPass,
		EvaluatedAt: time.Now(),
	}

	// Store with 2s TTL
	err := prov.PutTrait(ctx, "ttl-test", trait, 2*time.Second)
	require.NoError(t, err)

	// Should exist immediately
	got, err := prov.GetTrait(ctx, "ttl-test", "freshness")
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, types.TraitPass, got.Status)

	// Wait for TTL
	time.Sleep(3 * time.Second)

	// Should be gone
	got, err = prov.GetTrait(ctx, "ttl-test", "freshness")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestCompareAndSwap(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	run := types.RunState{
		RunID:      "cas-test",
		PipelineID: "test",
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	err := prov.PutRunState(ctx, run)
	require.NoError(t, err)

	// CAS with correct version succeeds
	run2 := run
	run2.Status = types.RunTriggering
	run2.Version = 2
	ok, err := prov.CompareAndSwapRunState(ctx, "cas-test", 1, run2)
	require.NoError(t, err)
	assert.True(t, ok)

	// CAS with stale version fails
	run3 := run2
	run3.Status = types.RunRunning
	run3.Version = 3
	ok, err = prov.CompareAndSwapRunState(ctx, "cas-test", 1, run3) // wrong version
	require.NoError(t, err)
	assert.False(t, ok)

	// Verify state is still at version 2
	got, err := prov.GetRunState(ctx, "cas-test")
	require.NoError(t, err)
	assert.Equal(t, types.RunTriggering, got.Status)
	assert.Equal(t, 2, got.Version)
}

func TestCASRaceCondition(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	run := types.RunState{
		RunID:      "race-test",
		PipelineID: "test",
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	require.NoError(t, prov.PutRunState(ctx, run))

	// 10 goroutines all try to CAS from version 1 to version 2
	var successCount atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			newRun := types.RunState{
				RunID:      "race-test",
				PipelineID: "test",
				Status:     types.RunTriggering,
				Version:    2,
				UpdatedAt:  time.Now(),
				Metadata:   map[string]interface{}{"winner": id},
			}
			ok, err := prov.CompareAndSwapRunState(ctx, "race-test", 1, newRun)
			if err == nil && ok {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	assert.Equal(t, int32(1), successCount.Load(), "exactly 1 goroutine should win the CAS")
}

func TestRunStateList(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		run := types.RunState{
			RunID:      fmt.Sprintf("list-test-%d", i),
			PipelineID: "list-pipeline",
			Status:     types.RunCompleted,
			Version:    1,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}
		require.NoError(t, prov.PutRunState(ctx, run))
	}

	runs, err := prov.ListRuns(ctx, "list-pipeline", 3)
	require.NoError(t, err)
	assert.Len(t, runs, 3)
}

func setupTestProviderWithTTL(t *testing.T, retentionTTL time.Duration) *RedisProvider {
	t.Helper()
	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	ctx := context.Background()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	prefix := fmt.Sprintf("interlock-test-%d:", time.Now().UnixNano())
	prov := NewFromClient(client, prefix)
	prov.retentionTTL = retentionTTL

	t.Cleanup(func() {
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, prefix+"*", 100).Result()
			if err != nil {
				break
			}
			if len(keys) > 0 {
				client.Del(ctx, keys...)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		client.Close()
	})

	return prov
}

func TestRunStateTTL(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	// Terminal run should get retentionTTL (7 days default)
	terminalRun := types.RunState{
		RunID:      "ttl-terminal",
		PipelineID: "ttl-test",
		Status:     types.RunCompleted,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	require.NoError(t, prov.PutRunState(ctx, terminalRun))

	ttl := prov.client.TTL(ctx, prov.runKey("ttl-terminal")).Val()
	assert.InDelta(t, defaultRetentionTTL.Seconds(), ttl.Seconds(), 10,
		"terminal run should have ~7 day TTL")

	// Active run should get retentionTTL + 24h
	activeRun := types.RunState{
		RunID:      "ttl-active",
		PipelineID: "ttl-test",
		Status:     types.RunRunning,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	require.NoError(t, prov.PutRunState(ctx, activeRun))

	ttl = prov.client.TTL(ctx, prov.runKey("ttl-active")).Val()
	expected := defaultRetentionTTL + 24*time.Hour
	assert.InDelta(t, expected.Seconds(), ttl.Seconds(), 10,
		"active run should have ~8 day TTL")
}

func TestCASRefreshesTTL(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	run := types.RunState{
		RunID:      "cas-ttl-test",
		PipelineID: "ttl-test",
		Status:     types.RunRunning,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	require.NoError(t, prov.PutRunState(ctx, run))

	// CAS to terminal state
	completed := types.RunState{
		RunID:      "cas-ttl-test",
		PipelineID: "ttl-test",
		Status:     types.RunCompleted,
		Version:    2,
		CreatedAt:  run.CreatedAt,
		UpdatedAt:  time.Now(),
	}
	ok, err := prov.CompareAndSwapRunState(ctx, "cas-ttl-test", 1, completed)
	require.NoError(t, err)
	assert.True(t, ok)

	ttl := prov.client.TTL(ctx, prov.runKey("cas-ttl-test")).Val()
	assert.InDelta(t, defaultRetentionTTL.Seconds(), ttl.Seconds(), 10,
		"CAS to terminal state should set ~7 day TTL")
}

func TestRunLogTTL(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	entry := types.RunLogEntry{
		PipelineID:    "ttl-test",
		Date:          "2025-01-15",
		Status:        types.RunCompleted,
		AttemptNumber: 1,
		RunID:         "log-ttl-run",
		StartedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	require.NoError(t, prov.PutRunLog(ctx, entry))

	ttl := prov.client.TTL(ctx, prov.runLogKey("ttl-test", "2025-01-15")).Val()
	assert.InDelta(t, defaultRetentionTTL.Seconds(), ttl.Seconds(), 10,
		"run log entry should have ~7 day TTL")
}

func TestRerunTTL(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	record := types.RerunRecord{
		RerunID:      "rerun-ttl-test",
		PipelineID:   "ttl-test",
		OriginalDate: "2025-01-15",
		Reason:       "test",
		Status:       types.RunCompleted,
		RequestedAt:  time.Now(),
	}
	require.NoError(t, prov.PutRerun(ctx, record))

	ttl := prov.client.TTL(ctx, prov.rerunKey("rerun-ttl-test")).Val()
	assert.InDelta(t, defaultRetentionTTL.Seconds(), ttl.Seconds(), 10,
		"completed rerun should have ~7 day TTL")
}

func TestReadiness(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	result := types.ReadinessResult{
		PipelineID:  "readiness-test",
		Status:      types.Ready,
		EvaluatedAt: time.Now(),
	}

	err := prov.PutReadiness(ctx, result)
	require.NoError(t, err)

	got, err := prov.GetReadiness(ctx, "readiness-test")
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, types.Ready, got.Status)
}

