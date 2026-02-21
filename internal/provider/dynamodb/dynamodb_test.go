//go:build integration

package dynamodb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/interlock-systems/interlock/pkg/types"
)

func setupTestProvider(t *testing.T) *DynamoDBProvider {
	t.Helper()
	ctx := context.Background()
	tableName := fmt.Sprintf("interlock-test-%d", time.Now().UnixNano())
	cfg := &types.DynamoDBConfig{
		TableName:   tableName,
		Region:      "us-east-1",
		Endpoint:    "http://localhost:8000",
		CreateTable: true,
	}
	prov, err := New(cfg)
	if err != nil {
		t.Skipf("DynamoDB Local not available: %v", err)
	}
	if err := prov.Start(ctx); err != nil {
		t.Skipf("DynamoDB Local not available: %v", err)
	}
	t.Cleanup(func() {
		_, _ = prov.client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: &tableName,
		})
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
	assert.Equal(t, 2, got.Tier)

	// List
	list, err := prov.ListPipelines(ctx)
	require.NoError(t, err)
	assert.Len(t, list, 1)
	assert.Equal(t, "test-pipeline", list[0].Name)

	// Register second
	err = prov.RegisterPipeline(ctx, types.PipelineConfig{Name: "second", Archetype: "streaming"})
	require.NoError(t, err)
	list, err = prov.ListPipelines(ctx)
	require.NoError(t, err)
	assert.Len(t, list, 2)

	// Delete
	err = prov.DeletePipeline(ctx, "test-pipeline")
	require.NoError(t, err)
	_, err = prov.GetPipeline(ctx, "test-pipeline")
	assert.Error(t, err)

	list, err = prov.ListPipelines(ctx)
	require.NoError(t, err)
	assert.Len(t, list, 1)
}

func TestTraitWithTTL(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	trait := types.TraitEvaluation{
		PipelineID:  "pipe1",
		TraitType:   "freshness",
		Status:      types.TraitPass,
		EvaluatedAt: time.Now(),
	}

	// Put with long TTL
	err := prov.PutTrait(ctx, "pipe1", trait, 1*time.Hour)
	require.NoError(t, err)

	// Get single
	got, err := prov.GetTrait(ctx, "pipe1", "freshness")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, types.TraitPass, got.Status)

	// Get all
	traits, err := prov.GetTraits(ctx, "pipe1")
	require.NoError(t, err)
	assert.Len(t, traits, 1)

	// Put second trait
	trait2 := types.TraitEvaluation{
		PipelineID:  "pipe1",
		TraitType:   "completeness",
		Status:      types.TraitFail,
		Reason:      "missing rows",
		EvaluatedAt: time.Now(),
	}
	err = prov.PutTrait(ctx, "pipe1", trait2, 1*time.Hour)
	require.NoError(t, err)

	traits, err = prov.GetTraits(ctx, "pipe1")
	require.NoError(t, err)
	assert.Len(t, traits, 2)

	// Verify expired TTL is filtered on read
	expiredTrait := types.TraitEvaluation{
		PipelineID:  "pipe1",
		TraitType:   "volume",
		Status:      types.TraitPass,
		EvaluatedAt: time.Now(),
	}
	// Put with TTL already in the past by using a raw PutItem
	err = prov.PutTrait(ctx, "pipe1", expiredTrait, 1*time.Second)
	require.NoError(t, err)
	// Wait for TTL to pass
	time.Sleep(2 * time.Second)
	got, err = prov.GetTrait(ctx, "pipe1", "volume")
	require.NoError(t, err)
	assert.Nil(t, got, "expired trait should be filtered on read")
}

func TestCompareAndSwap(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	run := types.RunState{
		RunID:      "run-1",
		PipelineID: "pipe1",
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	err := prov.PutRunState(ctx, run)
	require.NoError(t, err)

	// Correct version succeeds
	newState := run
	newState.Status = types.RunTriggering
	newState.Version = 2
	newState.UpdatedAt = time.Now()
	ok, err := prov.CompareAndSwapRunState(ctx, "run-1", 1, newState)
	require.NoError(t, err)
	assert.True(t, ok)

	// Stale version fails
	staleState := newState
	staleState.Status = types.RunRunning
	staleState.Version = 3
	ok, err = prov.CompareAndSwapRunState(ctx, "run-1", 1, staleState) // version 1 is stale
	require.NoError(t, err)
	assert.False(t, ok)

	// Verify current state
	got, err := prov.GetRunState(ctx, "run-1")
	require.NoError(t, err)
	assert.Equal(t, types.RunTriggering, got.Status)
	assert.Equal(t, 2, got.Version)
}

func TestCASRaceCondition(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	run := types.RunState{
		RunID:      "race-run",
		PipelineID: "pipe1",
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	err := prov.PutRunState(ctx, run)
	require.NoError(t, err)

	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			newState := run
			newState.Status = types.RunTriggering
			newState.Version = 2
			newState.UpdatedAt = time.Now()
			ok, err := prov.CompareAndSwapRunState(ctx, "race-run", 1, newState)
			if err == nil && ok {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), winners.Load(), "exactly 1 goroutine should win the CAS")
}

func TestRunStateList(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	base := time.Now()
	for i := 0; i < 5; i++ {
		run := types.RunState{
			RunID:      fmt.Sprintf("run-%d", i),
			PipelineID: "pipe1",
			Status:     types.RunCompleted,
			Version:    1,
			CreatedAt:  base.Add(time.Duration(i) * time.Second),
			UpdatedAt:  base.Add(time.Duration(i) * time.Second),
		}
		err := prov.PutRunState(ctx, run)
		require.NoError(t, err)
	}

	// List with limit
	runs, err := prov.ListRuns(ctx, "pipe1", 3)
	require.NoError(t, err)
	assert.Len(t, runs, 3)
	// Newest first (reverse SK order)
	assert.Equal(t, "run-4", runs[0].RunID)
	assert.Equal(t, "run-3", runs[1].RunID)
	assert.Equal(t, "run-2", runs[2].RunID)
}

func TestReadiness(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	// Get when not set
	got, err := prov.GetReadiness(ctx, "pipe1")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Put
	result := types.ReadinessResult{
		PipelineID:  "pipe1",
		Status:      types.Ready,
		EvaluatedAt: time.Now(),
		Traits: []types.TraitEvaluation{
			{TraitType: "freshness", Status: types.TraitPass},
		},
	}
	err = prov.PutReadiness(ctx, result)
	require.NoError(t, err)

	// Get
	got, err = prov.GetReadiness(ctx, "pipe1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, types.Ready, got.Status)
	assert.Len(t, got.Traits, 1)
}

func TestRunLogCRUD(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	entry := types.RunLogEntry{
		PipelineID:    "pipe1",
		Date:          "2026-02-21",
		ScheduleID:    "daily",
		Status:        types.RunCompleted,
		AttemptNumber: 1,
		RunID:         "run-1",
		StartedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	err := prov.PutRunLog(ctx, entry)
	require.NoError(t, err)

	// Get
	got, err := prov.GetRunLog(ctx, "pipe1", "2026-02-21", "daily")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "run-1", got.RunID)
	assert.Equal(t, "daily", got.ScheduleID)

	// Multi-schedule
	entry2 := types.RunLogEntry{
		PipelineID:    "pipe1",
		Date:          "2026-02-21",
		ScheduleID:    "morning",
		Status:        types.RunRunning,
		AttemptNumber: 1,
		RunID:         "run-2",
		StartedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	err = prov.PutRunLog(ctx, entry2)
	require.NoError(t, err)

	// List
	entries, err := prov.ListRunLogs(ctx, "pipe1", 10)
	require.NoError(t, err)
	assert.Len(t, entries, 2)

	// Not found
	got, err = prov.GetRunLog(ctx, "pipe1", "2026-01-01", "daily")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Default schedule
	entry3 := types.RunLogEntry{
		PipelineID:    "pipe1",
		Date:          "2026-02-22",
		Status:        types.RunPending,
		AttemptNumber: 1,
		RunID:         "run-3",
		StartedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	err = prov.PutRunLog(ctx, entry3)
	require.NoError(t, err)

	got, err = prov.GetRunLog(ctx, "pipe1", "2026-02-22", "daily")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "daily", got.ScheduleID)
}

func TestRerunCRUD(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	record := types.RerunRecord{
		RerunID:       "rerun-1",
		PipelineID:    "pipe1",
		OriginalDate:  "2026-02-20",
		OriginalRunID: "orig-run-1",
		Reason:        "late data",
		Status:        types.RunPending,
		RequestedAt:   time.Now(),
	}
	err := prov.PutRerun(ctx, record)
	require.NoError(t, err)

	// Get
	got, err := prov.GetRerun(ctx, "rerun-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "rerun-1", got.RerunID)
	assert.Equal(t, "pipe1", got.PipelineID)
	assert.Equal(t, "late data", got.Reason)

	// ListReruns
	record2 := types.RerunRecord{
		RerunID:      "rerun-2",
		PipelineID:   "pipe1",
		OriginalDate: "2026-02-19",
		Reason:       "correction",
		Status:       types.RunCompleted,
		RequestedAt:  time.Now().Add(time.Second),
	}
	err = prov.PutRerun(ctx, record2)
	require.NoError(t, err)

	reruns, err := prov.ListReruns(ctx, "pipe1", 10)
	require.NoError(t, err)
	assert.Len(t, reruns, 2)

	// ListAllReruns (includes reruns from all pipelines)
	record3 := types.RerunRecord{
		RerunID:      "rerun-3",
		PipelineID:   "pipe2",
		OriginalDate: "2026-02-18",
		Reason:       "test",
		Status:       types.RunPending,
		RequestedAt:  time.Now().Add(2 * time.Second),
	}
	err = prov.PutRerun(ctx, record3)
	require.NoError(t, err)

	allReruns, err := prov.ListAllReruns(ctx, 50)
	require.NoError(t, err)
	assert.Len(t, allReruns, 3)

	// Not found
	got, err = prov.GetRerun(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestLocking(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	// Acquire
	ok, err := prov.AcquireLock(ctx, "eval:pipe1:daily", 30*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)

	// Double acquire fails
	ok, err = prov.AcquireLock(ctx, "eval:pipe1:daily", 30*time.Second)
	require.NoError(t, err)
	assert.False(t, ok)

	// Different key succeeds
	ok, err = prov.AcquireLock(ctx, "eval:pipe2:daily", 30*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)

	// Release
	err = prov.ReleaseLock(ctx, "eval:pipe1:daily")
	require.NoError(t, err)

	// Re-acquire after release
	ok, err = prov.AcquireLock(ctx, "eval:pipe1:daily", 30*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestLockExpiry(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	// Acquire with very short TTL
	ok, err := prov.AcquireLock(ctx, "expiring-lock", 2*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)

	// Immediately re-acquire fails
	ok, err = prov.AcquireLock(ctx, "expiring-lock", 2*time.Second)
	require.NoError(t, err)
	assert.False(t, ok)

	// Wait for TTL to expire
	time.Sleep(3 * time.Second)

	// Re-acquire succeeds after expiry
	ok, err = prov.AcquireLock(ctx, "expiring-lock", 30*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestEventAppendAndList(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	now := time.Now()
	for i := 0; i < 5; i++ {
		ev := types.Event{
			Kind:       types.EventTraitEvaluated,
			PipelineID: "pipe1",
			TraitType:  fmt.Sprintf("trait-%d", i),
			Status:     "PASS",
			Timestamp:  now.Add(time.Duration(i) * time.Second),
		}
		err := prov.AppendEvent(ctx, ev)
		require.NoError(t, err)
		// Small delay to ensure unique event SKs
		time.Sleep(5 * time.Millisecond)
	}

	// ListEvents returns chronological order
	events, err := prov.ListEvents(ctx, "pipe1", 10)
	require.NoError(t, err)
	assert.Len(t, events, 5)
	// First event should be the oldest
	assert.Equal(t, "trait-0", events[0].TraitType)
	assert.Equal(t, "trait-4", events[4].TraitType)
}

func TestReadEventsSince(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	now := time.Now()
	for i := 0; i < 5; i++ {
		ev := types.Event{
			Kind:       types.EventRunStateChanged,
			PipelineID: "pipe1",
			RunID:      fmt.Sprintf("run-%d", i),
			Status:     string(types.RunCompleted),
			Timestamp:  now.Add(time.Duration(i) * time.Second),
		}
		err := prov.AppendEvent(ctx, ev)
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
	}

	// Read all from beginning
	records, err := prov.ReadEventsSince(ctx, "pipe1", "", 100)
	require.NoError(t, err)
	assert.Len(t, records, 5)

	// Read from cursor (after 2nd event)
	cursor := records[1].StreamID
	records, err = prov.ReadEventsSince(ctx, "pipe1", cursor, 100)
	require.NoError(t, err)
	assert.Len(t, records, 3)
	assert.Equal(t, "run-2", records[0].Event.RunID)

	// Read with "0-0" cursor
	records, err = prov.ReadEventsSince(ctx, "pipe1", "0-0", 100)
	require.NoError(t, err)
	assert.Len(t, records, 5)
}

func TestGetRunStateDirectLookup(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	run := types.RunState{
		RunID:      "direct-lookup-run",
		PipelineID: "pipe1",
		Status:     types.RunRunning,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	err := prov.PutRunState(ctx, run)
	require.NoError(t, err)

	// Direct lookup by runID only (no pipeline context needed)
	got, err := prov.GetRunState(ctx, "direct-lookup-run")
	require.NoError(t, err)
	assert.Equal(t, "direct-lookup-run", got.RunID)
	assert.Equal(t, "pipe1", got.PipelineID)
	assert.Equal(t, types.RunRunning, got.Status)

	// Not found
	_, err = prov.GetRunState(ctx, "nonexistent")
	assert.Error(t, err)
}
