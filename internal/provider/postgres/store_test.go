//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/interlock-systems/interlock/pkg/types"
)

func setupTestStore(t *testing.T) *Store {
	t.Helper()

	dsn := os.Getenv("INTERLOCK_TEST_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://interlock:interlock@localhost:5432/interlock?sslmode=disable"
	}

	ctx := context.Background()
	store, err := New(ctx, dsn)
	if err != nil {
		t.Skipf("Postgres not available: %v", err)
	}

	require.NoError(t, store.Migrate(ctx))

	t.Cleanup(func() {
		// Clean up test data
		store.pool.Exec(ctx, "DELETE FROM runs")
		store.pool.Exec(ctx, "DELETE FROM run_logs")
		store.pool.Exec(ctx, "DELETE FROM reruns")
		store.pool.Exec(ctx, "DELETE FROM events")
		store.pool.Exec(ctx, "DELETE FROM trait_evaluations")
		store.pool.Exec(ctx, "DELETE FROM archive_cursors")
		store.Close()
	})

	return store
}

func TestMigrate_CreatesTables(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	// Verify tables exist by querying them
	tables := []string{"runs", "run_logs", "reruns", "events", "trait_evaluations", "archive_cursors"}
	for _, table := range tables {
		var exists bool
		err := store.pool.QueryRow(ctx,
			"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)", table).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "table %s should exist", table)
	}
}

func TestUpsertRun_Idempotent(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond)
	run := types.RunState{
		RunID:      "test-run-1",
		PipelineID: "test-pipe",
		Status:     types.RunRunning,
		Version:    1,
		CreatedAt:  now,
		UpdatedAt:  now,
		Metadata:   map[string]interface{}{"key": "value1"},
	}

	require.NoError(t, store.UpsertRun(ctx, run))

	// Upsert again with updated status
	run.Status = types.RunCompleted
	run.Version = 2
	run.Metadata = map[string]interface{}{"key": "value2"}
	require.NoError(t, store.UpsertRun(ctx, run))

	// Verify only one row exists with updated values
	var status string
	var version int
	err := store.pool.QueryRow(ctx, "SELECT status, version FROM runs WHERE run_id = $1", "test-run-1").Scan(&status, &version)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", status)
	assert.Equal(t, 2, version)
}

func TestUpsertRunLog_Idempotent(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond)
	entry := types.RunLogEntry{
		PipelineID:    "test-pipe",
		Date:          "2025-01-15",
		Status:        types.RunFailed,
		AttemptNumber: 1,
		RunID:         "run-1",
		FailureMessage: "connection refused",
		StartedAt:     now,
		UpdatedAt:     now,
	}

	require.NoError(t, store.UpsertRunLog(ctx, entry))

	// Upsert with updated status
	entry.Status = types.RunCompleted
	entry.AttemptNumber = 2
	entry.FailureMessage = ""
	require.NoError(t, store.UpsertRunLog(ctx, entry))

	var status string
	var attempt int
	err := store.pool.QueryRow(ctx,
		"SELECT status, attempt_number FROM run_logs WHERE pipeline_id = $1 AND date = $2",
		"test-pipe", "2025-01-15").Scan(&status, &attempt)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", status)
	assert.Equal(t, 2, attempt)
}

func TestEventDedup(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond)
	records := []types.EventRecord{
		{
			StreamID: "1000-0",
			Event: types.Event{
				Kind: types.EventTraitEvaluated, PipelineID: "test-pipe",
				TraitType: "freshness", Status: "PASS", Timestamp: now,
			},
		},
	}

	require.NoError(t, store.InsertEvents(ctx, records))

	// Insert same stream ID again — should be a no-op
	require.NoError(t, store.InsertEvents(ctx, records))

	var count int
	err := store.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM events WHERE pipeline_id = $1 AND stream_id = $2",
		"test-pipe", "1000-0").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "duplicate event should be deduplicated")
}

func TestTraitEvaluationExtraction(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond)
	records := []types.EventRecord{
		{
			StreamID: "2000-0",
			Event: types.Event{
				Kind: types.EventTraitEvaluated, PipelineID: "test-pipe",
				TraitType: "freshness", Status: "PASS", Timestamp: now,
				Details: map[string]interface{}{
					"reason":          "data is fresh",
					"failureCategory": "",
					"maxAge":          3600,
				},
			},
		},
		{
			StreamID: "2001-0",
			Event: types.Event{
				Kind: types.EventRunStateChanged, PipelineID: "test-pipe",
				Status: "RUNNING", Timestamp: now,
			},
		},
	}

	require.NoError(t, store.InsertEvents(ctx, records))

	// Should have 1 trait evaluation (only from TRAIT_EVALUATED event)
	var count int
	err := store.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM trait_evaluations WHERE pipeline_id = $1", "test-pipe").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	evals, err := store.QueryTraitHistory(ctx, "test-pipe", now.Add(-time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, evals, 1)
	assert.Equal(t, "freshness", evals[0].TraitType)
	assert.Equal(t, "PASS", evals[0].Status)
}

func TestCursorRoundtrip(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	// Initially empty
	cursor, err := store.GetCursor(ctx, "test-pipe", "events")
	require.NoError(t, err)
	assert.Empty(t, cursor)

	// Set cursor
	require.NoError(t, store.SetCursor(ctx, "test-pipe", "events", "1500-0"))

	cursor, err = store.GetCursor(ctx, "test-pipe", "events")
	require.NoError(t, err)
	assert.Equal(t, "1500-0", cursor)

	// Update cursor
	require.NoError(t, store.SetCursor(ctx, "test-pipe", "events", "2000-0"))

	cursor, err = store.GetCursor(ctx, "test-pipe", "events")
	require.NoError(t, err)
	assert.Equal(t, "2000-0", cursor)
}

func TestQueryFailedPipelines(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond)

	require.NoError(t, store.UpsertRunLog(ctx, types.RunLogEntry{
		PipelineID: "pipe-a", Date: "2025-01-15", Status: types.RunFailed,
		AttemptNumber: 1, RunID: "r1", StartedAt: now, UpdatedAt: now,
	}))
	require.NoError(t, store.UpsertRunLog(ctx, types.RunLogEntry{
		PipelineID: "pipe-b", Date: "2025-01-15", Status: types.RunCompleted,
		AttemptNumber: 1, RunID: "r2", StartedAt: now, UpdatedAt: now,
	}))
	require.NoError(t, store.UpsertRunLog(ctx, types.RunLogEntry{
		PipelineID: "pipe-c", Date: "2025-01-15", Status: types.RunFailed,
		AttemptNumber: 1, RunID: "r3", StartedAt: now, UpdatedAt: now,
	}))

	failed, err := store.QueryFailedPipelines(ctx, "2025-01-15")
	require.NoError(t, err)
	assert.Equal(t, []string{"pipe-a", "pipe-c"}, failed)
}

func TestQueryRunHistory(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		now := time.Now().Add(time.Duration(i) * time.Hour).Truncate(time.Microsecond)
		require.NoError(t, store.UpsertRunLog(ctx, types.RunLogEntry{
			PipelineID: "history-pipe", Date: time.Now().AddDate(0, 0, -i).Format("2006-01-02"),
			Status: types.RunCompleted, AttemptNumber: 1, RunID: "r-" + time.Now().AddDate(0, 0, -i).Format("2006-01-02"),
			StartedAt: now, UpdatedAt: now,
		}))
	}

	entries, err := store.QueryRunHistory(ctx, "history-pipe", 3)
	require.NoError(t, err)
	assert.Len(t, entries, 3)
	// Should be ordered by started_at DESC
	assert.True(t, entries[0].StartedAt.After(entries[1].StartedAt) || entries[0].StartedAt.Equal(entries[1].StartedAt))
}

func TestUpsertRerun_Idempotent(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond)
	record := types.RerunRecord{
		RerunID: "rr-test-1", PipelineID: "test-pipe", OriginalDate: "2025-01-15",
		Reason: "drift", Status: types.RunPending, RequestedAt: now,
		Metadata: map[string]interface{}{"source": "test"},
	}

	require.NoError(t, store.UpsertRerun(ctx, record))

	// Update status
	record.Status = types.RunCompleted
	completed := now.Add(time.Hour)
	record.CompletedAt = &completed
	require.NoError(t, store.UpsertRerun(ctx, record))

	var status string
	var meta json.RawMessage
	err := store.pool.QueryRow(ctx, "SELECT status, metadata FROM reruns WHERE rerun_id = $1", "rr-test-1").Scan(&status, &meta)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", status)
}
