package archiver

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/testutil"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// mockPGStore records calls for testing without a real Postgres.
type mockPGStore struct {
	upsertedRuns    []types.RunState
	upsertedLogs    []types.RunLogEntry
	upsertedReruns  []types.RerunRecord
	insertedEvents  []types.EventRecord
	cursors         map[string]string
	insertEventsErr error
	upsertRunErr    error
	getCursorErr    error
}

// Compile-time interface check.
var _ Destination = (*mockPGStore)(nil)

func newMockPG() *mockPGStore {
	return &mockPGStore{
		cursors: make(map[string]string),
	}
}

func (m *mockPGStore) UpsertRun(_ context.Context, run types.RunState) error {
	if m.upsertRunErr != nil {
		return m.upsertRunErr
	}
	m.upsertedRuns = append(m.upsertedRuns, run)
	return nil
}

func (m *mockPGStore) UpsertRunLog(_ context.Context, entry types.RunLogEntry) error {
	m.upsertedLogs = append(m.upsertedLogs, entry)
	return nil
}

func (m *mockPGStore) UpsertRerun(_ context.Context, record types.RerunRecord) error {
	m.upsertedReruns = append(m.upsertedReruns, record)
	return nil
}

func (m *mockPGStore) InsertEvents(_ context.Context, records []types.EventRecord) error {
	if m.insertEventsErr != nil {
		return m.insertEventsErr
	}
	m.insertedEvents = append(m.insertedEvents, records...)
	return nil
}

func (m *mockPGStore) GetCursor(_ context.Context, pipelineID, dataType string) (string, error) {
	if m.getCursorErr != nil {
		return "", m.getCursorErr
	}
	return m.cursors[pipelineID+":"+dataType], nil
}

func (m *mockPGStore) SetCursor(_ context.Context, pipelineID, dataType, cursorValue string) error {
	m.cursors[pipelineID+":"+dataType] = cursorValue
	return nil
}

func newTestArchiver(prov *testutil.MockProvider, pg *mockPGStore) *Archiver {
	return New(prov, pg, time.Minute, slog.Default())
}

func TestNew_DefaultInterval(t *testing.T) {
	prov := testutil.NewMockProvider()
	a := New(prov, nil, 0, slog.Default())
	assert.Equal(t, defaultInterval, a.interval, "zero interval should fall back to default")
}

func TestNew_NegativeInterval(t *testing.T) {
	prov := testutil.NewMockProvider()
	a := New(prov, nil, -1*time.Second, slog.Default())
	assert.Equal(t, defaultInterval, a.interval, "negative interval should fall back to default")
}

func TestNew_CustomInterval(t *testing.T) {
	prov := testutil.NewMockProvider()
	a := New(prov, nil, 30*time.Second, slog.Default())
	assert.Equal(t, 30*time.Second, a.interval)
}

func TestArchiver_StartStop(t *testing.T) {
	prov := testutil.NewMockProvider()
	pg := newMockPG()
	a := New(prov, pg, 50*time.Millisecond, slog.Default())

	a.Start(context.Background())
	time.Sleep(100 * time.Millisecond) // let at least one tick run
	a.Stop(context.Background())
}

func TestArchiver_StopWithoutStart(t *testing.T) {
	prov := testutil.NewMockProvider()
	a := New(prov, nil, time.Minute, slog.Default())
	a.Stop(context.Background()) // should not panic
}

func TestTick_ArchivesPipelines(t *testing.T) {
	prov := testutil.NewMockProvider()
	pg := newMockPG()
	ctx := context.Background()

	require.NoError(t, prov.RegisterPipeline(ctx, types.PipelineConfig{Name: "p1", Archetype: "batch"}))
	require.NoError(t, prov.RegisterPipeline(ctx, types.PipelineConfig{Name: "p2", Archetype: "batch"}))

	now := time.Now()
	// Terminal run for p1
	require.NoError(t, prov.PutRunState(ctx, types.RunState{
		RunID: "run-1", PipelineID: "p1", Status: types.RunCompleted, Version: 1,
		CreatedAt: now, UpdatedAt: now,
	}))
	// Active run for p2 (should not be archived)
	require.NoError(t, prov.PutRunState(ctx, types.RunState{
		RunID: "run-2", PipelineID: "p2", Status: types.RunRunning, Version: 1,
		CreatedAt: now, UpdatedAt: now,
	}))

	a := newTestArchiver(prov, pg)
	a.tick(ctx)

	assert.Len(t, pg.upsertedRuns, 1, "only terminal runs should be archived")
	assert.Equal(t, "run-1", pg.upsertedRuns[0].RunID)
}

func TestArchiveRuns_TerminalOnly(t *testing.T) {
	prov := testutil.NewMockProvider()
	pg := newMockPG()
	ctx := context.Background()

	now := time.Now()
	require.NoError(t, prov.PutRunState(ctx, types.RunState{
		RunID: "completed", PipelineID: "test-pipe", Status: types.RunCompleted, Version: 2,
		CreatedAt: now, UpdatedAt: now,
	}))
	require.NoError(t, prov.PutRunState(ctx, types.RunState{
		RunID: "running", PipelineID: "test-pipe", Status: types.RunRunning, Version: 1,
		CreatedAt: now, UpdatedAt: now,
	}))
	require.NoError(t, prov.PutRunState(ctx, types.RunState{
		RunID: "failed", PipelineID: "test-pipe", Status: types.RunFailed, Version: 3,
		CreatedAt: now, UpdatedAt: now,
	}))

	a := newTestArchiver(prov, pg)
	a.archiveRuns(ctx, "test-pipe")

	assert.Len(t, pg.upsertedRuns, 2, "only terminal runs archived")
	statuses := map[types.RunStatus]bool{}
	for _, r := range pg.upsertedRuns {
		statuses[r.Status] = true
	}
	assert.True(t, statuses[types.RunCompleted])
	assert.True(t, statuses[types.RunFailed])
	assert.False(t, statuses[types.RunRunning])
}

func TestArchiveRuns_NoRuns(t *testing.T) {
	prov := testutil.NewMockProvider()
	pg := newMockPG()
	a := newTestArchiver(prov, pg)
	a.archiveRuns(context.Background(), "empty-pipe")
	assert.Empty(t, pg.upsertedRuns)
}

func TestArchiveRuns_UpsertError(t *testing.T) {
	prov := testutil.NewMockProvider()
	pg := newMockPG()
	pg.upsertRunErr = fmt.Errorf("pg down")
	ctx := context.Background()

	now := time.Now()
	require.NoError(t, prov.PutRunState(ctx, types.RunState{
		RunID: "run-1", PipelineID: "p", Status: types.RunCompleted, Version: 1,
		CreatedAt: now, UpdatedAt: now,
	}))

	a := newTestArchiver(prov, pg)
	a.archiveRuns(ctx, "p") // should log error, not panic
	assert.Empty(t, pg.upsertedRuns)
}

func TestArchiveRunLogs(t *testing.T) {
	prov := testutil.NewMockProvider()
	pg := newMockPG()
	ctx := context.Background()

	now := time.Now()
	require.NoError(t, prov.PutRunLog(ctx, types.RunLogEntry{
		PipelineID: "log-pipe", Date: "2025-01-15", ScheduleID: "daily",
		Status: types.RunCompleted, AttemptNumber: 1, RunID: "run-1",
		StartedAt: now, UpdatedAt: now,
	}))
	require.NoError(t, prov.PutRunLog(ctx, types.RunLogEntry{
		PipelineID: "log-pipe", Date: "2025-01-16", ScheduleID: "daily",
		Status: types.RunFailed, AttemptNumber: 2, RunID: "run-2",
		FailureMessage: "timeout", StartedAt: now, UpdatedAt: now,
	}))

	a := newTestArchiver(prov, pg)
	a.archiveRunLogs(ctx, "log-pipe")

	assert.Len(t, pg.upsertedLogs, 2)
}

func TestArchiveRunLogs_NoLogs(t *testing.T) {
	prov := testutil.NewMockProvider()
	pg := newMockPG()
	a := newTestArchiver(prov, pg)
	a.archiveRunLogs(context.Background(), "nolog-pipe")
	assert.Empty(t, pg.upsertedLogs)
}

func TestArchiveReruns(t *testing.T) {
	prov := testutil.NewMockProvider()
	pg := newMockPG()
	ctx := context.Background()

	require.NoError(t, prov.PutRerun(ctx, types.RerunRecord{
		RerunID: "rr-1", PipelineID: "rerun-pipe", OriginalDate: "2025-01-15",
		Reason: "drift", Status: types.RunCompleted, RequestedAt: time.Now(),
	}))

	a := newTestArchiver(prov, pg)
	a.archiveReruns(ctx, "rerun-pipe")
	assert.Len(t, pg.upsertedReruns, 1)
}

func TestArchiveEvents_WithEvents(t *testing.T) {
	prov := testutil.NewMockProvider()
	pg := newMockPG()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		require.NoError(t, prov.AppendEvent(ctx, types.Event{
			Kind:       types.EventTraitEvaluated,
			PipelineID: "ev-pipe",
			Timestamp:  time.Now(),
		}))
	}

	a := newTestArchiver(prov, pg)
	a.archiveEvents(ctx, "ev-pipe")

	assert.Len(t, pg.insertedEvents, 3)
	cursor, err := pg.GetCursor(ctx, "ev-pipe", "events")
	require.NoError(t, err)
	assert.NotEmpty(t, cursor, "cursor should be advanced after archival")
}

func TestArchiveEvents_NoEvents(t *testing.T) {
	prov := testutil.NewMockProvider()
	pg := newMockPG()
	a := newTestArchiver(prov, pg)
	a.archiveEvents(context.Background(), "no-events")
	assert.Empty(t, pg.insertedEvents)
}

func TestArchiveEvents_InsertFailure_CursorNotAdvanced(t *testing.T) {
	prov := testutil.NewMockProvider()
	pg := newMockPG()
	pg.insertEventsErr = fmt.Errorf("write error")
	ctx := context.Background()

	require.NoError(t, prov.AppendEvent(ctx, types.Event{
		Kind:       types.EventRunStateChanged,
		PipelineID: "fail-pipe",
		Timestamp:  time.Now(),
	}))

	a := newTestArchiver(prov, pg)
	a.archiveEvents(ctx, "fail-pipe")

	cursor, err := pg.GetCursor(ctx, "fail-pipe", "events")
	require.NoError(t, err)
	assert.Empty(t, cursor, "cursor should not advance on write failure")
}

func TestArchiveEvents_GetCursorError(t *testing.T) {
	prov := testutil.NewMockProvider()
	pg := newMockPG()
	pg.getCursorErr = fmt.Errorf("cursor read error")
	ctx := context.Background()

	require.NoError(t, prov.AppendEvent(ctx, types.Event{
		Kind:       types.EventTraitEvaluated,
		PipelineID: "cursor-err",
		Timestamp:  time.Now(),
	}))

	a := newTestArchiver(prov, pg)
	a.archiveEvents(ctx, "cursor-err") // should log error, not panic
	assert.Empty(t, pg.insertedEvents, "no events should be inserted when cursor read fails")
}

func TestArchiveEvents_IncrementalCursor(t *testing.T) {
	prov := testutil.NewMockProvider()
	pg := newMockPG()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		require.NoError(t, prov.AppendEvent(ctx, types.Event{
			Kind:       types.EventTraitEvaluated,
			PipelineID: "incr-pipe",
			Timestamp:  time.Now(),
		}))
	}

	a := newTestArchiver(prov, pg)

	// First archival gets all 5
	a.archiveEvents(ctx, "incr-pipe")
	assert.Len(t, pg.insertedEvents, 5)

	// Add 2 more events
	for i := 0; i < 2; i++ {
		require.NoError(t, prov.AppendEvent(ctx, types.Event{
			Kind:       types.EventRunStateChanged,
			PipelineID: "incr-pipe",
			Timestamp:  time.Now(),
		}))
	}

	// Second archival only gets 2 new events
	a.archiveEvents(ctx, "incr-pipe")
	assert.Len(t, pg.insertedEvents, 7, "should have archived 5+2 events total")
}

func TestArchivePipeline_AllTypes(t *testing.T) {
	prov := testutil.NewMockProvider()
	pg := newMockPG()
	ctx := context.Background()

	now := time.Now()
	require.NoError(t, prov.PutRunState(ctx, types.RunState{
		RunID: "r1", PipelineID: "full-pipe", Status: types.RunCompleted, Version: 1,
		CreatedAt: now, UpdatedAt: now,
	}))
	require.NoError(t, prov.PutRunLog(ctx, types.RunLogEntry{
		PipelineID: "full-pipe", Date: "2025-01-15", ScheduleID: "daily",
		Status: types.RunCompleted, AttemptNumber: 1, RunID: "r1",
		StartedAt: now, UpdatedAt: now,
	}))
	require.NoError(t, prov.PutRerun(ctx, types.RerunRecord{
		RerunID: "rr-1", PipelineID: "full-pipe", OriginalDate: "2025-01-15",
		Reason: "test", Status: types.RunPending, RequestedAt: now,
	}))
	require.NoError(t, prov.AppendEvent(ctx, types.Event{
		Kind:       types.EventTriggerFired,
		PipelineID: "full-pipe",
		Timestamp:  now,
	}))

	a := newTestArchiver(prov, pg)
	a.archivePipeline(ctx, "full-pipe")

	assert.Len(t, pg.upsertedRuns, 1)
	assert.Len(t, pg.upsertedLogs, 1)
	assert.Len(t, pg.upsertedReruns, 1)
	assert.Len(t, pg.insertedEvents, 1)
}
