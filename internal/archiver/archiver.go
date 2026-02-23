// Package archiver provides a background process that archives Redis operational
// data to Postgres for durable long-term storage.
package archiver

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/dwsmith1983/interlock/internal/lifecycle"
	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

const (
	defaultInterval = 5 * time.Minute
	eventBatchSize  = int64(500)
)

// Destination defines the write interface for the archival backend.
type Destination interface {
	UpsertRun(ctx context.Context, run types.RunState) error
	UpsertRunLog(ctx context.Context, entry types.RunLogEntry) error
	UpsertRerun(ctx context.Context, record types.RerunRecord) error
	InsertEvents(ctx context.Context, records []types.EventRecord) error
	GetCursor(ctx context.Context, pipelineID, dataType string) (string, error)
	SetCursor(ctx context.Context, pipelineID, dataType, cursorValue string) error
}

// Archiver periodically archives Redis data to Postgres.
type Archiver struct {
	source   provider.Provider
	dest     Destination
	interval time.Duration
	logger   *slog.Logger
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// New creates a new Archiver.
func New(source provider.Provider, dest Destination, interval time.Duration, logger *slog.Logger) *Archiver {
	if interval <= 0 {
		interval = defaultInterval
	}
	return &Archiver{
		source:   source,
		dest:     dest,
		interval: interval,
		logger:   logger,
	}
}

// Start begins the archiver background loop.
func (a *Archiver) Start(ctx context.Context) {
	ctx, a.cancel = context.WithCancel(ctx)
	a.wg.Add(1)
	go a.loop(ctx)
	a.logger.Info("archiver started", "interval", a.interval)
}

// Stop signals the archiver to stop and waits for it to finish.
func (a *Archiver) Stop(_ context.Context) {
	if a.cancel != nil {
		a.cancel()
	}
	a.wg.Wait()
	a.logger.Info("archiver stopped")
}

func (a *Archiver) loop(ctx context.Context) {
	defer a.wg.Done()
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	// Run once immediately on start
	a.tick(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.tick(ctx)
		}
	}
}

func (a *Archiver) tick(ctx context.Context) {
	pipelines, err := a.source.ListPipelines(ctx)
	if err != nil {
		a.logger.Error("archiver: failed to list pipelines", "error", err)
		return
	}

	for _, pl := range pipelines {
		if ctx.Err() != nil {
			return
		}
		a.archivePipeline(ctx, pl.Name)
	}
}

func (a *Archiver) archivePipeline(ctx context.Context, pipelineID string) {
	a.archiveRuns(ctx, pipelineID)
	a.archiveRunLogs(ctx, pipelineID)
	a.archiveReruns(ctx, pipelineID)
	a.archiveEvents(ctx, pipelineID)
}

func (a *Archiver) archiveRuns(ctx context.Context, pipelineID string) {
	runs, err := a.source.ListRuns(ctx, pipelineID, 0)
	if err != nil {
		a.logger.Error("archiver: list runs failed", "pipeline", pipelineID, "error", err)
		return
	}

	for _, run := range runs {
		if !lifecycle.IsTerminal(run.Status) {
			continue
		}
		if err := a.dest.UpsertRun(ctx, run); err != nil {
			a.logger.Error("archiver: upsert run failed", "pipeline", pipelineID, "runID", run.RunID, "error", err)
		}
	}
}

func (a *Archiver) archiveRunLogs(ctx context.Context, pipelineID string) {
	entries, err := a.source.ListRunLogs(ctx, pipelineID, 0)
	if err != nil {
		a.logger.Error("archiver: list run logs failed", "pipeline", pipelineID, "error", err)
		return
	}

	for _, entry := range entries {
		if err := a.dest.UpsertRunLog(ctx, entry); err != nil {
			a.logger.Error("archiver: upsert run log failed", "pipeline", pipelineID, "date", entry.Date, "error", err)
		}
	}
}

func (a *Archiver) archiveReruns(ctx context.Context, pipelineID string) {
	reruns, err := a.source.ListReruns(ctx, pipelineID, 0)
	if err != nil {
		a.logger.Error("archiver: list reruns failed", "pipeline", pipelineID, "error", err)
		return
	}

	for _, rerun := range reruns {
		if err := a.dest.UpsertRerun(ctx, rerun); err != nil {
			a.logger.Error("archiver: upsert rerun failed", "pipeline", pipelineID, "rerunID", rerun.RerunID, "error", err)
		}
	}
}

func (a *Archiver) archiveEvents(ctx context.Context, pipelineID string) {
	cursor, err := a.dest.GetCursor(ctx, pipelineID, "events")
	if err != nil {
		a.logger.Error("archiver: get cursor failed", "pipeline", pipelineID, "error", err)
		return
	}

	sinceID := cursor
	if sinceID == "" {
		sinceID = "0-0"
	}

	for {
		records, err := a.source.ReadEventsSince(ctx, pipelineID, sinceID, eventBatchSize)
		if err != nil {
			a.logger.Error("archiver: read events failed", "pipeline", pipelineID, "error", err)
			return
		}
		if len(records) == 0 {
			break
		}

		if err := a.dest.InsertEvents(ctx, records); err != nil {
			a.logger.Error("archiver: insert events failed", "pipeline", pipelineID, "error", err)
			return // Don't advance cursor on failure
		}

		lastID := records[len(records)-1].StreamID
		if err := a.dest.SetCursor(ctx, pipelineID, "events", lastID); err != nil {
			a.logger.Error("archiver: set cursor failed", "pipeline", pipelineID, "error", err)
			return
		}
		sinceID = lastID

		if int64(len(records)) < eventBatchSize {
			break
		}
	}
}
