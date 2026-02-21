package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/interlock-systems/interlock/pkg/types"
)

// UpsertRun upserts a run state into the runs table.
func (s *Store) UpsertRun(ctx context.Context, run types.RunState) error {
	metaJSON, err := json.Marshal(run.Metadata)
	if err != nil {
		return fmt.Errorf("marshal run metadata: %w", err)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO runs (run_id, pipeline_id, status, version, metadata, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (run_id) DO UPDATE SET
			status     = EXCLUDED.status,
			version    = EXCLUDED.version,
			metadata   = EXCLUDED.metadata,
			updated_at = EXCLUDED.updated_at,
			archived_at = NOW()
	`, run.RunID, run.PipelineID, string(run.Status), run.Version, metaJSON, run.CreatedAt, run.UpdatedAt)
	return err
}

// UpsertRunLog upserts a run log entry.
func (s *Store) UpsertRunLog(ctx context.Context, entry types.RunLogEntry) error {
	scheduleID := entry.ScheduleID
	if scheduleID == "" {
		scheduleID = types.DefaultScheduleID
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO run_logs (pipeline_id, date, schedule_id, status, attempt_number, run_id,
			failure_message, failure_category, alert_sent, started_at, completed_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (pipeline_id, date, schedule_id) DO UPDATE SET
			status           = EXCLUDED.status,
			attempt_number   = EXCLUDED.attempt_number,
			run_id           = EXCLUDED.run_id,
			failure_message  = EXCLUDED.failure_message,
			failure_category = EXCLUDED.failure_category,
			alert_sent       = EXCLUDED.alert_sent,
			completed_at     = EXCLUDED.completed_at,
			updated_at       = EXCLUDED.updated_at,
			archived_at      = NOW()
	`, entry.PipelineID, entry.Date, scheduleID, string(entry.Status), entry.AttemptNumber, entry.RunID,
		entry.FailureMessage, string(entry.FailureCategory), entry.AlertSent,
		entry.StartedAt, entry.CompletedAt, entry.UpdatedAt)
	return err
}

// UpsertRerun upserts a rerun record.
func (s *Store) UpsertRerun(ctx context.Context, record types.RerunRecord) error {
	metaJSON, err := json.Marshal(record.Metadata)
	if err != nil {
		return fmt.Errorf("marshal rerun metadata: %w", err)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO reruns (rerun_id, pipeline_id, original_date, original_run_id, reason,
			description, status, rerun_run_id, metadata, requested_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (rerun_id) DO UPDATE SET
			status       = EXCLUDED.status,
			rerun_run_id = EXCLUDED.rerun_run_id,
			metadata     = EXCLUDED.metadata,
			completed_at = EXCLUDED.completed_at,
			archived_at  = NOW()
	`, record.RerunID, record.PipelineID, record.OriginalDate, record.OriginalRunID,
		record.Reason, record.Description, string(record.Status), record.RerunRunID,
		metaJSON, record.RequestedAt, record.CompletedAt)
	return err
}

// InsertEvents batch-inserts events with dedup and extracts trait evaluations.
func (s *Store) InsertEvents(ctx context.Context, records []types.EventRecord) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, rec := range records {
		ev := rec.Event
		detailsJSON, err := json.Marshal(ev.Details)
		if err != nil {
			return fmt.Errorf("marshal event details: %w", err)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO events (stream_id, kind, pipeline_id, run_id, trait_type, status, message, details, timestamp)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (pipeline_id, stream_id) WHERE stream_id IS NOT NULL DO NOTHING
		`, rec.StreamID, string(ev.Kind), ev.PipelineID, ev.RunID, ev.TraitType,
			ev.Status, ev.Message, detailsJSON, ev.Timestamp)
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}

		// Extract trait evaluations from TRAIT_EVALUATED events
		if ev.Kind == types.EventTraitEvaluated && ev.TraitType != "" {
			valueJSON, _ := json.Marshal(ev.Details)
			failCat := ""
			if v, ok := ev.Details["failureCategory"]; ok {
				if s, ok := v.(string); ok {
					failCat = s
				}
			}
			reason := ""
			if v, ok := ev.Details["reason"]; ok {
				if s, ok := v.(string); ok {
					reason = s
				}
			}
			_, err = tx.Exec(ctx, `
				INSERT INTO trait_evaluations (pipeline_id, trait_type, status, value, reason, failure_category, evaluated_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
			`, ev.PipelineID, ev.TraitType, ev.Status, valueJSON, reason, failCat, ev.Timestamp)
			if err != nil {
				return fmt.Errorf("insert trait evaluation: %w", err)
			}
		}
	}

	return tx.Commit(ctx)
}

// GetCursor retrieves an archive cursor value for a pipeline and data type.
func (s *Store) GetCursor(ctx context.Context, pipelineID, dataType string) (string, error) {
	var cursor string
	err := s.pool.QueryRow(ctx, `
		SELECT cursor_value FROM archive_cursors
		WHERE pipeline_id = $1 AND data_type = $2
	`, pipelineID, dataType).Scan(&cursor)
	if err != nil {
		// Return empty string if no cursor found (pgx returns error for no rows)
		return "", nil
	}
	return cursor, nil
}

// SetCursor sets an archive cursor value for a pipeline and data type.
func (s *Store) SetCursor(ctx context.Context, pipelineID, dataType, cursorValue string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO archive_cursors (pipeline_id, data_type, cursor_value, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (pipeline_id, data_type) DO UPDATE SET
			cursor_value = EXCLUDED.cursor_value,
			updated_at   = NOW()
	`, pipelineID, dataType, cursorValue)
	return err
}
