package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/interlock-systems/interlock/pkg/types"
)

// QueryFailedPipelines returns pipeline IDs that have a FAILED run log for the given date.
func (s *Store) QueryFailedPipelines(ctx context.Context, date string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT pipeline_id FROM run_logs
		WHERE date = $1 AND status = 'FAILED'
		ORDER BY pipeline_id
	`, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// QueryRunHistory returns recent run log entries for a pipeline, most recent first.
func (s *Store) QueryRunHistory(ctx context.Context, pipelineID string, limit int) ([]types.RunLogEntry, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		SELECT pipeline_id, date, status, attempt_number, run_id,
			COALESCE(failure_message, ''), COALESCE(failure_category, ''),
			alert_sent, started_at, completed_at, updated_at
		FROM run_logs
		WHERE pipeline_id = $1
		ORDER BY started_at DESC
		LIMIT $2
	`, pipelineID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []types.RunLogEntry
	for rows.Next() {
		var e types.RunLogEntry
		var failCat string
		if err := rows.Scan(&e.PipelineID, &e.Date, &e.Status, &e.AttemptNumber,
			&e.RunID, &e.FailureMessage, &failCat, &e.AlertSent,
			&e.StartedAt, &e.CompletedAt, &e.UpdatedAt); err != nil {
			return nil, err
		}
		e.FailureCategory = types.FailureCategory(failCat)
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// TraitEvaluationRow represents a row from the trait_evaluations table.
type TraitEvaluationRow struct {
	PipelineID      string
	TraitType       string
	Status          string
	Value           json.RawMessage
	Reason          string
	FailureCategory string
	EvaluatedAt     time.Time
}

// QueryTraitHistory returns recent trait evaluations for a pipeline.
func (s *Store) QueryTraitHistory(ctx context.Context, pipelineID string, since time.Time, limit int) ([]TraitEvaluationRow, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT pipeline_id, trait_type, status, value,
			COALESCE(reason, ''), COALESCE(failure_category, ''), evaluated_at
		FROM trait_evaluations
		WHERE pipeline_id = $1 AND evaluated_at >= $2
		ORDER BY evaluated_at DESC
		LIMIT $3
	`, pipelineID, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var evals []TraitEvaluationRow
	for rows.Next() {
		var e TraitEvaluationRow
		if err := rows.Scan(&e.PipelineID, &e.TraitType, &e.Status,
			&e.Value, &e.Reason, &e.FailureCategory, &e.EvaluatedAt); err != nil {
			return nil, err
		}
		evals = append(evals, e)
	}
	return evals, rows.Err()
}
