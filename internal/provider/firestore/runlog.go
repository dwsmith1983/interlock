package firestore

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PutRunLog stores a run log entry.
func (p *FirestoreProvider) PutRunLog(ctx context.Context, entry types.RunLogEntry) error {
	if entry.ScheduleID == "" {
		entry.ScheduleID = types.DefaultScheduleID
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	id := docID(pipelinePK(entry.PipelineID), runLogSK(entry.Date, entry.ScheduleID))
	_, err = p.coll().Doc(id).Set(ctx, map[string]interface{}{
		"data": string(data),
		"ttl":  ttlEpoch(30 * 24 * time.Hour),
	})
	return err
}

// GetRunLog retrieves a run log entry for a pipeline, date, and schedule.
func (p *FirestoreProvider) GetRunLog(ctx context.Context, pipelineID, date, scheduleID string) (*types.RunLogEntry, error) {
	if scheduleID == "" {
		scheduleID = types.DefaultScheduleID
	}
	id := docID(pipelinePK(pipelineID), runLogSK(date, scheduleID))
	snap, err := p.coll().Doc(id).Get(ctx)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	ttlVal, _ := snapInt64(snap, "ttl")
	if isExpired(ttlVal) {
		return nil, nil
	}

	dataStr, err := snapString(snap, "data")
	if err != nil {
		return nil, err
	}
	var entry types.RunLogEntry
	if err := json.Unmarshal([]byte(dataStr), &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// ListRunLogs returns recent run log entries for a pipeline, newest first.
func (p *FirestoreProvider) ListRunLogs(ctx context.Context, pipelineID string, limit int) ([]types.RunLogEntry, error) {
	if limit <= 0 {
		limit = 20
	}

	pk := pipelinePK(pipelineID)
	start, end := docIDPrefix(pk, prefixRunLog)

	iter := p.coll().
		Where("__name__", ">=", p.coll().Doc(start)).
		Where("__name__", "<", p.coll().Doc(end)).
		OrderBy("__name__", firestore.Desc).
		Limit(limit).
		Documents(ctx)
	defer iter.Stop()

	var entries []types.RunLogEntry
	for {
		snap, err := iter.Next()
		if err != nil {
			break
		}

		ttlVal, _ := snapInt64(snap, "ttl")
		if isExpired(ttlVal) {
			continue
		}

		dataStr, err := snapString(snap, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt run log data", "error", err)
			continue
		}
		var entry types.RunLogEntry
		if err := json.Unmarshal([]byte(dataStr), &entry); err != nil {
			p.logger.Warn("skipping corrupt run log data", "error", err)
			continue
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
