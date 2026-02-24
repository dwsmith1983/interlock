package firestore

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PutReplay stores a replay request record.
func (p *FirestoreProvider) PutReplay(ctx context.Context, entry types.ReplayRequest) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	pk := replayPK(entry.PipelineID, entry.Date, entry.ScheduleID)
	sk := replayCreatedSK(entry.CreatedAt)
	id := docID(pk, sk)
	ttl := ttlEpoch(30 * 24 * time.Hour)

	_, err = p.coll().Doc(id).Set(ctx, map[string]interface{}{
		"data":   string(data),
		"gsi1pk": prefixType + "replay",
		"gsi1sk": entry.CreatedAt.UTC().Format(time.RFC3339Nano),
		"ttl":    ttl,
	})
	return err
}

// GetReplay retrieves the most recent replay request for a pipeline/date/schedule.
func (p *FirestoreProvider) GetReplay(ctx context.Context, pipelineID, date, scheduleID string) (*types.ReplayRequest, error) {
	pk := replayPK(pipelineID, date, scheduleID)
	start, end := docIDPrefix(pk, "CREATED#")

	iter := p.coll().
		Where("__name__", ">=", p.coll().Doc(start)).
		Where("__name__", "<", p.coll().Doc(end)).
		OrderBy("__name__", firestore.Desc).
		Limit(1).
		Documents(ctx)
	defer iter.Stop()

	snap, err := iter.Next()
	if err != nil {
		return nil, nil
	}

	ttlVal, _ := snapInt64(snap, "ttl")
	if isExpired(ttlVal) {
		return nil, nil
	}

	dataStr, err := snapString(snap, "data")
	if err != nil {
		return nil, err
	}
	var req types.ReplayRequest
	if err := json.Unmarshal([]byte(dataStr), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

// ListReplays returns recent replay requests across all pipelines via GSI1-equivalent query.
func (p *FirestoreProvider) ListReplays(ctx context.Context, limit int) ([]types.ReplayRequest, error) {
	if limit <= 0 {
		limit = 50
	}

	iter := p.coll().
		Where("gsi1pk", "==", prefixType+"replay").
		OrderBy("gsi1sk", firestore.Desc).
		Limit(limit).
		Documents(ctx)
	defer iter.Stop()

	var replays []types.ReplayRequest
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
			p.logger.Warn("skipping corrupt replay data", "error", err)
			continue
		}
		var req types.ReplayRequest
		if err := json.Unmarshal([]byte(dataStr), &req); err != nil {
			p.logger.Warn("skipping corrupt replay data", "error", err)
			continue
		}
		replays = append(replays, req)
	}
	return replays, nil
}
