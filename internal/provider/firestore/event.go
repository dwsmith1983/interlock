package firestore

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/firestore"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// AppendEvent writes an event to the pipeline's event partition.
func (p *FirestoreProvider) AppendEvent(ctx context.Context, event types.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	sk := eventSK(event.Timestamp)
	id := docID(pipelinePK(event.PipelineID), sk)

	_, err = p.coll().Doc(id).Set(ctx, map[string]interface{}{
		"data": string(data),
		"ttl":  ttlEpoch(p.retentionTTL),
	})
	return err
}

// ListEvents returns recent events for a pipeline in chronological order.
func (p *FirestoreProvider) ListEvents(ctx context.Context, pipelineID string, limit int) ([]types.Event, error) {
	if limit <= 0 {
		limit = 50
	}

	pk := pipelinePK(pipelineID)
	start, end := docIDPrefix(pk, prefixEvent)

	// Query newest-first, then reverse for chronological order.
	iter := p.coll().
		Where("__name__", ">=", p.coll().Doc(start)).
		Where("__name__", "<", p.coll().Doc(end)).
		OrderBy("__name__", firestore.Desc).
		Limit(limit).
		Documents(ctx)
	defer iter.Stop()

	var reversed []types.Event
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
			p.logger.Warn("skipping corrupt event data", "error", err)
			continue
		}
		var ev types.Event
		if err := json.Unmarshal([]byte(dataStr), &ev); err != nil {
			p.logger.Warn("skipping corrupt event data", "error", err)
			continue
		}
		reversed = append(reversed, ev)
	}

	// Reverse for chronological order.
	events := make([]types.Event, len(reversed))
	for i, ev := range reversed {
		events[len(reversed)-1-i] = ev
	}
	return events, nil
}

// ReadEventsSince reads events forward from after sinceID (exclusive).
// sinceID is the SK value from a previous EventRecord.StreamID.
// Use "" or "0-0" to read from the beginning.
func (p *FirestoreProvider) ReadEventsSince(ctx context.Context, pipelineID, sinceID string, count int64) ([]types.EventRecord, error) {
	if count <= 0 {
		count = 100
	}

	pk := pipelinePK(pipelineID)
	var q firestore.Query

	if sinceID == "" || sinceID == "0-0" {
		start, end := docIDPrefix(pk, prefixEvent)
		q = p.coll().
			Where("__name__", ">=", p.coll().Doc(start)).
			Where("__name__", "<", p.coll().Doc(end)).
			OrderBy("__name__", firestore.Asc).
			Limit(int(count))
	} else {
		// sinceID is the full document ID — query for docs with ID > sinceID.
		end := docIDPrefix2End(pk, prefixEvent)
		sinceDocID := sinceID
		q = p.coll().
			Where("__name__", ">", p.coll().Doc(sinceDocID)).
			Where("__name__", "<", p.coll().Doc(end)).
			OrderBy("__name__", firestore.Asc).
			Limit(int(count))
	}

	iter := q.Documents(ctx)
	defer iter.Stop()

	var records []types.EventRecord
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
			p.logger.Warn("skipping corrupt event data", "error", err)
			continue
		}
		var ev types.Event
		if err := json.Unmarshal([]byte(dataStr), &ev); err != nil {
			p.logger.Warn("skipping corrupt event data", "error", err)
			continue
		}
		records = append(records, types.EventRecord{
			StreamID: snap.Ref.ID,
			Event:    ev,
		})
	}
	return records, nil
}

// docIDPrefix2End returns only the end bound for a range query.
func docIDPrefix2End(pk, skPrefix string) string {
	return fmt.Sprintf("%s%s%s", pk, sep, incrementLastChar(skPrefix))
}
