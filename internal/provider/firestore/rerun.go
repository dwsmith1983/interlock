package firestore

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PutRerun stores a rerun record using dual-write: truth item + list copy.
func (p *FirestoreProvider) PutRerun(ctx context.Context, record types.RerunRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	ttl := ttlEpoch(30 * 24 * time.Hour)
	gsi1sk := record.RequestedAt.UTC().Format(time.RFC3339Nano)

	truthID := docID(rerunPK(record.RerunID), rerunSK(record.RerunID))
	listID := docID(pipelinePK(record.PipelineID), rerunSK(record.RerunID))

	return p.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := tx.Set(p.coll().Doc(truthID), map[string]interface{}{
			"data":   string(data),
			"gsi1pk": prefixType + "rerun",
			"gsi1sk": gsi1sk,
			"ttl":    ttl,
		}); err != nil {
			return err
		}
		return tx.Set(p.coll().Doc(listID), map[string]interface{}{
			"data":   string(data),
			"gsi1pk": prefixType + "rerun",
			"gsi1sk": gsi1sk,
			"ttl":    ttl,
		})
	})
}

// GetRerun retrieves a rerun record from the truth item.
func (p *FirestoreProvider) GetRerun(ctx context.Context, rerunID string) (*types.RerunRecord, error) {
	id := docID(rerunPK(rerunID), rerunSK(rerunID))
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
	var record types.RerunRecord
	if err := json.Unmarshal([]byte(dataStr), &record); err != nil {
		return nil, err
	}
	return &record, nil
}

// ListReruns returns recent rerun records for a pipeline, newest first.
func (p *FirestoreProvider) ListReruns(ctx context.Context, pipelineID string, limit int) ([]types.RerunRecord, error) {
	if limit <= 0 {
		limit = 20
	}

	pk := pipelinePK(pipelineID)
	start, end := docIDPrefix(pk, prefixRerun)

	iter := p.coll().
		Where("__name__", ">=", p.coll().Doc(start)).
		Where("__name__", "<", p.coll().Doc(end)).
		OrderBy("__name__", firestore.Desc).
		Limit(limit).
		Documents(ctx)
	defer iter.Stop()

	return p.unmarshalReruns(iter)
}

// ListAllReruns returns recent rerun records across all pipelines via GSI1-equivalent query.
func (p *FirestoreProvider) ListAllReruns(ctx context.Context, limit int) ([]types.RerunRecord, error) {
	if limit <= 0 {
		limit = 50
	}

	iter := p.coll().
		Where("gsi1pk", "==", prefixType+"rerun").
		OrderBy("gsi1sk", firestore.Desc).
		Limit(limit).
		Documents(ctx)
	defer iter.Stop()

	return p.unmarshalReruns(iter)
}

func (p *FirestoreProvider) unmarshalReruns(iter *firestore.DocumentIterator) ([]types.RerunRecord, error) {
	// Deduplicate: GSI1 query may return both truth + list copy items.
	seen := make(map[string]struct{})
	var records []types.RerunRecord

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
			p.logger.Warn("skipping corrupt rerun data", "error", err)
			continue
		}
		var record types.RerunRecord
		if err := json.Unmarshal([]byte(dataStr), &record); err != nil {
			p.logger.Warn("skipping corrupt rerun data", "error", err)
			continue
		}
		if _, dup := seen[record.RerunID]; dup {
			continue
		}
		seen[record.RerunID] = struct{}{}
		records = append(records, record)
	}
	return records, nil
}
