package firestore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PutLateArrival stores a late-arrival detection record.
func (p *FirestoreProvider) PutLateArrival(ctx context.Context, entry types.LateArrival) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	sk := lateArrivalSK(entry.Date, entry.ScheduleID, entry.DetectedAt)
	id := docID(pipelinePK(entry.PipelineID), sk)
	ttl := ttlEpoch(30 * 24 * time.Hour)

	_, err = p.coll().Doc(id).Set(ctx, map[string]interface{}{
		"data": string(data),
		"ttl":  ttl,
	})
	return err
}

// ListLateArrivals returns late-arrival records for a pipeline/date/schedule.
func (p *FirestoreProvider) ListLateArrivals(ctx context.Context, pipelineID, date, scheduleID string) ([]types.LateArrival, error) {
	pk := pipelinePK(pipelineID)
	prefix := fmt.Sprintf("%s%s#%s#", prefixLateArrival, date, scheduleID)
	start, end := docIDPrefix(pk, prefix)

	iter := p.coll().
		Where("__name__", ">=", p.coll().Doc(start)).
		Where("__name__", "<", p.coll().Doc(end)).
		OrderBy("__name__", firestore.Desc).
		Documents(ctx)
	defer iter.Stop()

	var arrivals []types.LateArrival
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
			p.logger.Warn("skipping corrupt late arrival data", "error", err)
			continue
		}
		var la types.LateArrival
		if err := json.Unmarshal([]byte(dataStr), &la); err != nil {
			p.logger.Warn("skipping corrupt late arrival data", "error", err)
			continue
		}
		arrivals = append(arrivals, la)
	}
	return arrivals, nil
}
