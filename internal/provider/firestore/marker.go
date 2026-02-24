package firestore

import (
	"context"
	"fmt"
	"time"
)

// WriteCascadeMarker writes a MARKER# record that the stream-router picks up
// to trigger downstream pipeline evaluation.
func (p *FirestoreProvider) WriteCascadeMarker(ctx context.Context, pipelineID, scheduleID, date, source string) error {
	now := time.Now()
	millis := now.UnixMilli()
	sk := fmt.Sprintf("%scascade#%s#%d", prefixMarker, source, millis)
	ttl := ttlEpoch(30 * 24 * time.Hour)

	id := docID(pipelinePK(pipelineID), sk)
	_, err := p.coll().Doc(id).Set(ctx, map[string]interface{}{
		"scheduleID": scheduleID,
		"date":       date,
		"source":     source,
		"ttl":        ttl,
	})
	return err
}
