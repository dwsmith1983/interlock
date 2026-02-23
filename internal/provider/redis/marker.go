package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// WriteCascadeMarker publishes a cascade marker that triggers downstream evaluation.
// In the local Redis variant, this writes to a hash key that the watcher can poll.
func (p *RedisProvider) WriteCascadeMarker(ctx context.Context, pipelineID, scheduleID, date, source string) error {
	key := fmt.Sprintf("%scascade:%s:%s:%s", p.prefix, pipelineID, date, scheduleID)
	data, err := json.Marshal(map[string]string{
		"pipelineID": pipelineID,
		"scheduleID": scheduleID,
		"date":       date,
		"source":     source,
		"timestamp":  time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}
	return p.client.Set(ctx, key, data, p.retentionTTL).Err()
}
