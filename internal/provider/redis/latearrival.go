package redis

import (
	"context"
	"encoding/json"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func (p *RedisProvider) lateArrivalKey(pipelineID, date, scheduleID string) string {
	return fmt.Sprintf("%slatearrival:%s:%s:%s", p.prefix, pipelineID, date, scheduleID)
}

// PutLateArrival stores a late-arrival detection record in a sorted set.
func (p *RedisProvider) PutLateArrival(ctx context.Context, entry types.LateArrival) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshaling late arrival for pipeline %q: %w", entry.PipelineID, err)
	}

	key := p.lateArrivalKey(entry.PipelineID, entry.Date, entry.ScheduleID)
	score := float64(entry.DetectedAt.UnixMilli())

	pipe := p.client.Pipeline()
	pipe.ZAdd(ctx, key, goredis.Z{
		Score:  score,
		Member: string(data),
	})
	pipe.Expire(ctx, key, p.retentionTTL)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("storing late arrival for pipeline %q: %w", entry.PipelineID, err)
	}
	return nil
}

// ListLateArrivals returns late-arrival records for a pipeline/date/schedule.
func (p *RedisProvider) ListLateArrivals(ctx context.Context, pipelineID, date, scheduleID string) ([]types.LateArrival, error) {
	key := p.lateArrivalKey(pipelineID, date, scheduleID)
	members, err := p.client.ZRangeArgs(ctx, goredis.ZRangeArgs{
		Key:   key,
		Start: 0,
		Stop:  -1,
		Rev:   true,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("listing late arrivals for pipeline %q: %w", pipelineID, err)
	}

	var arrivals []types.LateArrival
	for _, m := range members {
		var la types.LateArrival
		if err := json.Unmarshal([]byte(m), &la); err != nil {
			p.logger.Warn("skipping corrupt late arrival data", "error", err)
			continue
		}
		arrivals = append(arrivals, la)
	}
	return arrivals, nil
}
