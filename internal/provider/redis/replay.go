package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func (p *RedisProvider) replayKey(pipelineID, date, scheduleID string) string {
	return fmt.Sprintf("%sreplay:%s:%s:%s", p.prefix, pipelineID, date, scheduleID)
}

func (p *RedisProvider) replayIndexKey() string {
	return p.prefix + "replays:all"
}

// PutReplay stores a replay request.
func (p *RedisProvider) PutReplay(ctx context.Context, entry types.ReplayRequest) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	key := p.replayKey(entry.PipelineID, entry.Date, entry.ScheduleID)
	score := float64(entry.CreatedAt.UnixMilli())

	pipe := p.client.Pipeline()
	pipe.Set(ctx, key, data, p.retentionTTL)
	pipe.ZAdd(ctx, p.replayIndexKey(), goredis.Z{
		Score:  score,
		Member: key,
	})
	pipe.ZRemRangeByRank(ctx, p.replayIndexKey(), 0, -501)
	_, err = pipe.Exec(ctx)
	return err
}

// GetReplay retrieves the most recent replay request for a pipeline/date/schedule.
func (p *RedisProvider) GetReplay(ctx context.Context, pipelineID, date, scheduleID string) (*types.ReplayRequest, error) {
	key := p.replayKey(pipelineID, date, scheduleID)
	data, err := p.client.Get(ctx, key).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var req types.ReplayRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

// ListReplays returns recent replay requests across all pipelines.
func (p *RedisProvider) ListReplays(ctx context.Context, limit int) ([]types.ReplayRequest, error) {
	if limit <= 0 {
		limit = 50
	}
	keys, err := p.client.ZRevRange(ctx, p.replayIndexKey(), 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}

	var replays []types.ReplayRequest
	for _, key := range keys {
		data, err := p.client.Get(ctx, key).Bytes()
		if err != nil {
			if errors.Is(err, goredis.Nil) {
				continue
			}
			p.logger.Warn("skipping unreadable replay", "key", key, "error", err)
			continue
		}
		var req types.ReplayRequest
		if err := json.Unmarshal(data, &req); err != nil {
			p.logger.Warn("skipping corrupt replay data", "key", key, "error", err)
			continue
		}
		replays = append(replays, req)
	}
	return replays, nil
}
