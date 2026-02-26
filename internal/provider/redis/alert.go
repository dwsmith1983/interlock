package redis

import (
	"context"
	"encoding/json"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func (p *RedisProvider) alertKey(pipelineID string) string {
	return p.prefix + "alerts:" + pipelineID
}

func (p *RedisProvider) alertGlobalKey() string {
	return p.prefix + "alerts:all"
}

const defaultAlertIndexMax = 500

// PutAlert persists an alert record.
func (p *RedisProvider) PutAlert(ctx context.Context, alert types.Alert) error {
	if alert.AlertID == "" {
		alert.AlertID = fmt.Sprintf("%d", alert.Timestamp.UnixMilli())
	}
	data, err := json.Marshal(alert)
	if err != nil {
		return err
	}

	score := float64(alert.Timestamp.UnixMilli())
	member := string(data)

	pipe := p.client.Pipeline()
	pipe.ZAdd(ctx, p.alertKey(alert.PipelineID), goredis.Z{Score: score, Member: member})
	pipe.ZAdd(ctx, p.alertGlobalKey(), goredis.Z{Score: score, Member: member})
	pipe.ZRemRangeByRank(ctx, p.alertKey(alert.PipelineID), 0, -(defaultAlertIndexMax + 1))
	pipe.ZRemRangeByRank(ctx, p.alertGlobalKey(), 0, -(defaultAlertIndexMax + 1))
	_, err = pipe.Exec(ctx)
	return err
}

// ListAlerts returns recent alerts for a pipeline, newest first.
func (p *RedisProvider) ListAlerts(ctx context.Context, pipelineID string, limit int) ([]types.Alert, error) {
	if limit <= 0 {
		limit = 50
	}
	return p.fetchAlerts(ctx, p.alertKey(pipelineID), limit)
}

// ListAllAlerts returns recent alerts across all pipelines, newest first.
func (p *RedisProvider) ListAllAlerts(ctx context.Context, limit int) ([]types.Alert, error) {
	if limit <= 0 {
		limit = 50
	}
	return p.fetchAlerts(ctx, p.alertGlobalKey(), limit)
}

func (p *RedisProvider) fetchAlerts(ctx context.Context, key string, limit int) ([]types.Alert, error) {
	members, err := p.client.ZRangeArgs(ctx, goredis.ZRangeArgs{
		Key:   key,
		Start: 0,
		Stop:  int64(limit - 1),
		Rev:   true,
	}).Result()
	if err != nil {
		return nil, err
	}

	var alerts []types.Alert
	for _, m := range members {
		var a types.Alert
		if err := json.Unmarshal([]byte(m), &a); err != nil {
			p.logger.Warn("skipping corrupt alert", "error", err)
			continue
		}
		alerts = append(alerts, a)
	}
	return alerts, nil
}
