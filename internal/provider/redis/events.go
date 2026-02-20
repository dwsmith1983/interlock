package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/interlock-systems/interlock/pkg/types"
)

// PublishTraitChange publishes a trait change event to Redis Streams.
func (p *RedisProvider) PublishTraitChange(ctx context.Context, event types.TraitChangeEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: p.prefix + "events:trait-changes",
		MaxLen: 1000,
		Approx: true,
		Values: map[string]interface{}{
			"data": string(data),
		},
	}).Err()
}

// SubscribeTraitChanges watches for trait change events.
// The handler is called for each event. This blocks until ctx is cancelled.
func (p *RedisProvider) SubscribeTraitChanges(ctx context.Context, handler func(types.TraitChangeEvent)) error {
	stream := p.prefix + "events:trait-changes"
	lastID := "$" // only new messages

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		results, err := p.client.XRead(ctx, &goredis.XReadArgs{
			Streams: []string{stream, lastID},
			Count:   10,
			Block:   5 * time.Second,
		}).Result()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}

		for _, result := range results {
			for _, msg := range result.Messages {
				lastID = msg.ID
				data, ok := msg.Values["data"].(string)
				if !ok {
					continue
				}
				var event types.TraitChangeEvent
				if err := json.Unmarshal([]byte(data), &event); err != nil {
					continue
				}
				handler(event)
			}
		}
	}
}

// SubscribeSourceActivity is a placeholder for Phase 1 — source monitoring via file watching or polling.
func (p *RedisProvider) SubscribeSourceActivity(_ context.Context, _ types.SourceMonitorConfig, _ func(types.SourceEvent)) error {
	return fmt.Errorf("source activity monitoring not yet implemented")
}
