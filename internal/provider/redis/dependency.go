package redis

import (
	"context"
)

func (p *RedisProvider) depKey(upstreamID string) string {
	return p.prefix + "deps:" + upstreamID
}

// PutDependency records that downstreamID depends on upstreamID.
func (p *RedisProvider) PutDependency(ctx context.Context, upstreamID, downstreamID string) error {
	return p.client.SAdd(ctx, p.depKey(upstreamID), downstreamID).Err()
}

// RemoveDependency deletes a dependency link.
func (p *RedisProvider) RemoveDependency(ctx context.Context, upstreamID, downstreamID string) error {
	return p.client.SRem(ctx, p.depKey(upstreamID), downstreamID).Err()
}

// ListDependents returns all downstream pipeline IDs that depend on upstreamID.
func (p *RedisProvider) ListDependents(ctx context.Context, upstreamID string) ([]string, error) {
	return p.client.SMembers(ctx, p.depKey(upstreamID)).Result()
}
