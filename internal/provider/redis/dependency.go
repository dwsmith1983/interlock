package redis

import (
	"context"
	"fmt"
)

func (p *RedisProvider) depKey(upstreamID string) string {
	return p.prefix + "deps:" + upstreamID
}

// PutDependency records that downstreamID depends on upstreamID.
func (p *RedisProvider) PutDependency(ctx context.Context, upstreamID, downstreamID string) error {
	if err := p.client.SAdd(ctx, p.depKey(upstreamID), downstreamID).Err(); err != nil {
		return fmt.Errorf("adding dependency %q -> %q: %w", upstreamID, downstreamID, err)
	}
	return nil
}

// RemoveDependency deletes a dependency link.
func (p *RedisProvider) RemoveDependency(ctx context.Context, upstreamID, downstreamID string) error {
	if err := p.client.SRem(ctx, p.depKey(upstreamID), downstreamID).Err(); err != nil {
		return fmt.Errorf("removing dependency %q -> %q: %w", upstreamID, downstreamID, err)
	}
	return nil
}

// ListDependents returns all downstream pipeline IDs that depend on upstreamID.
func (p *RedisProvider) ListDependents(ctx context.Context, upstreamID string) ([]string, error) {
	result, err := p.client.SMembers(ctx, p.depKey(upstreamID)).Result()
	if err != nil {
		return nil, fmt.Errorf("listing dependents of %q: %w", upstreamID, err)
	}
	return result, nil
}
