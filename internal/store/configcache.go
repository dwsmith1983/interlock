package store

import (
	"context"
	"sync"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// ConfigCache caches all pipeline configs with TTL-based refresh.
type ConfigCache struct {
	store    *Store
	mu       sync.RWMutex
	configs  map[string]*types.PipelineConfig
	loadedAt time.Time
	ttl      time.Duration
}

// NewConfigCache creates a config cache with the given TTL.
func NewConfigCache(store *Store, ttl time.Duration) *ConfigCache {
	return &ConfigCache{
		store:   store,
		configs: make(map[string]*types.PipelineConfig),
		ttl:     ttl,
	}
}

// Get returns a single pipeline config by ID, refreshing if stale.
// Returns nil (without error) if the pipeline is not found.
func (c *ConfigCache) Get(ctx context.Context, pipelineID string) (*types.PipelineConfig, error) {
	configs, err := c.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	return configs[pipelineID], nil
}

// GetAll returns all cached pipeline configs, refreshing if stale.
// The returned map is a deep copy; callers cannot mutate cached values.
func (c *ConfigCache) GetAll(ctx context.Context) (map[string]*types.PipelineConfig, error) {
	c.mu.RLock()
	if time.Since(c.loadedAt) < c.ttl && len(c.configs) > 0 {
		cp := copyConfigs(c.configs)
		c.mu.RUnlock()
		return cp, nil
	}
	c.mu.RUnlock()
	return c.refresh(ctx)
}

// Invalidate forces a refresh on the next GetAll call.
func (c *ConfigCache) Invalidate() {
	c.mu.Lock()
	c.loadedAt = time.Time{}
	c.mu.Unlock()
}

func (c *ConfigCache) refresh(ctx context.Context) (map[string]*types.PipelineConfig, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock.
	if time.Since(c.loadedAt) < c.ttl && len(c.configs) > 0 {
		return copyConfigs(c.configs), nil
	}

	configs, err := c.store.ScanConfigs(ctx)
	if err != nil {
		return nil, err
	}
	c.configs = configs
	c.loadedAt = time.Now()
	return copyConfigs(configs), nil
}

// copyConfigs returns a deep copy of the config map so callers cannot
// mutate cached values.
func copyConfigs(src map[string]*types.PipelineConfig) map[string]*types.PipelineConfig {
	dst := make(map[string]*types.PipelineConfig, len(src))
	for k, v := range src {
		cp := *v
		dst[k] = &cp
	}
	return dst
}
