package store

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// ConfigCache caches all pipeline configs with TTL-based refresh. It holds
// only the configs ScanConfigs accepted, so every Lambda sees the same set,
// and keeps the rejected rows beside them.
type ConfigCache struct {
	store    *Store
	mu       sync.RWMutex
	configs  map[string]*types.PipelineConfig
	rejected []RejectedConfig
	loadedAt time.Time
	ttl      time.Duration
	// Logger receives one warning per rejected row on each refresh;
	// slog.Default() when nil.
	Logger *slog.Logger
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
	configs, _, err := c.load(ctx)
	return configs, err
}

// Rejected returns a copy of the CONFIG rows the last scan rejected,
// refreshing if stale.
func (c *ConfigCache) Rejected(ctx context.Context) ([]RejectedConfig, error) {
	_, rejected, err := c.load(ctx)
	return rejected, err
}

func (c *ConfigCache) load(ctx context.Context) (map[string]*types.PipelineConfig, []RejectedConfig, error) {
	c.mu.RLock()
	if c.fresh() {
		configs, rejected := copyConfigs(c.configs), slices.Clone(c.rejected)
		c.mu.RUnlock()
		return configs, rejected, nil
	}
	c.mu.RUnlock()
	return c.refresh(ctx)
}

// fresh reports whether the last load is within the TTL; c.mu must be held.
// It does not look at the contents, so a scan that accepted nothing is
// cached like any other.
func (c *ConfigCache) fresh() bool { return c.store.now().Sub(c.loadedAt) < c.ttl }

// Invalidate forces a refresh on the next GetAll call.
func (c *ConfigCache) Invalidate() {
	c.mu.Lock()
	c.loadedAt = time.Time{}
	c.mu.Unlock()
}

func (c *ConfigCache) refresh(ctx context.Context) (map[string]*types.PipelineConfig, []RejectedConfig, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock.
	if !c.fresh() {
		configs, rejected, err := c.store.ScanConfigs(ctx)
		if err != nil {
			return nil, nil, err
		}
		c.configs, c.rejected, c.loadedAt = configs, rejected, c.store.now()
		c.logRejected(ctx)
	}
	return copyConfigs(c.configs), slices.Clone(c.rejected), nil
}

// logRejected warns once for each row the last scan rejected; c.mu must be held.
func (c *ConfigCache) logRejected(ctx context.Context) {
	logger := c.Logger
	if logger == nil {
		logger = slog.Default()
	}
	for _, r := range c.rejected {
		logger.WarnContext(ctx, "pipeline config rejected, skipping",
			"pk", r.PK, "pipelineId", r.PipelineID, "reason", r.Reason, "hash", r.Hash)
	}
}

// copyConfigs returns a deep copy of the config map so callers cannot
// mutate cached values.
func copyConfigs(src map[string]*types.PipelineConfig) map[string]*types.PipelineConfig {
	dst := make(map[string]*types.PipelineConfig, len(src))
	for k, v := range src {
		dst[k] = v.DeepCopy()
	}
	return dst
}
