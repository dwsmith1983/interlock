// Package redis implements the Provider interface using Redis/Valkey.
package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dwsmith1983/interlock/internal/provider"
	luascripts "github.com/dwsmith1983/interlock/internal/provider/redis/lua"
)

// Compile-time interface satisfaction check.
var _ provider.Provider = (*RedisProvider)(nil)

// Redis storage defaults.
const (
	defaultReadinessTTL   = 5 * time.Minute
	defaultRetentionTTL   = 7 * 24 * time.Hour // 7 days
	defaultRunIndexLimit  = 100
	defaultEventStreamMax = 10000
)

// RedisProvider implements the Provider interface backed by Redis/Valkey.
type RedisProvider struct {
	client            *goredis.Client
	prefix            string
	casScript         *goredis.Script
	releaseLockScript *goredis.Script
	logger            *slog.Logger
	readinessTTL      time.Duration
	retentionTTL      time.Duration
	runIndexLimit     int64
	eventStreamMax    int64
}

// New creates a new RedisProvider.
func New(cfg *Config) *RedisProvider {
	client := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	prefix := cfg.KeyPrefix
	if prefix == "" {
		prefix = "interlock:"
	}

	readinessTTL := defaultReadinessTTL
	if cfg.ReadinessTTL != "" {
		if d, err := time.ParseDuration(cfg.ReadinessTTL); err == nil && d > 0 {
			readinessTTL = d
		}
	}
	retentionTTL := defaultRetentionTTL
	if cfg.RetentionTTL != "" {
		if d, err := time.ParseDuration(cfg.RetentionTTL); err == nil && d > 0 {
			retentionTTL = d
		}
	}
	runIndexLimit := int64(defaultRunIndexLimit)
	if cfg.RunIndexLimit > 0 {
		runIndexLimit = int64(cfg.RunIndexLimit)
	}
	eventStreamMax := int64(defaultEventStreamMax)
	if cfg.EventStreamMax > 0 {
		eventStreamMax = cfg.EventStreamMax
	}

	return &RedisProvider{
		client:            client,
		prefix:            prefix,
		casScript:         goredis.NewScript(luascripts.CompareAndSwap),
		releaseLockScript: goredis.NewScript(luascripts.ReleaseLock),
		logger:            slog.Default(),
		readinessTTL:      readinessTTL,
		retentionTTL:      retentionTTL,
		runIndexLimit:     runIndexLimit,
		eventStreamMax:    eventStreamMax,
	}
}

// NewFromClient creates a RedisProvider from an existing client (useful for testing).
func NewFromClient(client *goredis.Client, prefix string) *RedisProvider {
	if prefix == "" {
		prefix = "interlock:"
	}
	return &RedisProvider{
		client:            client,
		prefix:            prefix,
		casScript:         goredis.NewScript(luascripts.CompareAndSwap),
		releaseLockScript: goredis.NewScript(luascripts.ReleaseLock),
		logger:            slog.Default(),
		readinessTTL:      defaultReadinessTTL,
		retentionTTL:      defaultRetentionTTL,
		runIndexLimit:     defaultRunIndexLimit,
		eventStreamMax:    defaultEventStreamMax,
	}
}

// Start initializes the provider connection.
func (p *RedisProvider) Start(ctx context.Context) error {
	return p.Ping(ctx)
}

// Stop closes the provider connection.
func (p *RedisProvider) Stop(_ context.Context) error {
	return p.client.Close()
}

// Ping checks connectivity to the Redis server.
func (p *RedisProvider) Ping(ctx context.Context) error {
	if err := p.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping failed: %w", err)
	}
	return nil
}

// Client returns the underlying Redis client (for advanced usage/testing).
func (p *RedisProvider) Client() *goredis.Client {
	return p.client
}
