// Package redis implements the Provider interface using Redis/Valkey.
package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	luascripts "github.com/interlock-systems/interlock/internal/provider/redis/lua"
	"github.com/interlock-systems/interlock/pkg/types"
)

// RedisProvider implements the Provider interface backed by Redis/Valkey.
type RedisProvider struct {
	client    *goredis.Client
	prefix    string
	casScript *goredis.Script
}

// New creates a new RedisProvider.
func New(cfg *types.RedisConfig) *RedisProvider {
	client := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	prefix := cfg.KeyPrefix
	if prefix == "" {
		prefix = "interlock:"
	}

	return &RedisProvider{
		client:    client,
		prefix:    prefix,
		casScript: goredis.NewScript(luascripts.CompareAndSwap),
	}
}

// NewFromClient creates a RedisProvider from an existing client (useful for testing).
func NewFromClient(client *goredis.Client, prefix string) *RedisProvider {
	if prefix == "" {
		prefix = "interlock:"
	}
	return &RedisProvider{
		client:    client,
		prefix:    prefix,
		casScript: goredis.NewScript(luascripts.CompareAndSwap),
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
