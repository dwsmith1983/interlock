//go:build integration

package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/interlock-systems/interlock/internal/provider/providertest"
	"github.com/interlock-systems/interlock/pkg/types"
)

func setupTestProvider(t *testing.T) *RedisProvider {
	t.Helper()
	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	ctx := context.Background()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	prefix := fmt.Sprintf("interlock-test-%d:", time.Now().UnixNano())
	prov := NewFromClient(client, prefix)

	t.Cleanup(func() {
		// Clean up test keys
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, prefix+"*", 100).Result()
			if err != nil {
				break
			}
			if len(keys) > 0 {
				client.Del(ctx, keys...)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		client.Close()
	})

	return prov
}

func setupTestProviderWithTTL(t *testing.T, retentionTTL time.Duration) *RedisProvider {
	t.Helper()
	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	ctx := context.Background()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	prefix := fmt.Sprintf("interlock-test-%d:", time.Now().UnixNano())
	prov := NewFromClient(client, prefix)
	prov.retentionTTL = retentionTTL

	t.Cleanup(func() {
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, prefix+"*", 100).Result()
			if err != nil {
				break
			}
			if len(keys) > 0 {
				client.Del(ctx, keys...)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		client.Close()
	})

	return prov
}

func TestConformance(t *testing.T) {
	prov := setupTestProvider(t)
	providertest.RunAll(t, prov)
}

// Redis-specific TTL tests that inspect internal Redis key expiry.

func TestRunStateTTL(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	// Terminal run should get retentionTTL (7 days default)
	terminalRun := types.RunState{
		RunID:      "ttl-terminal",
		PipelineID: "ttl-test",
		Status:     types.RunCompleted,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	require.NoError(t, prov.PutRunState(ctx, terminalRun))

	ttl := prov.client.TTL(ctx, prov.runKey("ttl-terminal")).Val()
	assert.InDelta(t, defaultRetentionTTL.Seconds(), ttl.Seconds(), 10,
		"terminal run should have ~7 day TTL")

	// Active run should get retentionTTL + 24h
	activeRun := types.RunState{
		RunID:      "ttl-active",
		PipelineID: "ttl-test",
		Status:     types.RunRunning,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	require.NoError(t, prov.PutRunState(ctx, activeRun))

	ttl = prov.client.TTL(ctx, prov.runKey("ttl-active")).Val()
	expected := defaultRetentionTTL + 24*time.Hour
	assert.InDelta(t, expected.Seconds(), ttl.Seconds(), 10,
		"active run should have ~8 day TTL")
}

func TestCASRefreshesTTL(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	run := types.RunState{
		RunID:      "cas-ttl-test",
		PipelineID: "ttl-test",
		Status:     types.RunRunning,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	require.NoError(t, prov.PutRunState(ctx, run))

	// CAS to terminal state
	completed := types.RunState{
		RunID:      "cas-ttl-test",
		PipelineID: "ttl-test",
		Status:     types.RunCompleted,
		Version:    2,
		CreatedAt:  run.CreatedAt,
		UpdatedAt:  time.Now(),
	}
	ok, err := prov.CompareAndSwapRunState(ctx, "cas-ttl-test", 1, completed)
	require.NoError(t, err)
	assert.True(t, ok)

	ttl := prov.client.TTL(ctx, prov.runKey("cas-ttl-test")).Val()
	assert.InDelta(t, defaultRetentionTTL.Seconds(), ttl.Seconds(), 10,
		"CAS to terminal state should set ~7 day TTL")
}

func TestRunLogTTL(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	entry := types.RunLogEntry{
		PipelineID:    "ttl-test",
		Date:          "2025-01-15",
		Status:        types.RunCompleted,
		AttemptNumber: 1,
		RunID:         "log-ttl-run",
		StartedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	require.NoError(t, prov.PutRunLog(ctx, entry))

	ttl := prov.client.TTL(ctx, prov.runLogKey("ttl-test", "2025-01-15")).Val()
	assert.InDelta(t, defaultRetentionTTL.Seconds(), ttl.Seconds(), 10,
		"run log entry should have ~7 day TTL")
}

func TestRerunTTL(t *testing.T) {
	prov := setupTestProvider(t)
	ctx := context.Background()

	record := types.RerunRecord{
		RerunID:      "rerun-ttl-test",
		PipelineID:   "ttl-test",
		OriginalDate: "2025-01-15",
		Reason:       "test",
		Status:       types.RunCompleted,
		RequestedAt:  time.Now(),
	}
	require.NoError(t, prov.PutRerun(ctx, record))

	ttl := prov.client.TTL(ctx, prov.rerunKey("rerun-ttl-test")).Val()
	assert.InDelta(t, defaultRetentionTTL.Seconds(), ttl.Seconds(), 10,
		"completed rerun should have ~7 day TTL")
}
