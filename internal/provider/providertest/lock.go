package providertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/provider"
)

// TestLocking verifies acquire, double-acquire, different-key, release, re-acquire.
func TestLocking(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	// Acquire
	ok, err := prov.AcquireLock(ctx, "ct-lock:pipe1:daily", 30*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)

	// Double acquire fails
	ok, err = prov.AcquireLock(ctx, "ct-lock:pipe1:daily", 30*time.Second)
	require.NoError(t, err)
	assert.False(t, ok)

	// Different key succeeds
	ok, err = prov.AcquireLock(ctx, "ct-lock:pipe2:daily", 30*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)

	// Release
	err = prov.ReleaseLock(ctx, "ct-lock:pipe1:daily")
	require.NoError(t, err)

	// Re-acquire after release
	ok, err = prov.AcquireLock(ctx, "ct-lock:pipe1:daily", 30*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)
}

// TestLockExpiry verifies locks expire after their TTL.
func TestLockExpiry(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	// Acquire with short TTL
	ok, err := prov.AcquireLock(ctx, "ct-expiring-lock", 2*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)

	// Immediately re-acquire fails
	ok, err = prov.AcquireLock(ctx, "ct-expiring-lock", 2*time.Second)
	require.NoError(t, err)
	assert.False(t, ok)

	// Wait for TTL to expire
	time.Sleep(3 * time.Second)

	// Re-acquire succeeds after expiry
	ok, err = prov.AcquireLock(ctx, "ct-expiring-lock", 30*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)
}
