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
	token, err := prov.AcquireLock(ctx, "ct-lock:pipe1:daily", 30*time.Second)
	require.NoError(t, err)
	assert.NotEmpty(t, token, "expected non-empty token on acquire")

	// Double acquire fails
	token2, err := prov.AcquireLock(ctx, "ct-lock:pipe1:daily", 30*time.Second)
	require.NoError(t, err)
	assert.Empty(t, token2, "expected empty token on double-acquire")

	// Different key succeeds
	token3, err := prov.AcquireLock(ctx, "ct-lock:pipe2:daily", 30*time.Second)
	require.NoError(t, err)
	assert.NotEmpty(t, token3, "expected non-empty token for different key")

	// Release with correct token
	err = prov.ReleaseLock(ctx, "ct-lock:pipe1:daily", token)
	require.NoError(t, err)

	// Re-acquire after release
	token4, err := prov.AcquireLock(ctx, "ct-lock:pipe1:daily", 30*time.Second)
	require.NoError(t, err)
	assert.NotEmpty(t, token4, "expected non-empty token after release")

	// Release with wrong token should not release
	err = prov.ReleaseLock(ctx, "ct-lock:pipe1:daily", "wrong-token")
	require.NoError(t, err)

	// Lock should still be held
	token5, err := prov.AcquireLock(ctx, "ct-lock:pipe1:daily", 30*time.Second)
	require.NoError(t, err)
	assert.Empty(t, token5, "expected lock to still be held after wrong-token release")
}

// TestLockExpiry verifies locks expire after their TTL.
func TestLockExpiry(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	// Acquire with short TTL
	token, err := prov.AcquireLock(ctx, "ct-expiring-lock", 2*time.Second)
	require.NoError(t, err)
	assert.NotEmpty(t, token)

	// Immediately re-acquire fails
	token2, err := prov.AcquireLock(ctx, "ct-expiring-lock", 2*time.Second)
	require.NoError(t, err)
	assert.Empty(t, token2)

	// Wait for TTL to expire
	time.Sleep(3 * time.Second)

	// Re-acquire succeeds after expiry
	token3, err := prov.AcquireLock(ctx, "ct-expiring-lock", 30*time.Second)
	require.NoError(t, err)
	assert.NotEmpty(t, token3)
}
