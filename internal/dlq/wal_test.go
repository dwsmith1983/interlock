package dlq_test

import (
	"context"
	"sync"
	"testing"

	"github.com/dwsmith1983/interlock/internal/dlq"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALManager_CrashRecovery(t *testing.T) {
	t.Skip("not yet implemented")
	ctx := context.Background()
	manager := dlq.NewWALManager("test_wal.log")

	for i := 0; i < 100; i++ {
		err := manager.Route(ctx, dlq.Record{ID: dlq.GenerateID()})
		require.NoError(t, err)
	}

	// Simulate crash: Close without Flush
	manager.Close()

	// Reopen
	recoveredManager := dlq.NewWALManager("test_wal.log")
	assert.Equal(t, int64(100), recoveredManager.Pending(), "All 100 records should be recovered")
}

func TestWALManager_AckAndRejectUpdatesPending(t *testing.T) {
	t.Skip("not yet implemented")
	ctx := context.Background()
	manager := dlq.NewWALManager("test_wal2.log")

	var ids []ulid.ULID
	for i := 0; i < 100; i++ {
		id := dlq.GenerateID()
		ids = append(ids, id)
		_ = manager.Route(ctx, dlq.Record{ID: id})
	}

	for i := 0; i < 50; i++ {
		_ = manager.Ack(ctx, ids[i])
	}
	for i := 50; i < 80; i++ {
		_ = manager.Reject(ctx, ids[i])
	}

	assert.Equal(t, int64(20), manager.Pending(), "Pending should be 20 after 50 acks and 30 rejects")
}

func TestWALManager_ConcurrentRouteAndAck(t *testing.T) {
	t.Skip("not yet implemented")
	ctx := context.Background()
	manager := dlq.NewWALManager("test_wal3.log")

	var wg sync.WaitGroup
	wg.Add(2)

	id := dlq.GenerateID()

	go func() {
		defer wg.Done()
		err := manager.Route(ctx, dlq.Record{ID: id})
		assert.NoError(t, err)
	}()

	go func() {
		defer wg.Done()
		err := manager.Ack(ctx, id)
		assert.NoError(t, err)
	}()

	wg.Wait()
}

func TestWALManager_ContextCancellationDuringRoute(t *testing.T) {
	t.Skip("not yet implemented")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	manager := dlq.NewWALManager("test_wal4.log")
	err := manager.Route(ctx, dlq.Record{ID: dlq.GenerateID()})
	assert.ErrorIs(t, err, context.Canceled, "Should not hang and return context canceled")
}
