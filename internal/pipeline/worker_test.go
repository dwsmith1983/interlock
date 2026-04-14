package pipeline_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"interlock/internal/pipeline"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestWorkerPool_RespectsMaxConcurrency(t *testing.T) {
	t.Error("RED: implementation missing")
	ctx := context.Background()
	pool := pipeline.NewWorkerPool(ctx, 2)

	var activeWorkers int32
	var maxObserved int32

	for i := 0; i < 5; i++ {
		err := pool.Submit(func(c context.Context) error {
			current := atomic.AddInt32(&activeWorkers, 1)
			for {
				max := atomic.LoadInt32(&maxObserved)
				if current > max {
					if atomic.CompareAndSwapInt32(&maxObserved, max, current) {
						break
					}
				} else {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt32(&activeWorkers, -1)
			return nil
		})
		require.NoError(t, err)
	}

	err := pool.Wait()
	require.NoError(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&maxObserved), "Should not exceed max concurrency of 2")
}

func TestWorkerPool_CancelMidFlight(t *testing.T) {
	t.Error("RED: implementation missing")
	ctx, cancel := context.WithCancel(context.Background())
	pool := pipeline.NewWorkerPool(ctx, 2)

	err := pool.Submit(func(c context.Context) error {
		cancel() // cancel during execution
		<-c.Done()
		return c.Err()
	})
	require.NoError(t, err)

	err = pool.Submit(func(c context.Context) error {
		return nil
	})
	assert.Error(t, err, "Subsequent submits should fail after context cancellation")

	waitErr := pool.Wait()
	assert.ErrorIs(t, waitErr, context.Canceled)
}
