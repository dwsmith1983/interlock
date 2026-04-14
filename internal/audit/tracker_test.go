package audit

import (
	"sync"
	"testing"

	"github.com/dwsmith1983/interlock/internal/dlq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReconciliation(t *testing.T) {
	tracker := NewTracker()

	ids := make([]dlq.Record, 100)
	for i := range ids {
		ids[i].ID = dlq.GenerateID()
		require.NoError(t, tracker.Track(ids[i].ID))
	}

	// Ack the first 95.
	for i := 0; i < 95; i++ {
		err := tracker.Transition(ids[i].ID, dlq.StateAcked)
		require.NoError(t, err)
	}

	report := tracker.Reconcile()

	assert.Equal(t, 100, report.TotalTracked)
	assert.Equal(t, 95, report.Acked)
	assert.Equal(t, 0, report.Rejected)
	assert.Len(t, report.PendingRecords, 5)
}

func TestInvalidTransition_AckedToPending(t *testing.T) {
	tracker := NewTracker()
	id := dlq.GenerateID()
	require.NoError(t, tracker.Track(id))

	err := tracker.Transition(id, dlq.StateAcked)
	require.NoError(t, err)

	err = tracker.Transition(id, dlq.StatePending)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid transition")
}

func TestUnknownIDTransition(t *testing.T) {
	tracker := NewTracker()
	id := dlq.GenerateID()

	err := tracker.Transition(id, dlq.StateAcked)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown record")
}

func TestConcurrentAccess(t *testing.T) {
	tracker := NewTracker()
	const n = 200

	ids := make([]dlq.Record, n)
	for i := range ids {
		ids[i].ID = dlq.GenerateID()
	}

	var wg sync.WaitGroup

	// Half goroutines track, half transition.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = tracker.Track(ids[idx].ID)
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			state := dlq.StateAcked
			if idx%2 == 0 {
				state = dlq.StateRejected
			}
			_ = tracker.Transition(ids[idx].ID, state)
		}(i)
	}
	wg.Wait()

	report := tracker.Reconcile()
	assert.Equal(t, n, report.TotalTracked)
}
