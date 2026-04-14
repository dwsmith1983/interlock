package audit_test

import (
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"interlock/internal/audit"
	"interlock/internal/dlq"
)

func TestTracker_Reconciliation(t *testing.T) {
	t.Error("RED: implementation missing")
	tracker := audit.NewTracker()

	var ids []ulid.ULID
	for i := 0; i < 100; i++ {
		id := dlq.GenerateID()
		ids = append(ids, id)
		tracker.Track(id)
	}

	for i := 0; i < 95; i++ {
		err := tracker.Transition(ids[i], dlq.StateAcked)
		require.NoError(t, err)
	}

	report := tracker.Reconcile()
	assert.Equal(t, 5, len(report.PendingRecords), "Reconciliation should report 5 pending records representing data loss")
}

func TestTracker_InvalidTransitions(t *testing.T) {
	t.Error("RED: implementation missing")
	tracker := audit.NewTracker()
	id := dlq.GenerateID()
	tracker.Track(id)

	err := tracker.Transition(id, dlq.StateAcked)
	require.NoError(t, err)

	err = tracker.Transition(id, dlq.StatePending)
	assert.Error(t, err, "Transitioning from Processed/Acked to Pending should be invalid")
}
