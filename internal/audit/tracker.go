package audit

import (
	"fmt"
	"sync"

	"github.com/dwsmith1983/interlock/internal/dlq"
	"github.com/oklog/ulid/v2"
)

// Tracker monitors DLQ records through their lifecycle to detect data loss.
type Tracker struct {
	mu     sync.RWMutex
	states map[ulid.ULID]dlq.RecordState
}

// NewTracker returns a Tracker ready for use.
func NewTracker() *Tracker {
	return &Tracker{
		states: make(map[ulid.ULID]dlq.RecordState),
	}
}

// Track registers a record as PENDING. Returns an error if the ID is already tracked.
func (t *Tracker) Track(id ulid.ULID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.states[id]; exists {
		return fmt.Errorf("record %s already tracked", id)
	}
	t.states[id] = dlq.StatePending
	return nil
}

// Transition moves a tracked record to the given state.
// It returns an error for unknown IDs or invalid transitions.
func (t *Tracker) Transition(id ulid.ULID, to dlq.RecordState) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	current, ok := t.states[id]
	if !ok {
		return fmt.Errorf("unknown record %s", id)
	}

	if current != dlq.StatePending {
		return fmt.Errorf("invalid transition from %s to %s for record %s", current, to, id)
	}

	if to != dlq.StateAcked && to != dlq.StateRejected {
		return fmt.Errorf("invalid target state %s for record %s", to, id)
	}

	t.states[id] = to
	return nil
}

// ReconciliationReport summarises the current tracker state.
type ReconciliationReport struct {
	PendingRecords []ulid.ULID
	TotalTracked   int
	Acked          int
	Rejected       int
}

// Reconcile returns a report of current state counts.
// Records still in PENDING are listed as potential data loss.
func (t *Tracker) Reconcile() ReconciliationReport {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var report ReconciliationReport
	report.TotalTracked = len(t.states)

	for id, state := range t.states {
		switch state {
		case dlq.StatePending:
			report.PendingRecords = append(report.PendingRecords, id)
		case dlq.StateAcked:
			report.Acked++
		case dlq.StateRejected:
			report.Rejected++
		}
	}

	return report
}
