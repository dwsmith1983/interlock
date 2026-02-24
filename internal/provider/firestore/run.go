package firestore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/dwsmith1983/interlock/internal/lifecycle"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// runKeyTTL returns the TTL for a run-related key based on status.
func (p *FirestoreProvider) runKeyTTL(status types.RunStatus) time.Duration {
	if lifecycle.IsTerminal(status) {
		return p.retentionTTL
	}
	return p.retentionTTL + 24*time.Hour
}

// PutRunState stores a run state using dual-write: truth item + list copy.
func (p *FirestoreProvider) PutRunState(ctx context.Context, run types.RunState) error {
	data, err := json.Marshal(run)
	if err != nil {
		return err
	}

	ttl := ttlEpoch(p.runKeyTTL(run.Status))

	truthID := docID(runPK(run.RunID), runTruthSK(run.RunID))
	listID := docID(pipelinePK(run.PipelineID), runListSK(run.CreatedAt, run.RunID))

	return p.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := tx.Set(p.coll().Doc(truthID), map[string]interface{}{
			"data":    string(data),
			"version": run.Version,
			"ttl":     ttl,
		}); err != nil {
			return err
		}
		return tx.Set(p.coll().Doc(listID), map[string]interface{}{
			"data": string(data),
			"ttl":  ttl,
		})
	})
}

// GetRunState retrieves a run state from the truth item.
func (p *FirestoreProvider) GetRunState(ctx context.Context, runID string) (*types.RunState, error) {
	id := docID(runPK(runID), runTruthSK(runID))
	snap, err := p.coll().Doc(id).Get(ctx)
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("run %q not found", runID)
		}
		return nil, err
	}

	ttlVal, _ := snapInt64(snap, "ttl")
	if isExpired(ttlVal) {
		return nil, fmt.Errorf("run %q not found", runID)
	}

	dataStr, err := snapString(snap, "data")
	if err != nil {
		return nil, err
	}
	var run types.RunState
	if err := json.Unmarshal([]byte(dataStr), &run); err != nil {
		return nil, err
	}
	return &run, nil
}

// ListRuns returns recent runs for a pipeline, newest first.
func (p *FirestoreProvider) ListRuns(ctx context.Context, pipelineID string, limit int) ([]types.RunState, error) {
	if limit <= 0 {
		limit = 10
	}

	pk := pipelinePK(pipelineID)
	start, end := docIDPrefix(pk, prefixRun)

	iter := p.coll().
		Where("__name__", ">=", p.coll().Doc(start)).
		Where("__name__", "<", p.coll().Doc(end)).
		OrderBy("__name__", firestore.Desc).
		Limit(limit).
		Documents(ctx)
	defer iter.Stop()

	var runs []types.RunState
	for {
		snap, err := iter.Next()
		if err != nil {
			break
		}

		ttlVal, _ := snapInt64(snap, "ttl")
		if isExpired(ttlVal) {
			continue
		}

		dataStr, err := snapString(snap, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt run data", "error", err)
			continue
		}
		var run types.RunState
		if err := json.Unmarshal([]byte(dataStr), &run); err != nil {
			p.logger.Warn("skipping corrupt run data", "error", err)
			continue
		}
		runs = append(runs, run)
	}
	return runs, nil
}

// CompareAndSwapRunState atomically updates a run state if the version matches.
func (p *FirestoreProvider) CompareAndSwapRunState(ctx context.Context, runID string, expectedVersion int, newState types.RunState) (bool, error) {
	data, err := json.Marshal(newState)
	if err != nil {
		return false, err
	}

	ttl := ttlEpoch(p.runKeyTTL(newState.Status))
	truthID := docID(runPK(runID), runTruthSK(runID))
	listID := docID(pipelinePK(newState.PipelineID), runListSK(newState.CreatedAt, runID))

	var swapped bool
	err = p.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		swapped = false

		snap, err := tx.Get(p.coll().Doc(truthID))
		if err != nil {
			return err
		}

		currentVersion := snapVersion(snap)
		if currentVersion != expectedVersion {
			// Version mismatch — CAS fails but not an error.
			return nil
		}

		swapped = true

		// Update truth item.
		if err := tx.Set(p.coll().Doc(truthID), map[string]interface{}{
			"data":    string(data),
			"version": newState.Version,
			"ttl":     ttl,
		}); err != nil {
			return err
		}

		// Best-effort update of list copy.
		return tx.Set(p.coll().Doc(listID), map[string]interface{}{
			"data": string(data),
			"ttl":  ttl,
		})
	})
	if err != nil {
		return false, err
	}
	return swapped, nil
}

// snapVersion extracts the version field from a Firestore document snapshot.
func snapVersion(snap *firestore.DocumentSnapshot) int {
	raw, err := snap.DataAt("version")
	if err != nil {
		return 0
	}
	switch v := raw.(type) {
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}
