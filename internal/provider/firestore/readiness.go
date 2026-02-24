package firestore

import (
	"context"
	"encoding/json"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PutReadiness stores a readiness result with TTL.
func (p *FirestoreProvider) PutReadiness(ctx context.Context, result types.ReadinessResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}

	id := docID(pipelinePK(result.PipelineID), readinessSK())
	_, err = p.coll().Doc(id).Set(ctx, map[string]interface{}{
		"data": string(data),
		"ttl":  ttlEpoch(p.readinessTTL),
	})
	return err
}

// GetReadiness retrieves a cached readiness result.
func (p *FirestoreProvider) GetReadiness(ctx context.Context, pipelineID string) (*types.ReadinessResult, error) {
	id := docID(pipelinePK(pipelineID), readinessSK())
	snap, err := p.coll().Doc(id).Get(ctx)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	ttlVal, _ := snapInt64(snap, "ttl")
	if isExpired(ttlVal) {
		return nil, nil
	}

	dataStr, err := snapString(snap, "data")
	if err != nil {
		return nil, err
	}
	var result types.ReadinessResult
	if err := json.Unmarshal([]byte(dataStr), &result); err != nil {
		return nil, err
	}
	return &result, nil
}
