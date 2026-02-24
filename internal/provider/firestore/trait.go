package firestore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PutTrait stores a trait evaluation result with TTL.
func (p *FirestoreProvider) PutTrait(ctx context.Context, pipelineID string, trait types.TraitEvaluation, ttl time.Duration) error {
	data, err := json.Marshal(trait)
	if err != nil {
		return err
	}

	id := docID(pipelinePK(pipelineID), traitSK(trait.TraitType))
	fields := map[string]interface{}{
		"data": string(data),
	}
	if ttl > 0 {
		fields["ttl"] = ttlEpoch(ttl)
	}

	_, err = p.coll().Doc(id).Set(ctx, fields)
	return err
}

// GetTrait retrieves a single trait evaluation.
func (p *FirestoreProvider) GetTrait(ctx context.Context, pipelineID, traitType string) (*types.TraitEvaluation, error) {
	id := docID(pipelinePK(pipelineID), traitSK(traitType))
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
	var te types.TraitEvaluation
	if err := json.Unmarshal([]byte(dataStr), &te); err != nil {
		return nil, err
	}
	return &te, nil
}

// GetTraits retrieves all trait evaluations for a pipeline.
func (p *FirestoreProvider) GetTraits(ctx context.Context, pipelineID string) ([]types.TraitEvaluation, error) {
	pk := pipelinePK(pipelineID)
	start, end := docIDPrefix(pk, prefixTrait)

	iter := p.coll().
		Where("__name__", ">=", p.coll().Doc(start)).
		Where("__name__", "<", p.coll().Doc(end)).
		Documents(ctx)
	defer iter.Stop()

	var traits []types.TraitEvaluation
	for {
		snap, err := iter.Next()
		if err != nil {
			break
		}

		ttlVal, _ := snapInt64(snap, "ttl")
		if isExpired(ttlVal) {
			continue
		}

		// Verify the doc ID has the TRAIT# prefix in its SK portion.
		sk := extractSK(snap.Ref.ID)
		if !strings.HasPrefix(sk, prefixTrait) {
			continue
		}

		dataStr, err := snapString(snap, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt trait data", "error", err)
			continue
		}

		var te types.TraitEvaluation
		if err := json.Unmarshal([]byte(dataStr), &te); err != nil {
			p.logger.Warn("skipping corrupt trait data", "error", err)
			continue
		}
		traits = append(traits, te)
	}
	return traits, nil
}

// traitDocIDForPipeline returns the prefix for trait documents for range query validation.
func traitDocIDForPipeline(pipelineID string) string {
	return fmt.Sprintf("%s%s%s", pipelinePK(pipelineID), sep, prefixTrait)
}
