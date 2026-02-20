// Package engine implements the core STAMP readiness logic.
package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/interlock-systems/interlock/internal/archetype"
	"github.com/interlock-systems/interlock/internal/evaluator"
	"github.com/interlock-systems/interlock/internal/provider"
	"github.com/interlock-systems/interlock/pkg/types"
)

// Engine is the core readiness evaluation engine. It resolves archetypes,
// runs evaluators, stores results, and determines pipeline readiness.
type Engine struct {
	provider provider.Provider
	registry *archetype.Registry
	runner   *evaluator.Runner
	alertFn  func(types.Alert)
}

// New creates a new Engine.
func New(p provider.Provider, reg *archetype.Registry, runner *evaluator.Runner, alertFn func(types.Alert)) *Engine {
	return &Engine{
		provider: p,
		registry: reg,
		runner:   runner,
		alertFn:  alertFn,
	}
}

// Evaluate runs all trait evaluators for a pipeline and determines readiness.
func (e *Engine) Evaluate(ctx context.Context, pipelineID string) (*types.ReadinessResult, error) {
	pipeline, err := e.provider.GetPipeline(ctx, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("loading pipeline %q: %w", pipelineID, err)
	}

	arch, err := e.registry.Get(pipeline.Archetype)
	if err != nil {
		return nil, fmt.Errorf("resolving archetype %q: %w", pipeline.Archetype, err)
	}

	resolved := archetype.ResolveTraits(arch, pipeline)

	type evalResult struct {
		trait archetype.ResolvedTrait
		eval  *types.TraitEvaluation
		err   error
	}

	results := make([]evalResult, len(resolved))
	var wg sync.WaitGroup

	for i, rt := range resolved {
		wg.Add(1)
		go func(idx int, trait archetype.ResolvedTrait) {
			defer wg.Done()

			te, err := e.evaluateTrait(ctx, pipelineID, trait)
			results[idx] = evalResult{trait: trait, eval: te, err: err}
		}(i, rt)
	}

	wg.Wait()

	// Collect results and determine readiness
	var (
		traitEvals []types.TraitEvaluation
		blocking   []string
		allPass    = true
	)

	for _, r := range results {
		if r.err != nil {
			te := types.TraitEvaluation{
				PipelineID:  pipelineID,
				TraitType:   r.trait.Type,
				Status:      types.TraitFail,
				Reason:      fmt.Sprintf("evaluation error: %v", r.err),
				EvaluatedAt: time.Now(),
			}
			traitEvals = append(traitEvals, te)
			if r.trait.Required {
				blocking = append(blocking, r.trait.Type)
				allPass = false
			}
			e.fireAlert(types.Alert{
				Level:      "error",
				PipelineID: pipelineID,
				TraitType:  r.trait.Type,
				Message:    fmt.Sprintf("Trait %s failed for %s: %v", r.trait.Type, pipelineID, r.err),
				Timestamp:  time.Now(),
			})
			continue
		}

		traitEvals = append(traitEvals, *r.eval)

		if r.eval.Status != types.TraitPass && r.trait.Required {
			blocking = append(blocking, r.trait.Type)
			allPass = false
			e.fireAlert(types.Alert{
				Level:      "warning",
				PipelineID: pipelineID,
				TraitType:  r.trait.Type,
				Message:    fmt.Sprintf("Trait %s failed for %s: %s", r.trait.Type, pipelineID, r.eval.Reason),
				Timestamp:  time.Now(),
			})
		}
	}

	status := types.Ready
	if !allPass {
		status = types.NotReady
	}

	result := &types.ReadinessResult{
		PipelineID:  pipelineID,
		Status:      status,
		Traits:      traitEvals,
		Blocking:    blocking,
		EvaluatedAt: time.Now(),
	}

	if err := e.provider.PutReadiness(ctx, *result); err != nil {
		return result, fmt.Errorf("storing readiness result: %w", err)
	}

	_ = e.provider.AppendEvent(ctx, types.Event{
		Kind:       types.EventReadinessChecked,
		PipelineID: pipelineID,
		Status:     string(status),
		Details:    map[string]interface{}{"blocking": blocking},
		Timestamp:  time.Now(),
	})

	if status == types.NotReady {
		e.fireAlert(types.Alert{
			Level:      "warning",
			PipelineID: pipelineID,
			Message:    fmt.Sprintf("Pipeline %s blocked by: %v", pipelineID, blocking),
			Timestamp:  time.Now(),
		})
	}

	return result, nil
}

// CheckReadiness checks cached trait state without running evaluators.
func (e *Engine) CheckReadiness(ctx context.Context, pipelineID string) (*types.ReadinessResult, error) {
	pipeline, err := e.provider.GetPipeline(ctx, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("loading pipeline %q: %w", pipelineID, err)
	}

	arch, err := e.registry.Get(pipeline.Archetype)
	if err != nil {
		return nil, fmt.Errorf("resolving archetype %q: %w", pipeline.Archetype, err)
	}

	resolved := archetype.ResolveTraits(arch, pipeline)

	var (
		traitEvals []types.TraitEvaluation
		blocking   []string
		allPass    = true
	)

	for _, rt := range resolved {
		existing, err := e.provider.GetTrait(ctx, pipelineID, rt.Type)
		if err != nil || existing == nil {
			// Missing = STALE
			traitEvals = append(traitEvals, types.TraitEvaluation{
				PipelineID:  pipelineID,
				TraitType:   rt.Type,
				Status:      types.TraitStale,
				Reason:      "no evaluation found or expired",
				EvaluatedAt: time.Now(),
			})
			if rt.Required {
				blocking = append(blocking, rt.Type+" (STALE)")
				allPass = false
			}
			continue
		}

		traitEvals = append(traitEvals, *existing)
		if existing.Status != types.TraitPass && rt.Required {
			blocking = append(blocking, rt.Type)
			allPass = false
		}
	}

	status := types.Ready
	if !allPass {
		status = types.NotReady
	}

	return &types.ReadinessResult{
		PipelineID:  pipelineID,
		Status:      status,
		Traits:      traitEvals,
		Blocking:    blocking,
		EvaluatedAt: time.Now(),
	}, nil
}

func (e *Engine) evaluateTrait(ctx context.Context, pipelineID string, trait archetype.ResolvedTrait) (*types.TraitEvaluation, error) {
	if trait.Evaluator == "" {
		return &types.TraitEvaluation{
			PipelineID:  pipelineID,
			TraitType:   trait.Type,
			Status:      types.TraitFail,
			Reason:      "no evaluator configured",
			EvaluatedAt: time.Now(),
		}, nil
	}

	input := types.EvaluatorInput{
		PipelineID: pipelineID,
		TraitType:  trait.Type,
		Config:     trait.Config,
	}

	timeout := time.Duration(trait.Timeout) * time.Second
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	output, err := e.runner.Run(ctx, trait.Evaluator, input, timeout)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	te := &types.TraitEvaluation{
		PipelineID:  pipelineID,
		TraitType:   trait.Type,
		Status:      output.Status,
		Value:       output.Value,
		Reason:      output.Reason,
		EvaluatedAt: now,
	}

	ttl := time.Duration(trait.TTL) * time.Second
	if ttl > 0 {
		exp := now.Add(ttl)
		te.ExpiresAt = &exp
	}

	if err := e.provider.PutTrait(ctx, pipelineID, *te, ttl); err != nil {
		return te, fmt.Errorf("storing trait result: %w", err)
	}

	_ = e.provider.AppendEvent(ctx, types.Event{
		Kind:       types.EventTraitEvaluated,
		PipelineID: pipelineID,
		TraitType:  trait.Type,
		Status:     string(output.Status),
		Message:    output.Reason,
		Details:    output.Value,
		Timestamp:  now,
	})

	return te, nil
}

func (e *Engine) fireAlert(alert types.Alert) {
	if e.alertFn != nil {
		e.alertFn(alert)
	}
}
