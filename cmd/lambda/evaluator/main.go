// evaluator Lambda evaluates a single trait for a pipeline.
package main

import (
	"context"
	"log/slog"
	"os"
	"sync"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/interlock-systems/interlock/internal/archetype"
	intlambda "github.com/interlock-systems/interlock/internal/lambda"
)

var (
	deps     *intlambda.Deps
	depsOnce sync.Once
	depsErr  error
)

func getDeps() (*intlambda.Deps, error) {
	depsOnce.Do(func() {
		deps, depsErr = intlambda.Init(context.Background())
	})
	return deps, depsErr
}

// handleEvaluate evaluates a single trait and returns the result.
func handleEvaluate(ctx context.Context, d *intlambda.Deps, req intlambda.EvaluatorRequest) (intlambda.EvaluatorResponse, error) {
	trait := archetype.ResolvedTrait{
		Type:      req.TraitType,
		Evaluator: req.Evaluator,
		Config:    req.Config,
		Timeout:   req.Timeout,
		TTL:       req.TTL,
		Required:  req.Required,
	}

	result, err := d.Engine.EvaluateTrait(ctx, req.PipelineID, trait)
	if err != nil {
		d.Logger.Error("trait evaluation failed",
			"pipeline", req.PipelineID,
			"trait", req.TraitType,
			"error", err,
		)
		return intlambda.EvaluatorResponse{
			TraitType: req.TraitType,
			Status:    "FAIL",
			Reason:    err.Error(),
			Required:  req.Required,
		}, nil
	}

	return intlambda.EvaluatorResponse{
		TraitType: req.TraitType,
		Status:    result.Status,
		Value:     result.Value,
		Reason:    result.Reason,
		Required:  req.Required,
	}, nil
}

func handler(ctx context.Context, req intlambda.EvaluatorRequest) (intlambda.EvaluatorResponse, error) {
	d, err := getDeps()
	if err != nil {
		return intlambda.EvaluatorResponse{}, err
	}
	return handleEvaluate(ctx, d, req)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	awslambda.Start(handler)
}
