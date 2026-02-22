// run-checker Lambda polls external systems for job status.
package main

import (
	"context"
	"log/slog"
	"os"
	"sync"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/trigger"
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

// handleRunCheck implements the core run-checker logic.
func handleRunCheck(ctx context.Context, d *intlambda.Deps, req intlambda.RunCheckRequest) (intlambda.RunCheckResponse, error) {
	result, err := d.Runner.CheckStatus(ctx, req.TriggerType, req.Metadata, req.Headers)
	if err != nil {
		d.Logger.Error("status check failed",
			"pipeline", req.PipelineID,
			"runID", req.RunID,
			"triggerType", req.TriggerType,
			"error", err,
		)
		return intlambda.RunCheckResponse{
			State:   trigger.RunCheckFailed,
			Message: err.Error(),
		}, nil
	}

	return intlambda.RunCheckResponse{
		State:   result.State,
		Message: result.Message,
	}, nil
}

func handler(ctx context.Context, req intlambda.RunCheckRequest) (intlambda.RunCheckResponse, error) {
	d, err := getDeps()
	if err != nil {
		return intlambda.RunCheckResponse{}, err
	}
	return handleRunCheck(ctx, d, req)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	awslambda.Start(handler)
}
