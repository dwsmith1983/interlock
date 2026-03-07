// orchestrator Lambda is the multi-mode "brain" of the Step Function
// workflow. It handles evaluate, trigger, check-job, and post-run modes
// delegated by the state machine.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	ilambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// statusCheckerAdapter wraps trigger.Runner to satisfy ilambda.StatusChecker.
type statusCheckerAdapter struct {
	runner *trigger.Runner
}

func (a *statusCheckerAdapter) CheckStatus(ctx context.Context, triggerType types.TriggerType, metadata map[string]interface{}, headers map[string]string) (ilambda.StatusResult, error) {
	result, err := a.runner.CheckStatus(ctx, triggerType, metadata, headers)
	if err != nil {
		return ilambda.StatusResult{}, err
	}
	return ilambda.StatusResult{
		State:           string(result.State),
		Message:         result.Message,
		FailureCategory: result.FailureCategory,
	}, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		logger.Error("failed to load AWS config", "error", err)
		os.Exit(1)
	}

	ddbClient := dynamodb.NewFromConfig(cfg)
	s := &store.Store{
		Client:       ddbClient,
		ControlTable: os.Getenv("CONTROL_TABLE"),
		JobLogTable:  os.Getenv("JOBLOG_TABLE"),
		RerunTable:   os.Getenv("RERUN_TABLE"),
	}
	cache := store.NewConfigCache(s, 5*time.Minute)

	runner := trigger.NewRunner()
	deps := &ilambda.Deps{
		Store:           s,
		ConfigCache:     cache,
		SFNClient:       sfn.NewFromConfig(cfg),
		EventBridge:     eventbridge.NewFromConfig(cfg),
		TriggerRunner:   runner,
		StatusChecker:   &statusCheckerAdapter{runner: runner},
		StateMachineARN: os.Getenv("STATE_MACHINE_ARN"),
		EventBusName:    os.Getenv("EVENT_BUS_NAME"),
		Logger:          logger,
	}

	lambda.Start(func(ctx context.Context, input ilambda.OrchestratorInput) (ilambda.OrchestratorOutput, error) {
		return ilambda.HandleOrchestrator(ctx, deps, input)
	})
}
