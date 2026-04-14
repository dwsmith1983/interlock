// stream-router Lambda receives DynamoDB Stream events and routes them
// to the appropriate handler (sensor evaluation, config invalidation,
// job-log rerun/success). It starts Step Function executions when
// trigger conditions are met.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	ilambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/stream"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/telemetry"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := ilambda.ValidateEnv("stream-router"); err != nil {
		logger.Error("environment validation failed", "error", err)
		os.Exit(1)
	}

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

	tel, telShutdown, err := telemetry.NewTelemetry(context.Background(), "interlock-stream-router")
	if err != nil {
		logger.Error("failed to init telemetry", "error", err)
		os.Exit(1)
	}
	_ = telShutdown // shutdown reserved for Lambda container termination, not per-invocation

	deps := &ilambda.Deps{
		Store:           s,
		ConfigCache:     cache,
		SFNClient:       sfn.NewFromConfig(cfg),
		EventBridge:     eventbridge.NewFromConfig(cfg),
		StateMachineARN: os.Getenv("STATE_MACHINE_ARN"),
		EventBusName:    os.Getenv("EVENT_BUS_NAME"),
		Logger:          logger,
	}

	lambda.Start(func(ctx context.Context, event ilambda.StreamEvent) (events.DynamoDBEventResponse, error) {
		defer tel.Flush(context.Background())
		return stream.HandleStreamEvent(ctx, deps, event)
	})
}
