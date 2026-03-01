// watchdog Lambda runs periodic health checks for the pipeline framework.
// It detects stale trigger executions (Step Function timeouts) and missed
// cron schedules, publishing events to EventBridge. Invoked by EventBridge
// on a regular interval (e.g. every 5 minutes).
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

	ilambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
)

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

	deps := &ilambda.Deps{
		Store:        s,
		ConfigCache:  cache,
		EventBridge:  eventbridge.NewFromConfig(cfg),
		EventBusName: os.Getenv("EVENT_BUS_NAME"),
		Logger:       logger,
	}

	lambda.Start(func(ctx context.Context) error {
		return ilambda.HandleWatchdog(ctx, deps)
	})
}
