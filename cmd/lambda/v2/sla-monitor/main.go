// sla-monitor Lambda handles SLA calculations and alert firing for the v2
// Step Function workflow. It supports two modes: "calculate" (compute warning
// and breach times) and "fire-alert" (publish SLA events to EventBridge).
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"

	v2lambda "github.com/dwsmith1983/interlock/internal/lambda/v2"
	store "github.com/dwsmith1983/interlock/internal/store/v2"
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

	deps := &v2lambda.Deps{
		Store:        s,
		EventBridge:  eventbridge.NewFromConfig(cfg),
		EventBusName: os.Getenv("EVENT_BUS_NAME"),
		Logger:       logger,
	}

	lambda.Start(func(ctx context.Context, input v2lambda.SLAMonitorInput) (v2lambda.SLAMonitorOutput, error) {
		return v2lambda.HandleSLAMonitor(ctx, deps, input)
	})
}
