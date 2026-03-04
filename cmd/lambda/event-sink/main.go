// event-sink Lambda receives EventBridge events from the interlock custom bus
// and writes them to the centralized DynamoDB events table for observability.
package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"

	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

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

	ttlDays := 90
	if v := os.Getenv("EVENTS_TTL_DAYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			ttlDays = n
		}
	}

	ddbClient := dynamodb.NewFromConfig(cfg)
	s := &store.Store{
		Client:      ddbClient,
		EventsTable: os.Getenv("EVENTS_TABLE"),
	}

	deps := &ilambda.Deps{
		Store:         s,
		EventsTTLDays: ttlDays,
		Logger:        logger,
	}

	lambda.Start(func(ctx context.Context, input ilambda.EventBridgeInput) error {
		return ilambda.HandleEventSink(ctx, deps, input)
	})
}
