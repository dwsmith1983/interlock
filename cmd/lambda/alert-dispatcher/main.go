// alert-dispatcher Lambda receives SQS messages containing EventBridge alert
// events and sends Slack notifications for pipeline observability.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	ilambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
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
		Client:      ddbClient,
		EventsTable: os.Getenv("EVENTS_TABLE"),
	}

	deps := &ilambda.Deps{
		Store:           s,
		SlackWebhookURL: os.Getenv("SLACK_WEBHOOK_URL"),
		HTTPClient:      &http.Client{},
		Logger:          logger,
	}

	lambda.Start(func(ctx context.Context, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {
		return ilambda.HandleAlertDispatcher(ctx, deps, sqsEvent)
	})
}
