// alert-dispatcher Lambda receives SQS messages containing EventBridge alert
// events and sends Slack notifications for pipeline observability.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	ilambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := ilambda.ValidateEnv("alert-dispatcher"); err != nil {
		logger.Error("environment validation failed", "error", err)
		os.Exit(1)
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		logger.Error("load AWS config", "error", err)
		os.Exit(1)
	}

	ttl := 90
	if v := os.Getenv("EVENTS_TTL_DAYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			ttl = n
		}
	}

	deps := &ilambda.Deps{
		Store: &store.Store{
			Client:      dynamodb.NewFromConfig(cfg),
			EventsTable: os.Getenv("EVENTS_TABLE"),
		},
		SlackBotToken:  os.Getenv("SLACK_BOT_TOKEN"),
		SlackChannelID: os.Getenv("SLACK_CHANNEL_ID"),
		EventsTTLDays:  ttl,
		HTTPClient:     &http.Client{Timeout: 10 * time.Second},
		Logger:         logger,
	}

	lambda.Start(func(ctx context.Context, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {
		return ilambda.HandleAlertDispatcher(ctx, deps, sqsEvent)
	})
}
