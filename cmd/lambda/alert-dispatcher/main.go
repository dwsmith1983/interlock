// alert-dispatcher Lambda receives SQS messages containing EventBridge alert
// events and sends Slack notifications for pipeline observability.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	ilambda "github.com/dwsmith1983/interlock/internal/lambda"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	deps := &ilambda.Deps{
		SlackWebhookURL: os.Getenv("SLACK_WEBHOOK_URL"),
		HTTPClient:      &http.Client{Timeout: 10 * time.Second},
		Logger:          logger,
	}

	lambda.Start(func(ctx context.Context, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {
		return ilambda.HandleAlertDispatcher(ctx, deps, sqsEvent)
	})
}
