// alert-dispatcher Lambda receives SQS messages containing EventBridge alert
// events and sends Slack notifications for pipeline observability.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/dwsmith1983/interlock/internal/client"
	ilambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/alert"
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
		HTTPClient:     client.NewBreakerClient(&http.Client{Timeout: 10 * time.Second}, client.DefaultBreakerConfig("slack-api")),
		Logger:         logger,
	}

	// Override Slack token from Secrets Manager when configured.
	if secretARN := os.Getenv("SLACK_SECRET_ARN"); secretARN != "" {
		smClient := secretsmanager.NewFromConfig(cfg)
		out, err := smClient.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
			SecretId: &secretARN,
		})
		if err != nil {
			logger.Error("failed to read Slack secret from Secrets Manager", "arn", secretARN, "error", err)
			os.Exit(1)
		}
		if out.SecretString == nil {
			logger.Error("Secrets Manager returned nil SecretString", "arn", secretARN)
			os.Exit(1)
		}
		token := strings.TrimSpace(*out.SecretString)
		if !strings.HasPrefix(token, "xoxb-") && !strings.HasPrefix(token, "xoxe-") {
			logger.Warn("SLACK_SECRET_ARN value does not look like a Slack bot token (expected xoxb-/xoxe- prefix)")
		}
		deps.SlackBotToken = token
	}

	loggingOnly, err := slackMode(deps.SlackBotToken, deps.SlackChannelID, os.Getenv("SLACK_SECRET_ARN") != "")
	if err != nil {
		logger.Error("invalid Slack configuration", "error", err)
		os.Exit(1)
	}
	if loggingOnly {
		if deps.SlackChannelID != "" {
			logger.Warn(loggingOnlyWarning(deps.SlackChannelID), "channel", deps.SlackChannelID)
		} else {
			logger.Warn(loggingOnlyWarning(deps.SlackChannelID))
		}
	}

	lambda.Start(func(ctx context.Context, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {
		return alert.HandleAlertDispatcher(ctx, deps, sqsEvent)
	})
}

// loggingOnlyWarning returns the startup warning for logging-only mode. A
// channel without a token is likely an operator slip, so that case says
// plainly that alerts are not reaching Slack.
func loggingOnlyWarning(channelID string) string {
	if channelID != "" {
		return "SLACK_CHANNEL_ID is set but no Slack token is configured (SLACK_BOT_TOKEN / SLACK_SECRET_ARN); alerts are NOT delivered to Slack, logging-only mode"
	}
	return "no Slack token configured (SLACK_BOT_TOKEN / SLACK_SECRET_ARN); running in logging-only mode"
}

// slackMode decides how the dispatcher delivers alerts. With no token it runs
// in logging-only mode, which is what the Terraform defaults produce. Two
// cases are misconfigurations and must stop startup: a token without a
// channel, since every Slack post would fail, and a configured
// SLACK_SECRET_ARN whose secret is empty, since the operator asked for Slack
// delivery and would otherwise lose it silently.
func slackMode(token, channelID string, secretConfigured bool) (loggingOnly bool, err error) {
	if token == "" && secretConfigured {
		return false, errors.New("the secret named by SLACK_SECRET_ARN is empty")
	}
	if token == "" {
		return true, nil
	}
	if channelID == "" {
		return false, errors.New("a Slack token is configured but SLACK_CHANNEL_ID is empty")
	}
	return false, nil
}
