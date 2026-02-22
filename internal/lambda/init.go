package lambda

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/interlock-systems/interlock/internal/alert"
	"github.com/interlock-systems/interlock/internal/engine"
	"github.com/interlock-systems/interlock/internal/evaluator"
	"github.com/interlock-systems/interlock/internal/provider"
	"github.com/interlock-systems/interlock/internal/provider/dynamodb"
	"github.com/interlock-systems/interlock/internal/trigger"
	"github.com/interlock-systems/interlock/pkg/types"
)

// Deps holds shared dependencies for Lambda handlers.
type Deps struct {
	Provider provider.Provider
	Engine   *engine.Engine
	Runner   *trigger.Runner
	AlertFn  func(types.Alert)
	Logger   *slog.Logger
}

// Init creates shared dependencies from environment variables.
// Reads: TABLE_NAME, AWS_REGION, SNS_TOPIC_ARN, EVALUATOR_BASE_URL
func Init(ctx context.Context) (*Deps, error) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	tableName := os.Getenv("TABLE_NAME")
	region := os.Getenv("AWS_REGION")
	if tableName == "" {
		return nil, fmt.Errorf("TABLE_NAME environment variable required")
	}
	if region == "" {
		return nil, fmt.Errorf("AWS_REGION environment variable required")
	}

	// Create DynamoDB provider
	ddbCfg := types.DynamoDBConfig{
		TableName:    tableName,
		Region:       region,
		ReadinessTTL: envOrDefault("READINESS_TTL", "1h"),
		RetentionTTL: envOrDefault("RETENTION_TTL", "168h"),
	}
	prov, err := dynamodb.New(&ddbCfg)
	if err != nil {
		return nil, fmt.Errorf("creating DynamoDB provider: %w", err)
	}

	// Create alert function
	var alertFn func(types.Alert)
	if topicARN := os.Getenv("SNS_TOPIC_ARN"); topicARN != "" {
		snsSink, err := alert.NewSNSSink(topicARN)
		if err != nil {
			return nil, fmt.Errorf("creating SNS sink: %w", err)
		}
		dispatcher, err := alert.NewDispatcher(nil, logger)
		if err != nil {
			return nil, fmt.Errorf("creating alert dispatcher: %w", err)
		}
		dispatcher.AddSink(snsSink)
		alertFn = dispatcher.AlertFunc()
	} else {
		alertFn = func(a types.Alert) {
			logger.Info("alert", "level", a.Level, "pipeline", a.PipelineID, "message", a.Message)
		}
	}

	// Create HTTP evaluator runner for Lambda
	var runner engine.TraitRunner
	if baseURL := os.Getenv("EVALUATOR_BASE_URL"); baseURL != "" {
		runner = evaluator.NewHTTPRunner(baseURL)
	} else {
		runner = evaluator.NewHTTPRunner("")
	}

	// Create engine
	eng := engine.New(prov, nil, runner, alertFn)
	eng.SetLogger(logger)

	// Create trigger runner
	triggerRunner := trigger.NewRunner()

	return &Deps{
		Provider: prov,
		Engine:   eng,
		Runner:   triggerRunner,
		AlertFn:  alertFn,
		Logger:   logger,
	}, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
