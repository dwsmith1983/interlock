package lambda

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/dwsmith1983/interlock/internal/alert"
	"github.com/dwsmith1983/interlock/internal/archetype"
	"github.com/dwsmith1983/interlock/internal/engine"
	"github.com/dwsmith1983/interlock/internal/evaluator"
	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/internal/provider/dynamodb"
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
)

// SFNAPI is the subset of the Step Functions client used by Lambda handlers.
type SFNAPI interface {
	StartExecution(ctx context.Context, input *sfn.StartExecutionInput, opts ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error)
}

// Deps holds shared dependencies for Lambda handlers.
type Deps struct {
	Provider          provider.Provider
	Engine            *engine.Engine
	Runner            *trigger.Runner
	ArchetypeReg      *archetype.Registry
	AlertFn           func(types.Alert)
	Logger            *slog.Logger
	SFNClient         SFNAPI
	StateMachineARN   string
	SNSClient         SNSAPI
	LifecycleTopicARN string
}

// Init creates shared dependencies from environment variables.
// Reads: TABLE_NAME, AWS_REGION, SNS_TOPIC_ARN, EVALUATOR_BASE_URL
func Init(ctx context.Context) (*Deps, error) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	prov, err := newProvider()
	if err != nil {
		return nil, err
	}

	// Create alert dispatcher and sinks
	dispatcher, err := alert.NewDispatcher(nil, logger)
	if err != nil {
		return nil, fmt.Errorf("creating alert dispatcher: %w", err)
	}
	if topicARN := os.Getenv("SNS_TOPIC_ARN"); topicARN != "" {
		snsSink, err := alert.NewSNSSink(topicARN)
		if err != nil {
			return nil, fmt.Errorf("creating SNS sink: %w", err)
		}
		dispatcher.AddSink(snsSink)
	}
	if bucket := os.Getenv("S3_ALERT_BUCKET"); bucket != "" {
		prefix := envOrDefault("S3_ALERT_PREFIX", "alerts")
		s3Sink, err := alert.NewS3Sink(bucket, prefix)
		if err != nil {
			return nil, fmt.Errorf("creating S3 alert sink: %w", err)
		}
		dispatcher.AddSink(s3Sink)
	}
	originalAlertFn := dispatcher.AlertFunc()
	// Wrap alert function to also persist alerts to the provider.
	alertFn := func(a types.Alert) {
		originalAlertFn(a)
		if err := prov.PutAlert(context.Background(), a); err != nil {
			logger.Warn("failed to persist alert", "pipeline", a.PipelineID, "error", err)
		}
	}

	// Create composite evaluator runner for Lambda (HTTP + built-in handlers)
	httpRunner := evaluator.NewHTTPRunner(envOrDefault("EVALUATOR_BASE_URL", ""))
	composite := evaluator.NewCompositeRunner(httpRunner)
	composite.Register("upstream-job-log", evaluator.NewUpstreamJobLogHandler(prov))
	composite.Register("sensor-threshold", evaluator.NewSensorThresholdHandler(prov))
	composite.Register("sensor-freshness", evaluator.NewSensorFreshnessHandler(prov))
	composite.Register("window-completeness", evaluator.NewWindowCompletenessHandler(prov))
	composite.Register("data-quality", evaluator.NewDataQualityHandler(prov))

	// Circuit breaker opt-in via env var.
	if cbThreshold := envOrDefault("CIRCUIT_BREAKER_THRESHOLD", ""); cbThreshold != "" {
		config := evaluator.DefaultCircuitBreakerConfig()
		if n, err := strconv.Atoi(cbThreshold); err == nil && n > 0 {
			config.FailThreshold = n
		}
		composite.SetCircuitBreaker(evaluator.NewCircuitBreaker(config))
	}

	var runner engine.TraitRunner = composite

	// Load archetype registry
	archetypeDir := envOrDefault("ARCHETYPE_DIR", "/var/task/archetypes")
	archetypeReg := archetype.NewRegistry()
	if info, err := os.Stat(archetypeDir); err == nil && info.IsDir() {
		if err := archetypeReg.LoadDir(archetypeDir); err != nil {
			return nil, fmt.Errorf("loading archetypes from %s: %w", archetypeDir, err)
		}
	}

	// Create engine
	eng := engine.New(prov, archetypeReg, runner, alertFn)
	eng.SetLogger(logger)

	// Create trigger runner
	triggerRunner := trigger.NewRunner()

	deps := &Deps{
		Provider:     prov,
		Engine:       eng,
		Runner:       triggerRunner,
		ArchetypeReg: archetypeReg,
		AlertFn:      alertFn,
		Logger:       logger,
	}

	// Initialize SFN client for stream-router/replay if configured.
	if smARN := os.Getenv("STATE_MACHINE_ARN"); smARN != "" {
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("loading AWS config for SFN: %w", err)
		}
		deps.SFNClient = sfn.NewFromConfig(awsCfg)
		deps.StateMachineARN = smARN
	}

	// Initialize SNS client for lifecycle events if configured (opt-in).
	if topicARN := os.Getenv("LIFECYCLE_TOPIC_ARN"); topicARN != "" {
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("loading AWS config for SNS lifecycle: %w", err)
		}
		deps.SNSClient = awssns.NewFromConfig(awsCfg)
		deps.LifecycleTopicARN = topicARN
	}

	return deps, nil
}

// newProvider creates the storage provider from environment variables.
// Reads: PROVIDER (default "dynamodb"), TABLE_NAME, AWS_REGION, READINESS_TTL, RETENTION_TTL.
func newProvider() (provider.Provider, error) {
	providerType := envOrDefault("PROVIDER", "dynamodb")
	switch providerType {
	case "dynamodb":
		tableName := os.Getenv("TABLE_NAME")
		region := os.Getenv("AWS_REGION")
		if tableName == "" {
			return nil, fmt.Errorf("TABLE_NAME environment variable required")
		}
		if region == "" {
			return nil, fmt.Errorf("AWS_REGION environment variable required")
		}
		ddbCfg := types.DynamoDBConfig{
			TableName:    tableName,
			Region:       region,
			ReadinessTTL: envOrDefault("READINESS_TTL", "1h"),
			RetentionTTL: envOrDefault("RETENTION_TTL", "168h"),
		}
		return dynamodb.New(&ddbCfg)
	default:
		return nil, fmt.Errorf("unsupported PROVIDER: %s", providerType)
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
