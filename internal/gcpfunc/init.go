// Package gcpfunc provides shared types and initialization for Cloud Function handlers.
package gcpfunc

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/dwsmith1983/interlock/internal/alert"
	"github.com/dwsmith1983/interlock/internal/archetype"
	"github.com/dwsmith1983/interlock/internal/engine"
	"github.com/dwsmith1983/interlock/internal/evaluator"
	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/internal/provider/firestore"
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// Deps holds shared dependencies for Cloud Function handlers.
type Deps struct {
	Provider     provider.Provider
	Engine       *engine.Engine
	Runner       *trigger.Runner
	ArchetypeReg *archetype.Registry
	AlertFn      func(types.Alert)
	Logger       *slog.Logger
}

// Init creates shared dependencies from environment variables.
// Reads: PROJECT_ID, COLLECTION, PUBSUB_TOPIC, EVALUATOR_BASE_URL, ARCHETYPE_DIR
func Init(ctx context.Context) (*Deps, error) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	projectID := os.Getenv("PROJECT_ID")
	collection := envOrDefault("COLLECTION", "interlock")
	if projectID == "" {
		return nil, fmt.Errorf("PROJECT_ID environment variable required")
	}

	// Create Firestore provider
	fsCfg := &types.FirestoreConfig{
		ProjectID:    projectID,
		Collection:   collection,
		ReadinessTTL: envOrDefault("READINESS_TTL", "1h"),
		RetentionTTL: envOrDefault("RETENTION_TTL", "168h"),
	}
	prov, err := firestore.New(fsCfg)
	if err != nil {
		return nil, fmt.Errorf("creating Firestore provider: %w", err)
	}

	// Create alert dispatcher and sinks
	dispatcher, err := alert.NewDispatcher(nil, logger)
	if err != nil {
		return nil, fmt.Errorf("creating alert dispatcher: %w", err)
	}
	if topicID := os.Getenv("PUBSUB_TOPIC"); topicID != "" {
		pubsubSink, err := alert.NewPubSubSink(projectID, topicID)
		if err != nil {
			return nil, fmt.Errorf("creating Pub/Sub sink: %w", err)
		}
		dispatcher.AddSink(pubsubSink)
	}
	alertFn := dispatcher.AlertFunc()

	// Create composite evaluator runner for Cloud Functions (HTTP + built-in handlers)
	httpRunner := evaluator.NewHTTPRunner(envOrDefault("EVALUATOR_BASE_URL", ""))
	composite := evaluator.NewCompositeRunner(httpRunner)
	composite.Register("upstream-job-log", evaluator.NewUpstreamJobLogHandler(prov))
	var runner engine.TraitRunner = composite

	// Load archetype registry
	archetypeDir := envOrDefault("ARCHETYPE_DIR", "/workspace/archetypes")
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

	return &Deps{
		Provider:     prov,
		Engine:       eng,
		Runner:       triggerRunner,
		ArchetypeReg: archetypeReg,
		AlertFn:      alertFn,
		Logger:       logger,
	}, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
