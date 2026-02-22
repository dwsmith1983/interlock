// stream-router Lambda receives DynamoDB Stream events and starts Step Function executions.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	awslambda "github.com/aws/aws-lambda-go/lambda"
	intlambda "github.com/interlock-systems/interlock/internal/lambda"
)

// SFNAPI is the subset of the Step Functions client we use.
type SFNAPI interface {
	StartExecution(ctx context.Context, input *sfn.StartExecutionInput, opts ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error)
}

var (
	sfnClient       SFNAPI
	sfnClientOnce   sync.Once
	stateMachineARN string
)

func getSFNClient() (SFNAPI, error) {
	var err error
	sfnClientOnce.Do(func() {
		stateMachineARN = os.Getenv("STATE_MACHINE_ARN")
		if stateMachineARN == "" {
			err = fmt.Errorf("STATE_MACHINE_ARN environment variable required")
			return
		}
		var cfg interface{ Region() string }
		_ = cfg
		awsCfg, loadErr := awsconfig.LoadDefaultConfig(context.Background())
		if loadErr != nil {
			err = fmt.Errorf("loading AWS config: %w", loadErr)
			return
		}
		sfnClient = sfn.NewFromConfig(awsCfg)
	})
	return sfnClient, err
}

// handleStreamEvent processes DynamoDB Stream records and starts Step Function executions.
func handleStreamEvent(ctx context.Context, client SFNAPI, smARN string, event intlambda.StreamEvent) error {
	logger := slog.Default()

	for _, record := range event.Records {
		if record.EventName != "INSERT" && record.EventName != "MODIFY" {
			continue
		}

		keys := record.Change.Keys
		pkAttr, hasPK := keys["PK"]
		skAttr, hasSK := keys["SK"]
		if !hasPK || !hasSK {
			logger.Warn("stream record missing PK/SK", "eventID", record.EventID)
			continue
		}

		pk := pkAttr.String()
		sk := skAttr.String()

		// Only process MARKER# records (trait evaluations)
		if !strings.HasPrefix(sk, "MARKER#") {
			continue
		}

		// PK format: PIPELINE#<pipelineID>
		pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
		if pipelineID == pk {
			logger.Warn("unexpected PK format", "pk", pk)
			continue
		}

		// Extract schedule from marker or default
		scheduleID := "daily"
		markerParts := strings.SplitN(sk, "#", 3)
		markerSource := ""
		if len(markerParts) >= 2 {
			markerSource = markerParts[1]
		}

		date := time.Now().UTC().Format("2006-01-02")

		// Dedup execution name: pipelineID:date:scheduleID (replace invalid chars)
		execName := sanitizeExecName(fmt.Sprintf("%s:%s:%s", pipelineID, date, scheduleID))

		sfnInput, _ := json.Marshal(map[string]interface{}{
			"pipelineID":   pipelineID,
			"scheduleID":   scheduleID,
			"markerSource": markerSource,
			"date":         date,
		})

		_, err := client.StartExecution(ctx, &sfn.StartExecutionInput{
			StateMachineArn: &smARN,
			Name:            &execName,
			Input:           strPtr(string(sfnInput)),
		})
		if err != nil {
			// ExecutionAlreadyExists is expected for dedup — not an error
			if strings.Contains(err.Error(), "ExecutionAlreadyExists") {
				logger.Info("execution already exists (dedup)",
					"pipeline", pipelineID, "date", date, "schedule", scheduleID)
				continue
			}
			logger.Error("failed to start execution",
				"pipeline", pipelineID,
				"error", err,
			)
			return fmt.Errorf("starting execution for %s: %w", pipelineID, err)
		}

		logger.Info("started step function execution",
			"pipeline", pipelineID,
			"date", date,
			"schedule", scheduleID,
			"execName", execName,
		)
	}

	return nil
}

func handler(ctx context.Context, event intlambda.StreamEvent) error {
	client, err := getSFNClient()
	if err != nil {
		return err
	}
	return handleStreamEvent(ctx, client, stateMachineARN, event)
}

// sanitizeExecName replaces characters invalid for SFN execution names.
// Valid: a-z, A-Z, 0-9, -, _  (max 80 chars)
func sanitizeExecName(name string) string {
	var b strings.Builder
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
			b.WriteRune(c)
		default:
			b.WriteRune('_')
		}
	}
	s := b.String()
	if len(s) > 80 {
		s = s[:80]
	}
	return s
}

func strPtr(s string) *string { return &s }

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	awslambda.Start(handler)
}
