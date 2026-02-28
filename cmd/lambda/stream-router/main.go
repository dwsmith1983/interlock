// stream-router Lambda receives DynamoDB Stream events and starts Step Function executions.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// handleStreamEvent processes DynamoDB Stream records, starts Step Function
// executions for MARKER# records, and publishes lifecycle events for RUNLOG#
// records that reach a terminal status.
func handleStreamEvent(ctx context.Context, d *intlambda.Deps, event intlambda.StreamEvent) error {
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

		switch {
		case strings.HasPrefix(sk, "MARKER#"):
			if err := handleMarkerRecord(ctx, d, logger, pk, sk, record); err != nil {
				return err
			}
		case strings.HasPrefix(sk, "RUNLOG#"):
			intlambda.PublishLifecycleEvent(ctx, d, logger, pk, record.Change.NewImage)
		}

		// Publish to observability topic (best-effort, all eligible records)
		intlambda.PublishObservabilityEvent(ctx, d, logger, pk, sk, record)
	}

	return nil
}

// handleMarkerRecord starts a Step Function execution for a MARKER# DynamoDB stream record.
func handleMarkerRecord(ctx context.Context, d *intlambda.Deps, logger *slog.Logger, pk, sk string, record events.DynamoDBEventRecord) error {
	// PK format: PIPELINE#<pipelineID>
	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		logger.Warn("unexpected PK format", "pk", pk)
		return nil
	}

	// Extract schedule from NewImage attribute, defaulting to "daily"
	scheduleID := "daily"
	if record.Change.NewImage != nil {
		if schedAttr, ok := record.Change.NewImage["scheduleID"]; ok {
			if s := schedAttr.String(); s != "" {
				scheduleID = s
			}
		}
	}

	markerParts := strings.SplitN(sk, "#", 3)
	markerSource := ""
	if len(markerParts) >= 2 {
		markerSource = markerParts[1]
	}

	today := time.Now().UTC().Format("2006-01-02")
	date := today
	if record.Change.NewImage != nil {
		if dateAttr, ok := record.Change.NewImage["date"]; ok {
			if d := dateAttr.String(); d != "" {
				date = d
			}
		}
	}

	// Skip stale MARKERs — only trigger executions for today's data.
	// Historical backfill should be run separately, not through live orchestration.
	if date != today {
		logger.Info("skipping stale marker (date != today)",
			"pipeline", pipelineID, "date", date, "today", today,
			"schedule", scheduleID)
		return nil
	}

	// Dedup execution name: pipelineID:date:scheduleID (replace invalid chars)
	execName := intlambda.SanitizeExecName(fmt.Sprintf("%s:%s:%s", pipelineID, date, scheduleID))

	sfnInput, err := json.Marshal(map[string]interface{}{
		"pipelineID":   pipelineID,
		"scheduleID":   scheduleID,
		"markerSource": markerSource,
		"date":         date,
	})
	if err != nil {
		return fmt.Errorf("marshaling SFN input for %s: %w", pipelineID, err)
	}

	_, err = d.SFNClient.StartExecution(ctx, &sfn.StartExecutionInput{
		StateMachineArn: &d.StateMachineARN,
		Name:            &execName,
		Input:           strPtr(string(sfnInput)),
	})
	if err != nil {
		if strings.Contains(err.Error(), "ExecutionAlreadyExists") {
			logger.Info("execution already exists (dedup)",
				"pipeline", pipelineID, "date", date, "schedule", scheduleID)
			// Check if previous run failed — if so, start a retry execution
			if d.Provider != nil {
				entry, getErr := d.Provider.GetRunLog(ctx, pipelineID, date, scheduleID)
				if getErr != nil {
					logger.Warn("failed to check run log for retry",
						"pipeline", pipelineID, "error", getErr)
				} else if entry != nil && entry.Status == types.RunFailed {
					retryName := intlambda.SanitizeExecName(fmt.Sprintf("%s:%s:%s:a%d",
						pipelineID, date, scheduleID, entry.AttemptNumber+1))
					_, retryErr := d.SFNClient.StartExecution(ctx, &sfn.StartExecutionInput{
						StateMachineArn: &d.StateMachineARN,
						Name:            &retryName,
						Input:           strPtr(string(sfnInput)),
					})
					if retryErr != nil {
						if strings.Contains(retryErr.Error(), "ExecutionAlreadyExists") {
							logger.Info("retry execution already exists",
								"pipeline", pipelineID, "retryName", retryName)
						} else {
							logger.Error("failed to start retry execution",
								"pipeline", pipelineID, "retryName", retryName, "error", retryErr)
						}
					} else {
						logger.Info("started retry execution after failed run",
							"pipeline", pipelineID, "retryName", retryName,
							"attempt", entry.AttemptNumber+1)
					}
				}
			}
			return nil
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
	return nil
}

func handler(ctx context.Context, event intlambda.StreamEvent) error {
	d, err := intlambda.GetDeps()
	if err != nil {
		return fmt.Errorf("stream-router init: %w", err)
	}
	if d.SFNClient == nil {
		return fmt.Errorf("STATE_MACHINE_ARN environment variable required")
	}
	return handleStreamEvent(ctx, d, event)
}

func strPtr(s string) *string { return &s }

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	awslambda.Start(handler)
}
