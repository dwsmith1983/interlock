// replay Lambda receives DynamoDB Stream events from the replays table
// and starts Step Function executions for replay requests.
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
)

// handleReplayEvent processes DynamoDB Stream records from the replays table
// and starts SFN executions with replay=true.
func handleReplayEvent(ctx context.Context, client intlambda.SFNAPI, smARN string, event intlambda.StreamEvent) error {
	logger := slog.Default()

	for _, record := range event.Records {
		if record.EventName != "INSERT" {
			continue
		}

		newImage := record.Change.NewImage
		if newImage == nil {
			continue
		}

		pipelineID := ddbAttrString(newImage, "pipelineID")
		scheduleID := ddbAttrString(newImage, "scheduleID")
		date := ddbAttrString(newImage, "date")

		if pipelineID == "" || date == "" {
			logger.Warn("replay record missing required fields", "eventID", record.EventID)
			continue
		}
		if scheduleID == "" {
			scheduleID = "daily"
		}

		// Unique execution name for replay
		ts := time.Now().UnixMilli()
		execName := intlambda.SanitizeExecName(fmt.Sprintf("replay_%s_%s_%s_%d", pipelineID, date, scheduleID, ts))

		sfnInput, err := json.Marshal(map[string]interface{}{
			"pipelineID": pipelineID,
			"scheduleID": scheduleID,
			"date":       date,
			"replay":     true,
		})
		if err != nil {
			return fmt.Errorf("marshaling replay input for %s: %w", pipelineID, err)
		}

		_, err = client.StartExecution(ctx, &sfn.StartExecutionInput{
			StateMachineArn: &smARN,
			Name:            &execName,
			Input:           strPtr(string(sfnInput)),
		})
		if err != nil {
			if strings.Contains(err.Error(), "ExecutionAlreadyExists") {
				logger.Info("replay execution already exists (dedup)",
					"pipeline", pipelineID, "date", date, "schedule", scheduleID)
				continue
			}
			logger.Error("failed to start replay execution",
				"pipeline", pipelineID, "error", err)
			return fmt.Errorf("starting replay execution for %s: %w", pipelineID, err)
		}

		logger.Info("started replay execution",
			"pipeline", pipelineID, "date", date, "schedule", scheduleID, "execName", execName)
	}

	return nil
}

func ddbAttrString(attrs map[string]events.DynamoDBAttributeValue, key string) string {
	if v, ok := attrs[key]; ok {
		return v.String()
	}
	return ""
}

func handler(ctx context.Context, event intlambda.StreamEvent) error {
	d, err := intlambda.GetDeps()
	if err != nil {
		return fmt.Errorf("replay init: %w", err)
	}
	if d.SFNClient == nil {
		return fmt.Errorf("STATE_MACHINE_ARN environment variable required")
	}
	return handleReplayEvent(ctx, d.SFNClient, d.StateMachineARN, event)
}

func strPtr(s string) *string { return &s }

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	awslambda.Start(handler)
}
