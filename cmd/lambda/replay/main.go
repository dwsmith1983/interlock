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
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	awslambda "github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
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
		awsCfg, loadErr := awsconfig.LoadDefaultConfig(context.Background())
		if loadErr != nil {
			err = fmt.Errorf("loading AWS config: %w", loadErr)
			return
		}
		sfnClient = sfn.NewFromConfig(awsCfg)
	})
	return sfnClient, err
}

// handleReplayEvent processes DynamoDB Stream records from the replays table
// and starts SFN executions with replay=true.
func handleReplayEvent(ctx context.Context, client SFNAPI, smARN string, event intlambda.StreamEvent) error {
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
		execName := sanitizeExecName(fmt.Sprintf("replay_%s_%s_%s_%d", pipelineID, date, scheduleID, ts))

		sfnInput, _ := json.Marshal(map[string]interface{}{
			"pipelineID": pipelineID,
			"scheduleID": scheduleID,
			"date":       date,
			"replay":     true,
		})

		_, err := client.StartExecution(ctx, &sfn.StartExecutionInput{
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
	client, err := getSFNClient()
	if err != nil {
		return err
	}
	return handleReplayEvent(ctx, client, stateMachineARN, event)
}

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
