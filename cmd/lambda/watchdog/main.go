// watchdog Lambda scans for missed pipeline schedules, stuck runs, and
// re-triggers eligible failed runs. Invoked by EventBridge on a regular
// interval (e.g. every 5 minutes).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/watchdog"
)

func handler(ctx context.Context) error {
	d, err := intlambda.GetDeps()
	if err != nil {
		return fmt.Errorf("watchdog init: %w", err)
	}

	opts := watchdog.CheckOptions{
		Provider: d.Provider,
		AlertFn:  d.AlertFn,
		Logger:   d.Logger,
	}

	// Wire SFN re-trigger if state machine ARN is configured.
	if d.SFNClient != nil && d.StateMachineARN != "" {
		opts.RetriggerFn = func(ctx context.Context, pipelineID, scheduleID string) error {
			input, err := json.Marshal(map[string]string{
				"pipelineID": pipelineID,
				"scheduleID": scheduleID,
			})
			if err != nil {
				return fmt.Errorf("marshaling retrigger input for %s/%s: %w", pipelineID, scheduleID, err)
			}
			_, err = d.SFNClient.StartExecution(ctx, &sfn.StartExecutionInput{
				StateMachineArn: aws.String(d.StateMachineARN),
				Input:           aws.String(string(input)),
				Name:            aws.String(fmt.Sprintf("watchdog-%s-%s", pipelineID, scheduleID)),
			})
			return err
		}
	}

	missed := watchdog.CheckMissedSchedules(ctx, opts)
	stuck := watchdog.CheckStuckRuns(ctx, opts)
	expired := watchdog.CheckCompletedMonitoring(ctx, opts)
	retriggered := watchdog.CheckFailedRuns(ctx, opts)

	d.Logger.Info("watchdog scan complete",
		"missed", len(missed),
		"stuck", len(stuck),
		"monitoringExpired", len(expired),
		"retriggered", len(retriggered))
	return nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	awslambda.Start(handler)
}
