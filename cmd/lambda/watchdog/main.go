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
	"sync"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/watchdog"
)

var (
	deps     *intlambda.Deps
	depsOnce sync.Once
	depsErr  error
)

func getDeps() (*intlambda.Deps, error) {
	depsOnce.Do(func() {
		deps, depsErr = intlambda.Init(context.Background())
	})
	return deps, depsErr
}

func handler(ctx context.Context) error {
	d, err := getDeps()
	if err != nil {
		return err
	}

	opts := watchdog.CheckOptions{
		Provider: d.Provider,
		AlertFn:  d.AlertFn,
		Logger:   d.Logger,
	}

	// Wire SFN re-trigger if state machine ARN is configured.
	if d.SFNClient != nil && d.StateMachineARN != "" {
		opts.RetriggerFn = func(ctx context.Context, pipelineID, scheduleID string) error {
			input, _ := json.Marshal(map[string]string{
				"pipelineID": pipelineID,
				"scheduleID": scheduleID,
			})
			_, err := d.SFNClient.StartExecution(ctx, &sfn.StartExecutionInput{
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
