// watchdog Lambda scans for missed pipeline schedules.
// Invoked by EventBridge on a regular interval (e.g. every 5 minutes).
package main

import (
	"context"
	"log/slog"
	"os"
	"sync"

	awslambda "github.com/aws/aws-lambda-go/lambda"
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

	missed := watchdog.CheckMissedSchedules(ctx, watchdog.CheckOptions{
		Provider: d.Provider,
		AlertFn:  d.AlertFn,
		Logger:   d.Logger,
	})

	d.Logger.Info("watchdog scan complete", "missed", len(missed))
	return nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	awslambda.Start(handler)
}
