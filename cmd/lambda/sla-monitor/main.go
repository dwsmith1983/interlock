// sla-monitor Lambda handles SLA calculations, alert firing, and schedule
// management for the Step Function workflow. Modes: calculate, fire-alert,
// schedule, cancel, reconcile.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"

	ilambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := ilambda.ValidateEnv("sla-monitor"); err != nil {
		logger.Error("environment validation failed", "error", err)
		os.Exit(1)
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		logger.Error("failed to load AWS config", "error", err)
		os.Exit(1)
	}

	ddbClient := dynamodb.NewFromConfig(cfg)
	s := &store.Store{
		Client:       ddbClient,
		ControlTable: os.Getenv("CONTROL_TABLE"),
		JobLogTable:  os.Getenv("JOBLOG_TABLE"),
		RerunTable:   os.Getenv("RERUN_TABLE"),
	}

	deps := &ilambda.Deps{
		Store:              s,
		EventBridge:        eventbridge.NewFromConfig(cfg),
		Scheduler:          scheduler.NewFromConfig(cfg),
		EventBusName:       os.Getenv("EVENT_BUS_NAME"),
		SLAMonitorARN:      os.Getenv("SLA_MONITOR_ARN"),
		SchedulerRoleARN:   os.Getenv("SCHEDULER_ROLE_ARN"),
		SchedulerGroupName: os.Getenv("SCHEDULER_GROUP_NAME"),
		Logger:             logger,
	}

	lambda.Start(func(ctx context.Context, input ilambda.SLAMonitorInput) (ilambda.SLAMonitorOutput, error) {
		return ilambda.HandleSLAMonitor(ctx, deps, input)
	})
}
