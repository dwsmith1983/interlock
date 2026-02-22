package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/dwsmith1983/interlock/internal/config"
	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// NewStatusCmd creates the status command.
func NewStatusCmd() *cobra.Command {
	var pipelineName string

	cmd := &cobra.Command{
		Use:   "status [pipeline-name]",
		Short: "Show pipeline status and recent runs",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				pipelineName = args[0]
			}
			return runStatus(pipelineName)
		},
	}
	return cmd
}

func runStatus(pipelineName string) error {
	cfg, err := config.Load(".")
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	prov, err := newProvider(cfg)
	if err != nil {
		return fmt.Errorf("creating provider: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := prov.Start(ctx); err != nil {
		return fmt.Errorf("connecting to provider: %w", err)
	}
	defer func() { _ = prov.Stop(ctx) }()

	if pipelineName != "" {
		return showPipelineStatus(ctx, prov, pipelineName)
	}
	return showAllPipelines(ctx, prov)
}

func showAllPipelines(ctx context.Context, prov provider.Provider) error {
	pipelines, err := prov.ListPipelines(ctx)
	if err != nil {
		return fmt.Errorf("listing pipelines: %w", err)
	}

	if len(pipelines) == 0 {
		fmt.Println("No pipelines registered.")
		return nil
	}

	bold := color.New(color.Bold)
	_, _ = bold.Println("Registered Pipelines:")
	fmt.Println()

	for _, p := range pipelines {
		readiness, _ := prov.GetReadiness(ctx, p.Name)
		statusStr := color.YellowString("UNKNOWN")
		if readiness != nil {
			if readiness.Status == types.Ready {
				statusStr = color.GreenString("READY")
			} else {
				statusStr = color.RedString("NOT READY")
			}
		}

		fmt.Printf("  %-30s %-15s archetype=%-20s tier=%d\n",
			p.Name, statusStr, p.Archetype, p.Tier)
	}
	fmt.Println()
	return nil
}

func showPipelineStatus(ctx context.Context, prov provider.Provider, name string) error {
	pipeline, err := prov.GetPipeline(ctx, name)
	if err != nil {
		return fmt.Errorf("pipeline not found: %w", err)
	}

	bold := color.New(color.Bold)
	_, _ = bold.Printf("Pipeline: %s\n", pipeline.Name)
	fmt.Printf("  Archetype: %s\n", pipeline.Archetype)
	fmt.Printf("  Tier:      %d\n", pipeline.Tier)
	if pipeline.Trigger != nil {
		fmt.Printf("  Trigger:   %s\n", pipeline.Trigger.Type)
	}

	// Show readiness
	readiness, _ := prov.GetReadiness(ctx, name)
	if readiness != nil {
		fmt.Println()
		if readiness.Status == types.Ready {
			color.Green("  Readiness: READY ✓")
		} else {
			color.Red("  Readiness: NOT READY ✗")
		}
		fmt.Printf("  Evaluated: %s\n", readiness.EvaluatedAt.Format(time.RFC3339))
	}

	// Show traits
	traits, _ := prov.GetTraits(ctx, name)
	if len(traits) > 0 {
		fmt.Println()
		_, _ = bold.Println("  Traits:")
		for _, t := range traits {
			switch t.Status {
			case types.TraitPass:
				color.Green("    ✓ %s: PASS", t.TraitType)
			case types.TraitFail:
				color.Red("    ✗ %s: FAIL (%s)", t.TraitType, t.Reason)
			case types.TraitStale:
				color.Yellow("    ○ %s: STALE", t.TraitType)
			}
		}
	}

	// Show recent runs
	runs, _ := prov.ListRuns(ctx, name, 5)
	if len(runs) > 0 {
		fmt.Println()
		_, _ = bold.Println("  Recent Runs:")
		for _, r := range runs {
			statusStr := string(r.Status)
			switch r.Status {
			case types.RunCompleted:
				statusStr = color.GreenString(statusStr)
			case types.RunFailed:
				statusStr = color.RedString(statusStr)
			case types.RunRunning, types.RunTriggering:
				statusStr = color.CyanString(statusStr)
			}
			fmt.Printf("    %s  %s  %s\n", r.RunID, statusStr, r.UpdatedAt.Format(time.RFC3339))
		}
	}

	fmt.Println()
	return nil
}
