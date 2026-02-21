package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/interlock-systems/interlock/internal/alert"
	"github.com/interlock-systems/interlock/internal/archetype"
	"github.com/interlock-systems/interlock/internal/config"
	"github.com/interlock-systems/interlock/internal/engine"
	"github.com/interlock-systems/interlock/internal/evaluator"
	"github.com/interlock-systems/interlock/internal/provider/redis"
	"github.com/interlock-systems/interlock/pkg/types"
)

// NewEvaluateCmd creates the evaluate command.
func NewEvaluateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "evaluate [pipeline-name]",
		Short: "Evaluate readiness for a pipeline",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runEvaluate(args[0])
		},
	}
}

func runEvaluate(pipelineName string) error {
	cfg, err := config.Load(".")
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	eng, prov, cleanup, err := buildEngine(cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	result, err := eng.Evaluate(ctx, pipelineName)
	if err != nil {
		return fmt.Errorf("evaluation failed: %w", err)
	}

	printReadinessResult(result, prov)
	return nil
}

func buildEngine(cfg *types.ProjectConfig) (*engine.Engine, *redis.RedisProvider, func(), error) {
	prov := redis.New(cfg.Redis)
	ctx := context.Background()

	if err := prov.Start(ctx); err != nil {
		return nil, nil, nil, fmt.Errorf("connecting to Redis: %w", err)
	}

	// Load archetypes
	reg := archetype.NewRegistry()
	for _, dir := range cfg.ArchetypeDirs {
		if err := reg.LoadDir(dir); err != nil {
			_ = prov.Stop(ctx)
			return nil, nil, nil, fmt.Errorf("loading archetypes from %s: %w", dir, err)
		}
	}

	// Load pipelines into Redis
	if err := loadPipelines(ctx, cfg, prov); err != nil {
		_ = prov.Stop(ctx)
		return nil, nil, nil, err
	}

	// Alert dispatcher
	dispatcher, err := alert.NewDispatcher(cfg.Alerts, nil)
	if err != nil {
		_ = prov.Stop(ctx)
		return nil, nil, nil, fmt.Errorf("creating alert dispatcher: %w", err)
	}

	runner := evaluator.NewRunner(cfg.EvaluatorDirs)
	eng := engine.New(prov, reg, runner, dispatcher.AlertFunc())

	cleanup := func() {
		_ = prov.Stop(context.Background())
	}

	return eng, prov, cleanup, nil
}

func loadPipelines(ctx context.Context, cfg *types.ProjectConfig, prov *redis.RedisProvider) error {
	for _, dir := range cfg.PipelineDirs {
		pipelines, err := loadPipelineDir(dir)
		if err != nil {
			return fmt.Errorf("loading pipelines from %s: %w", dir, err)
		}
		for _, p := range pipelines {
			if err := prov.RegisterPipeline(ctx, p); err != nil {
				return fmt.Errorf("registering pipeline %s: %w", p.Name, err)
			}
		}
	}
	return nil
}

func printReadinessResult(result *types.ReadinessResult, prov *redis.RedisProvider) {
	bold := color.New(color.Bold)

	_, _ = bold.Printf("\nPipeline: %s\n", result.PipelineID)

	if result.Status == types.Ready {
		color.Green("Status: READY ✓")
	} else {
		color.Red("Status: NOT READY ✗")
		if len(result.Blocking) > 0 {
			color.Red("Blocking traits:")
			for _, b := range result.Blocking {
				fmt.Printf("  - %s\n", b)
			}
		}
	}

	fmt.Println("\nTrait evaluations:")
	for _, trait := range result.Traits {
		switch trait.Status {
		case types.TraitPass:
			color.Green("  ✓ %s: PASS", trait.TraitType)
		case types.TraitFail:
			color.Red("  ✗ %s: FAIL (%s)", trait.TraitType, trait.Reason)
		case types.TraitStale:
			color.Yellow("  ○ %s: STALE", trait.TraitType)
		default:
			fmt.Printf("  ? %s: %s\n", trait.TraitType, trait.Status)
		}
	}
	fmt.Println()
	_ = prov // available for future use
}
