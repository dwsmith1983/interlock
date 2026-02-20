package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/interlock-systems/interlock/internal/config"
	"github.com/interlock-systems/interlock/internal/lifecycle"
	"github.com/interlock-systems/interlock/internal/trigger"
	"github.com/interlock-systems/interlock/pkg/types"
)

// NewRunCmd creates the run command.
func NewRunCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "run [pipeline-name]",
		Short: "Evaluate and trigger a pipeline",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPipeline(args[0])
		},
	}
}

func runPipeline(pipelineName string) error {
	cfg, err := config.Load(".")
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	eng, prov, cleanup, err := buildEngine(cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Step 1: Evaluate readiness
	result, err := eng.Evaluate(ctx, pipelineName)
	if err != nil {
		return fmt.Errorf("evaluation failed: %w", err)
	}

	printReadinessResult(result, prov)

	if result.Status != types.Ready {
		return fmt.Errorf("pipeline %s is not ready", pipelineName)
	}

	// Step 2: Create run state
	runID := fmt.Sprintf("%s-%d", pipelineName, time.Now().UnixNano())
	run := types.RunState{
		RunID:      runID,
		PipelineID: pipelineName,
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	if err := prov.PutRunState(ctx, run); err != nil {
		return fmt.Errorf("creating run state: %w", err)
	}

	// Step 3: CAS to TRIGGERING
	run.Status = types.RunTriggering
	run.Version = 2
	run.UpdatedAt = time.Now()
	ok, err := prov.CompareAndSwapRunState(ctx, runID, 1, run)
	if err != nil {
		return fmt.Errorf("CAS failed: %w", err)
	}
	if !ok {
		return fmt.Errorf("another process is already triggering this pipeline")
	}

	// Step 4: Execute trigger
	pipeline, err := prov.GetPipeline(ctx, pipelineName)
	if err != nil {
		return fmt.Errorf("loading pipeline: %w", err)
	}

	color.Cyan("Triggering pipeline %s (run: %s)...\n", pipelineName, runID)

	triggerErr := trigger.Execute(ctx, pipeline.Trigger)

	// Step 5: Update final state
	var finalStatus types.RunStatus
	if triggerErr != nil {
		finalStatus = types.RunFailed
		color.Red("Trigger failed: %v", triggerErr)
	} else {
		if pipeline.Trigger != nil && pipeline.Trigger.Type == types.TriggerCommand {
			finalStatus = types.RunCompleted
			color.Green("Pipeline completed successfully")
		} else {
			finalStatus = types.RunRunning
			color.Cyan("Pipeline triggered, awaiting completion callback")
			fmt.Printf("  Complete with: POST /api/runs/%s/complete\n", runID)
		}
	}

	if err := lifecycle.Transition(run.Status, finalStatus); err != nil {
		return fmt.Errorf("invalid state transition: %w", err)
	}

	run.Status = finalStatus
	run.Version = 3
	run.UpdatedAt = time.Now()
	_, err = prov.CompareAndSwapRunState(ctx, runID, 2, run)
	if err != nil {
		return fmt.Errorf("updating run state: %w", err)
	}

	return triggerErr
}
