package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/dwsmith1983/interlock/internal/config"
	"github.com/dwsmith1983/interlock/pkg/types"
)

const addPipelineTimeout = 10 * time.Second

// NewAddPipelineCmd creates the add-pipeline command.
func NewAddPipelineCmd() *cobra.Command {
	var (
		name           string
		archetypeName  string
		tier           int
		triggerType    string
		triggerCommand string
		triggerURL     string
	)

	cmd := &cobra.Command{
		Use:   "add-pipeline",
		Short: "Register a new pipeline",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAddPipeline(name, archetypeName, tier, triggerType, triggerCommand, triggerURL)
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "Pipeline name (required)")
	cmd.Flags().StringVar(&archetypeName, "archetype", "batch-ingestion", "Archetype to use")
	cmd.Flags().IntVar(&tier, "tier", 3, "Pipeline tier (1=critical, 2=important, 3=standard)")
	cmd.Flags().StringVar(&triggerType, "trigger-type", "command", "Trigger type: http or command")
	cmd.Flags().StringVar(&triggerCommand, "trigger-command", "", "Command to execute (for command triggers)")
	cmd.Flags().StringVar(&triggerURL, "trigger-url", "", "URL to POST (for http triggers)")
	_ = cmd.MarkFlagRequired("name")

	return cmd
}

func runAddPipeline(name, archetypeName string, tier int, triggerType, triggerCommand, triggerURL string) error {
	cfg, err := config.Load(".")
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	pipeline := types.PipelineConfig{
		Name:      name,
		Archetype: archetypeName,
		Tier:      tier,
		Trigger: &types.TriggerConfig{
			Type: types.TriggerType(triggerType),
		},
	}

	switch triggerType {
	case "command":
		pipeline.Trigger.Command = &types.CommandTriggerConfig{
			Command: triggerCommand,
		}
	case "http":
		pipeline.Trigger.HTTP = &types.HTTPTriggerConfig{
			URL:    triggerURL,
			Method: "POST",
		}
	}

	// Write pipeline YAML to pipelines dir
	pipelineDir := cfg.PipelineDirs[0]
	pipelinePath := filepath.Join(pipelineDir, name+".yaml")
	data, err := yaml.Marshal(pipeline)
	if err != nil {
		return fmt.Errorf("marshaling pipeline: %w", err)
	}
	if err := os.WriteFile(pipelinePath, data, 0o644); err != nil {
		return fmt.Errorf("writing pipeline file: %w", err)
	}

	// Register in provider
	prov, err := newProvider(cfg)
	if err != nil {
		return fmt.Errorf("creating provider: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), addPipelineTimeout)
	defer cancel()

	if err := prov.Start(ctx); err != nil {
		return fmt.Errorf("connecting to provider: %w", err)
	}
	defer func() { _ = prov.Stop(ctx) }()

	if err := prov.RegisterPipeline(ctx, pipeline); err != nil {
		return fmt.Errorf("registering pipeline: %w", err)
	}

	color.Green("Pipeline %q registered with archetype %q", name, archetypeName)
	return nil
}
