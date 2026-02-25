package commands

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

const initPipelineTimeout = 60 * time.Second

// NewInitCmd creates the init command.
func NewInitCmd() *cobra.Command {
	var skipValkey bool

	cmd := &cobra.Command{
		Use:   "init [project-name]",
		Short: "Initialize a new Interlock project",
		Long:  "Creates project scaffolding and optionally starts a local Valkey container.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInit(args[0], skipValkey)
		},
	}

	cmd.Flags().BoolVar(&skipValkey, "skip-valkey", false, "Skip starting Valkey container")
	return cmd
}

func runInit(projectName string, skipValkey bool) error {
	bold := color.New(color.Bold)

	_, _ = bold.Printf("Initializing Interlock project: %s\n", projectName)

	// Create directory structure
	dirs := []string{
		"evaluators",
		"archetypes",
		"pipelines",
	}

	for _, dir := range dirs {
		path := filepath.Join(projectName, dir)
		if err := os.MkdirAll(path, 0o755); err != nil {
			return fmt.Errorf("creating directory %s: %w", path, err)
		}
	}

	// Write interlock.yaml
	configPath := filepath.Join(projectName, "interlock.yaml")
	configContent := `provider: redis
redis:
  addr: localhost:6379
  keyPrefix: "interlock:"
server:
  addr: ":3000"
archetypeDirs:
  - ./archetypes
evaluatorDirs:
  - ./evaluators
pipelineDirs:
  - ./pipelines
alerts:
  - type: console
`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		return fmt.Errorf("writing config: %w", err)
	}

	// Write starter archetypes
	if err := writeStarterArchetypes(filepath.Join(projectName, "archetypes")); err != nil {
		return fmt.Errorf("writing starter archetypes: %w", err)
	}

	// Write example evaluator
	evalPath := filepath.Join(projectName, "evaluators", "check-source-freshness")
	evalContent := `#!/bin/bash
# Example evaluator: always passes.
# Reads config from stdin, writes result JSON to stdout.
# Replace with real checks for your data sources.
echo '{"status":"PASS","value":{"source":"example","lagSeconds":0}}'
`
	if err := os.WriteFile(evalPath, []byte(evalContent), 0o755); err != nil {
		return fmt.Errorf("writing example evaluator: %w", err)
	}

	// Write example pipeline
	pipelinePath := filepath.Join(projectName, "pipelines", "example.yaml")
	pipelineContent := `name: example
archetype: batch-ingestion
tier: 3

traits:
  source-freshness:
    evaluator: ./evaluators/check-source-freshness
    config:
      source: example_table
      maxLagSeconds: 300

trigger:
  type: command
  command: "echo 'Pipeline executed successfully'"
`
	if err := os.WriteFile(pipelinePath, []byte(pipelineContent), 0o644); err != nil {
		return fmt.Errorf("writing example pipeline: %w", err)
	}

	color.Green("  ✓ Project scaffolded")

	// Start Valkey container
	if !skipValkey {
		if err := startValkey(); err != nil {
			color.Yellow("  ⚠ Valkey setup skipped: %v", err)
			color.Yellow("    Run manually: docker run -d --name interlock-valkey -p 6379:6379 valkey/valkey:8")
		} else {
			color.Green("  ✓ Valkey container started")
		}
	} else {
		color.Yellow("  → Valkey setup skipped (--skip-valkey)")
	}

	fmt.Println()
	_, _ = bold.Println("Next steps:")
	fmt.Printf("  cd %s\n", projectName)
	fmt.Println("  interlock evaluate example")
	fmt.Println("  interlock serve")
	return nil
}

func startValkey() error {
	// Check Docker availability
	if _, err := exec.LookPath("docker"); err != nil {
		return fmt.Errorf("docker not found in PATH")
	}

	// Check if container already exists
	checkCmd := exec.Command("docker", "inspect", "interlock-valkey")
	if checkCmd.Run() == nil {
		// Container exists, try starting it
		startCmd := exec.Command("docker", "start", "interlock-valkey")
		if err := startCmd.Run(); err != nil {
			return fmt.Errorf("starting existing container: %w", err)
		}
		return nil
	}

	// Create and start new container
	ctx, cancel := context.WithTimeout(context.Background(), initPipelineTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", "interlock-valkey",
		"-p", "6379:6379",
		"valkey/valkey:8",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func writeStarterArchetypes(dir string) error {
	archetypes := map[string]string{
		"batch-ingestion.yaml": `name: batch-ingestion
requiredTraits:
  - type: source-freshness
    description: "Source data is recent enough"
    defaultConfig:
      maxLagSeconds: 300
    defaultTtl: 300
    defaultTimeout: 30
  - type: upstream-dependency
    description: "Upstream pipelines completed"
    defaultTtl: 600
    defaultTimeout: 60
  - type: resource-availability
    description: "Resources available"
    defaultTtl: 120
    defaultTimeout: 10
optionalTraits:
  - type: schema-contract
    description: "Schema compatibility verified"
  - type: data-quality-sample
    description: "Sample data quality check"
readinessRule:
  type: all-required-pass
`,
		"streaming-enrichment.yaml": `name: streaming-enrichment
requiredTraits:
  - type: source-freshness
    description: "Stream is active and producing"
    defaultConfig:
      maxLagSeconds: 60
    defaultTtl: 60
    defaultTimeout: 10
  - type: enrichment-source
    description: "Enrichment data source available"
    defaultTtl: 300
    defaultTimeout: 30
  - type: resource-availability
    description: "Compute and memory available"
    defaultTtl: 60
    defaultTimeout: 10
optionalTraits:
  - type: schema-contract
    description: "Schema compatibility verified"
readinessRule:
  type: all-required-pass
`,
		"ml-feature-pipeline.yaml": `name: ml-feature-pipeline
requiredTraits:
  - type: source-freshness
    description: "Training data is current"
    defaultConfig:
      maxLagSeconds: 3600
    defaultTtl: 3600
    defaultTimeout: 60
  - type: upstream-dependency
    description: "Upstream feature pipelines completed"
    defaultTtl: 3600
    defaultTimeout: 120
  - type: resource-availability
    description: "GPU/compute resources available"
    defaultTtl: 300
    defaultTimeout: 15
  - type: model-registry
    description: "Model registry accessible"
    defaultTtl: 600
    defaultTimeout: 30
optionalTraits:
  - type: data-drift-check
    description: "Input data distribution within bounds"
  - type: feature-store-health
    description: "Feature store is healthy"
readinessRule:
  type: all-required-pass
`,
	}

	for name, content := range archetypes {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", name, err)
		}
	}
	return nil
}
