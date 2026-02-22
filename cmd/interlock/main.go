package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/dwsmith1983/interlock/internal/commands"
)

var version = "dev"

func main() {
	root := &cobra.Command{
		Use:   "interlock",
		Short: "STAMP-based safety framework for data pipeline reliability",
		Long: `Interlock prevents pipelines from executing when preconditions aren't safe.
It applies Leveson's Systems-Theoretic Accident Model to data engineering:
pipelines have control structures with traits (feedback), readiness predicates
(process models), and conditional execution (safe control actions).`,
		Version: version,
	}

	root.AddCommand(
		commands.NewInitCmd(),
		commands.NewAddPipelineCmd(),
		commands.NewEvaluateCmd(),
		commands.NewRunCmd(),
		commands.NewStatusCmd(),
		commands.NewServeCmd(),
	)

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
