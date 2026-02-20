package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/interlock-systems/interlock/internal/alert"
	"github.com/interlock-systems/interlock/internal/archetype"
	"github.com/interlock-systems/interlock/internal/config"
	"github.com/interlock-systems/interlock/internal/engine"
	"github.com/interlock-systems/interlock/internal/evaluator"
	"github.com/interlock-systems/interlock/internal/provider/redis"
	"github.com/interlock-systems/interlock/internal/server"
)

// NewServeCmd creates the serve command.
func NewServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start the Interlock HTTP API server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServe()
		},
	}
}

func runServe() error {
	cfg, err := config.Load(".")
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Provider
	prov := redis.New(cfg.Redis)
	ctx := context.Background()
	if err := prov.Start(ctx); err != nil {
		return fmt.Errorf("connecting to Redis: %w", err)
	}

	// Archetypes
	reg := archetype.NewRegistry()
	for _, dir := range cfg.ArchetypeDirs {
		if err := reg.LoadDir(dir); err != nil {
			return fmt.Errorf("loading archetypes from %s: %w", dir, err)
		}
	}

	// Load pipelines
	if err := loadPipelines(ctx, cfg, prov); err != nil {
		return err
	}

	// Alerts
	dispatcher, err := alert.NewDispatcher(cfg.Alerts)
	if err != nil {
		return fmt.Errorf("creating alert dispatcher: %w", err)
	}

	// Engine
	runner := evaluator.NewRunner(cfg.EvaluatorDirs)
	eng := engine.New(prov, reg, runner, dispatcher.AlertFunc())

	// Server
	addr := ":3000"
	if cfg.Server != nil && cfg.Server.Addr != "" {
		addr = cfg.Server.Addr
	}
	srv := server.New(addr, eng, prov)

	// Graceful shutdown
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case sig := <-sigCh:
		color.Yellow("\nReceived %s, shutting down...", sig)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Stop(shutdownCtx); err != nil {
			return fmt.Errorf("server shutdown: %w", err)
		}
		_ = prov.Stop(shutdownCtx)
		color.Green("Server stopped gracefully")
		return nil
	}
}
