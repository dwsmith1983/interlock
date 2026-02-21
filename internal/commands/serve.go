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

	"log/slog"

	"github.com/interlock-systems/interlock/internal/alert"
	"github.com/interlock-systems/interlock/internal/archetype"
	"github.com/interlock-systems/interlock/internal/archiver"
	"github.com/interlock-systems/interlock/internal/calendar"
	"github.com/interlock-systems/interlock/internal/config"
	"github.com/interlock-systems/interlock/internal/engine"
	"github.com/interlock-systems/interlock/internal/evaluator"
	pgstore "github.com/interlock-systems/interlock/internal/provider/postgres"
	"github.com/interlock-systems/interlock/internal/provider/redis"
	"github.com/interlock-systems/interlock/internal/server"
	"github.com/interlock-systems/interlock/internal/watcher"
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

	// Calendars
	calReg := calendar.NewRegistry()
	for _, dir := range cfg.CalendarDirs {
		if err := calReg.LoadDir(dir); err != nil {
			return fmt.Errorf("loading calendars from %s: %w", dir, err)
		}
	}

	// Load pipelines
	if err := loadPipelines(ctx, cfg, prov); err != nil {
		return err
	}

	// Logger
	logger := slog.Default()

	// Alerts
	dispatcher, err := alert.NewDispatcher(cfg.Alerts, logger)
	if err != nil {
		return fmt.Errorf("creating alert dispatcher: %w", err)
	}

	// Engine
	runner := evaluator.NewRunner()
	eng := engine.New(prov, reg, runner, dispatcher.AlertFunc())
	eng.SetLogger(logger)
	if cfg.Engine != nil && cfg.Engine.DefaultTimeout != "" {
		if d, err := time.ParseDuration(cfg.Engine.DefaultTimeout); err == nil {
			eng.SetDefaultTimeout(d)
		}
	}

	// Watcher
	var w *watcher.Watcher
	if cfg.Watcher != nil && cfg.Watcher.Enabled {
		w = watcher.New(prov, eng, calReg, dispatcher.AlertFunc(), logger, *cfg.Watcher)
	}

	// Server
	addr := ":3000"
	var apiKey string
	var maxBody int64
	if cfg.Server != nil {
		if cfg.Server.Addr != "" {
			addr = cfg.Server.Addr
		}
		apiKey = cfg.Server.APIKey
		maxBody = cfg.Server.MaxRequestBody
	}
	srv := server.New(addr, eng, prov, reg, apiKey, maxBody)

	// Start watcher
	if w != nil {
		w.Start(ctx)
	}

	// Archiver
	var arc *archiver.Archiver
	if cfg.Archiver != nil && cfg.Archiver.Enabled {
		pg, err := pgstore.New(ctx, cfg.Archiver.DSN)
		if err != nil {
			return fmt.Errorf("connecting to Postgres: %w", err)
		}
		if err := pg.Migrate(ctx); err != nil {
			pg.Close()
			return fmt.Errorf("migrating Postgres: %w", err)
		}
		interval := 5 * time.Minute
		if cfg.Archiver.Interval != "" {
			if d, err := time.ParseDuration(cfg.Archiver.Interval); err == nil && d > 0 {
				interval = d
			}
		}
		arc = archiver.New(prov, pg, interval, logger)
		arc.Start(ctx)
	}

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
		if arc != nil {
			arc.Stop(shutdownCtx)
		}
		if w != nil {
			w.Stop(shutdownCtx)
		}
		if err := srv.Stop(shutdownCtx); err != nil {
			return fmt.Errorf("server shutdown: %w", err)
		}
		_ = prov.Stop(shutdownCtx)
		color.Green("Server stopped gracefully")
		return nil
	}
}
