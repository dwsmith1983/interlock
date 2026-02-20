// Package watcher implements reactive pipeline evaluation and triggering.
package watcher

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/interlock-systems/interlock/internal/engine"
	"github.com/interlock-systems/interlock/internal/provider"
	"github.com/interlock-systems/interlock/pkg/types"
)

// Watcher periodically evaluates pipelines and triggers them when ready.
type Watcher struct {
	provider provider.Provider
	engine   *engine.Engine
	alertFn  func(types.Alert)
	logger   *slog.Logger
	config   types.WatcherConfig

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a new Watcher.
func New(prov provider.Provider, eng *engine.Engine, alertFn func(types.Alert), logger *slog.Logger, cfg types.WatcherConfig) *Watcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Watcher{
		provider: prov,
		engine:   eng,
		alertFn:  alertFn,
		logger:   logger,
		config:   cfg,
	}
}

// Start begins the watcher polling loop.
func (w *Watcher) Start(ctx context.Context) {
	ctx, w.cancel = context.WithCancel(ctx)

	interval, err := time.ParseDuration(w.config.DefaultInterval)
	if err != nil || interval <= 0 {
		interval = 30 * time.Second
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.logger.Info("watcher started", "interval", interval)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Run immediately on start
		w.poll(ctx, interval)

		for {
			select {
			case <-ctx.Done():
				w.logger.Info("watcher stopping")
				return
			case <-ticker.C:
				w.poll(ctx, interval)
			}
		}
	}()
}

// Stop gracefully shuts down the watcher.
func (w *Watcher) Stop(ctx context.Context) {
	if w.cancel != nil {
		w.cancel()
	}

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.logger.Info("watcher stopped")
	case <-ctx.Done():
		w.logger.Warn("watcher stop timed out")
	}
}

func (w *Watcher) poll(ctx context.Context, defaultInterval time.Duration) {
	pipelines, err := w.provider.ListPipelines(ctx)
	if err != nil {
		w.logger.Error("failed to list pipelines", "error", err)
		return
	}

	for _, pipeline := range pipelines {
		if ctx.Err() != nil {
			return
		}

		// Check if pipeline has watcher disabled
		if pipeline.Watch != nil && pipeline.Watch.Enabled != nil && !*pipeline.Watch.Enabled {
			continue
		}

		// No trigger configured — skip
		if pipeline.Trigger == nil {
			continue
		}

		interval := defaultInterval
		if pipeline.Watch != nil && pipeline.Watch.Interval != "" {
			if d, err := time.ParseDuration(pipeline.Watch.Interval); err == nil && d > 0 {
				interval = d
			}
		}

		w.tick(ctx, pipeline, interval)
	}
}

func (w *Watcher) fireAlert(alert types.Alert) {
	if w.alertFn != nil {
		w.alertFn(alert)
	}
}
