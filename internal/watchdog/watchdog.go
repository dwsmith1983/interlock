// Package watchdog detects missed pipeline schedules — the absence of an
// expected control action (a STAMP safety constraint violation).  When upstream
// ingestion fails silently, no MARKER is written, no execution starts, and no
// SLA check ever runs.  The watchdog independently monitors whether pipelines
// have been evaluated within their expected schedule windows.
package watchdog

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dwsmith1983/interlock/internal/calendar"
	"github.com/dwsmith1983/interlock/internal/metrics"
	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/internal/schedule"
	"github.com/dwsmith1983/interlock/pkg/types"
)

const (
	defaultInterval = 5 * time.Minute
	dedupLockTTL    = 24 * time.Hour
)

// MissedSchedule records a single missed schedule detection.
type MissedSchedule struct {
	PipelineID string
	ScheduleID string
	Date       string
	Deadline   string
}

// CheckOptions configures a single watchdog scan pass.
type CheckOptions struct {
	Provider    provider.Provider
	CalendarReg *calendar.Registry
	AlertFn     func(types.Alert)
	Logger      *slog.Logger
	Now         time.Time // injectable for testing
}

// CheckMissedSchedules scans all registered pipelines for schedules whose
// evaluation deadline has passed without a RunLog entry.  It is a pure function
// suitable for any execution mode (local polling, Lambda, Cloud Function).
func CheckMissedSchedules(ctx context.Context, opts CheckOptions) []MissedSchedule {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}

	pipelines, err := opts.Provider.ListPipelines(ctx)
	if err != nil {
		opts.Logger.Error("watchdog: failed to list pipelines", "error", err)
		return nil
	}

	var missed []MissedSchedule

	for _, pl := range pipelines {
		if ctx.Err() != nil {
			return missed
		}

		// Skip pipelines without a trigger or with watch explicitly disabled.
		if pl.Trigger == nil {
			continue
		}
		if pl.Watch != nil && pl.Watch.Enabled != nil && !*pl.Watch.Enabled {
			continue
		}

		// Skip excluded days.
		if schedule.IsExcluded(pl, opts.CalendarReg, opts.Now) {
			continue
		}

		for _, sched := range types.ResolveSchedules(pl) {
			m := checkSchedule(ctx, opts, pl, sched)
			if m != nil {
				missed = append(missed, *m)
			}
		}
	}

	return missed
}

func checkSchedule(ctx context.Context, opts CheckOptions, pl types.PipelineConfig, sched types.ScheduleConfig) *MissedSchedule {
	// Resolve deadline: schedule-level Deadline first, then SLA.EvaluationDeadline.
	deadline, ok := resolveWatchdogDeadline(sched, pl, opts.Now)
	if !ok {
		return nil // no deadline configured — nothing to watch
	}

	// Deadline hasn't passed yet.
	if !opts.Now.After(deadline) {
		return nil
	}

	// Check RunLog — any entry (even FAILED) means the schedule was evaluated.
	date := opts.Now.UTC().Format("2006-01-02")
	entry, err := opts.Provider.GetRunLog(ctx, pl.Name, date, sched.Name)
	if err != nil {
		opts.Logger.Error("watchdog: failed to get run log",
			"pipeline", pl.Name, "schedule", sched.Name, "error", err)
		return nil
	}
	if entry != nil {
		return nil // already evaluated
	}

	// Dedup lock: one alert per pipeline/schedule/day.
	lockKey := fmt.Sprintf("watchdog:%s:%s:%s", pl.Name, sched.Name, date)
	acquired, err := opts.Provider.AcquireLock(ctx, lockKey, dedupLockTTL)
	if err != nil {
		opts.Logger.Error("watchdog: failed to acquire dedup lock",
			"key", lockKey, "error", err)
		return nil
	}
	if !acquired {
		return nil // already alerted
	}

	deadlineStr := deadline.Format("15:04")

	// Fire alert.
	if opts.AlertFn != nil {
		opts.AlertFn(types.Alert{
			Level:      types.AlertLevelError,
			PipelineID: pl.Name,
			Message: fmt.Sprintf("Pipeline %s schedule %s missed: no evaluation started by deadline %s on %s",
				pl.Name, sched.Name, deadlineStr, date),
			Details: map[string]interface{}{
				"scheduleId": sched.Name,
				"date":       date,
				"deadline":   deadlineStr,
				"type":       "schedule_missed",
			},
			Timestamp: opts.Now,
		})
	}

	// Append audit event.
	if err := opts.Provider.AppendEvent(ctx, types.Event{
		Kind:       types.EventScheduleMissed,
		PipelineID: pl.Name,
		Message: fmt.Sprintf("schedule %s missed: deadline %s on %s",
			sched.Name, deadlineStr, date),
		Details: map[string]interface{}{
			"scheduleId": sched.Name,
			"date":       date,
			"deadline":   deadlineStr,
		},
		Timestamp: opts.Now,
	}); err != nil {
		opts.Logger.Error("watchdog: failed to append event",
			"pipeline", pl.Name, "schedule", sched.Name, "error", err)
	}

	metrics.SchedulesMissed.Add(1)

	opts.Logger.Warn("watchdog: missed schedule detected",
		"pipeline", pl.Name, "schedule", sched.Name,
		"deadline", deadlineStr, "date", date)

	return &MissedSchedule{
		PipelineID: pl.Name,
		ScheduleID: sched.Name,
		Date:       date,
		Deadline:   deadlineStr,
	}
}

// resolveWatchdogDeadline returns the evaluation deadline for a schedule.
// Priority: schedule Deadline > SLA.EvaluationDeadline.
func resolveWatchdogDeadline(sched types.ScheduleConfig, pl types.PipelineConfig, now time.Time) (time.Time, bool) {
	// 1. Schedule-level Deadline (same field used by ScheduleDeadline).
	if sched.Deadline != "" {
		loc := time.UTC
		if sched.Timezone != "" {
			if l, err := time.LoadLocation(sched.Timezone); err == nil {
				loc = l
			}
		}
		if t, err := schedule.ParseTimeOfDay(sched.Deadline, now, loc); err == nil {
			return t, true
		}
	}

	// 2. Pipeline SLA.EvaluationDeadline.
	if pl.SLA != nil && pl.SLA.EvaluationDeadline != "" {
		if t, err := schedule.ParseSLADeadline(pl.SLA.EvaluationDeadline, pl.SLA.Timezone, now); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

// ---------------------------------------------------------------------------
// Watchdog — polling wrapper for local mode
// ---------------------------------------------------------------------------

// Watchdog runs CheckMissedSchedules on a regular interval.
type Watchdog struct {
	provider    provider.Provider
	calendarReg *calendar.Registry
	alertFn     func(types.Alert)
	logger      *slog.Logger
	interval    time.Duration
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// New creates a new Watchdog.
func New(prov provider.Provider, calReg *calendar.Registry, alertFn func(types.Alert), logger *slog.Logger, interval time.Duration) *Watchdog {
	if interval <= 0 {
		interval = defaultInterval
	}
	return &Watchdog{
		provider:    prov,
		calendarReg: calReg,
		alertFn:     alertFn,
		logger:      logger,
		interval:    interval,
	}
}

// Start begins the watchdog polling loop.
func (w *Watchdog) Start(ctx context.Context) {
	ctx, w.cancel = context.WithCancel(ctx)
	w.wg.Add(1)
	go w.loop(ctx)
	w.logger.Info("watchdog started", "interval", w.interval)
}

// Stop signals the watchdog to stop and waits for it to finish.
func (w *Watchdog) Stop(_ context.Context) {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
	w.logger.Info("watchdog stopped")
}

func (w *Watchdog) loop(ctx context.Context) {
	defer w.wg.Done()
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	// Run once immediately on start.
	w.scan(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.scan(ctx)
		}
	}
}

func (w *Watchdog) scan(ctx context.Context) {
	CheckMissedSchedules(ctx, CheckOptions{
		Provider:    w.provider,
		CalendarReg: w.calendarReg,
		AlertFn:     w.alertFn,
		Logger:      w.logger,
	})
}
