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
	"github.com/dwsmith1983/interlock/internal/lifecycle"
	"github.com/dwsmith1983/interlock/internal/metrics"
	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/internal/schedule"
	"github.com/dwsmith1983/interlock/pkg/types"
)

const (
	defaultInterval       = 5 * time.Minute
	dedupLockTTL          = 24 * time.Hour
	defaultStuckThreshold = 30 * time.Minute
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
	Provider          provider.Provider
	CalendarReg       *calendar.Registry
	AlertFn           func(context.Context, types.Alert)
	Logger            *slog.Logger
	Now               time.Time                                                      // injectable for testing
	StuckRunThreshold time.Duration                                                  // defaults to 30m if zero
	RetriggerFn       func(ctx context.Context, pipelineID, scheduleID string) error // nil = alert-only
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
		opts.AlertFn(ctx, types.Alert{
			Level:      types.AlertLevelError,
			Category:   "schedule_missed",
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
// Stuck-run detection
// ---------------------------------------------------------------------------

// StuckRun records a pipeline run that has been in a non-terminal state too long.
type StuckRun struct {
	PipelineID string
	ScheduleID string
	Date       string
	Status     types.RunStatus
	Duration   time.Duration
}

// CheckStuckRuns scans all registered pipelines for runs that have been in a
// non-terminal state (PENDING, TRIGGERING, RUNNING) longer than the threshold.
func CheckStuckRuns(ctx context.Context, opts CheckOptions) []StuckRun {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	threshold := opts.StuckRunThreshold
	if threshold <= 0 {
		threshold = defaultStuckThreshold
	}

	pipelines, err := opts.Provider.ListPipelines(ctx)
	if err != nil {
		opts.Logger.Error("watchdog: failed to list pipelines for stuck-run check", "error", err)
		return nil
	}

	var stuck []StuckRun

	for _, pl := range pipelines {
		if ctx.Err() != nil {
			return stuck
		}

		// Skip pipelines without a trigger or with watch explicitly disabled.
		if pl.Trigger == nil {
			continue
		}
		if pl.Watch != nil && pl.Watch.Enabled != nil && !*pl.Watch.Enabled {
			continue
		}

		date := opts.Now.UTC().Format("2006-01-02")

		for _, sched := range types.ResolveSchedules(pl) {
			entry, err := opts.Provider.GetRunLog(ctx, pl.Name, date, sched.Name)
			if err != nil {
				opts.Logger.Error("watchdog: failed to get run log for stuck check",
					"pipeline", pl.Name, "schedule", sched.Name, "error", err)
				continue
			}
			if entry == nil {
				continue // no run started
			}

			if lifecycle.IsTerminal(entry.Status) {
				continue
			}

			age := opts.Now.Sub(entry.UpdatedAt)
			if age < threshold {
				continue
			}

			// Dedup lock: one stuck alert per pipeline/schedule/day.
			lockKey := fmt.Sprintf("watchdog:stuck:%s:%s:%s", pl.Name, sched.Name, date)
			acquired, err := opts.Provider.AcquireLock(ctx, lockKey, dedupLockTTL)
			if err != nil {
				opts.Logger.Error("watchdog: failed to acquire stuck dedup lock",
					"key", lockKey, "error", err)
				continue
			}
			if !acquired {
				continue
			}

			// Fire alert.
			if opts.AlertFn != nil {
				opts.AlertFn(ctx, types.Alert{
					Level:      types.AlertLevelError,
					Category:   "stuck_run",
					PipelineID: pl.Name,
					Message: fmt.Sprintf("Pipeline %s schedule %s run stuck in %s for %s on %s",
						pl.Name, sched.Name, entry.Status, age.Truncate(time.Second), date),
					Details: map[string]interface{}{
						"scheduleId": sched.Name,
						"date":       date,
						"status":     string(entry.Status),
						"duration":   age.String(),
						"runId":      entry.RunID,
					},
					Timestamp: opts.Now,
				})
			}

			// Append audit event.
			if err := opts.Provider.AppendEvent(ctx, types.Event{
				Kind:       types.EventRunStuck,
				PipelineID: pl.Name,
				RunID:      entry.RunID,
				Status:     string(entry.Status),
				Message: fmt.Sprintf("run stuck in %s for %s",
					entry.Status, age.Truncate(time.Second)),
				Details: map[string]interface{}{
					"scheduleId": sched.Name,
					"date":       date,
					"duration":   age.String(),
				},
				Timestamp: opts.Now,
			}); err != nil {
				opts.Logger.Error("watchdog: failed to append stuck-run event",
					"pipeline", pl.Name, "schedule", sched.Name, "error", err)
			}

			metrics.RunsStuck.Add(1)

			opts.Logger.Warn("watchdog: stuck run detected",
				"pipeline", pl.Name, "schedule", sched.Name,
				"status", entry.Status, "age", age.String(), "date", date)

			stuck = append(stuck, StuckRun{
				PipelineID: pl.Name,
				ScheduleID: sched.Name,
				Date:       date,
				Status:     entry.Status,
				Duration:   age,
			})
		}
	}

	return stuck
}

// ---------------------------------------------------------------------------
// Post-completion monitoring expiry
// ---------------------------------------------------------------------------

// MonitoringResult records a single completed-monitoring expiry action.
type MonitoringResult struct {
	PipelineID string
	ScheduleID string
	Date       string
	Action     string // "expired"
}

// CheckCompletedMonitoring scans all registered pipelines for runs in
// COMPLETED_MONITORING state whose monitoring window has expired, and
// transitions them to COMPLETED.  This offloads the monitoring wait from
// the Step Function execution to the watchdog's periodic scan.
func CheckCompletedMonitoring(ctx context.Context, opts CheckOptions) []MonitoringResult {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}

	pipelines, err := opts.Provider.ListPipelines(ctx)
	if err != nil {
		opts.Logger.Error("watchdog: failed to list pipelines for monitoring check", "error", err)
		return nil
	}

	var results []MonitoringResult

	for _, pl := range pipelines {
		if ctx.Err() != nil {
			return results
		}

		// Only relevant for pipelines with monitoring enabled.
		if pl.Watch == nil || pl.Watch.Monitoring == nil || !pl.Watch.Monitoring.Enabled {
			continue
		}

		duration, err := time.ParseDuration(pl.Watch.Monitoring.Duration)
		if err != nil || duration <= 0 {
			opts.Logger.Warn("watchdog: invalid monitoring duration",
				"pipeline", pl.Name, "duration", pl.Watch.Monitoring.Duration, "error", err)
			continue
		}

		date := opts.Now.UTC().Format("2006-01-02")

		for _, sched := range types.ResolveSchedules(pl) {
			r := checkMonitoringExpiry(ctx, opts, pl, sched, date, duration)
			if r != nil {
				results = append(results, *r)
			}
		}
	}

	return results
}

func checkMonitoringExpiry(ctx context.Context, opts CheckOptions, pl types.PipelineConfig, sched types.ScheduleConfig, date string, monitorDuration time.Duration) *MonitoringResult {
	entry, err := opts.Provider.GetRunLog(ctx, pl.Name, date, sched.Name)
	if err != nil {
		opts.Logger.Error("watchdog: failed to get run log for monitoring check",
			"pipeline", pl.Name, "schedule", sched.Name, "error", err)
		return nil
	}
	if entry == nil || entry.Status != types.RunCompletedMonitoring {
		return nil
	}

	// Use UpdatedAt as the monitoring start time (set when status changed
	// to COMPLETED_MONITORING).
	elapsed := opts.Now.Sub(entry.UpdatedAt)
	if elapsed < monitorDuration {
		return nil // still within monitoring window
	}

	// Monitoring window expired — transition to COMPLETED.
	now := opts.Now
	entry.Status = types.RunCompleted
	entry.CompletedAt = &now
	entry.UpdatedAt = now

	if err := opts.Provider.PutRunLog(ctx, *entry); err != nil {
		opts.Logger.Error("watchdog: failed to transition monitoring run to COMPLETED",
			"pipeline", pl.Name, "schedule", sched.Name, "error", err)
		return nil
	}

	// Append audit event.
	if err := opts.Provider.AppendEvent(ctx, types.Event{
		Kind:       types.EventMonitoringCompleted,
		PipelineID: pl.Name,
		RunID:      entry.RunID,
		Status:     string(types.RunCompleted),
		Message: fmt.Sprintf("monitoring window expired after %s, transitioned to COMPLETED",
			monitorDuration),
		Details: map[string]interface{}{
			"scheduleId":       sched.Name,
			"date":             date,
			"monitorDuration":  monitorDuration.String(),
			"elapsedInMonitor": elapsed.Truncate(time.Second).String(),
		},
		Timestamp: opts.Now,
	}); err != nil {
		opts.Logger.Error("watchdog: failed to append monitoring-completed event",
			"pipeline", pl.Name, "schedule", sched.Name, "error", err)
	}

	opts.Logger.Info("watchdog: monitoring window expired, run transitioned to COMPLETED",
		"pipeline", pl.Name, "schedule", sched.Name, "date", date,
		"elapsed", elapsed.Truncate(time.Second))

	return &MonitoringResult{
		PipelineID: pl.Name,
		ScheduleID: sched.Name,
		Date:       date,
		Action:     "expired",
	}
}

// ---------------------------------------------------------------------------
// Failed-run re-trigger
// ---------------------------------------------------------------------------

// RetriggeredRun records a single watchdog-initiated re-trigger.
type RetriggeredRun struct {
	PipelineID string
	ScheduleID string
	Date       string
	Attempt    int
}

// CheckFailedRuns scans all registered pipelines for FAILED runs today and
// re-triggers eligible ones via RetriggerFn. A run is eligible if the pipeline
// has a retry policy with retryable failure categories that match the run's
// failure category, and the attempt count is below MaxAttempts.
// If RetriggerFn is nil, this function is a no-op.
func CheckFailedRuns(ctx context.Context, opts CheckOptions) []RetriggeredRun {
	if opts.RetriggerFn == nil {
		return nil
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}

	pipelines, err := opts.Provider.ListPipelines(ctx)
	if err != nil {
		opts.Logger.Error("watchdog: failed to list pipelines for failed-run check", "error", err)
		return nil
	}

	var retriggered []RetriggeredRun
	date := opts.Now.UTC().Format("2006-01-02")

	for _, pl := range pipelines {
		if ctx.Err() != nil {
			return retriggered
		}
		if pl.Trigger == nil || pl.Retry == nil {
			continue
		}
		if pl.Watch != nil && pl.Watch.Enabled != nil && !*pl.Watch.Enabled {
			continue
		}

		for _, sched := range types.ResolveSchedules(pl) {
			entry, err := opts.Provider.GetRunLog(ctx, pl.Name, date, sched.Name)
			if err != nil {
				opts.Logger.Error("watchdog: failed to get run log for failed-run check",
					"pipeline", pl.Name, "schedule", sched.Name, "error", err)
				continue
			}
			if entry == nil || entry.Status != types.RunFailed {
				continue
			}

			// Check retry eligibility.
			if entry.AttemptNumber >= pl.Retry.MaxAttempts {
				continue
			}
			if !isRetryableCategory(entry.FailureCategory, pl.Retry.RetryableFailures) {
				continue
			}

			// Dedup lock: one retrigger per pipeline/schedule/day/attempt.
			nextAttempt := entry.AttemptNumber + 1
			lockKey := fmt.Sprintf("watchdog:retrigger:%s:%s:%s:%d", pl.Name, sched.Name, date, nextAttempt)
			acquired, err := opts.Provider.AcquireLock(ctx, lockKey, dedupLockTTL)
			if err != nil {
				opts.Logger.Error("watchdog: failed to acquire retrigger dedup lock",
					"key", lockKey, "error", err)
				continue
			}
			if !acquired {
				continue
			}

			// Re-trigger execution.
			if err := opts.RetriggerFn(ctx, pl.Name, sched.Name); err != nil {
				opts.Logger.Error("watchdog: retrigger failed",
					"pipeline", pl.Name, "schedule", sched.Name, "error", err)
				continue
			}

			// Fire info alert.
			if opts.AlertFn != nil {
				opts.AlertFn(ctx, types.Alert{
					Level:      types.AlertLevelInfo,
					Category:   "watchdog_retrigger",
					PipelineID: pl.Name,
					Message: fmt.Sprintf("Watchdog re-triggered %s schedule %s (attempt %d)",
						pl.Name, sched.Name, nextAttempt),
					Details: map[string]interface{}{
						"scheduleId":      sched.Name,
						"date":            date,
						"attempt":         nextAttempt,
						"failureCategory": string(entry.FailureCategory),
					},
					Timestamp: opts.Now,
				})
			}

			// Append audit event.
			if err := opts.Provider.AppendEvent(ctx, types.Event{
				Kind:       types.EventWatchdogRetrigger,
				PipelineID: pl.Name,
				RunID:      entry.RunID,
				Status:     string(types.RunFailed),
				Message: fmt.Sprintf("watchdog re-triggered schedule %s attempt %d",
					sched.Name, nextAttempt),
				Details: map[string]interface{}{
					"scheduleId":      sched.Name,
					"date":            date,
					"attempt":         nextAttempt,
					"failureCategory": string(entry.FailureCategory),
				},
				Timestamp: opts.Now,
			}); err != nil {
				opts.Logger.Error("watchdog: failed to append retrigger event",
					"pipeline", pl.Name, "schedule", sched.Name, "error", err)
			}

			opts.Logger.Info("watchdog: re-triggered failed run",
				"pipeline", pl.Name, "schedule", sched.Name,
				"attempt", nextAttempt, "date", date)

			retriggered = append(retriggered, RetriggeredRun{
				PipelineID: pl.Name,
				ScheduleID: sched.Name,
				Date:       date,
				Attempt:    nextAttempt,
			})
		}
	}

	return retriggered
}

func isRetryableCategory(category types.FailureCategory, retryable []types.FailureCategory) bool {
	if len(retryable) == 0 {
		// No retryable categories configured — retry all.
		return true
	}
	for _, r := range retryable {
		if r == category {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Watchdog — polling wrapper for local mode
// ---------------------------------------------------------------------------

// Watchdog runs CheckMissedSchedules on a regular interval.
type Watchdog struct {
	provider    provider.Provider
	calendarReg *calendar.Registry
	alertFn     func(context.Context, types.Alert)
	logger      *slog.Logger
	interval    time.Duration
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// New creates a new Watchdog.
func New(prov provider.Provider, calReg *calendar.Registry, alertFn func(context.Context, types.Alert), logger *slog.Logger, interval time.Duration) *Watchdog {
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
	opts := CheckOptions{
		Provider:    w.provider,
		CalendarReg: w.calendarReg,
		AlertFn:     w.alertFn,
		Logger:      w.logger,
	}
	CheckMissedSchedules(ctx, opts)
	CheckStuckRuns(ctx, opts)
	CheckCompletedMonitoring(ctx, opts)
}
