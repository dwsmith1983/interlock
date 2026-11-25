package watchdog

import (
	"context"
	"fmt"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// The RUN# query window around now: wide enough for any timezone and a
// deadlineDayOffset or cutoffDayOffset up to 7. The breach time (or the
// cutoff) decides what is still due.
const (
	runKeysDaysBack  = 8
	runKeysDaysAhead = 2
)

// scheduleSLAAlerts proactively creates EventBridge Scheduler entries for
// every expected run date (RUN# row) of each pipeline with an sla.deadline.
func scheduleSLAAlerts(ctx context.Context, d *lambda.Deps) error {
	if d.Scheduler == nil {
		return nil
	}
	if lambda.SkipScheduler() {
		d.Logger.Info("SKIP_SCHEDULER set, no-op proactive SLA scheduling")
		return nil
	}

	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.Now()
	from := now.UTC().AddDate(0, 0, -runKeysDaysBack).Format("2006-01-02")
	to := now.UTC().AddDate(0, 0, runKeysDaysAhead).Format("2006-01-02")

	for id, cfg := range configs {
		// A relative-only SLA (maxDuration) has no absolute deadline to schedule.
		if cfg.SLA == nil || cfg.SLA.Deadline == "" || cfg.DryRun {
			continue
		}

		keys, err := d.Store.ListRunKeys(ctx, id, from, to)
		if err != nil {
			d.Logger.Error("run key lookup failed in SLA scheduling", "pipelineId", id, "error", err)
			continue
		}
		if len(keys) == 0 {
			d.Logger.Debug("no RUN# rows in range, no proactive SLA alerts", "pipelineId", id)
			continue
		}

		for _, key := range keys {
			if !lambda.IsValidExecutionDate(key) {
				d.Logger.Warn("skipping malformed RUN# key", "pipelineId", id, "runKey", key)
				continue
			}
			if !lambda.SLADeadlineFitsDate(cfg.SLA.Deadline, key) || lambda.IsExcludedDate(cfg, key) {
				continue
			}
			scheduleSLAForDate(ctx, d, id, cfg, key, now)
		}
	}
	return nil
}

// scheduleSLAForDate ensures the warning and breach schedules for one run
// date, unless the run has finished or its breach has passed.
func scheduleSLAForDate(ctx context.Context, d *lambda.Deps, id string, cfg *types.PipelineConfig, date string, now time.Time) {
	scheduleID := lambda.ResolveScheduleID(cfg)
	calc, err := lambda.HandleSLACalculate(lambda.SLAMonitorInput{
		PipelineID:       id,
		ScheduleID:       scheduleID,
		Date:             lambda.ResolveSLADate(cfg, date),
		Deadline:         cfg.SLA.Deadline,
		ExpectedDuration: cfg.SLA.ExpectedDuration,
		Timezone:         cfg.SLA.Timezone,
	}, now)
	if err != nil {
		d.Logger.Error("SLA calculate failed", "pipelineId", id, "date", date, "error", err)
		return
	}
	// A passed breach needs no schedules, so a past row costs no reads.
	breachAt, _ := time.Parse(time.RFC3339, calc.BreachAt)
	if !breachAt.IsZero() && !breachAt.After(now) {
		return
	}

	tr, err := d.Store.GetTrigger(ctx, id, scheduleID, date)
	if err != nil {
		d.Logger.Warn("trigger lookup failed in SLA scheduling", "pipelineId", id, "date", date, "error", err)
		return
	}
	if finished, _ := lambda.RunFinished(ctx, d, tr, id, scheduleID, date); finished {
		return
	}

	// A warning at or before now has fired already (or is due): a
	// schedule with a past at() would fire it again.
	toCreate := calc
	if warningAt, err := time.Parse(time.RFC3339, calc.WarningAt); err == nil && !warningAt.After(now) {
		toCreate.WarningAt = ""
	}
	if err := lambda.CreateSLASchedules(ctx, d, id, scheduleID, date, toCreate, true); err != nil {
		d.Logger.Error("create SLA schedule failed",
			"pipelineId", id, "date", date, "error", err)
		return
	}
	d.Logger.Info("proactive SLA schedules ensured",
		"pipelineId", id, "date", date,
		"warningAt", calc.WarningAt, "breachAt", calc.BreachAt)
}
