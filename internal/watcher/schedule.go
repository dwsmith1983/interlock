package watcher

import (
	"log/slog"
	"time"

	"github.com/interlock-systems/interlock/internal/calendar"
	"github.com/interlock-systems/interlock/internal/schedule"
	"github.com/interlock-systems/interlock/pkg/types"
)

func isScheduleActive(sched types.ScheduleConfig, now time.Time, logger *slog.Logger) bool {
	return schedule.IsScheduleActive(sched, now, logger)
}

func parseTimeOfDay(hhmm string, ref time.Time, loc *time.Location) (time.Time, error) {
	return schedule.ParseTimeOfDay(hhmm, ref, loc)
}

func scheduleDeadline(sched types.ScheduleConfig, pipeline types.PipelineConfig, now time.Time) (time.Time, bool) {
	return schedule.ScheduleDeadline(sched, pipeline, now)
}

func isExcluded(pipeline types.PipelineConfig, calReg *calendar.Registry, now time.Time) bool {
	return schedule.IsExcluded(pipeline, calReg, now)
}
