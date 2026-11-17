package lambda

import (
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PostRunWatchEnd is when the post-run watch on a completed date ends: the
// trigger row's completedAt plus postRun.watchWindow. An unset, unparseable or
// non-positive window uses types.DefaultPostRunWatchWindow; validation rejects
// a bad one when the config loads, so the fallback only covers unset ones in
// practice. It is the zero time when tr is nil or has no parseable completedAt.
func PostRunWatchEnd(cfg *types.PipelineConfig, tr *types.ControlRecord) time.Time {
	if tr == nil {
		return time.Time{}
	}
	completed, err := time.Parse(time.RFC3339, tr.CompletedAt)
	if err != nil {
		return time.Time{}
	}
	window := types.DefaultPostRunWatchWindow
	if cfg != nil && cfg.PostRun != nil {
		if d, err := time.ParseDuration(cfg.PostRun.WatchWindow); err == nil && d > 0 {
			window = d
		}
	}
	return completed.Add(window)
}

// PostRunWatched reports whether now is before the date's post-run watch end.
// At the end instant itself the date is no longer watched.
func PostRunWatched(cfg *types.PipelineConfig, tr *types.ControlRecord, now time.Time) bool {
	end := PostRunWatchEnd(cfg, tr)
	return !end.IsZero() && now.Before(end)
}
