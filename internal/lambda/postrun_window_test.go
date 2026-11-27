package lambda_test

import (
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestPostRunWatched(t *testing.T) {
	ts := func(s string) time.Time {
		v, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatalf("parse %q: %v", s, err)
		}
		return v
	}
	six := &types.PostRunConfig{WatchWindow: "6h"}
	const completed = "2026-03-10T12:00:00Z"
	sixEnd, defaultEnd := "2026-03-10T18:00:00Z", "2026-03-10T16:00:00Z"
	tests := []struct {
		name        string
		nilCfg      bool
		postRun     *types.PostRunConfig
		nilRow      bool
		completedAt string
		now         string
		wantWatched bool
		wantEnd     string // "" is the zero time
	}{
		{"at completion", false, six, false, completed, "2026-03-10T12:00:00Z", true, sixEnd},
		{"one second before the limit", false, six, false, completed, "2026-03-10T17:59:59Z", true, sixEnd},
		{"at the limit", false, six, false, completed, "2026-03-10T18:00:00Z", false, sixEnd},
		{"after", false, six, false, completed, "2026-03-10T18:00:01Z", false, sixEnd},
		{"default, postRun nil, inside", false, nil, false, completed, "2026-03-10T15:59:59Z", true, defaultEnd},
		{"default, postRun nil, at the limit", false, nil, false, completed, "2026-03-10T16:00:00Z", false, defaultEnd},
		{"default, window empty", false, &types.PostRunConfig{}, false, completed, "2026-03-10T15:59:59Z", true, defaultEnd},
		{"window does not parse", false, &types.PostRunConfig{WatchWindow: "abc"}, false, completed, "2026-03-10T15:59:59Z", true, defaultEnd},
		{"negative window", false, &types.PostRunConfig{WatchWindow: "-1h"}, false, completed, "2026-03-10T15:59:59Z", true, defaultEnd},
		{"no completedAt", false, six, false, "", "2026-03-10T12:00:01Z", false, ""},
		{"bad completedAt", false, six, false, "yesterday", "2026-03-10T12:00:01Z", false, ""},
		{"nil trigger row", false, six, true, "", "2026-03-10T12:00:01Z", false, ""},
		{"nil config", true, nil, false, completed, "2026-03-10T15:59:59Z", true, defaultEnd},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg *types.PipelineConfig
			if !tt.nilCfg {
				cfg = &types.PipelineConfig{PostRun: tt.postRun}
			}
			var tr *types.ControlRecord
			if !tt.nilRow {
				tr = &types.ControlRecord{Status: types.TriggerStatusCompleted, CompletedAt: tt.completedAt}
			}
			var wantEnd time.Time
			if tt.wantEnd != "" {
				wantEnd = ts(tt.wantEnd)
			}
			if got := lambda.PostRunWatchEnd(cfg, tr); !got.Equal(wantEnd) {
				t.Errorf("PostRunWatchEnd = %v, want %v", got, wantEnd)
			}
			if got := lambda.PostRunWatched(cfg, tr, ts(tt.now)); got != tt.wantWatched {
				t.Errorf("PostRunWatched at %s = %v, want %v", tt.now, got, tt.wantWatched)
			}
		})
	}
}

// TestAutoRerunAllowed: a completed date may be rerun automatically until
// completedAt plus the shorter of postRun.watchWindow and job.maxAutoRerunAge
// (4h and 120h by default). At that instant, or with no parseable
// completedAt, it may not.
func TestAutoRerunAllowed(t *testing.T) {
	ts := func(s string) time.Time {
		v, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatalf("parse %q: %v", s, err)
		}
		return v
	}
	const completed = "2026-03-10T12:00:00Z"
	const ageEnd = "2026-03-15T12:00:00Z" // completed plus the default 120h
	tests := []struct {
		name        string
		nilCfg      bool
		window, age string
		nilRow      bool
		completedAt string
		now         string
		wantAllowed bool
		wantEnd     string // "" is the zero time
	}{
		{"defaults, window wins, inside", false, "", "", false, completed, "2026-03-10T15:59:59Z", true, "2026-03-10T16:00:00Z"},
		{"defaults, window wins, at the end", false, "", "", false, completed, "2026-03-10T16:00:00Z", false, "2026-03-10T16:00:00Z"},
		{"window 200h, default age wins, one second before", false, "200h", "", false, completed, "2026-03-15T11:59:59Z", true, ageEnd},
		{"window 200h, default age wins, at the end", false, "200h", "", false, completed, ageEnd, false, ageEnd},
		{"age 1h, window 2h, one second before", false, "2h", "1h", false, completed, "2026-03-10T12:59:59Z", true, "2026-03-10T13:00:00Z"},
		{"age 1h, window 2h, at the end", false, "2h", "1h", false, completed, "2026-03-10T13:00:00Z", false, "2026-03-10T13:00:00Z"},
		{"age does not parse, default age", false, "200h", "abc", false, completed, "2026-03-15T11:59:59Z", true, ageEnd},
		{"nil config, defaults", true, "", "", false, completed, "2026-03-10T15:59:59Z", true, "2026-03-10T16:00:00Z"},
		{"nil trigger row", false, "2h", "1h", true, "", "2026-03-10T12:00:01Z", false, ""},
		{"bad completedAt", false, "2h", "1h", false, "yesterday", "2026-03-10T12:00:01Z", false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg *types.PipelineConfig
			if !tt.nilCfg {
				cfg = &types.PipelineConfig{Job: types.JobConfig{MaxAutoRerunAge: tt.age}}
				if tt.window != "" {
					cfg.PostRun = &types.PostRunConfig{WatchWindow: tt.window}
				}
			}
			var tr *types.ControlRecord
			if !tt.nilRow {
				tr = &types.ControlRecord{Status: types.TriggerStatusCompleted, CompletedAt: tt.completedAt}
			}
			var wantEnd time.Time
			if tt.wantEnd != "" {
				wantEnd = ts(tt.wantEnd)
			}
			if got := lambda.AutoRerunEnd(cfg, tr); !got.Equal(wantEnd) {
				t.Errorf("AutoRerunEnd = %v, want %v", got, wantEnd)
			}
			if got := lambda.AutoRerunAllowed(cfg, tr, ts(tt.now)); got != tt.wantAllowed {
				t.Errorf("AutoRerunAllowed at %s = %v, want %v", tt.now, got, tt.wantAllowed)
			}
		})
	}
}
