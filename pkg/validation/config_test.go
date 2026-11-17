package validation

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func containsSubstr(ss []string, sub string) bool {
	for _, s := range ss {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func intPtr(v int) *int { return &v }

func TestValidatePipelineConfig(t *testing.T) {
	tests := []struct {
		name       string
		cfg        types.PipelineConfig
		wantCount  int
		wantSubstr string
	}{
		{
			name:      "all defaults, no errors",
			cfg:       types.PipelineConfig{},
			wantCount: 0,
		},
		{
			name: "all fields valid",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{
					MaxRetries:           5,
					MaxDriftReruns:       intPtr(3),
					MaxManualReruns:      intPtr(2),
					MaxCodeRetries:       intPtr(2),
					JobPollWindowSeconds: intPtr(120),
				},
			},
			wantCount: 0,
		},
		{
			name: "MaxRetries negative",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{MaxRetries: -1},
			},
			wantCount:  1,
			wantSubstr: "job.maxRetries -1 out of range [0,10]",
		},
		{
			name: "MaxRetries too high",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{MaxRetries: 11},
			},
			wantCount:  1,
			wantSubstr: "job.maxRetries 11 out of range [0,10]",
		},
		{
			name: "MaxRetries at upper bound is valid",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{MaxRetries: 10},
			},
			wantCount: 0,
		},
		{
			name: "MaxDriftReruns negative",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{MaxDriftReruns: intPtr(-1)},
			},
			wantCount:  1,
			wantSubstr: "job.maxDriftReruns -1 out of range [0,5]",
		},
		{
			name: "MaxDriftReruns too high",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{MaxDriftReruns: intPtr(6)},
			},
			wantCount:  1,
			wantSubstr: "job.maxDriftReruns 6 out of range [0,5]",
		},
		{
			name: "MaxDriftReruns zero is valid",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{MaxDriftReruns: intPtr(0)},
			},
			wantCount: 0,
		},
		{
			name: "MaxManualReruns too high",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{MaxManualReruns: intPtr(6)},
			},
			wantCount:  1,
			wantSubstr: "job.maxManualReruns 6 out of range [0,5]",
		},
		{
			name: "MaxCodeRetries too high",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{MaxCodeRetries: intPtr(4)},
			},
			wantCount:  1,
			wantSubstr: "job.maxCodeRetries 4 out of range [0,3]",
		},
		{
			name: "JobPollWindowSeconds too low",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{JobPollWindowSeconds: intPtr(30)},
			},
			wantCount:  1,
			wantSubstr: "job.jobPollWindowSeconds 30 out of range [60,86400]",
		},
		{
			name: "JobPollWindowSeconds too high",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{JobPollWindowSeconds: intPtr(100000)},
			},
			wantCount:  1,
			wantSubstr: "job.jobPollWindowSeconds 100000 out of range [60,86400]",
		},
		{
			name: "JobPollWindowSeconds at boundary is valid",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{JobPollWindowSeconds: intPtr(60)},
			},
			wantCount: 0,
		},
		{
			name: "JobPollWindowSeconds zero means default, valid",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{JobPollWindowSeconds: intPtr(0)},
			},
			wantCount: 0,
		},
		{
			name: "JobPollWindowSeconds negative",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{JobPollWindowSeconds: intPtr(-5)},
			},
			wantCount:  1,
			wantSubstr: "job.jobPollWindowSeconds -5 out of range [60,86400]",
		},
		{
			name: "nil pointer fields are valid",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{
					MaxRetries:           3,
					MaxDriftReruns:       nil,
					MaxManualReruns:      nil,
					MaxCodeRetries:       nil,
					JobPollWindowSeconds: nil,
				},
			},
			wantCount: 0,
		},
		{
			name: "multiple violations",
			cfg: types.PipelineConfig{
				Job: types.JobConfig{
					MaxRetries:           15,
					MaxDriftReruns:       intPtr(10),
					MaxCodeRetries:       intPtr(5),
					JobPollWindowSeconds: intPtr(10),
				},
			},
			wantCount: 4,
		},
		// --- v0.8.0: inclusion calendar + relative SLA validation ---
		{
			name: "cron and include mutually exclusive",
			cfg: types.PipelineConfig{
				Schedule: types.ScheduleConfig{
					Cron:    "0 2 * * *",
					Include: &types.InclusionConfig{Dates: []string{"2026-03-31"}},
				},
			},
			wantCount:  1,
			wantSubstr: "schedule.cron and schedule.include are mutually exclusive",
		},
		{
			name: "include with valid dates",
			cfg: types.PipelineConfig{
				Schedule: types.ScheduleConfig{
					Include: &types.InclusionConfig{Dates: []string{"2026-03-31", "2026-06-30"}},
				},
			},
			wantCount: 0,
		},
		{
			name: "include with empty dates",
			cfg: types.PipelineConfig{
				Schedule: types.ScheduleConfig{
					Include: &types.InclusionConfig{Dates: []string{}},
				},
			},
			wantCount:  1,
			wantSubstr: "schedule.include.dates must not be empty",
		},
		{
			name: "include with invalid date format",
			cfg: types.PipelineConfig{
				Schedule: types.ScheduleConfig{
					Include: &types.InclusionConfig{Dates: []string{"2026-03-31", "03/31/2026"}},
				},
			},
			wantCount:  1,
			wantSubstr: "schedule.include.dates[1] invalid format",
		},
		{
			name: "maxDuration valid",
			cfg: types.PipelineConfig{
				Schedule: types.ScheduleConfig{
					Trigger: &types.TriggerCondition{Key: "some-sensor", Check: types.CheckExists},
				},
				SLA: &types.SLAConfig{MaxDuration: "2h"},
			},
			wantCount: 0,
		},
		{
			name: "maxDuration exceeds 24h",
			cfg: types.PipelineConfig{
				Schedule: types.ScheduleConfig{
					Trigger: &types.TriggerCondition{Key: "some-sensor", Check: types.CheckExists},
				},
				SLA: &types.SLAConfig{MaxDuration: "25h"},
			},
			wantCount:  1,
			wantSubstr: "sla.maxDuration exceeds 24h",
		},
		{
			name: "maxDuration invalid format",
			cfg: types.PipelineConfig{
				Schedule: types.ScheduleConfig{
					Trigger: &types.TriggerCondition{Key: "some-sensor", Check: types.CheckExists},
				},
				SLA: &types.SLAConfig{MaxDuration: "not-a-duration"},
			},
			wantCount:  1,
			wantSubstr: "sla.maxDuration invalid Go duration",
		},
		{
			name: "maxDuration requires trigger",
			cfg: types.PipelineConfig{
				SLA: &types.SLAConfig{MaxDuration: "2h"},
			},
			wantCount:  1,
			wantSubstr: "sla.maxDuration requires schedule.trigger",
		},
		{
			name: "maxDuration and deadline coexist",
			cfg: types.PipelineConfig{
				Schedule: types.ScheduleConfig{
					Trigger: &types.TriggerCondition{Key: "some-sensor", Check: types.CheckExists},
				},
				SLA: &types.SLAConfig{Deadline: "08:00", MaxDuration: "2h"},
			},
			wantCount: 0,
		},
		// --- job type ---
		{
			name:       "command job type is rejected",
			cfg:        types.PipelineConfig{Job: types.JobConfig{Type: "command"}},
			wantCount:  1,
			wantSubstr: `job.type "command" is not a supported trigger type`,
		},
		{
			name:       "unknown job type is rejected",
			cfg:        types.PipelineConfig{Job: types.JobConfig{Type: "Glue"}},
			wantCount:  1,
			wantSubstr: `job.type "Glue" is not a supported trigger type`,
		},
		// --- dry-run validation ---
		{
			name: "dryRun without job.type or trigger",
			cfg: types.PipelineConfig{
				DryRun: true,
				Job:    types.JobConfig{Type: ""},
			},
			wantCount:  2,
			wantSubstr: "dryRun requires job.type",
		},
		{
			name: "dryRun with job.type but no trigger",
			cfg: types.PipelineConfig{
				DryRun: true,
				Job:    types.JobConfig{Type: "glue"},
			},
			wantCount:  1,
			wantSubstr: "dryRun requires schedule.trigger",
		},
		{
			name: "dryRun fully configured",
			cfg: types.PipelineConfig{
				DryRun: true,
				Schedule: types.ScheduleConfig{
					Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists},
				},
				Job: types.JobConfig{Type: "glue"},
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cfg.Pipeline.ID = "orders" // the ID rule has its own test
			errs := ValidatePipelineConfig(&tt.cfg)
			assert.Len(t, errs, tt.wantCount)
			if tt.wantSubstr != "" {
				assert.True(t, containsSubstr(errs, tt.wantSubstr),
					"expected one of %v to contain %q", errs, tt.wantSubstr)
			}
		})
	}
}

func TestValidatePipelineConfig_PipelineID(t *testing.T) {
	maxID := strings.Repeat("a", MaxPipelineIDLen)
	for _, id := range []string{"orders", "Gold_Revenue-2", "0", "a-b_c", maxID} {
		cfg := types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: id}}
		assert.Empty(t, ValidatePipelineConfig(&cfg), "ID %q should be accepted", id)
	}
	for _, id := range []string{"", maxID + "a", "a b", "a/b", "a#b", "a:b", "a.b", "a\nb", "orders\n", "caf\u00e9"} {
		cfg := types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: id}}
		errs := ValidatePipelineConfig(&cfg)
		assert.Len(t, errs, 1, "ID %q should be rejected", id)
		assert.True(t, containsSubstr(errs, "pipeline.id"), "errors %v should name pipeline.id", errs)
	}
}

func TestValidate_SLADeadlineDayOffset(t *testing.T) {
	tests := []struct {
		name    string
		sla     types.SLAConfig
		wantErr string
	}{
		{"negative", types.SLAConfig{Deadline: "10:00", DeadlineDayOffset: -1}, "sla.deadlineDayOffset -1 out of range [0,7]"},
		{"too large", types.SLAConfig{Deadline: "10:00", DeadlineDayOffset: 8}, "sla.deadlineDayOffset 8 out of range [0,7]"},
		{"hourly deadline", types.SLAConfig{Deadline: ":30", DeadlineDayOffset: 1}, `sla.deadlineDayOffset is not supported with hourly ":MM" deadlines`},
		{"no deadline", types.SLAConfig{MaxDuration: "2h", DeadlineDayOffset: 1}, "sla.deadlineDayOffset is not supported without sla.deadline"},
		{"zero", types.SLAConfig{Deadline: "10:00"}, ""},
		{"one", types.SLAConfig{Deadline: "10:00", DeadlineDayOffset: 1}, ""},
		{"seven", types.SLAConfig{Deadline: "10:00", DeadlineDayOffset: 7}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "orders"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}},
				SLA:      &tt.sla,
			}
			errs := ValidatePipelineConfig(&cfg)
			if tt.wantErr == "" {
				assert.Empty(t, errs)
				return
			}
			assert.Equal(t, []string{tt.wantErr}, errs)
		})
	}
}

func TestValidate_Cutoff(t *testing.T) {
	tests := []struct {
		name    string
		cutoff  string
		offset  int
		wantErr string
	}{
		{"daily", "14:00", 0, ""},
		{"hourly", ":15", 0, ""},
		{"offset with daily", "02:00", 1, ""},
		{"hour out of range", "25:00", 0, `schedule.trigger.cutoff "25:00" must be HH:MM or :MM`},
		{"no minutes", "14", 0, `schedule.trigger.cutoff "14" must be HH:MM or :MM`},
		{"minute out of range", ":75", 0, `schedule.trigger.cutoff ":75" must be HH:MM or :MM`},
		{"negative offset", "02:00", -1, "schedule.trigger.cutoffDayOffset -1 out of range [0,7]"},
		{"offset too large", "02:00", 8, "schedule.trigger.cutoffDayOffset 8 out of range [0,7]"},
		{"offset with hourly", ":15", 1, `schedule.trigger.cutoffDayOffset is not supported with hourly ":MM" cutoffs`},
		{"offset without cutoff", "", 1, "schedule.trigger.cutoffDayOffset is not supported without schedule.trigger.cutoff"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "orders"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{
					Key: "upstream", Check: types.CheckExists, Cutoff: tt.cutoff, CutoffDayOffset: tt.offset,
				}},
			}
			errs := ValidatePipelineConfig(&cfg)
			if tt.wantErr == "" {
				assert.Empty(t, errs)
				return
			}
			assert.Equal(t, []string{tt.wantErr}, errs)
		})
	}
}

func TestValidate_Cron(t *testing.T) {
	tests := []struct {
		name    string
		cron    string
		wantErr bool
	}{
		{"four fields", "0 8 * *", true},
		{"minute out of range", "61 8 * * *", true},
		{"six fields", "0 0 8 * * *", true},
		{"tag", "@daily", true},
		{"weekday list item out of range", "0 8 * * 0-6,9", true},
		{"month list item out of range", "0 8 * 1-12,13 *", true},
		{"every descriptor", "@every 1h", true},
		{"TZ prefix", "TZ=UTC 0 8 * * *", true},
		{"CRON_TZ prefix", "CRON_TZ=UTC 0 8 * * *", true},
		{"TZ prefix with no spaces", "TZ=UTC\t0\t8\t*\t*", true},
		{"last day of month", "0 8 L * *", true},
		{"nearest weekday", "0 8 15W * *", true},
		{"nth weekday", "0 8 * * 5#3", true},
		{"last weekday of month", "0 8 * * 5L", true},
		{"weekday range with L", "0 8 * * 1-3-L", true},
		{"day of month range with L", "0 8 3-5-L * *", true},
		{"sunday as 7", "0 8 * * 7", true},
		{"day of month of only commas", "0 8 , * *", true},
		{"minute of only commas", ", 8 * * *", true},
		{"weekday range", "0 8 * * 1-5", false},
		{"step", "*/15 * * * *", false},
		{"day of month list", "30 6 1,15 * *", false},
		{"names", "0 8 * JAN,JUL MON-FRI", false},
		{"question mark", "0 8 ? * MON", false},
		{"trailing newline", "0 8 * * *\n", false},
		{"double space", "0  8 * * *", false},
		{"tab", "0\t8 * * *", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "orders"},
				Schedule: types.ScheduleConfig{Cron: tt.cron},
			}
			errs := ValidatePipelineConfig(&cfg)
			if !tt.wantErr {
				assert.Empty(t, errs)
				return
			}
			want := fmt.Sprintf("schedule.cron %q is not a valid five-field cron expression", tt.cron)
			assert.Equal(t, []string{want}, errs)
		})
	}
}

// TestValidate_ScheduleTimeWithCron: with cron the cron time is the start-by
// time, so schedule.time is rejected there; an inclusion calendar still uses it.
func TestValidate_ScheduleTimeWithCron(t *testing.T) {
	cfg := types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: "orders"}, Schedule: types.ScheduleConfig{Cron: "0 8 * * *", Time: "08:30"}}
	assert.Equal(t, []string{"schedule.time is not used with schedule.cron; the cron time is the start-by time"}, ValidatePipelineConfig(&cfg))
	cfg.Schedule = types.ScheduleConfig{Time: "08:30", Include: &types.InclusionConfig{Dates: []string{"2026-03-31"}}}
	assert.Empty(t, ValidatePipelineConfig(&cfg))
}

func TestValidate_PostRunWatchWindow(t *testing.T) {
	rule := []types.ValidationRule{{Key: "quality", Check: types.CheckExists}}
	window := func(w string) *types.PostRunConfig { return &types.PostRunConfig{WatchWindow: w} }
	timeout := func(s, w string) *types.PostRunConfig {
		return &types.PostRunConfig{Rules: rule, SensorTimeout: s, WatchWindow: w}
	}
	tests := []struct {
		name     string
		postRun  *types.PostRunConfig
		wantErrs []string
	}{
		{"postRun nil", nil, nil},
		{"window empty", window(""), nil},
		{"6h", window("6h"), nil},
		{"90m", window("90m"), nil},
		{"1s", window("1s"), nil},
		{"720h", window("720h"), nil},
		{"abc", window("abc"), []string{`postRun.watchWindow invalid Go duration "abc"`}},
		{"no unit", window("6"), []string{`postRun.watchWindow invalid Go duration "6"`}},
		{"0s", window("0s"), []string{`postRun.watchWindow "0s" out of range (0s,720h]`}},
		{"negative", window("-1h"), []string{`postRun.watchWindow "-1h" out of range (0s,720h]`}},
		{"721h", window("721h"), []string{`postRun.watchWindow "721h" out of range (0s,720h]`}},
		{"timeout 2h, default window", timeout("2h", ""), nil},
		{"timeout 4h, default window", timeout("4h", ""), nil},
		{"timeout 5h, default window", timeout("5h", ""), []string{"postRun.sensorTimeout 5h0m0s exceeds postRun.watchWindow 4h0m0s (default)"}},
		{"timeout 5h, window 6h", timeout("5h", "6h"), nil},
		{"timeout 6h, window 6h", timeout("6h", "6h"), nil},
		{"timeout 5h, window does not parse", timeout("5h", "abc"), []string{`postRun.watchWindow invalid Go duration "abc"`}},
		{"timeout 7h, window 6h", timeout("7h", "6h"), []string{"postRun.sensorTimeout 7h0m0s exceeds postRun.watchWindow 6h0m0s"}},
		{"timeout 5h, no rules", &types.PostRunConfig{SensorTimeout: "5h"}, nil},
		{"timeout does not parse", timeout("abc", ""), []string{`postRun.sensorTimeout invalid Go duration "abc"`}},
		{"timeout zero", timeout("0s", ""), []string{`postRun.sensorTimeout "0s" must be positive`}},
		{"timeout negative", timeout("-1h", ""), []string{`postRun.sensorTimeout "-1h" must be positive`}},
		{"default timeout, window 1h", timeout("", "1h"), []string{"postRun.sensorTimeout 2h0m0s (default) exceeds postRun.watchWindow 1h0m0s"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: "orders"}, PostRun: tt.postRun}
			assert.Equal(t, tt.wantErrs, ValidatePipelineConfig(&cfg))
		})
	}
}
