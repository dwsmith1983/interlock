package validation

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dwsmith1983/interlock/pkg/sla"
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

// validate runs ValidatePipelineConfig after giving cfg a valid
// schedule.trigger when it has none, so a test sees only the errors it is about.
// Its cutoff is the latest there is, after any SLA breach.
func validate(cfg *types.PipelineConfig) []string {
	if cfg.Schedule.Trigger == nil {
		cfg.Schedule.Trigger = &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}
	}
	return ValidatePipelineConfig(cfg)
}

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
				SLA: &types.SLAConfig{MaxDuration: "2h"},
			},
			wantCount: 0,
		},
		{
			name: "maxDuration exceeds 24h",
			cfg: types.PipelineConfig{
				SLA: &types.SLAConfig{MaxDuration: "25h"},
			},
			wantCount:  1,
			wantSubstr: "sla.maxDuration exceeds 24h",
		},
		{
			name: "maxDuration invalid format",
			cfg: types.PipelineConfig{
				SLA: &types.SLAConfig{MaxDuration: "not-a-duration"},
			},
			wantCount:  1,
			wantSubstr: "sla.maxDuration invalid Go duration",
		},
		{
			name: "maxDuration and deadline coexist",
			cfg: types.PipelineConfig{
				SLA: &types.SLAConfig{Deadline: "08:00", ExpectedDuration: "30m", MaxDuration: "2h"},
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
			name: "dryRun without job.type",
			cfg: types.PipelineConfig{
				DryRun: true,
				Job:    types.JobConfig{Type: ""},
			},
			wantCount:  1,
			wantSubstr: "dryRun requires job.type",
		},
		{
			name: "dryRun fully configured",
			cfg: types.PipelineConfig{
				DryRun: true,
				Job:    types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "etl"}},
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cfg.Pipeline.ID = "orders" // the ID rule has its own test
			errs := validate(&tt.cfg)
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
		assert.Empty(t, validate(&cfg), "ID %q should be accepted", id)
	}
	for _, id := range []string{"", maxID + "a", "a b", "a/b", "a#b", "a:b", "a.b", "a\nb", "orders\n", "caf\u00e9"} {
		cfg := types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: id}}
		errs := validate(&cfg)
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
		{"negative", types.SLAConfig{Deadline: "10:00", ExpectedDuration: "30m", DeadlineDayOffset: -1}, "sla.deadlineDayOffset -1 out of range [0,7]"},
		{"too large", types.SLAConfig{Deadline: "10:00", ExpectedDuration: "30m", DeadlineDayOffset: 8}, "sla.deadlineDayOffset 8 out of range [0,7]"},
		{"hourly deadline", types.SLAConfig{Deadline: ":30", ExpectedDuration: "30m", DeadlineDayOffset: 1}, `sla.deadlineDayOffset is not supported with hourly ":MM" deadlines`},
		{"no deadline", types.SLAConfig{MaxDuration: "2h", DeadlineDayOffset: 1}, "sla.deadlineDayOffset is not supported without sla.deadline"},
		{"zero", types.SLAConfig{Deadline: "10:00", ExpectedDuration: "30m"}, ""},
		{"one", types.SLAConfig{Deadline: "10:00", ExpectedDuration: "30m", DeadlineDayOffset: 1}, ""},
		{"seven", types.SLAConfig{Deadline: "10:00", ExpectedDuration: "30m", DeadlineDayOffset: 7}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "orders"},
				SLA:      &tt.sla,
			}
			errs := validate(&cfg)
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "orders"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{
					Key: "upstream", Check: types.CheckExists, Cutoff: tt.cutoff, CutoffDayOffset: tt.offset,
				}},
			}
			errs := validate(&cfg)
			if tt.wantErr == "" {
				assert.Empty(t, errs)
				return
			}
			assert.Equal(t, []string{tt.wantErr}, errs)
		})
	}
}

// breachErr is the error for a cutoff short of the SLA breach of key.
func breachErr(cutoff string, cutoffOffset int, cutoffZone, deadline string, deadlineOffset int, deadlineZone, key, cutoffAt, breachAt, short string) string {
	return fmt.Sprintf("schedule.trigger.cutoff %q (cutoffDayOffset %d, %s) is earlier than the sla.deadline %q (deadlineDayOffset %d, %s) breach time for run key %s (cutoff %s, breach %s); move the cutoff at least %s later",
		cutoff, cutoffOffset, cutoffZone, deadline, deadlineOffset, deadlineZone, key, cutoffAt, breachAt, short)
}

// hourlyKey adds the note an error for an hourly run key ends with.
func hourlyKey(msg string) string {
	return msg + " (the check covers hourly run keys because sensors may send them)"
}

// TestValidate_CutoffAfterBreach: with sla.deadline, the cutoff of every run
// key form (daily, and hourly T00 and T23) is not earlier than its breach, on
// a winter and a summer date.
func TestValidate_CutoffAfterBreach(t *testing.T) {
	daily := func(deadline string, offset int) types.SLAConfig {
		return types.SLAConfig{Deadline: deadline, DeadlineDayOffset: offset, ExpectedDuration: "30m"}
	}
	tests := []struct {
		name         string
		scheduleZone string
		sla          types.SLAConfig
		cutoff       string
		cutoffOffset int
		wantErrs     []string
	}{
		{name: "daily after", sla: daily("10:00", 0), cutoff: "12:00"},
		{name: "daily equal", sla: daily("10:00", 0), cutoff: "10:00"},
		{name: "daily before", sla: daily("10:00", 0), cutoff: "09:59", wantErrs: []string{
			breachErr("09:59", 0, "UTC", "10:00", 0, "UTC", "2025-01-15", "2025-01-15T09:59:00Z", "2025-01-15T10:00:00Z", "1m0s")}},
		{name: "deadline offset, cutoff same day", sla: daily("10:00", 1), cutoff: "23:00", wantErrs: []string{
			breachErr("23:00", 0, "UTC", "10:00", 1, "UTC", "2025-01-15", "2025-01-15T23:00:00Z", "2025-01-16T10:00:00Z", "11h0m0s")}},
		{name: "deadline offset, cutoff offset", sla: daily("10:00", 1), cutoff: "12:00", cutoffOffset: 1},
		{name: "cutoff offset short of deadline offset", sla: daily("10:00", 2), cutoff: "12:00", cutoffOffset: 1, wantErrs: []string{
			breachErr("12:00", 1, "UTC", "10:00", 2, "UTC", "2025-01-15", "2025-01-16T12:00:00Z", "2025-01-17T10:00:00Z", "22h0m0s")}},
		{name: "cutoff offset only", sla: daily("10:00", 0), cutoff: "02:00", cutoffOffset: 1},
		{name: "New York cutoff equal in winter, early in summer", scheduleZone: "America/New_York",
			sla: types.SLAConfig{Deadline: "12:00", ExpectedDuration: "30m", Timezone: "UTC"}, cutoff: "07:00", wantErrs: []string{
				breachErr("07:00", 0, "America/New_York", "12:00", 0, "UTC", "2025-03-20", "2025-03-20T11:00:00Z", "2025-03-20T12:00:00Z", "1h0m0s")}},
		{name: "New York cutoff after in winter, early in summer", scheduleZone: "America/New_York",
			sla: types.SLAConfig{Deadline: "12:00", ExpectedDuration: "30m", Timezone: "UTC"}, cutoff: "07:30", wantErrs: []string{
				breachErr("07:30", 0, "America/New_York", "12:00", 0, "UTC", "2025-03-20", "2025-03-20T11:30:00Z", "2025-03-20T12:00:00Z", "30m0s")}},
		{name: "New York cutoff short all year, the largest shortfall reported", scheduleZone: "America/New_York",
			sla: types.SLAConfig{Deadline: "12:00", ExpectedDuration: "30m", Timezone: "UTC"}, cutoff: "06:30", wantErrs: []string{
				breachErr("06:30", 0, "America/New_York", "12:00", 0, "UTC", "2025-03-20", "2025-03-20T10:30:00Z", "2025-03-20T12:00:00Z", "1h30m0s")}},
		{name: "New York cutoff after all year", scheduleZone: "America/New_York",
			sla: types.SLAConfig{Deadline: "12:00", ExpectedDuration: "30m", Timezone: "UTC"}, cutoff: "08:00"},
		{name: "New York deadline, UTC cutoff early in winter only", scheduleZone: "UTC",
			sla: types.SLAConfig{Deadline: "07:00", ExpectedDuration: "30m", Timezone: "America/New_York"}, cutoff: "11:30", wantErrs: []string{
				breachErr("11:30", 0, "UTC", "07:00", 0, "America/New_York", "2025-01-15", "2025-01-15T11:30:00Z", "2025-01-15T12:00:00Z", "30m0s")}},
		{name: "New York cutoff, London deadline, early between the US and EU changes", scheduleZone: "America/New_York",
			sla: types.SLAConfig{Deadline: "12:00", ExpectedDuration: "30m", Timezone: "Europe/London"}, cutoff: "07:00", wantErrs: []string{
				breachErr("07:00", 0, "America/New_York", "12:00", 0, "Europe/London", "2025-03-20", "2025-03-20T11:00:00Z", "2025-03-20T12:00:00Z", "1h0m0s")}},
		{name: "cutoff in the sla zone, equal", sla: types.SLAConfig{Deadline: "10:00", ExpectedDuration: "30m", Timezone: "Asia/Tokyo"}, cutoff: "10:00"},
		{name: "cutoff in the sla zone, before", sla: types.SLAConfig{Deadline: "10:00", ExpectedDuration: "30m", Timezone: "Asia/Tokyo"}, cutoff: "02:00", wantErrs: []string{
			breachErr("02:00", 0, "Asia/Tokyo", "10:00", 0, "Asia/Tokyo", "2025-01-15", "2025-01-14T17:00:00Z", "2025-01-15T01:00:00Z", "8h0m0s")}},
		{name: "hourly after", sla: daily(":30", 0), cutoff: ":45"},
		{name: "hourly equal", sla: daily(":30", 0), cutoff: ":30"},
		{name: "hourly before", sla: daily(":30", 0), cutoff: ":15", wantErrs: []string{
			hourlyKey(breachErr(":15", 0, "UTC", ":30", 0, "UTC", "2025-01-15T00", "2025-01-15T01:15:00Z", "2025-01-15T01:30:00Z", "15m0s"))}},
		{name: "hourly deadline, daily cutoff next day", sla: daily(":30", 0), cutoff: "02:00", cutoffOffset: 1},
		{name: "hourly deadline in New York names the T23 key", sla: types.SLAConfig{Deadline: ":30", ExpectedDuration: "30m", Timezone: "America/New_York"}, cutoff: "19:00", wantErrs: []string{
			hourlyKey(breachErr("19:00", 0, "America/New_York", ":30", 0, "America/New_York", "2025-01-15T23", "2025-01-16T00:00:00Z", "2025-01-16T05:30:00Z", "5h30m0s"))}},
		{name: "hourly deadline, daily cutoff same day", sla: daily(":30", 0), cutoff: "23:00", wantErrs: []string{
			hourlyKey(breachErr("23:00", 0, "UTC", ":30", 0, "UTC", "2025-01-15T23", "2025-01-15T23:00:00Z", "2025-01-16T00:30:00Z", "1h30m0s"))}},
		{name: "daily deadline, hourly cutoff closes T00 first", sla: daily("10:00", 0), cutoff: ":15", wantErrs: []string{
			hourlyKey(breachErr(":15", 0, "UTC", "10:00", 0, "UTC", "2025-01-15T00", "2025-01-15T01:15:00Z", "2025-01-15T10:00:00Z", "8h45m0s"))}},
		{name: "daily deadline next day, hourly cutoff", sla: daily("10:00", 1), cutoff: ":15", wantErrs: []string{
			breachErr(":15", 0, "UTC", "10:00", 1, "UTC", "2025-01-15", "2025-01-16T00:15:00Z", "2025-01-16T10:00:00Z", "9h45m0s")}},
		{name: "maxDuration only, daily cutoff", sla: types.SLAConfig{MaxDuration: "2h"}, cutoff: "00:00"},
		{name: "maxDuration only, hourly cutoff", sla: types.SLAConfig{MaxDuration: "2h"}, cutoff: ":00"},
		{name: "deadline and maxDuration, after", sla: types.SLAConfig{Deadline: "10:00", ExpectedDuration: "30m", MaxDuration: "2h"}, cutoff: "12:00"},
		{name: "deadline and maxDuration, before", sla: types.SLAConfig{Deadline: "10:00", ExpectedDuration: "30m", MaxDuration: "2h"}, cutoff: "09:00", wantErrs: []string{
			breachErr("09:00", 0, "UTC", "10:00", 0, "UTC", "2025-01-15", "2025-01-15T09:00:00Z", "2025-01-15T10:00:00Z", "1h0m0s")}},
		// The breach does not depend on expectedDuration, so each mistake gives its own error.
		{name: "bad expectedDuration and early cutoff are two mistakes", sla: types.SLAConfig{Deadline: "10:00", ExpectedDuration: "30 minutes"}, cutoff: "09:00", wantErrs: []string{
			`sla.expectedDuration invalid Go duration "30 minutes"`,
			breachErr("09:00", 0, "UTC", "10:00", 0, "UTC", "2025-01-15", "2025-01-15T09:00:00Z", "2025-01-15T10:00:00Z", "1h0m0s")}},
		// One mistake gives one error: no comparison with an input that failed its own check.
		{name: "bad cutoff", sla: daily("10:00", 0), cutoff: "9:00", wantErrs: []string{`schedule.trigger.cutoff "9:00" must be HH:MM or :MM`}},
		{name: "bad deadline", sla: daily("8:00", 0), cutoff: "07:00", wantErrs: []string{`sla.deadline "8:00" must be HH:MM or :MM`}},
		{name: "bad deadline offset", sla: daily("10:00", 8), cutoff: "12:00", wantErrs: []string{"sla.deadlineDayOffset 8 out of range [0,7]"}},
		{name: "bad cutoff offset", sla: daily("10:00", 0), cutoff: "09:00", cutoffOffset: -1, wantErrs: []string{"schedule.trigger.cutoffDayOffset -1 out of range [0,7]"}},
		{name: "bad schedule zone", scheduleZone: "Mars/Olympus", sla: daily("10:00", 0), cutoff: "09:00", wantErrs: []string{`schedule.timezone "Mars/Olympus" is not an IANA time zone name`}},
		{name: "bad sla zone", sla: types.SLAConfig{Deadline: "10:00", ExpectedDuration: "30m", Timezone: "Local"}, cutoff: "09:00", wantErrs: []string{`sla.timezone "Local" is not an IANA time zone name`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "orders"},
				Schedule: types.ScheduleConfig{Timezone: tt.scheduleZone, Trigger: &types.TriggerCondition{
					Key: "upstream", Check: types.CheckExists, Cutoff: tt.cutoff, CutoffDayOffset: tt.cutoffOffset,
				}},
				SLA: &tt.sla,
			}
			errs := ValidatePipelineConfig(&cfg)
			assert.Equal(t, tt.wantErrs, errs)
			// No clock: a second call gives the same list.
			assert.Equal(t, errs, ValidatePipelineConfig(&cfg))
		})
	}
}

// TestValidate_TriggerRequired: every pipeline sets schedule.trigger with a
// cutoff, its latest automatic start.
func TestValidate_TriggerRequired(t *testing.T) {
	glue := types.JobConfig{Type: types.TriggerGlue, Config: map[string]interface{}{"jobName": "etl"}}
	noTrigger := []types.PipelineConfig{
		{},
		{DryRun: true, Job: glue},
		{SLA: &types.SLAConfig{MaxDuration: "2h"}},
	}
	for _, cfg := range noTrigger {
		cfg.Pipeline.ID = "orders"
		assert.Equal(t, []string{"schedule.trigger is required"}, ValidatePipelineConfig(&cfg))
	}
	for _, offset := range []int{0, 1} {
		cfg := types.PipelineConfig{
			Pipeline: types.PipelineIdentity{ID: "orders"},
			Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, CutoffDayOffset: offset}},
		}
		assert.Equal(t, []string{"schedule.trigger.cutoff is required"}, ValidatePipelineConfig(&cfg), "cutoffDayOffset %d", offset)
		cfg.Schedule.Trigger.Cutoff = "14:00"
		assert.Empty(t, ValidatePipelineConfig(&cfg), "cutoffDayOffset %d", offset)
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
			errs := validate(&cfg)
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
	assert.Equal(t, []string{"schedule.time is not used with schedule.cron; the cron time is the start-by time"}, validate(&cfg))
	cfg.Schedule = types.ScheduleConfig{Time: "08:30", Include: &types.InclusionConfig{Dates: []string{"2026-03-31"}}}
	assert.Empty(t, validate(&cfg))
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
			assert.Equal(t, tt.wantErrs, validate(&cfg))
		})
	}
}

// TestValidate_Evaluation: schedule.evaluation.window and interval are whole
// seconds of at least 1s, the interval fits in the window, and the window allows
// at most 1000 evaluations.
func TestValidate_Evaluation(t *testing.T) {
	tests := []struct {
		name, window, interval string
		wantErrs               []string
	}{
		{"both unset", "", "", nil},
		{"1h/5m", "1h", "5m", nil},
		{"60s/5s", "60s", "5s", nil},
		{"10s/5s", "10s", "5s", nil},
		{"1000s/1s", "1000s", "1s", nil},
		{"window not a duration", "1 hour", "", []string{`schedule.evaluation.window invalid Go duration "1 hour"`}},
		{"interval 500ms", "", "500ms", []string{`schedule.evaluation.interval "500ms" must be whole seconds, at least 1s`}},
		{"interval 1500ms", "", "1500ms", []string{`schedule.evaluation.interval "1500ms" must be whole seconds, at least 1s`}},
		{"interval 0s", "", "0s", []string{`schedule.evaluation.interval "0s" must be whole seconds, at least 1s`}},
		{"window -1h", "-1h", "", []string{`schedule.evaluation.window "-1h" must be whole seconds, at least 1s`}},
		{"window 2m, default interval", "2m", "", []string{"schedule.evaluation.interval 5m0s (default) exceeds schedule.evaluation.window 2m0s"}},
		{"6h/10s", "6h", "10s", []string{"schedule.evaluation.window 6h0m0s allows more than 1000 evaluations at interval 10s"}},
		{"1001s/1s", "1001s", "1s", []string{"schedule.evaluation.window 16m41s allows more than 1000 evaluations at interval 1s"}},
		{"2001s/2s", "2001s", "2s", []string{"schedule.evaluation.window 33m21s allows more than 1000 evaluations at interval 2s"}},
		{"default window, 1s", "", "1s", []string{"schedule.evaluation.window 1h0m0s (default) allows more than 1000 evaluations at interval 1s"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "orders"},
				Schedule: types.ScheduleConfig{Evaluation: types.EvaluationWindow{Window: tt.window, Interval: tt.interval}},
			}
			assert.Equal(t, tt.wantErrs, validate(&cfg))
		})
	}
}

// TestValidate_RelativeSLAExpectedDuration: with sla.maxDuration, the warning
// offset sla.expectedDuration must be a positive duration below it.
func TestValidate_RelativeSLAExpectedDuration(t *testing.T) {
	below := func(v string) []string {
		return []string{fmt.Sprintf(`sla.expectedDuration %q must be positive and below sla.maxDuration "2h"`, v)}
	}
	tests := []struct {
		name, maxDuration, expected string
		wantErrs                    []string
	}{
		{"empty", "2h", "", nil},
		{"30m", "2h", "30m", nil},
		{"1h59m", "2h", "1h59m", nil},
		{"abc", "2h", "abc", []string{`sla.expectedDuration invalid Go duration "abc"`}},
		{"0s", "2h", "0s", below("0s")},
		{"-1m", "2h", "-1m", below("-1m")},
		{"2h", "2h", "2h", below("2h")},
		{"3h", "2h", "3h", below("3h")},
		{"no maxDuration", "", "3h", []string{"sla must set deadline or maxDuration"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "orders"},
				SLA:      &types.SLAConfig{MaxDuration: tt.maxDuration, ExpectedDuration: tt.expected},
			}
			assert.Equal(t, tt.wantErrs, validate(&cfg))
		})
	}
}

// TestValidate_SLADeadline: an sla block must be able to produce alerts. A
// deadline needs an expectedDuration and a format the SLA Lambda can parse,
// and an sla block needs a deadline or a maxDuration.
func TestValidate_SLADeadline(t *testing.T) {
	badFormat := func(v string) []string {
		return []string{fmt.Sprintf("sla.deadline %q must be HH:MM or :MM", v)}
	}
	positive := func(v string) []string {
		return []string{fmt.Sprintf("sla.expectedDuration %q must be positive", v)}
	}
	tests := []struct {
		name     string
		sla      types.SLAConfig
		wantErrs []string
	}{
		{"daily", types.SLAConfig{Deadline: "08:00", ExpectedDuration: "30m"}, nil},
		{"daily with zone", types.SLAConfig{Deadline: "23:59", ExpectedDuration: "1h", Timezone: "Asia/Tokyo"}, nil},
		{"hourly", types.SLAConfig{Deadline: ":30", ExpectedDuration: "10m"}, nil},
		{"maxDuration alone", types.SLAConfig{MaxDuration: "2h"}, nil},
		{"deadline and maxDuration", types.SLAConfig{Deadline: "08:00", ExpectedDuration: "30m", MaxDuration: "2h"}, nil},
		{"no expectedDuration", types.SLAConfig{Deadline: "08:00"}, []string{"sla.expectedDuration is required with sla.deadline"}},
		{"8am", types.SLAConfig{Deadline: "8am", ExpectedDuration: "30m"}, badFormat("8am")},
		{"8am, no expectedDuration", types.SLAConfig{Deadline: "8am"},
			append(badFormat("8am"), "sla.expectedDuration is required with sla.deadline")},
		{"deadline and maxDuration, no expectedDuration", types.SLAConfig{Deadline: "08:00", MaxDuration: "2h"},
			[]string{"sla.expectedDuration is required with sla.deadline"}},
		{"one-digit hour", types.SLAConfig{Deadline: "8:00", ExpectedDuration: "30m"}, badFormat("8:00")},
		{"hour 24", types.SLAConfig{Deadline: "24:00", ExpectedDuration: "30m"}, badFormat("24:00")},
		{"minute 60", types.SLAConfig{Deadline: "08:60", ExpectedDuration: "30m"}, badFormat("08:60")},
		{"one-digit minute", types.SLAConfig{Deadline: ":7", ExpectedDuration: "30m"}, badFormat(":7")},
		{"no colon", types.SLAConfig{Deadline: "0800", ExpectedDuration: "30m"}, badFormat("0800")},
		{"expectedDuration not a duration", types.SLAConfig{Deadline: "08:00", ExpectedDuration: "30 minutes"},
			[]string{`sla.expectedDuration invalid Go duration "30 minutes"`}},
		{"expectedDuration zero", types.SLAConfig{Deadline: "08:00", ExpectedDuration: "0s"}, positive("0s")},
		{"expectedDuration negative", types.SLAConfig{Deadline: "08:00", ExpectedDuration: "-5m"}, positive("-5m")},
		{"expectedDuration above maxDuration", types.SLAConfig{Deadline: "08:00", ExpectedDuration: "3h", MaxDuration: "2h"},
			[]string{`sla.expectedDuration "3h" must be positive and below sla.maxDuration "2h"`}},
		{"timezone only", types.SLAConfig{Timezone: "UTC"}, []string{"sla must set deadline or maxDuration"}},
		{"expectedDuration only", types.SLAConfig{ExpectedDuration: "30m"}, []string{"sla must set deadline or maxDuration"}},
	}
	fixedNow := time.Date(2026, 3, 10, 6, 0, 0, 0, time.UTC)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "orders"},
				SLA:      &tt.sla,
			}
			assert.Equal(t, tt.wantErrs, validate(&cfg))
			if tt.wantErrs != nil || tt.sla.Deadline == "" {
				return
			}
			// What validation accepts, the SLA Lambda can schedule.
			_, _, err := sla.CalculateAbsoluteDeadline("2026-03-10", tt.sla.Deadline, tt.sla.ExpectedDuration, tt.sla.Timezone, fixedNow)
			assert.NoError(t, err)
		})
	}
}

// TestValidate_SLADeadlineEveryClockTime: every HH:MM and :MM deadline is
// accepted, and the SLA Lambda computes each one.
func TestValidate_SLADeadlineEveryClockTime(t *testing.T) {
	var deadlines []string
	for h := 0; h < 24; h++ {
		for m := 0; m < 60; m++ {
			deadlines = append(deadlines, fmt.Sprintf("%02d:%02d", h, m))
		}
	}
	for m := 0; m < 60; m++ {
		deadlines = append(deadlines, fmt.Sprintf(":%02d", m))
	}
	fixedNow := time.Date(2026, 3, 10, 6, 0, 0, 0, time.UTC)
	for _, deadline := range deadlines {
		cfg := types.PipelineConfig{
			Pipeline: types.PipelineIdentity{ID: "orders"},
			SLA:      &types.SLAConfig{Deadline: deadline, ExpectedDuration: "30m"},
		}
		assert.Empty(t, validate(&cfg), "deadline %q", deadline)
		date := "2026-03-10"
		if strings.HasPrefix(deadline, ":") {
			date = "2026-03-10T07"
		}
		_, _, err := sla.CalculateAbsoluteDeadline(date, deadline, "30m", "", fixedNow)
		assert.NoError(t, err, "deadline %q", deadline)
	}
}

func TestValidate_Timezone(t *testing.T) {
	valid := []string{"", "UTC", "America/New_York", "Asia/Tokyo", "Europe/Berlin"}
	invalid := []string{"America/NewYork", "EST5EDTX", "Mars/Olympus", "Local", "+05:00"}
	withZones := func(schedule, slaZone string) *types.PipelineConfig {
		return &types.PipelineConfig{
			Pipeline: types.PipelineIdentity{ID: "orders"},
			Schedule: types.ScheduleConfig{Timezone: schedule},
			SLA:      &types.SLAConfig{Deadline: "08:00", ExpectedDuration: "30m", Timezone: slaZone},
		}
	}
	for _, tz := range valid {
		t.Run("valid "+tz, func(t *testing.T) {
			assert.Empty(t, validate(withZones(tz, "")), "schedule.timezone")
			assert.Empty(t, validate(withZones("", tz)), "sla.timezone")
		})
	}
	for _, tz := range invalid {
		t.Run("invalid "+tz, func(t *testing.T) {
			scheduleErr := fmt.Sprintf("schedule.timezone %q is not an IANA time zone name", tz)
			slaErr := fmt.Sprintf("sla.timezone %q is not an IANA time zone name", tz)
			assert.Equal(t, []string{scheduleErr}, validate(withZones(tz, "")))
			assert.Equal(t, []string{slaErr}, validate(withZones("", tz)))
			assert.Equal(t, []string{scheduleErr, slaErr}, validate(withZones(tz, tz)))
		})
	}
}

func TestValidate_AirflowAPIVersion(t *testing.T) {
	withVersion := func(jobType types.TriggerType, version interface{}) *types.PipelineConfig {
		return &types.PipelineConfig{
			Pipeline: types.PipelineIdentity{ID: "orders"},
			Job:      types.JobConfig{Type: jobType, Config: map[string]interface{}{"url": "https://a", "dagId": "etl", "apiVersion": version}},
		}
	}
	// YAML decodes the number as an int, JSON as a float64.
	for _, v := range []interface{}{0, 2, 3, float64(2), float64(3), nil} {
		assert.Empty(t, validate(withVersion(types.TriggerAirflow, v)), "apiVersion %#v should be accepted", v)
	}
	assert.Empty(t, validate(&types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "orders"},
		Job:      types.JobConfig{Type: types.TriggerAirflow, Config: map[string]interface{}{"url": "https://a", "dagId": "etl"}},
	}), "an unset apiVersion should be accepted")
	for v, shown := range map[interface{}]string{1: "1", 4: "4", float64(4): "4", 2.5: "2.5", "3": `"3"`} {
		assert.Equal(t, []string{"job.config.apiVersion " + shown + " must be 2 or 3"},
			validate(withVersion(types.TriggerAirflow, v)), "apiVersion %#v should be rejected", v)
	}
	// The trigger config decoder matches keys without regard to case.
	assert.Equal(t, []string{"job.config.apiVersion 4 must be 2 or 3"}, validate(&types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "orders"},
		Job:      types.JobConfig{Type: types.TriggerAirflow, Config: map[string]interface{}{"url": "https://a", "dagId": "etl", "apiversion": 4}},
	}))
	assert.Equal(t, []string{`job.config: unknown field "apiVersion"`}, validate(withVersion(types.TriggerHTTP, 4)),
		"apiVersion is an airflow setting")
}

// The databricks jobId is checked as the orchestrator decodes it: a string
// holding a positive integer. A JSON or YAML number fails the decode and would
// lose precision above 2^53, so it is refused with a hint to quote it.
func TestValidate_DatabricksJobID(t *testing.T) {
	withConfig := func(jobType types.TriggerType, conf map[string]interface{}) *types.PipelineConfig {
		return &types.PipelineConfig{
			Pipeline: types.PipelineIdentity{ID: "orders"},
			Job:      types.JobConfig{Type: jobType, Config: conf},
		}
	}
	const quote = "job.config.jobId %s must be a string: quote the job ID, as a number loses precision above 2^53"
	tests := []struct {
		name string
		conf map[string]interface{}
		want []string
	}{
		{"valid", map[string]interface{}{"workspaceUrl": "https://dbc", "jobId": "12345"}, nil},
		{"above 2^53", map[string]interface{}{"workspaceUrl": "https://dbc", "jobId": "9007199254740993"}, nil},
		{"case-folded key", map[string]interface{}{"workspaceUrl": "https://dbc", "jobid": "abc"}, []string{`job.config.jobId "abc" must be a positive base-10 integer job ID`}},
		{"not an integer", map[string]interface{}{"workspaceUrl": "https://dbc", "jobId": "abc"}, []string{`job.config.jobId "abc" must be a positive base-10 integer job ID`}},
		{"zero", map[string]interface{}{"workspaceUrl": "https://dbc", "jobId": "0"}, []string{`job.config.jobId "0" must be a positive base-10 integer job ID`}},
		{"empty", map[string]interface{}{"workspaceUrl": "https://dbc", "jobId": ""}, []string{"job.config.jobId is required"}},
		{"null", map[string]interface{}{"workspaceUrl": "https://dbc", "jobId": nil}, []string{"job.config.jobId is required"}},
		{"missing", map[string]interface{}{"workspaceUrl": "https://dbc"}, []string{"job.config.jobId is required"}},
		{"no config", nil, []string{"job.config.workspaceUrl is required", "job.config.jobId is required"}},
		// YAML decodes the number as an int, JSON as a float64.
		{"yaml number", map[string]interface{}{"workspaceUrl": "https://dbc", "jobId": 12345}, []string{fmt.Sprintf(quote, "12345")}},
		{"json number", map[string]interface{}{"workspaceUrl": "https://dbc", "jobId": float64(12345)}, []string{fmt.Sprintf(quote, "12345")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, validate(withConfig(types.TriggerDatabricks, tt.conf)))
		})
	}
	assert.Equal(t, []string{`job.config: unknown field "jobId"`}, validate(withConfig(types.TriggerHTTP, map[string]interface{}{"jobId": 1})),
		"jobId is a databricks setting")
}

// validTriggerConfigs holds a complete job.config for every trigger type.
func validTriggerConfigs() map[types.TriggerType]map[string]interface{} {
	return map[types.TriggerType]map[string]interface{}{
		types.TriggerHTTP:    {"url": "https://example.com/start", "method": "POST"},
		types.TriggerAirflow: {"url": "https://airflow.example.com", "dagId": "etl", "apiVersion": 3},
		types.TriggerGlue:    {"jobName": "etl"},
		types.TriggerEMR:     {"clusterId": "j-1", "stepName": "etl", "command": "s3://bucket/etl.jar"},
		types.TriggerEMRServerless: {"applicationId": "app-1", "jobName": "etl", "executionRoleArn": "arn:aws:iam::1:role/r",
			"entryPoint": "s3://bucket/etl.py", "entryPointArguments": []interface{}{"--par_day", "x", "--par_hour=05"}},
		types.TriggerDatabricks:   {"workspaceUrl": "https://dbc.example.com", "jobId": "123"},
		types.TriggerStepFunction: {"stateMachineArn": "arn:aws:states:eu-west-1:1:stateMachine:sm"},
		types.TriggerLambda:       {"functionName": "audit"},
	}
}

// requiredTriggerFields lists, in the order the trigger checks them, the
// fields each trigger rejects when empty.
var requiredTriggerFields = map[types.TriggerType][]string{
	types.TriggerHTTP:          {"url"},
	types.TriggerAirflow:       {"url", "dagId"},
	types.TriggerGlue:          {"jobName"},
	types.TriggerEMR:           {"clusterId", "stepName", "command"},
	types.TriggerEMRServerless: {"applicationId", "jobName", "executionRoleArn", "entryPoint"},
	types.TriggerDatabricks:    {"workspaceUrl", "jobId"},
	types.TriggerStepFunction:  {"stateMachineArn"},
	types.TriggerLambda:        {"functionName"},
}

func TestValidate_TriggerRequiredFields(t *testing.T) {
	withConfig := func(jobType types.TriggerType, conf map[string]interface{}) *types.PipelineConfig {
		return &types.PipelineConfig{
			Pipeline: types.PipelineIdentity{ID: "orders"},
			Job:      types.JobConfig{Type: jobType, Config: conf},
		}
	}
	for typ, conf := range validTriggerConfigs() {
		t.Run(string(typ)+" valid", func(t *testing.T) {
			assert.Empty(t, validate(withConfig(typ, conf)))
		})
	}
	for typ, fields := range requiredTriggerFields {
		for _, field := range fields {
			t.Run(string(typ)+" missing "+field, func(t *testing.T) {
				conf := validTriggerConfigs()[typ]
				delete(conf, field)
				assert.Equal(t, []string{"job.config." + field + " is required"}, validate(withConfig(typ, conf)))
			})
		}
		t.Run(string(typ)+" no config", func(t *testing.T) {
			var want []string
			for _, field := range fields {
				want = append(want, "job.config."+field+" is required")
			}
			assert.Equal(t, want, validate(withConfig(typ, nil)))
		})
	}

	tests := []struct {
		name string
		typ  types.TriggerType
		conf map[string]interface{}
		want []string
	}{
		{"snake_case key is unknown", types.TriggerAirflow, map[string]interface{}{"url": "https://a", "dag_id": "etl"},
			[]string{`job.config: unknown field "dag_id"`}},
		{"key of another type is unknown", types.TriggerGlue, map[string]interface{}{"jobName": "etl", "clusterId": "j-1"},
			[]string{`job.config: unknown field "clusterId"`}},
		{"keys match without regard to case", types.TriggerAirflow, map[string]interface{}{"URL": "https://a", "dagID": "etl"}, nil},
		// A config that does not decode reports only the decode error.
		{"wrong value type", types.TriggerGlue, map[string]interface{}{"jobName": 5},
			[]string{"job.config.jobName holds a number where a string is expected"}},
		{"wrong value type behind a raw field", types.TriggerAirflow, map[string]interface{}{"url": "https://a", "dagId": 5},
			[]string{"job.config.dagId holds a number where a string is expected"}},
		{"wrong value type in a map", types.TriggerGlue, map[string]interface{}{"jobName": "etl", "arguments": map[string]interface{}{"--limit": 100}},
			[]string{"job.config.arguments holds a number where a string is expected"}},
		{"wrong value type in a list", types.TriggerEMRServerless, map[string]interface{}{"applicationId": "a", "jobName": "j",
			"executionRoleArn": "r", "entryPoint": "e", "entryPointArguments": []interface{}{"--x", true}},
			[]string{"job.config.entryPointArguments holds a boolean where a string is expected"}},
		{"object for a string", types.TriggerDatabricks, map[string]interface{}{"workspaceUrl": map[string]interface{}{"u": "x"}, "jobId": "1"},
			[]string{"job.config.workspaceUrl holds an object where a string is expected"}},
		{"string for an integer", types.TriggerHTTP, map[string]interface{}{"url": "https://a", "timeout": "30"},
			[]string{"job.config.timeout holds a string where an integer is expected"}},
		{"fraction for an integer", types.TriggerHTTP, map[string]interface{}{"url": "https://a", "timeout": 1.5},
			[]string{"job.config.timeout holds a number where an integer is expected"}},
		{"http method unset", types.TriggerHTTP, map[string]interface{}{"url": "https://a"}, nil},
		{"http method lowercase", types.TriggerHTTP, map[string]interface{}{"url": "https://a", "method": "get"},
			[]string{`job.config.method "get" must be empty or one of GET, HEAD, POST, PUT, PATCH, DELETE`}},
		{"http method unsupported", types.TriggerHTTP, map[string]interface{}{"url": "https://a", "method": "OPTIONS"},
			[]string{`job.config.method "OPTIONS" must be empty or one of GET, HEAD, POST, PUT, PATCH, DELETE`}},
	}
	for _, m := range []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"} {
		tests = append(tests, struct {
			name string
			typ  types.TriggerType
			conf map[string]interface{}
			want []string
		}{"http method " + m, types.TriggerHTTP, map[string]interface{}{"url": "https://a", "method": m}, nil})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, validate(withConfig(tt.typ, tt.conf)))
		})
	}
}

// The orchestrator writes the run date into the argument after --par_day and
// --par_hour, so each must have a value after it that is not another flag.
func TestValidate_EMRServerlessDateFlags(t *testing.T) {
	withArgs := func(args ...interface{}) *types.PipelineConfig {
		conf := validTriggerConfigs()[types.TriggerEMRServerless]
		conf["entryPointArguments"] = args
		return &types.PipelineConfig{
			Pipeline: types.PipelineIdentity{ID: "orders"},
			Job:      types.JobConfig{Type: types.TriggerEMRServerless, Config: conf},
		}
	}
	noValue := func(flag string) []string {
		return []string{"job.config.entryPointArguments has no value after " + flag}
	}
	assert.Empty(t, validate(withArgs()))
	assert.Empty(t, validate(withArgs("--par_day", "x", "--par_hour", "y")))
	assert.Empty(t, validate(withArgs("--par_day=x", "--par_hour=y")), "the = form is left alone")
	assert.Empty(t, validate(withArgs("--input", "--par_day", "x")))
	assert.Equal(t, noValue("--par_day"), validate(withArgs("--input", "s3://in", "--par_day")))
	assert.Equal(t, noValue("--par_hour"), validate(withArgs("--par_hour", "--input", "s3://in")))
	assert.Equal(t, append(noValue("--par_day"), noValue("--par_hour")...),
		validate(withArgs("--par_day", "--par_hour")))
}

// Every job.type validation accepts must have its own config check.
func TestValidate_EveryTriggerTypeHasARequiredFieldCheck(t *testing.T) {
	for typ := range triggerTypes {
		assert.Contains(t, triggerConfigChecks, typ, "job.type %q has no config check", typ)
	}
	assert.Len(t, triggerConfigChecks, len(triggerTypes))
}
