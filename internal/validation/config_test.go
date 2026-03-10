package validation

import (
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidatePipelineConfig(&tt.cfg)
			assert.Len(t, errs, tt.wantCount)
			if tt.wantSubstr != "" {
				assert.True(t, containsSubstr(errs, tt.wantSubstr),
					"expected one of %v to contain %q", errs, tt.wantSubstr)
			}
		})
	}
}
