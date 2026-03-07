package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dwsmith1983/interlock/pkg/types"
)

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidatePipelineConfig(&tt.cfg)
			assert.Len(t, errs, tt.wantCount)
			if tt.wantSubstr != "" && len(errs) > 0 {
				assert.Contains(t, errs[0], tt.wantSubstr)
			}
		})
	}
}
