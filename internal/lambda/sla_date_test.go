package lambda_test

import (
	"testing"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestResolveSLADate(t *testing.T) {
	sensorDaily := &types.PipelineConfig{
		SLA: &types.SLAConfig{Deadline: "10:00"},
	}
	cron := &types.PipelineConfig{
		Schedule: types.ScheduleConfig{Cron: "0 8 * * *"},
		SLA:      &types.SLAConfig{Deadline: "10:00"},
	}
	hourly := &types.PipelineConfig{
		SLA: &types.SLAConfig{Deadline: ":30"},
	}
	nilSLA := &types.PipelineConfig{}

	tests := []struct {
		name string
		cfg  *types.PipelineConfig
		date string
		want string
	}{
		{"cron pipeline unchanged", cron, "2026-03-10", "2026-03-10"},
		{"sensor daily shifted", sensorDaily, "2026-03-10", "2026-03-11"},
		{"month end", sensorDaily, "2026-03-31", "2026-04-01"},
		{"leap day", sensorDaily, "2028-02-28", "2028-02-29"},
		{"hourly deadline unchanged", hourly, "2026-03-10", "2026-03-10"},
		{"composite hourly date unchanged", sensorDaily, "2026-03-10T13", "2026-03-10T13"},
		{"nil SLA unchanged", nilSLA, "2026-03-10", "2026-03-10"},
		{"unparseable date unchanged", sensorDaily, "bad", "bad"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lambda.ResolveSLADate(tt.cfg, tt.date)
			if got != tt.want {
				t.Errorf("ResolveSLADate(cfg, %q) = %q, want %q", tt.date, got, tt.want)
			}
		})
	}
}
