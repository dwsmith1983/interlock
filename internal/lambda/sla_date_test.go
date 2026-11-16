package lambda_test

import (
	"testing"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestResolveSLADate(t *testing.T) {
	sensor := func(offset int) *types.PipelineConfig {
		return &types.PipelineConfig{SLA: &types.SLAConfig{Deadline: "10:00", DeadlineDayOffset: offset}}
	}
	cronOffset1 := &types.PipelineConfig{
		Schedule: types.ScheduleConfig{Cron: "0 8 * * *"},
		SLA:      &types.SLAConfig{Deadline: "10:00", DeadlineDayOffset: 1},
	}

	tests := []struct {
		name string
		cfg  *types.PipelineConfig
		date string
		want string
	}{
		{"offset 0 same day", sensor(0), "2026-03-10", "2026-03-10"},
		{"offset 1 next day", sensor(1), "2026-03-10", "2026-03-11"},
		{"offset 1 month end", sensor(1), "2026-03-31", "2026-04-01"},
		{"cron pipeline with offset 1 shifted", cronOffset1, "2026-03-10", "2026-03-11"},
		{"composite date unchanged", sensor(1), "2026-03-10T13", "2026-03-10T13"},
		{"hourly deadline unchanged", &types.PipelineConfig{SLA: &types.SLAConfig{Deadline: ":30", DeadlineDayOffset: 1}}, "2026-03-10", "2026-03-10"},
		{"unparseable date unchanged", sensor(1), "bad", "bad"},
		{"nil SLA unchanged", &types.PipelineConfig{}, "2026-03-10", "2026-03-10"},
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
