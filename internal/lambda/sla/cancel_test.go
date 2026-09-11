package sla_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/sla"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// countingEventBridge is a lambda.EventBridgeAPI double that records how many
// times PutEvents was called, so a test can prove no verdict was published.
type countingEventBridge struct {
	calls int
}

func (c *countingEventBridge) PutEvents(context.Context, *eventbridge.PutEventsInput, ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	c.calls++
	return &eventbridge.PutEventsOutput{}, nil
}

// configItem builds the control-table CONFIG row that store.GetConfig expects.
func configItem(t *testing.T, cfg types.PipelineConfig) map[string]ddbtypes.AttributeValue {
	t.Helper()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	return map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	}
}

// triggerItem builds a minimal TRIGGER row so store.GetTrigger returns non-nil.
func triggerItem() map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", "2026-03-10")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
	}
}

// newFakeDynamo returns a FakeDynamo that serves cfg for CONFIG-key reads
// (or cfgErr, if set) and a minimal TRIGGER row for every TRIGGER-key read.
func newFakeDynamo(t *testing.T, cfg types.PipelineConfig, cfgErr error) *storetest.FakeDynamo {
	t.Helper()
	return &storetest.FakeDynamo{
		GetItemFn: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			skAttr, ok := in.Key["SK"].(*ddbtypes.AttributeValueMemberS)
			if !ok {
				return &dynamodb.GetItemOutput{}, nil
			}
			switch {
			case skAttr.Value == types.ConfigSK:
				if cfgErr != nil {
					return nil, cfgErr
				}
				return &dynamodb.GetItemOutput{Item: configItem(t, cfg)}, nil
			case strings.HasPrefix(skAttr.Value, "TRIGGER#"):
				return &dynamodb.GetItemOutput{Item: triggerItem()}, nil
			default:
				return &dynamodb.GetItemOutput{}, nil
			}
		},
	}
}

func testDeps(fake *storetest.FakeDynamo, now time.Time) *lambda.Deps {
	return &lambda.Deps{
		Store:   storetest.NewStore(fake),
		Logger:  slog.Default(),
		NowFunc: func() time.Time { return now },
	}
}

// TestSLACancel_AppliesT1SLADate pins the T+1 SLA-date rule on the cancel
// path: sensor-triggered daily pipelines shift the deadline date to D+1 (the
// same rule the watchdog applies), while cron pipelines keep their own date.
func TestSLACancel_AppliesT1SLADate(t *testing.T) {
	sensorDailyCfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{},
		SLA:      &types.SLAConfig{Deadline: "10:00", ExpectedDuration: "30m"},
	}
	cronCfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{Cron: "0 8 * * *"},
		SLA:      &types.SLAConfig{Deadline: "10:00", ExpectedDuration: "30m"},
	}

	tests := []struct {
		name          string
		cfg           types.PipelineConfig
		cfgErr        error
		scheduleID    string
		now           time.Time
		wantAlertType string
		wantBreachAt  string
	}{
		{
			name:          "sensor-daily met before T+1 deadline",
			cfg:           sensorDailyCfg,
			scheduleID:    "stream",
			now:           time.Date(2026, 3, 11, 4, 0, 0, 0, time.UTC),
			wantAlertType: string(types.EventSLAMet),
			wantBreachAt:  "2026-03-11T10:00:00Z",
		},
		{
			name:          "sensor-daily breached after T+1 deadline",
			cfg:           sensorDailyCfg,
			scheduleID:    "stream",
			now:           time.Date(2026, 3, 11, 11, 0, 0, 0, time.UTC),
			wantAlertType: string(types.EventSLABreach),
			wantBreachAt:  "2026-03-11T10:00:00Z",
		},
		{
			name:          "cron pipeline breached, no roll-forward",
			cfg:           cronCfg,
			scheduleID:    "cron",
			now:           time.Date(2026, 3, 10, 11, 0, 0, 0, time.UTC),
			wantAlertType: string(types.EventSLABreach),
			wantBreachAt:  "2026-03-10T10:00:00Z",
		},
		{
			name:          "cron pipeline met, no roll-forward",
			cfg:           cronCfg,
			scheduleID:    "cron",
			now:           time.Date(2026, 3, 10, 9, 0, 0, 0, time.UTC),
			wantAlertType: string(types.EventSLAMet),
			wantBreachAt:  "2026-03-10T10:00:00Z",
		},
		{
			name:          "config lookup failure falls back to unshifted date",
			cfg:           sensorDailyCfg,
			cfgErr:        errors.New("dynamodb: internal error"),
			scheduleID:    "stream",
			now:           time.Date(2026, 3, 10, 11, 0, 0, 0, time.UTC),
			wantAlertType: string(types.EventSLABreach),
			wantBreachAt:  "2026-03-10T10:00:00Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeDynamo(t, tt.cfg, tt.cfgErr)
			d := testDeps(fake, tt.now)

			out, err := sla.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
				Mode:             "cancel",
				PipelineID:       "p",
				ScheduleID:       tt.scheduleID,
				Date:             "2026-03-10",
				Deadline:         "10:00",
				ExpectedDuration: "30m",
			})
			if err != nil {
				t.Fatalf("HandleSLAMonitor returned error: %v", err)
			}
			if out.AlertType != tt.wantAlertType {
				t.Errorf("AlertType = %q, want %q", out.AlertType, tt.wantAlertType)
			}
			if out.BreachAt != tt.wantBreachAt {
				t.Errorf("BreachAt = %q, want %q", out.BreachAt, tt.wantBreachAt)
			}
		})
	}
}

// TestSLACancel_NoDeadlineSkipsVerdict pins fix 3: when neither an absolute
// deadline nor a relative (maxDuration + sensorArrivalAt) deadline can be
// determined, cancel must not fabricate an SLA_MET verdict.
func TestSLACancel_NoDeadlineSkipsVerdict(t *testing.T) {
	fake := newFakeDynamo(t, types.PipelineConfig{}, nil)
	d := testDeps(fake, time.Date(2026, 3, 10, 11, 0, 0, 0, time.UTC))
	eb := &countingEventBridge{}
	d.EventBridge = eb
	d.EventBusName = "test-bus"

	out, err := sla.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:            "cancel",
		PipelineID:      "p",
		ScheduleID:      "stream",
		Date:            "2026-03-10",
		MaxDuration:     "2h",
		SensorArrivalAt: "",
		Deadline:        "",
	})
	if err != nil {
		t.Fatalf("HandleSLAMonitor returned error: %v", err)
	}
	if out.BreachAt != "" {
		t.Errorf("BreachAt = %q, want empty (no deadline could be determined)", out.BreachAt)
	}
	if out.WarningAt != "" {
		t.Errorf("WarningAt = %q, want empty (no deadline could be determined)", out.WarningAt)
	}
	if eb.calls != 0 {
		t.Errorf("PutEvents called %d times, want 0 — no verdict should be published without a deadline", eb.calls)
	}
}
