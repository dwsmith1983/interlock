package sla_test

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/sla"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// countingEventBridge is a lambda.EventBridgeAPI double that records every
// PutEvents call (its detail type and decoded detail), so a test can prove
// what was published, or that nothing was. A set err fails every call.
type countingEventBridge struct {
	calls       int
	detailTypes []string
	details     []types.InterlockEvent
	err         error
}

func (c *countingEventBridge) PutEvents(_ context.Context, in *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	for _, e := range in.Entries {
		c.detailTypes = append(c.detailTypes, aws.ToString(e.DetailType))
		var evt types.InterlockEvent
		if err := json.Unmarshal([]byte(aws.ToString(e.Detail)), &evt); err != nil {
			return nil, err
		}
		c.details = append(c.details, evt)
	}
	return &eventbridge.PutEventsOutput{}, nil
}

// deletingScheduler is a lambda.SchedulerAPI double that records the names
// of the schedules DeleteSchedule was called for, then returns deleteErr.
type deletingScheduler struct {
	deleted   []string
	deleteErr error
}

func (s *deletingScheduler) CreateSchedule(context.Context, *scheduler.CreateScheduleInput, ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	return &scheduler.CreateScheduleOutput{}, nil
}

func (s *deletingScheduler) DeleteSchedule(_ context.Context, in *scheduler.DeleteScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	s.deleted = append(s.deleted, aws.ToString(in.Name))
	if s.deleteErr != nil {
		return nil, s.deleteErr
	}
	return &scheduler.DeleteScheduleOutput{}, nil
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

// triggerItem builds a minimal TRIGGER row with the given status.
func triggerItem(status string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", "2026-03-10")},
		"status": &ddbtypes.AttributeValueMemberS{Value: status},
	}
}

// newFakeDynamo returns a FakeDynamo that serves cfg for CONFIG-key reads
// (cfgErr, if set, or no row when cfg has no pipeline ID) and a minimal
// COMPLETED TRIGGER row for every TRIGGER-key read.
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
				if cfg.Pipeline.ID == "" {
					return &dynamodb.GetItemOutput{}, nil
				}
				return &dynamodb.GetItemOutput{Item: configItem(t, cfg)}, nil
			case strings.HasPrefix(skAttr.Value, "TRIGGER#"):
				return &dynamodb.GetItemOutput{Item: triggerItem(types.TriggerStatusCompleted)}, nil
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

// TestSLACancel_AppliesDeadlineDayOffset pins the SLA date on the cancel
// path: the deadline falls sla.deadlineDayOffset days after the execution
// date, for sensor and cron pipelines alike. Every case deletes both
// schedules, even when the config read fails.
func TestSLACancel_AppliesDeadlineDayOffset(t *testing.T) {
	t.Setenv("SKIP_SCHEDULER", "")
	sensorCfg := func(offset int) types.PipelineConfig {
		return types.PipelineConfig{
			Pipeline: types.PipelineIdentity{ID: "p"},
			SLA:      &types.SLAConfig{Deadline: "10:00", DeadlineDayOffset: offset, ExpectedDuration: "30m"},
		}
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
		wantErr       bool
		wantAlertType string
		wantBreachAt  string
		wantPublished bool
	}{
		{
			name:          "sensor offset 0 breached after same-day deadline",
			cfg:           sensorCfg(0),
			scheduleID:    "stream",
			now:           time.Date(2026, 3, 10, 11, 0, 0, 0, time.UTC),
			wantAlertType: string(types.EventSLABreach),
			wantBreachAt:  "2026-03-10T10:00:00Z",
			wantPublished: true,
		},
		{
			name:          "sensor offset 1 met before next-day deadline",
			cfg:           sensorCfg(1),
			scheduleID:    "stream",
			now:           time.Date(2026, 3, 10, 11, 0, 0, 0, time.UTC),
			wantAlertType: string(types.EventSLAMet),
			wantBreachAt:  "2026-03-11T10:00:00Z",
			wantPublished: true,
		},
		{
			name:          "cron pipeline breached, no roll-forward",
			cfg:           cronCfg,
			scheduleID:    "cron",
			now:           time.Date(2026, 3, 10, 11, 0, 0, 0, time.UTC),
			wantAlertType: string(types.EventSLABreach),
			wantBreachAt:  "2026-03-10T10:00:00Z",
			wantPublished: true,
		},
		{
			name:          "cron pipeline met, no roll-forward",
			cfg:           cronCfg,
			scheduleID:    "cron",
			now:           time.Date(2026, 3, 10, 9, 0, 0, 0, time.UTC),
			wantAlertType: string(types.EventSLAMet),
			wantBreachAt:  "2026-03-10T10:00:00Z",
			wantPublished: true,
		},
		{
			name:          "missing config uses the execution date",
			scheduleID:    "stream",
			now:           time.Date(2026, 3, 10, 11, 0, 0, 0, time.UTC),
			wantAlertType: string(types.EventSLABreach),
			wantBreachAt:  "2026-03-10T10:00:00Z",
			wantPublished: true,
		},
		{
			name:       "config lookup failure returns an error so the step retries",
			cfg:        sensorCfg(1),
			cfgErr:     errors.New("dynamodb: internal error"),
			scheduleID: "stream",
			now:        time.Date(2026, 3, 10, 11, 0, 0, 0, time.UTC),
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeDynamo(t, tt.cfg, tt.cfgErr)
			d := testDeps(fake, tt.now)
			sched := &deletingScheduler{}
			d.Scheduler = sched
			eb := &countingEventBridge{}
			d.EventBridge, d.EventBusName = eb, "test-bus"

			out, err := sla.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
				Mode:             "cancel",
				PipelineID:       "p",
				ScheduleID:       tt.scheduleID,
				Date:             "2026-03-10",
				Deadline:         "10:00",
				ExpectedDuration: "30m",
			})
			wantDeleted := []string{
				lambda.SLAScheduleName("p", tt.scheduleID, "2026-03-10", "warning"),
				lambda.SLAScheduleName("p", tt.scheduleID, "2026-03-10", "breach"),
			}
			if !slices.Equal(sched.deleted, wantDeleted) {
				t.Errorf("deleted schedules = %v, want %v", sched.deleted, wantDeleted)
			}
			var wantTypes []string
			if tt.wantPublished {
				wantTypes = []string{tt.wantAlertType}
			}
			if !slices.Equal(eb.detailTypes, wantTypes) {
				t.Errorf("published %v, want %v", eb.detailTypes, wantTypes)
			}
			if tt.wantErr {
				if err == nil {
					t.Fatalf("HandleSLAMonitor returned %+v, want an error", out)
				}
				return
			}
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

// TestSLACancel_Verdict pins the verdict cancel publishes when a run ends:
// SLA_MET before the breach time, SLA_BREACH at or after it, from the times
// given or recalculated from the deadline, and none for a run that never
// triggered or whose date is of the other cadence than its deadline (a relative
// SLA still gives one). SLA_MET needs a COMPLETED trigger row; a FAILED_FINAL or
// RUNNING row, or a trigger read that fails, still gets SLA_BREACH. Both schedules
// are deleted first, whatever the deletes return, and the output is the same
// whether or not a verdict is published.
func TestSLACancel_Verdict(t *testing.T) {
	t.Setenv("SKIP_SCHEDULER", "")
	now := time.Date(2026, 3, 10, 11, 0, 0, 0, time.UTC)
	met, breach := string(types.EventSLAMet), string(types.EventSLABreach)
	type in = lambda.SLAMonitorInput
	// verdict is the output for a run that ends at now.
	verdict := func(alertType, warningAt, breachAt string) lambda.SLAMonitorOutput {
		return lambda.SLAMonitorOutput{AlertType: alertType, WarningAt: warningAt, BreachAt: breachAt, FiredAt: "2026-03-10T11:00:00Z"}
	}
	metIn := in{WarningAt: "2026-03-10T11:45:00Z", BreachAt: "2026-03-10T11:59:00Z"}
	metOut := verdict(met, "2026-03-10T11:45:00Z", "2026-03-10T11:59:00Z")
	// deadlineRun ends a run of date whose stored config has the same deadline.
	deadlineRun := func(deadline, date string) in {
		return in{PipelineID: "p", Date: date, Deadline: deadline, ExpectedDuration: "30m"}
	}
	offsetRun := deadlineRun("10:00", "2026-03-10")
	offsetRun.Timezone = "America/New_York"
	relativeRun := deadlineRun(":30", "2026-03-10")
	relativeRun.MaxDuration, relativeRun.SensorArrivalAt = "2h", "2026-03-10T08:00:00Z"

	tests := []struct {
		name        string
		in          lambda.SLAMonitorInput
		noScheduler bool
		deleteErr   error
		ebErr       error
		neverRan    bool   // an empty store with no trigger row
		status      string // the trigger row's status in a store holding only that row
		triggerErr  error  // every store read fails with this
		wantErr     string
		want        lambda.SLAMonitorOutput
		wantTypes   []string
		now         time.Time // zero means 11:00
		offset      int       // the stored config's deadlineDayOffset
	}{
		{name: "met", in: metIn, want: metOut, wantTypes: []string{met}},
		{name: "past breach", in: in{BreachAt: "2026-03-10T10:00:00Z"}, want: verdict(breach, "", "2026-03-10T10:00:00Z"), wantTypes: []string{breach}},
		{name: "at breach", in: in{BreachAt: "2026-03-10T11:00:00Z"}, want: verdict(breach, "", "2026-03-10T11:00:00Z"), wantTypes: []string{breach}},
		{
			name: "past warning, future breach", in: in{WarningAt: "2020-01-01T00:50:00Z", BreachAt: "2099-12-31T23:59:00Z"},
			want: verdict(met, "2020-01-01T00:50:00Z", "2099-12-31T23:59:00Z"), wantTypes: []string{met},
		},
		{
			name: "echoes the given times", in: in{WarningAt: "2099-01-01T00:00:00Z", BreachAt: "2099-01-01T01:00:00Z"},
			want: verdict(met, "2099-01-01T00:00:00Z", "2099-01-01T01:00:00Z"), wantTypes: []string{met},
		},
		{name: "no scheduler", in: metIn, noScheduler: true, want: metOut, wantTypes: []string{met}},
		{name: "delete error still publishes", in: metIn, deleteErr: errors.New("internal service error"), want: metOut, wantTypes: []string{met}},
		{
			name: "publish error", in: metIn, ebErr: errors.New("EventBridge unavailable"),
			wantErr: "publish SLA cancel verdict: publish SLA_MET event: EventBridge unavailable",
		},
		{name: "recalculate error", in: in{Deadline: "not-a-time", ExpectedDuration: "30m"}, wantErr: "cancel recalculate: parse deadline"},
		{
			name: "relative recalculate", in: in{MaxDuration: "2h", SensorArrivalAt: "2026-03-10T08:00:00Z"},
			want: verdict(breach, "2026-03-10T09:30:00Z", "2026-03-10T10:00:00Z"), wantTypes: []string{breach},
		},
		{name: "relative recalculate error", in: in{MaxDuration: "bogus", SensorArrivalAt: "2026-03-10T08:00:00Z"}, wantErr: "cancel recalculate (relative)"},
		{
			name: "never triggered", neverRan: true,
			in:   in{PipelineID: "never-ran", Date: "2026-03-13", WarningAt: "2099-12-31T23:45:00Z", BreachAt: "2099-12-31T23:59:00Z"},
			want: verdict(met, "2099-12-31T23:45:00Z", "2099-12-31T23:59:00Z"),
		},
		{
			name: "never triggered past the breach", neverRan: true,
			in:   in{PipelineID: "never-ran", Date: "2026-03-13", BreachAt: "2026-03-10T10:00:00Z"},
			want: verdict(breach, "", "2026-03-10T10:00:00Z"),
		},
		{name: "COMPLETED before the breach", status: types.TriggerStatusCompleted, in: metIn, want: metOut, wantTypes: []string{met}},
		{name: "FAILED_FINAL before the breach", status: types.TriggerStatusFailedFinal, in: metIn, want: metOut},
		{
			name: "FAILED_FINAL at or after the breach", status: types.TriggerStatusFailedFinal, in: in{BreachAt: "2026-03-10T11:00:00Z"},
			want: verdict(breach, "", "2026-03-10T11:00:00Z"), wantTypes: []string{breach},
		},
		{name: "RUNNING at cancel", status: types.TriggerStatusRunning, in: metIn, want: metOut},
		{
			name: "RUNNING past the breach", status: types.TriggerStatusRunning, in: in{BreachAt: "2026-03-10T10:00:00Z"},
			want: verdict(breach, "", "2026-03-10T10:00:00Z"), wantTypes: []string{breach},
		},
		{name: "trigger read fails before the breach", triggerErr: errors.New("throttled"), in: metIn, want: metOut},
		{
			name: "trigger read fails past the breach", triggerErr: errors.New("throttled"), in: in{BreachAt: "2026-03-10T10:00:00Z"},
			want: verdict(breach, "", "2026-03-10T10:00:00Z"), wantTypes: []string{breach},
		},
		{name: "hourly deadline, daily date", in: deadlineRun(":30", "2026-03-10"), want: verdict(met, "", "")},
		{name: "daily deadline, hourly date", in: deadlineRun("10:00", "2026-03-10T07"), want: verdict(met, "", "")},
		{
			name: "hourly deadline, hourly date", in: deadlineRun(":30", "2026-03-10T07"), now: time.Date(2026, 3, 10, 7, 45, 0, 0, time.UTC),
			want:      lambda.SLAMonitorOutput{AlertType: met, WarningAt: "2026-03-10T08:00:00Z", BreachAt: "2026-03-10T08:30:00Z", FiredAt: "2026-03-10T07:45:00Z"},
			wantTypes: []string{met},
		},
		{
			name: "daily deadline a day later, daily date", in: offsetRun, offset: 1, now: time.Date(2026, 3, 10, 15, 0, 0, 0, time.UTC),
			want:      lambda.SLAMonitorOutput{AlertType: met, WarningAt: "2026-03-11T13:30:00Z", BreachAt: "2026-03-11T14:00:00Z", FiredAt: "2026-03-10T15:00:00Z"},
			wantTypes: []string{met},
		},
		{
			name: "hourly deadline, daily date, relative SLA", in: relativeRun,
			want: verdict(breach, "2026-03-10T09:30:00Z", "2026-03-10T10:00:00Z"), wantTypes: []string{breach},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg types.PipelineConfig
			if tt.in.Deadline != "" {
				cfg = types.PipelineConfig{
					Pipeline: types.PipelineIdentity{ID: "p"},
					SLA: &types.SLAConfig{Deadline: tt.in.Deadline, DeadlineDayOffset: tt.offset,
						ExpectedDuration: tt.in.ExpectedDuration, MaxDuration: tt.in.MaxDuration, Timezone: tt.in.Timezone},
				}
			}
			fake := newFakeDynamo(t, cfg, nil)
			if tt.neverRan || tt.status != "" {
				var mem *storetest.Mem
				fake, mem = storetest.NewMem()
				if tt.status != "" {
					mem.Put("control", triggerItem(tt.status))
				}
			}
			if tt.triggerErr != nil {
				fake = &storetest.FakeDynamo{GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return nil, tt.triggerErr
				}}
			}
			d := testDeps(fake, cmp.Or(tt.now, now))
			sched := &deletingScheduler{deleteErr: tt.deleteErr}
			if !tt.noScheduler {
				d.Scheduler = sched
			}
			eb := &countingEventBridge{err: tt.ebErr}
			d.EventBridge, d.EventBusName = eb, "test-bus"
			input := tt.in
			input.Mode, input.ScheduleID = "cancel", "stream"
			if input.PipelineID == "" {
				input.PipelineID, input.Date = "p", "2026-03-10"
			}

			out, err := sla.HandleSLAMonitor(context.Background(), d, input)

			var wantDeleted []string
			if !tt.noScheduler {
				wantDeleted = []string{
					lambda.SLAScheduleName(input.PipelineID, "stream", input.Date, "warning"),
					lambda.SLAScheduleName(input.PipelineID, "stream", input.Date, "breach"),
				}
			}
			if !slices.Equal(sched.deleted, wantDeleted) {
				t.Errorf("deleted schedules = %v, want %v", sched.deleted, wantDeleted)
			}
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("HandleSLAMonitor returned error: %v", err)
			}
			if out != tt.want {
				t.Errorf("output = %+v, want %+v", out, tt.want)
			}
			if !slices.Equal(eb.detailTypes, tt.wantTypes) {
				t.Errorf("published %v, want %v", eb.detailTypes, tt.wantTypes)
			}
		})
	}
}

// TestSLACancel_EventDetail pins the details of the verdict cancel publishes:
// the trigger row's status when it was read, the breach time, and what that
// time was measured from. That is the deadline and its time zone, or the
// maxDuration and first sensor arrival of a relative SLA, which decides when a
// pipeline has both. A skipped verdict publishes nothing.
func TestSLACancel_EventDetail(t *testing.T) {
	const day = "2026-03-10"
	type in = lambda.SLAMonitorInput
	type m = map[string]interface{}
	met, breach := string(types.EventSLAMet), string(types.EventSLABreach)
	completed, failed := types.TriggerStatusCompleted, types.TriggerStatusFailedFinal
	// Breach times: absolute and relative 12:00, both 10:00 in New York (14:00
	// UTC) by its deadline and 12:00 by its relative SLA.
	absolute := in{Date: day, Deadline: "12:00", ExpectedDuration: "30m", Timezone: "UTC"}
	relative := in{Date: day, MaxDuration: "4h", SensorArrivalAt: day + "T08:00:00Z"}
	both := relative
	both.Deadline, both.ExpectedDuration, both.Timezone = "10:00", "30m", "America/New_York"
	given := both
	given.WarningAt, given.BreachAt = day+"T11:30:00Z", day+"T12:00:00Z"

	absoluteDetail := m{"source": "cancel", "status": completed, "deadline": "12:00", "timezone": "UTC", "breachAt": day + "T12:00:00Z"}
	relativeDetail := m{"source": "cancel", "status": completed, "maxDuration": "4h", "sensorArrivalAt": day + "T08:00:00Z", "breachAt": day + "T12:00:00Z"}

	tests := []struct {
		name       string
		in         in
		hour       int    // the hour the run ends, UTC
		status     string // "" means the trigger read fails
		wantType   string // "" means nothing is published
		wantDetail m
	}{
		{"absolute met", absolute, 11, completed, met, absoluteDetail},
		{"absolute breach", absolute, 12, completed, breach, absoluteDetail},
		{"absolute, no timezone", in{Date: day, Deadline: "12:00", ExpectedDuration: "30m"}, 11, completed, met, absoluteDetail},
		{"absolute, maxDuration without an arrival", in{Date: day, Deadline: "12:00", ExpectedDuration: "30m", MaxDuration: "4h", Timezone: "UTC"}, 11, completed, met, absoluteDetail},
		{"absolute, failed run", absolute, 12, failed, breach, m{"source": "cancel", "status": failed, "deadline": "12:00", "timezone": "UTC", "breachAt": day + "T12:00:00Z"}},
		{"absolute, configured zone", in{Date: day, Deadline: "06:00", ExpectedDuration: "30m", Timezone: "America/New_York"}, 11, types.TriggerStatusRunning, breach,
			m{"source": "cancel", "status": types.TriggerStatusRunning, "deadline": "06:00", "timezone": "America/New_York", "breachAt": day + "T10:00:00Z"}},
		{"absolute, hourly", in{Date: day + "T10", Deadline: ":30", ExpectedDuration: "30m", Timezone: "UTC"}, 11, completed, met,
			m{"source": "cancel", "status": completed, "deadline": ":30", "timezone": "UTC", "breachAt": day + "T11:30:00Z"}},
		{"absolute, trigger read fails", absolute, 12, "", breach, m{"source": "cancel", "deadline": "12:00", "timezone": "UTC", "breachAt": day + "T12:00:00Z"}},
		{"relative met", relative, 11, completed, met, relativeDetail},
		{"relative breach", relative, 12, completed, breach, relativeDetail},
		{"both, relative decides", both, 11, completed, met, relativeDetail},
		{"given times with a deadline", given, 11, completed, met, m{"source": "cancel", "status": completed, "deadline": "10:00", "timezone": "America/New_York", "breachAt": day + "T12:00:00Z"}},
		{"given breach time only", in{Date: day, BreachAt: day + "T10:00:00Z", Timezone: "UTC"}, 11, completed, breach, m{"source": "cancel", "status": completed, "breachAt": day + "T10:00:00Z"}},
		{"given warning time only", in{Date: day, WarningAt: day + "T10:00:00Z"}, 11, completed, met, m{"source": "cancel", "status": completed}},
		{"skipped, no deadline", in{Date: day}, 11, completed, "", nil},
		{"skipped, other cadence", in{Date: day, Deadline: ":30", ExpectedDuration: "30m"}, 11, completed, "", nil},
		{"skipped, failed before the breach", absolute, 11, failed, "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &storetest.FakeDynamo{GetItemFn: func(_ context.Context, get *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
				if sk, ok := get.Key["SK"].(*ddbtypes.AttributeValueMemberS); ok && sk.Value == types.ConfigSK {
					return &dynamodb.GetItemOutput{}, nil
				}
				if tt.status == "" {
					return nil, errors.New("throttled")
				}
				return &dynamodb.GetItemOutput{Item: triggerItem(tt.status)}, nil
			}}
			d := testDeps(fake, time.Date(2026, 3, 10, tt.hour, 0, 0, 0, time.UTC))
			eb := &countingEventBridge{}
			d.EventBridge, d.EventBusName = eb, "test-bus"
			input := tt.in
			input.Mode, input.PipelineID, input.ScheduleID = "cancel", "p", "stream"

			if _, err := sla.HandleSLAMonitor(context.Background(), d, input); err != nil {
				t.Fatalf("HandleSLAMonitor returned error: %v", err)
			}

			if tt.wantType == "" {
				if eb.calls != 0 {
					t.Errorf("published %v with %+v, want nothing", eb.detailTypes, eb.details)
				}
				return
			}
			if !slices.Equal(eb.detailTypes, []string{tt.wantType}) {
				t.Fatalf("published %v, want %s", eb.detailTypes, tt.wantType)
			}
			evt := eb.details[0]
			if want := "pipeline p: " + tt.wantType; evt.Message != want {
				t.Errorf("Message = %q, want %q", evt.Message, want)
			}
			if !reflect.DeepEqual(evt.Detail, tt.wantDetail) {
				t.Errorf("Detail = %v, want %v", evt.Detail, tt.wantDetail)
			}
		})
	}
}
