package watchdog

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

const slaTestDate = "2026-03-10"

// slaRecorder records the schedules created and the event types published.
type slaRecorder struct {
	schedules []string
	events    []string
}

func (r *slaRecorder) CreateSchedule(_ context.Context, in *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	r.schedules = append(r.schedules, aws.ToString(in.Name))
	return &scheduler.CreateScheduleOutput{}, nil
}

func (r *slaRecorder) DeleteSchedule(context.Context, *scheduler.DeleteScheduleInput, ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	return &scheduler.DeleteScheduleOutput{}, nil
}

func (r *slaRecorder) PutEvents(_ context.Context, in *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	for _, e := range in.Entries {
		r.events = append(r.events, aws.ToString(e.DetailType))
	}
	return &eventbridge.PutEventsOutput{}, nil
}

func sItem(kv ...string) map[string]ddbtypes.AttributeValue {
	item := map[string]ddbtypes.AttributeValue{}
	for i := 0; i < len(kv); i += 2 {
		item[kv[i]] = &ddbtypes.AttributeValueMemberS{Value: kv[i+1]}
	}
	return item
}

// slaDeps wires an in-memory store holding one sensor-triggered pipeline "p"
// with an absolute and a relative SLA, an optional trigger row and job row
// for slaTestDate, and the recorder. The clock is 09:00 on slaTestDate.
// tweak, when given, edits the config before it is stored.
func slaDeps(t *testing.T, trigger, jobRow string, tweak ...func(*types.PipelineConfig)) (*lambda.Deps, *storetest.Mem, *slaRecorder) {
	t.Helper()
	t.Setenv("SKIP_SCHEDULER", "")
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		SLA:      &types.SLAConfig{Deadline: "10:00", ExpectedDuration: "1h", MaxDuration: "1h"},
	}
	for _, f := range tweak {
		f(&cfg)
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	fake, mem := storetest.NewMem()
	pk := types.PipelinePK("p")
	mem.Put("control", sItem("PK", pk, "SK", types.ConfigSK, "config", string(data)))
	if trigger != "" {
		mem.Put("control", sItem("PK", pk, "SK", types.TriggerSK("stream", slaTestDate), "status", trigger))
	}
	if jobRow != "" {
		mem.Put("joblog", sItem("PK", pk, "SK", types.JobSK("stream", slaTestDate, "1773129600000"), "event", jobRow))
	}
	s := storetest.NewStore(fake)
	rec := &slaRecorder{}
	now := time.Date(2026, 3, 10, 9, 0, 0, 0, time.UTC)
	return &lambda.Deps{
		Store:        s,
		ConfigCache:  store.NewConfigCache(s, time.Minute),
		Scheduler:    rec,
		EventBridge:  rec,
		EventBusName: "bus",
		NowFunc:      func() time.Time { return now },
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, mem, rec
}

// slaSiteCases are the trigger and job rows both SLA checks must agree on:
// a retry (RUNNING with the previous attempt's fail or timeout) is not
// finished; RUNNING with any other terminal newest row, COMPLETED and
// FAILED_FINAL are; without a trigger row a terminal job row is.
var slaSiteCases = []struct {
	name, trigger, jobRow string
	wantWork              bool
}{
	{"RetryAfterFail", types.TriggerStatusRunning, types.JobEventFail, true},
	{"RetryAfterTimeout", types.TriggerStatusRunning, types.JobEventTimeout, true},
	{"RunningSuccess", types.TriggerStatusRunning, types.JobEventSuccess, false},
	{"RunningValidationExhausted", types.TriggerStatusRunning, types.JobEventValidationExhausted, false},
	{"Completed", types.TriggerStatusCompleted, types.JobEventSuccess, false},
	{"FailedFinal", types.TriggerStatusFailedFinal, types.JobEventFail, false},
	{"NoTriggerTerminalRow", "", types.JobEventInfraTriggerExhausted, false},
	{"NotStarted", "", "", true},
}

func TestScheduleSLAAlerts_RetryKeepsSchedules(t *testing.T) {
	for _, tt := range slaSiteCases {
		t.Run(tt.name, func(t *testing.T) {
			d, _, rec := slaDeps(t, tt.trigger, tt.jobRow)

			if err := scheduleSLAAlerts(context.Background(), d); err != nil {
				t.Fatalf("scheduleSLAAlerts: %v", err)
			}

			var want []string
			if tt.wantWork {
				want = []string{
					lambda.SLAScheduleName("p", "stream", slaTestDate, "warning"),
					lambda.SLAScheduleName("p", "stream", slaTestDate, "breach"),
				}
			}
			if !slices.Equal(rec.schedules, want) {
				t.Fatalf("schedules created = %v, want %v", rec.schedules, want)
			}
		})
	}
}

// TestScheduleSLAAlerts_SkipsDueWarning pins that a pass made after the
// warning time schedules only the breach: the warning has fired already (or
// is due), and a schedule with a past at() would fire it again.
func TestScheduleSLAAlerts_SkipsDueWarning(t *testing.T) {
	tests := []struct {
		name     string
		expected string // warning = 10:00 breach - expected; the clock is 09:00
		want     []string
	}{
		{name: "WarningPast", expected: "2h", want: []string{"breach"}},
		{name: "WarningNow", expected: "1h", want: []string{"breach"}},
		{name: "WarningAhead", expected: "30m", want: []string{"warning", "breach"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A cron pipeline keeps its own date, so the 10:00 breach is today.
			d, _, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
				c.Schedule.Cron = "0 8 * * *"
				c.SLA.ExpectedDuration = tt.expected
			})

			if err := scheduleSLAAlerts(context.Background(), d); err != nil {
				t.Fatalf("scheduleSLAAlerts: %v", err)
			}

			var want []string
			for _, suffix := range tt.want {
				want = append(want, lambda.SLAScheduleName("p", "cron", slaTestDate, suffix))
			}
			if !slices.Equal(rec.schedules, want) {
				t.Fatalf("schedules created = %v, want %v", rec.schedules, want)
			}
		})
	}
}

func TestDetectRelativeSLABreaches_RetryKeepsBreach(t *testing.T) {
	for _, tt := range slaSiteCases {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, rec := slaDeps(t, tt.trigger, tt.jobRow)
			if err := d.Store.WriteSensor(context.Background(), "p", "first-sensor-arrival#"+slaTestDate,
				map[string]interface{}{"arrivedAt": "2026-03-10T07:00:00Z"}); err != nil {
				t.Fatalf("write arrival: %v", err)
			}

			if err := detectRelativeSLABreaches(context.Background(), d); err != nil {
				t.Fatalf("detectRelativeSLABreaches: %v", err)
			}

			var want []string
			if tt.wantWork {
				want = []string{string(types.EventRelativeSLABreach)}
			}
			if !slices.Equal(rec.events, want) {
				t.Fatalf("events published = %v, want %v", rec.events, want)
			}
			marker := mem.Get("control", types.PipelinePK("p"), types.SensorSK("relative-sla-breach-check#"+slaTestDate))
			if (marker != nil) != tt.wantWork {
				t.Fatalf("dedup marker written = %v, want %v", marker != nil, tt.wantWork)
			}
		})
	}
}
