package watchdog

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulerTypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

const slaTestDate = "2026-03-10"

// slaRecorder records the schedules created (names and, in step, their
// expressions) and the event types published, and both in call order. While
// failEvents > 0, PutEvents fails and counts it down. With exists set,
// CreateSchedule records the name and answers as for an existing schedule;
// with createErr set, it records the name and returns createErr.
type slaRecorder struct {
	schedules  []string
	exprs      []string
	events     []string
	details    []types.InterlockEvent
	order      []string
	failEvents int
	exists     bool
	createErr  error
}

func (r *slaRecorder) CreateSchedule(_ context.Context, in *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	r.schedules = append(r.schedules, aws.ToString(in.Name))
	r.exprs = append(r.exprs, aws.ToString(in.ScheduleExpression))
	r.order = append(r.order, aws.ToString(in.Name))
	if r.exists {
		return nil, &schedulerTypes.ConflictException{Message: aws.String("schedule exists")}
	}
	if r.createErr != nil {
		return nil, r.createErr
	}
	return &scheduler.CreateScheduleOutput{}, nil
}

func (r *slaRecorder) DeleteSchedule(context.Context, *scheduler.DeleteScheduleInput, ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	return &scheduler.DeleteScheduleOutput{}, nil
}

func (r *slaRecorder) PutEvents(_ context.Context, in *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	if r.failEvents > 0 {
		r.failEvents--
		return nil, errors.New("put events failed")
	}
	for _, e := range in.Entries {
		var evt types.InterlockEvent
		if err := json.Unmarshal([]byte(aws.ToString(e.Detail)), &evt); err != nil {
			return nil, err
		}
		r.events = append(r.events, aws.ToString(e.DetailType))
		r.details = append(r.details, evt)
		r.order = append(r.order, aws.ToString(e.DetailType))
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
// with an absolute and a relative SLA, a RUN# row, an optional trigger row
// and job row for slaTestDate, and the recorder. The clock is 09:00 on
// slaTestDate, so the 09:30 warning and 10:00 breach are both ahead.
// tweak, when given, edits the config before it is stored.
func slaDeps(t *testing.T, trigger, jobRow string, tweak ...func(*types.PipelineConfig)) (*lambda.Deps, *storetest.Mem, *slaRecorder) {
	t.Helper()
	t.Setenv("SKIP_SCHEDULER", "")
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}},
		SLA:      &types.SLAConfig{Deadline: "10:00", ExpectedDuration: "30m", MaxDuration: "1h"},
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
	mem.Put("control", sItem("PK", pk, "SK", types.RunSK(slaTestDate)))
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
	{"RunningTriggerExhausted", types.TriggerStatusRunning, types.JobEventInfraTriggerExhausted, false},
	{"Completed", types.TriggerStatusCompleted, types.JobEventSuccess, false},
	{"FailedFinal", types.TriggerStatusFailedFinal, types.JobEventFail, false},
	{"NoTriggerTerminalRow", "", types.JobEventInfraTriggerExhausted, false},
	{"NotStarted", "", "", true},
	{"ReleasedEvaluation", "", types.JobEventEvaluationReleased, true},
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

// TestScheduleSLAAlerts_ExistingSchedulesAreNotErrors pins that a pass over a
// run date whose warning and breach schedules exist already (a released date
// that restarts, or any later pass) attempts both, logs no error and returns nil.
func TestScheduleSLAAlerts_ExistingSchedulesAreNotErrors(t *testing.T) {
	d, _, rec := slaDeps(t, "", "")
	rec.exists = true
	var logs bytes.Buffer
	d.Logger = slog.New(slog.NewTextHandler(&logs, nil))

	if err := scheduleSLAAlerts(context.Background(), d); err != nil {
		t.Fatalf("scheduleSLAAlerts: %v", err)
	}

	want := []string{
		lambda.SLAScheduleName("p", "stream", slaTestDate, "warning"),
		lambda.SLAScheduleName("p", "stream", slaTestDate, "breach"),
	}
	if !slices.Equal(rec.schedules, want) {
		t.Fatalf("schedules attempted = %v, want %v", rec.schedules, want)
	}
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("logs = %q, want no error", logs.String())
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
			d, _, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
				// No maxDuration: expectedDuration must stay below it, and these rows reach it.
				c.SLA.ExpectedDuration, c.SLA.MaxDuration = tt.expected, ""
			})

			if err := scheduleSLAAlerts(context.Background(), d); err != nil {
				t.Fatalf("scheduleSLAAlerts: %v", err)
			}

			var want []string
			for _, suffix := range tt.want {
				want = append(want, lambda.SLAScheduleName("p", "stream", slaTestDate, suffix))
			}
			if !slices.Equal(rec.schedules, want) {
				t.Fatalf("schedules created = %v, want %v", rec.schedules, want)
			}
		})
	}
}

// TestScheduleSLAAlerts_DeadlineDayOffset pins that the breach schedule
// fires sla.deadlineDayOffset days after the execution date and is named for
// the execution date, and that offset 0 keeps the deadline on the same day.
func TestScheduleSLAAlerts_DeadlineDayOffset(t *testing.T) {
	tests := []struct {
		name      string
		offset    int
		now       time.Time
		want      []string
		wantExprs []string
	}{
		{
			name:   "Offset1",
			offset: 1,
			now:    time.Date(2026, 3, 10, 0, 5, 0, 0, time.UTC),
			want: []string{
				lambda.SLAScheduleName("p", "stream", slaTestDate, "warning"),
				lambda.SLAScheduleName("p", "stream", slaTestDate, "breach"),
			},
			wantExprs: []string{"at(2026-03-11T01:30:00)", "at(2026-03-11T02:00:00)"},
		},
		{
			name: "Offset0PastDeadline",
			now:  time.Date(2026, 3, 10, 3, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, _, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
				c.SLA.Deadline = "02:00"
				c.SLA.DeadlineDayOffset = tt.offset
			})
			now := tt.now
			d.NowFunc = func() time.Time { return now }

			if err := scheduleSLAAlerts(context.Background(), d); err != nil {
				t.Fatalf("scheduleSLAAlerts: %v", err)
			}

			if !slices.Equal(rec.schedules, tt.want) {
				t.Fatalf("schedules created = %v, want %v", rec.schedules, tt.want)
			}
			if !slices.Equal(rec.exprs, tt.wantExprs) {
				t.Fatalf("schedule expressions = %v, want %v", rec.exprs, tt.wantExprs)
			}
		})
	}
}

// TestScheduleSLAAlerts_RunRows pins that proactive SLA schedules come from
// the pipeline's RUN# rows: one pair per due key of the deadline's cadence,
// none without a row. Each case replaces slaDeps' row with its keys. The
// deadline is 10:00 (warning 09:30) and the clock 08:00 on slaTestDate.
// A run that finished is covered by TestScheduleSLAAlerts_RetryKeepsSchedules.
func TestScheduleSLAAlerts_RunRows(t *testing.T) {
	names := func(dates ...string) []string {
		var out []string
		for _, date := range dates {
			out = append(out, lambda.SLAScheduleName("p", "stream", date, "warning"),
				lambda.SLAScheduleName("p", "stream", date, "breach"))
		}
		return out
	}
	tests := []struct {
		name      string
		keys      []string
		now       time.Time // zero means 08:00 on slaTestDate
		tweak     func(*types.PipelineConfig)
		want      []string
		wantExprs []string
		wantLog   string
		wantLevel string // level of the wantLog line; "" means WARN
	}{
		{name: "RowForDate", keys: []string{slaTestDate}, want: names(slaTestDate),
			wantExprs: []string{"at(2026-03-10T09:30:00)", "at(2026-03-10T10:00:00)"}},
		{name: "NoRows"},
		{name: "TwoDates", keys: []string{slaTestDate, "2026-03-11"}, want: names(slaTestDate, "2026-03-11")},
		{name: "BreachPassed", keys: []string{"2026-03-09"}},
		{name: "MalformedKeys", keys: []string{"2026-3-10", "2026-03-10x", slaTestDate}, want: names(slaTestDate),
			wantLog: "runKey=2026-03-10x"},
		{name: "HourlyDeadline", keys: []string{slaTestDate, "2026-03-10T07"}, want: names("2026-03-10T07"),
			wantExprs: []string{"at(2026-03-10T08:15:00)", "at(2026-03-10T08:30:00)"},
			tweak:     func(c *types.PipelineConfig) { c.SLA.Deadline, c.SLA.ExpectedDuration = ":30", "15m" }},
		{name: "DailyDeadlineSkipsHourlyKey", keys: []string{"2026-03-10T07"}},
		{name: "ExcludedWeekend", keys: []string{"2026-03-13", "2026-03-14"}, want: names("2026-03-13"),
			now:   time.Date(2026, 3, 13, 8, 0, 0, 0, time.UTC),
			tweak: func(c *types.PipelineConfig) { c.Schedule.Exclude = &types.ExclusionConfig{Weekends: true} }},
		{name: "DeadlineDayOffset", keys: []string{"2026-03-09"}, want: names("2026-03-09"),
			wantExprs: []string{"at(2026-03-10T09:30:00)", "at(2026-03-10T10:00:00)"},
			tweak:     func(c *types.PipelineConfig) { c.SLA.DeadlineDayOffset = 1 }},
		{name: "NoSLA", keys: []string{slaTestDate}, tweak: func(c *types.PipelineConfig) { c.SLA = nil }},
		{name: "InvalidDeadline", keys: []string{slaTestDate}, wantLog: "SLA calculate failed", wantLevel: "ERROR",
			tweak: func(c *types.PipelineConfig) { c.SLA.Deadline = "INVALID" }},
		{name: "DryRun", keys: []string{slaTestDate}, tweak: dryRunHTTP},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tweaks []func(*types.PipelineConfig)
			if tt.tweak != nil {
				tweaks = append(tweaks, tt.tweak)
			}
			d, mem, rec := slaDeps(t, "", "", tweaks...)
			if _, err := d.Store.Client.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
				TableName: aws.String("control"),
				Key:       sItem("PK", types.PipelinePK("p"), "SK", types.RunSK(slaTestDate)),
			}); err != nil {
				t.Fatalf("delete default RUN# row: %v", err)
			}
			for _, key := range tt.keys {
				mem.Put("control", sItem("PK", types.PipelinePK("p"), "SK", types.RunSK(key)))
			}
			now := time.Date(2026, 3, 10, 8, 0, 0, 0, time.UTC)
			if !tt.now.IsZero() {
				now = tt.now
			}
			d.NowFunc = func() time.Time { return now }
			var logs bytes.Buffer
			d.Logger = slog.New(slog.NewTextHandler(&logs, nil))

			if err := scheduleSLAAlerts(context.Background(), d); err != nil {
				t.Fatalf("scheduleSLAAlerts: %v", err)
			}

			if !slices.Equal(rec.schedules, tt.want) {
				t.Fatalf("schedules created = %v, want %v", rec.schedules, tt.want)
			}
			if tt.wantExprs != nil && !slices.Equal(rec.exprs, tt.wantExprs) {
				t.Fatalf("schedule expressions = %v, want %v", rec.exprs, tt.wantExprs)
			}
			if tt.wantLog != "" && !slices.ContainsFunc(strings.Split(logs.String(), "\n"), func(line string) bool {
				return strings.Contains(line, "level="+cmp.Or(tt.wantLevel, "WARN")) && strings.Contains(line, tt.wantLog)
			}) {
				t.Fatalf("logs = %q, want a %s line with %q", logs.String(), cmp.Or(tt.wantLevel, "WARN"), tt.wantLog)
			}
		})
	}
}

// TestScheduleSLAAlerts_CreateErrorContinues: a failed CreateSchedule is
// logged as an error, ends that date's schedules at the warning, and the
// pass moves on to the next date.
func TestScheduleSLAAlerts_CreateErrorContinues(t *testing.T) {
	d, mem, rec := slaDeps(t, "", "")
	mem.Put("control", sItem("PK", types.PipelinePK("p"), "SK", types.RunSK("2026-03-11")))
	now := time.Date(2026, 3, 10, 8, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return now }
	rec.createErr = errors.New("service unavailable")
	var logs bytes.Buffer
	d.Logger = slog.New(slog.NewTextHandler(&logs, nil))

	if err := scheduleSLAAlerts(context.Background(), d); err != nil {
		t.Fatalf("scheduleSLAAlerts: %v", err)
	}

	want := []string{
		lambda.SLAScheduleName("p", "stream", slaTestDate, "warning"),
		lambda.SLAScheduleName("p", "stream", "2026-03-11", "warning"),
	}
	if !slices.Equal(rec.schedules, want) {
		t.Fatalf("schedules attempted = %v, want %v", rec.schedules, want)
	}
	if !slices.ContainsFunc(strings.Split(logs.String(), "\n"), func(line string) bool {
		return strings.Contains(line, "level=ERROR") && strings.Contains(line, "create SLA schedule failed")
	}) {
		t.Fatalf("logs = %q, want an error with %q", logs.String(), "create SLA schedule failed")
	}
}

// TestScheduleSLAAlerts_NilScheduler: without a Scheduler client the pass
// does nothing and returns nil.
func TestScheduleSLAAlerts_NilScheduler(t *testing.T) {
	d, _, _ := slaDeps(t, "", "")
	d.Scheduler = nil

	if err := scheduleSLAAlerts(context.Background(), d); err != nil {
		t.Fatalf("scheduleSLAAlerts: %v", err)
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
