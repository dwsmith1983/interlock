package watchdog

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestRelativeSLA: with the first sensor at 10:00, maxDuration 2h and
// expectedDuration 30m, the watchdog publishes RELATIVE_SLA_WARNING once from
// 11:30 while the run is unfinished and RELATIVE_SLA_BREACH once from 12:00.
// An arrival late on slaTestDate is still checked on the next day.
func TestRelativeSLA(t *testing.T) {
	const (
		warnKey   = "relative-sla-warning-check#" + slaTestDate
		breachKey = "relative-sla-breach-check#" + slaTestDate
	)
	at := func(hour, minute, second int) time.Time {
		return time.Date(2026, 3, 10, hour, minute, second, 0, time.UTC)
	}
	tests := []struct {
		name        string
		expected    string // sla.expectedDuration
		trigger     string
		jobRow      string // event of a job row for slaTestDate
		noArrival   bool
		arrivedAt   string // "" means 2026-03-10T10:00:00Z
		dryRun      bool
		failPublish bool
		markers     []string // present before the pass
		now         time.Time
		warningAt   string // of a RELATIVE_SLA_WARNING; "" means 11:30:00Z
		wantEvents  []string
		wantMarkers []string
	}{
		{name: "before the warning", expected: "30m", now: at(11, 29, 59)},
		{name: "at the warning", expected: "30m", now: at(11, 30, 0),
			wantEvents: []string{string(types.EventRelativeSLAWarning)}, wantMarkers: []string{warnKey}},
		{name: "warning deduplicated", expected: "30m", markers: []string{warnKey}, now: at(11, 35, 0),
			wantMarkers: []string{warnKey}},
		{name: "at the breach", expected: "30m", markers: []string{warnKey}, now: at(12, 0, 0),
			wantEvents: []string{string(types.EventRelativeSLABreach)}, wantMarkers: []string{warnKey, breachKey}},
		{name: "first seen after the breach", expected: "30m", now: at(12, 10, 0),
			wantEvents: []string{string(types.EventRelativeSLABreach)}, wantMarkers: []string{breachKey}},
		{name: "default warning offset", now: at(11, 30, 0),
			wantEvents: []string{string(types.EventRelativeSLAWarning)}, wantMarkers: []string{warnKey}},
		{name: "expectedDuration 1h", expected: "1h", now: at(11, 0, 0), warningAt: "2026-03-10T11:00:00Z",
			wantEvents: []string{string(types.EventRelativeSLAWarning)}, wantMarkers: []string{warnKey}},
		{name: "finished run", expected: "30m", trigger: types.TriggerStatusCompleted, now: at(11, 30, 0)},
		{name: "rerun of an exhausted date", expected: "30m", trigger: types.TriggerStatusRunning, jobRow: types.JobEventInfraTriggerExhausted,
			now: at(12, 0, 0), wantEvents: []string{string(types.EventRelativeSLABreach)}, wantMarkers: []string{breachKey}},
		{name: "warning publish fails", expected: "30m", failPublish: true, now: at(11, 30, 0)},
		{name: "breach publish fails", expected: "30m", markers: []string{warnKey}, failPublish: true, now: at(12, 0, 0),
			wantMarkers: []string{warnKey}},
		{name: "no first-sensor-arrival", expected: "30m", noArrival: true, now: at(11, 30, 0)},
		{name: "arrival the day before, breached", expected: "30m", arrivedAt: "2026-03-10T23:00:00Z",
			now: time.Date(2026, 3, 11, 3, 0, 0, 0, time.UTC), wantEvents: []string{string(types.EventRelativeSLABreach)}, wantMarkers: []string{breachKey}},
		{name: "arrival the day before, not yet warned", expected: "30m", arrivedAt: "2026-03-10T23:00:00Z",
			now: time.Date(2026, 3, 11, 0, 29, 59, 0, time.UTC)},
		{name: "dry run", expected: "30m", dryRun: true, now: at(12, 10, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, rec := slaDeps(t, tt.trigger, tt.jobRow, func(c *types.PipelineConfig) {
				c.SLA = &types.SLAConfig{MaxDuration: "2h", ExpectedDuration: tt.expected}
				if tt.dryRun {
					c.DryRun, c.Job.Type, c.Job.Config = true, types.TriggerHTTP, httpJobConfig
				}
			})
			now := tt.now
			d.NowFunc = func() time.Time { return now }
			ctx := context.Background()
			if !tt.noArrival {
				if err := d.Store.WriteSensor(ctx, "p", "first-sensor-arrival#"+slaTestDate,
					map[string]interface{}{"arrivedAt": cmp.Or(tt.arrivedAt, "2026-03-10T10:00:00Z")}); err != nil {
					t.Fatalf("write arrival: %v", err)
				}
			}
			for _, key := range tt.markers {
				if err := d.Store.WriteSensor(ctx, "p", key, map[string]interface{}{"alerted": "true"}); err != nil {
					t.Fatalf("write marker %s: %v", key, err)
				}
			}
			if tt.failPublish {
				rec.failEvents = 1
			}

			if err := detectRelativeSLABreaches(ctx, d); err != nil {
				t.Fatalf("detectRelativeSLABreaches: %v", err)
			}

			if !slices.Equal(rec.events, tt.wantEvents) {
				t.Fatalf("events = %v, want %v", rec.events, tt.wantEvents)
			}
			for i, evt := range rec.details {
				if evt.Date != slaTestDate {
					t.Errorf("%s on %q, want %q", rec.events[i], evt.Date, slaTestDate)
				}
				if rec.events[i] != string(types.EventRelativeSLAWarning) {
					continue
				}
				want := map[string]interface{}{
					"source":           "watchdog",
					"maxDuration":      "2h",
					"expectedDuration": tt.expected,
					"sensorArrivalAt":  "2026-03-10T10:00:00Z",
					"warningAt":        cmp.Or(tt.warningAt, "2026-03-10T11:30:00Z"),
					"breachAt":         "2026-03-10T12:00:00Z",
					"actionHint":       "relative SLA at risk; the run has not finished and the breach is at 2026-03-10T12:00:00Z",
				}
				if !maps.Equal(evt.Detail, want) {
					t.Errorf("warning detail = %v, want %v", evt.Detail, want)
				}
				if wantMsg := "relative SLA warning for p on " + slaTestDate; evt.Message != wantMsg || evt.Date != slaTestDate {
					t.Errorf("warning message %q on %q, want %q on %q", evt.Message, evt.Date, wantMsg, slaTestDate)
				}
			}
			for _, key := range []string{warnKey, breachKey} {
				present := mem.Get("control", types.PipelinePK("p"), types.SensorSK(key)) != nil
				if want := slices.Contains(tt.wantMarkers, key); present != want {
					t.Errorf("marker %s present = %v, want %v", key, present, want)
				}
			}
		})
	}
}

// TestRelativeSLARunDates: every run date from 8 days back to 2 ahead (UTC),
// hourly ones included, is checked under its own key, until 24 hours after its
// breach. maxDuration is 2h and expectedDuration 30m. Each case makes two
// passes, so an alert sent twice fails it. slaTestDate's own run is COMPLETED
// unless the case says otherwise.
func TestRelativeSLARunDates(t *testing.T) {
	const warning, breach, running = string(types.EventRelativeSLAWarning), string(types.EventRelativeSLABreach), types.TriggerStatusRunning
	at := func(day, hour, minute, second int) time.Time {
		return time.Date(2026, 3, day, hour, minute, second, 0, time.UTC)
	}
	noon := at(10, 12, 0, 0)
	tests := []struct {
		name      string
		dates     []string // suffixes of the first-sensor-arrival# rows
		arrivedAt time.Time
		trigger   string // of each of dates; "" means no trigger row
		jobRow    string // event of a job row for each of dates
		now       time.Time
		wantEvent string
		wantDates []string // dates alerted, in order; none means no events and no markers
		noReads   bool     // the pass reads no single row
		malformed int      // dates skipped with one warning each per pass
	}{
		{name: "hourly date, breached", dates: []string{"2026-03-10T07"}, arrivedAt: at(10, 7, 5, 0), trigger: running,
			now: at(10, 9, 5, 0), wantEvent: breach, wantDates: []string{"2026-03-10T07"}},
		{name: "hourly date, at the warning", dates: []string{"2026-03-10T07"}, arrivedAt: at(10, 7, 5, 0), trigger: running,
			now: at(10, 8, 35, 0), wantEvent: warning, wantDates: []string{"2026-03-10T07"}},
		{name: "hourly date, no trigger row", dates: []string{"2026-03-10T07"}, arrivedAt: at(10, 7, 5, 0),
			now: at(10, 9, 5, 0), wantEvent: breach, wantDates: []string{"2026-03-10T07"}},
		{name: "hourly date, completed", dates: []string{"2026-03-10T07"}, arrivedAt: at(10, 7, 5, 0),
			trigger: types.TriggerStatusCompleted, now: at(10, 9, 5, 0)},
		{name: "hourly date, finished job row only", dates: []string{"2026-03-10T07"}, arrivedAt: at(10, 7, 5, 0),
			jobRow: types.JobEventSuccess, now: at(10, 9, 5, 0)},
		{name: "dates up to three days old, oldest first", dates: []string{"2026-03-10T07", "2026-03-09", "2026-03-08", "2026-03-07"}, arrivedAt: at(10, 10, 0, 0),
			trigger: running, now: noon, wantEvent: breach, wantDates: []string{"2026-03-07", "2026-03-08", "2026-03-09", "2026-03-10T07"}},
		{name: "breach exactly 24h ago", dates: []string{"2026-03-09"}, arrivedAt: at(9, 10, 0, 0), trigger: running,
			now: noon, wantEvent: breach, wantDates: []string{"2026-03-09"}},
		{name: "breach 24h and a second ago", dates: []string{"2026-03-09"}, arrivedAt: at(9, 10, 0, 0), trigger: running,
			now: noon.Add(time.Second), noReads: true},
		{name: "first date of the window", dates: []string{"2026-03-02"}, arrivedAt: at(10, 10, 0, 0), trigger: running,
			now: noon, wantEvent: breach, wantDates: []string{"2026-03-02"}},
		{name: "before the window", dates: []string{"2026-03-01T23"}, arrivedAt: at(10, 10, 0, 0), trigger: running,
			now: noon, noReads: true},
		{name: "last hour of the window", dates: []string{"2026-03-12T23"}, arrivedAt: at(10, 10, 0, 0), trigger: running,
			now: noon, wantEvent: breach, wantDates: []string{"2026-03-12T23"}},
		{name: "after the window", dates: []string{"2026-03-13"}, arrivedAt: at(10, 10, 0, 0), trigger: running,
			now: noon, noReads: true},
		{name: "window in UTC, clock east of it", dates: []string{"2026-03-02", "2026-03-13"}, arrivedAt: at(10, 10, 0, 0), trigger: running,
			now: noon.In(time.FixedZone("", 14*3600)), wantEvent: breach, wantDates: []string{"2026-03-02"}},
		{name: "malformed date keys", dates: []string{"2026-03-10x", "2026-03-10T7", "2026-03-11"}, arrivedAt: at(10, 10, 0, 0), trigger: running,
			now: noon, wantEvent: breach, wantDates: []string{"2026-03-11"}, malformed: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, rec := slaDeps(t, types.TriggerStatusCompleted, "", func(c *types.PipelineConfig) {
				c.SLA = &types.SLAConfig{MaxDuration: "2h", ExpectedDuration: "30m"}
			})
			now := tt.now
			d.NowFunc = func() time.Time { return now }
			var logs bytes.Buffer
			d.Logger = slog.New(slog.NewTextHandler(&logs, nil))
			ctx, pk := context.Background(), types.PipelinePK("p")
			for _, date := range tt.dates {
				if err := d.Store.WriteSensor(ctx, "p", "first-sensor-arrival#"+date,
					map[string]interface{}{"arrivedAt": tt.arrivedAt.Format(time.RFC3339)}); err != nil {
					t.Fatalf("write arrival: %v", err)
				}
				if tt.trigger != "" {
					mem.Put("control", sItem("PK", pk, "SK", types.TriggerSK("stream", date), "status", tt.trigger))
				}
				if tt.jobRow != "" {
					mem.Put("joblog", sItem("PK", pk, "SK", types.JobSK("stream", date, "1773129600000"), "event", tt.jobRow))
				}
			}
			fake := d.Store.Client.(*storetest.FakeDynamo)
			reads, getItem := 0, fake.GetItemFn
			fake.GetItemFn = func(ctx context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
				reads++
				return getItem(ctx, in)
			}

			for pass := 1; pass <= 2; pass++ {
				if err := detectRelativeSLABreaches(ctx, d); err != nil {
					t.Fatalf("detectRelativeSLABreaches, pass %d: %v", pass, err)
				}
			}

			var got, want, gotMarkers, wantMarkers []string
			for i, evt := range rec.details {
				got = append(got, rec.events[i]+" "+evt.Date)
			}
			for _, date := range tt.wantDates {
				want = append(want, tt.wantEvent+" "+date)
				wantMarkers = append(wantMarkers, types.SensorSK(
					map[string]string{warning: "relative-sla-warning-check#", breach: "relative-sla-breach-check#"}[tt.wantEvent]+date))
			}
			for _, item := range mem.Items("control") {
				if sk := item["SK"].(*ddbtypes.AttributeValueMemberS).Value; strings.HasPrefix(sk, types.SensorSK("relative-sla-")) {
					gotMarkers = append(gotMarkers, sk)
				}
			}
			if !slices.Equal(got, want) {
				t.Errorf("events = %v, want %v", got, want)
			}
			if !slices.Equal(gotMarkers, wantMarkers) {
				t.Errorf("markers = %v, want %v", gotMarkers, wantMarkers)
			}
			if tt.noReads && reads != 0 {
				t.Errorf("GetItem calls = %d, want none", reads)
			}
			if n := strings.Count(logs.String(), `level=WARN msg="skipping malformed first-sensor-arrival key"`); n != 2*tt.malformed {
				t.Errorf("malformed-key warnings in two passes = %d, want %d: %s", n, 2*tt.malformed, logs.String())
			}
		})
	}
}

// TestRelativeSLAQueryError: a pipeline whose arrival query fails is logged,
// and the other pipeline is still checked. The first query of the pass fails,
// whichever pipeline it is for.
func TestRelativeSLAQueryError(t *testing.T) {
	d, mem, rec := slaDeps(t, types.TriggerStatusRunning, "")
	var logs bytes.Buffer
	d.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	ctx, pk := context.Background(), types.PipelinePK("q")
	mem.Put("control", sItem("PK", pk, "SK", types.ConfigSK, "config",
		`{"pipeline":{"id":"q"},"schedule":{"trigger":{"key":"upstream","check":"exists","cutoff":"23:59"}},"sla":{"maxDuration":"1h"}}`))
	mem.Put("control", sItem("PK", pk, "SK", types.TriggerSK("stream", slaTestDate), "status", types.TriggerStatusRunning))
	for _, id := range []string{"p", "q"} {
		if err := d.Store.WriteSensor(ctx, id, "first-sensor-arrival#"+slaTestDate,
			map[string]interface{}{"arrivedAt": "2026-03-10T07:00:00Z"}); err != nil {
			t.Fatalf("write arrival: %v", err)
		}
	}
	fake := d.Store.Client.(*storetest.FakeDynamo)
	failed, query := "", fake.QueryFn
	fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if aws.ToString(in.TableName) == "control" && failed == "" {
			failed = strings.TrimPrefix(in.ExpressionAttributeValues[":pk"].(*ddbtypes.AttributeValueMemberS).Value, types.PipelinePK(""))
			return nil, errors.New("throttled")
		}
		return query(ctx, in)
	}

	if err := detectRelativeSLABreaches(ctx, d); err != nil {
		t.Fatalf("detectRelativeSLABreaches: %v", err)
	}

	want := map[string]string{"p": "q", "q": "p"}[failed]
	if len(rec.details) != 1 || rec.events[0] != string(types.EventRelativeSLABreach) || rec.details[0].PipelineID != want {
		t.Errorf("events = %v for %+v, want one breach for %q (%q failed)", rec.events, rec.details, want, failed)
	}
	if line := `level=ERROR msg="first-sensor-arrival lookup failed" pipelineId=` + failed; !strings.Contains(logs.String(), line) {
		t.Errorf("logs = %q, want %q", logs.String(), line)
	}
}
