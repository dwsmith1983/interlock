package watchdog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func hash16(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}

// configReports returns the CONFIG_INVALID events rec saw.
func configReports(rec *slaRecorder) []types.InterlockEvent {
	var out []types.InterlockEvent
	for i, typ := range rec.events {
		if typ == string(types.EventConfigInvalid) {
			out = append(out, rec.details[i])
		}
	}
	return out
}

// runReports runs reportInvalidConfigs once and returns what it published.
func runReports(t *testing.T, d *lambda.Deps, rec *slaRecorder) []types.InterlockEvent {
	t.Helper()
	rec.events, rec.details = nil, nil
	d.ConfigCache.Invalidate()
	if err := reportInvalidConfigs(context.Background(), d); err != nil {
		t.Fatalf("reportInvalidConfigs: %v", err)
	}
	return configReports(rec)
}

// TestHandleWatchdog_ReportsInvalidConfig: a pass over one good pipeline (p)
// and one malformed config (q) does p's work, then reports q last, once per
// broken version and day.
func TestHandleWatchdog_ReportsInvalidConfig(t *testing.T) {
	d, mem, rec := slaDeps(t, "", "")
	clock := d.Now()
	d.NowFunc = func() time.Time { return clock }
	fake := d.Store.Client.(*storetest.FakeDynamo)
	memScan := fake.ScanFn
	fake.ScanFn = func(ctx context.Context, in *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
		if in.FilterExpression != nil && *in.FilterExpression != "SK = :sk" {
			return &dynamodb.ScanOutput{}, nil // the running-trigger scan: none
		}
		return memScan(ctx, in)
	}
	putQ := func(config string) {
		mem.Put("control", sItem("PK", types.PipelinePK("q"), "SK", types.ConfigSK, "config", config))
		d.ConfigCache.Invalidate()
	}
	const wantReason = "unmarshal config: json: cannot unmarshal string into Go struct field JobConfig.job.maxRetries of type int"
	putQ(`{"pipeline":{"id":"q"},"job":{"maxRetries":"3"}}`)

	pass := func(advance time.Duration, wantReports int) {
		t.Helper()
		clock = clock.Add(advance)
		rec.events, rec.details, rec.order = nil, nil, nil
		if err := HandleWatchdog(context.Background(), d); err != nil {
			t.Fatalf("HandleWatchdog: %v", err)
		}
		reports := configReports(rec)
		if len(reports) != wantReports {
			t.Fatalf("CONFIG_INVALID events = %+v (all events %v), want %d", reports, rec.events, wantReports)
		}
		for _, r := range reports {
			if r.PipelineID != "q" || r.Detail["reason"] != wantReason || len(fmt.Sprint(r.Detail["hash"])) != 16 ||
				r.Message != "config for q rejected, pipeline not running: "+wantReason {
				t.Fatalf("CONFIG_INVALID = %+v, want pipeline q, reason %q and a hash", r, wantReason)
			}
		}
	}

	pass(0, 1)
	want := []string{
		lambda.SLAScheduleName("p", "stream", slaTestDate, "warning"),
		lambda.SLAScheduleName("p", "stream", slaTestDate, "breach"),
	}
	if !slices.Equal(rec.schedules, want) || rec.order[len(rec.order)-1] != string(types.EventConfigInvalid) {
		t.Fatalf("schedules = %v, call order %v: want p's %v, then the report last", rec.schedules, rec.order, want)
	}
	pass(time.Hour, 0)
	pass(23*time.Hour+time.Second, 1) // the first marker has expired
	putQ(`{"pipeline":{"id":"q"},"job":{"maxRetries":"4"}}`)
	pass(time.Second, 1) // a new broken version reports at once
	pass(time.Second, 0)
}

// TestReportInvalidConfigs_UnparsedRows: a PK that does not parse reports under
// the raw PK, and a row with no config string is marked by its reason's hash
// and reports no hash.
func TestReportInvalidConfigs_UnparsedRows(t *testing.T) {
	d, mem, rec := slaDeps(t, "", "")
	mem.Put("control", sItem("PK", "ORPHAN", "SK", types.ConfigSK, "config", "{}"))
	mem.Put("control", sItem("PK", types.PipelinePK("noconf"), "SK", types.ConfigSK))

	got := runReports(t, d, rec)
	if len(got) != 2 {
		t.Fatalf("reports = %+v, want 2", got)
	}
	orphan, noconf := got[0], got[1] // the scan returns rows in key order
	if orphan.PipelineID != "ORPHAN" || orphan.Detail["reason"] != `invalid PK "ORPHAN"` || orphan.Detail["hash"] != hash16("{}") ||
		orphan.Message != `config for ORPHAN rejected, pipeline not running: invalid PK "ORPHAN"` {
		t.Errorf("orphan report = %+v", orphan)
	}
	if _, hasHash := noconf.Detail["hash"]; noconf.PipelineID != "noconf" || noconf.Detail["reason"] != "config attribute missing" || hasHash {
		t.Errorf("noconf report = %+v, want reason only", noconf)
	}
	if mem.Get("control", types.PipelinePK("noconf"), "CONFIG_INVALID#"+hash16("config attribute missing")) == nil {
		t.Error("no marker keyed by the reason's hash for the row without a config string")
	}
}

// TestReportInvalidConfigs_CapsReportsPerPass: at most 20 reports per pass;
// the rest get no marker and report on the next pass.
func TestReportInvalidConfigs_CapsReportsPerPass(t *testing.T) {
	d, mem, rec := slaDeps(t, "", "")
	for i := range 21 {
		mem.Put("control", sItem("PK", types.PipelinePK(fmt.Sprintf("bad-%02d", i)), "SK", types.ConfigSK, "config", "{"))
	}
	for i, want := range []int{20, 1, 0} {
		if got := runReports(t, d, rec); len(got) != want {
			t.Fatalf("pass %d: %d reports, want %d", i+1, len(got), want)
		}
	}
}

// TestReportInvalidConfigs_Errors: a failed marker put skips only that row;
// a failed publish removes the marker, so the next pass reports again.
func TestReportInvalidConfigs_Errors(t *testing.T) {
	d, mem, rec := slaDeps(t, "", "")
	mem.Put("control", sItem("PK", types.PipelinePK("a"), "SK", types.ConfigSK, "config", "{"))
	mem.Put("control", sItem("PK", types.PipelinePK("b"), "SK", types.ConfigSK, "config", "{"))
	fake := d.Store.Client.(*storetest.FakeDynamo)
	memPut, failA := fake.PutItemFn, true
	fake.PutItemFn = func(ctx context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		if pk, _ := in.Item["PK"].(*ddbtypes.AttributeValueMemberS); failA && pk.Value == types.PipelinePK("a") {
			return nil, errors.New("throttled")
		}
		return memPut(ctx, in)
	}

	if got := runReports(t, d, rec); len(got) != 1 || got[0].PipelineID != "b" {
		t.Fatalf("with a's put failing: reports = %+v, want only b", got)
	}
	failA, rec.failEvents = false, 1
	if got := runReports(t, d, rec); len(got) != 0 {
		t.Fatalf("with the publish failing: reports = %+v, want none", got)
	}
	if mem.Get("control", types.PipelinePK("a"), "CONFIG_INVALID#"+hash16("{")) != nil {
		t.Fatal("marker for a kept after its publish failed")
	}
	if got := runReports(t, d, rec); len(got) != 1 || got[0].PipelineID != "a" {
		t.Fatalf("after the failed publish: reports = %+v, want a again", got)
	}
}
