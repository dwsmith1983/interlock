package store_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

func sAttr(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberS{Value: v} }

// configRow is a CONFIG row with the given PK and config attribute (none when nil).
func configRow(pk string, config ddbtypes.AttributeValue) map[string]ddbtypes.AttributeValue {
	item := map[string]ddbtypes.AttributeValue{"PK": sAttr(pk), "SK": sAttr(types.ConfigSK)}
	if config != nil {
		item["config"] = config
	}
	return item
}

func shortSHA(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}

// TestScanConfigs_SkipsBadRows: one team's bad CONFIG row must not hide every
// other pipeline. Each bad row comes back as one rejection, the rest load.
func TestScanConfigs_SkipsBadRows(t *testing.T) {
	bad := []struct {
		name, pk, wantID, wantReason string
		config                       ddbtypes.AttributeValue
	}{
		{"PK without prefix", "ORPHAN", "", "invalid PK", sAttr(`{"pipeline":{"id":"x"}}`)},
		{"PK prefix only", "PIPELINE#", "", "invalid PK", sAttr(`{}`)},
		{"config missing", types.PipelinePK("no-config"), "no-config", "config attribute missing", nil},
		{"config not a string", types.PipelinePK("num-config"), "num-config", "config is not a string", &ddbtypes.AttributeValueMemberN{Value: "1"}},
		{"bad JSON", types.PipelinePK("bad-json"), "bad-json", "unmarshal config", sAttr(`{not valid`)},
		{"JSON type mismatch", types.PipelinePK("type-mismatch"), "type-mismatch", "unmarshal config", sAttr(`{"job":{"maxRetries":"3"}}`)},
	}
	fake, mem := storetest.NewMem()
	for _, id := range []string{"good-a", "good-b"} {
		mem.Put("control", configRow(types.PipelinePK(id), sAttr(`{"pipeline":{"id":"`+id+`"}}`)))
	}
	for _, b := range bad {
		mem.Put("control", configRow(b.pk, b.config))
	}

	configs, rejected, err := storetest.NewStore(fake).ScanConfigs(context.Background())
	if err != nil {
		t.Fatalf("ScanConfigs: %v", err)
	}
	if len(configs) != 2 || configs["good-a"] == nil || configs["good-b"] == nil {
		t.Fatalf("configs = %v, want good-a and good-b", configs)
	}
	if len(rejected) != len(bad) {
		t.Fatalf("rejected = %+v, want %d rows", rejected, len(bad))
	}
	for _, b := range bad {
		i := slices.IndexFunc(rejected, func(r store.RejectedConfig) bool { return r.PK == b.pk })
		if i < 0 {
			t.Errorf("%s: no rejection for PK %q", b.name, b.pk)
			continue
		}
		r, wantHash := rejected[i], ""
		if s, ok := b.config.(*ddbtypes.AttributeValueMemberS); ok {
			wantHash = shortSHA(s.Value)
		}
		if r.PipelineID != b.wantID || !strings.Contains(r.Reason, b.wantReason) || r.Hash != wantHash {
			t.Errorf("%s: rejection = %+v, want id %q, reason containing %q, hash %q", b.name, r, b.wantID, b.wantReason, wantHash)
		}
	}
}

// TestConfigCache_RejectsInvalidConfig: a config that parses but fails
// validation is rejected like a malformed one, so every Lambda sees the same
// set of pipelines.
func TestConfigCache_RejectsInvalidConfig(t *testing.T) {
	fake, mem := storetest.NewMem()
	invalid := `{"pipeline":{"id":"bad"},"job":{"maxRetries":11}}`
	mem.Put("control", configRow(types.PipelinePK("good"), sAttr(`{"pipeline":{"id":"good"}}`)))
	mem.Put("control", configRow(types.PipelinePK("bad"), sAttr(invalid)))
	cache := store.NewConfigCache(storetest.NewStore(fake), time.Minute)
	ctx := context.Background()

	all, err := cache.GetAll(ctx)
	if err != nil || len(all) != 1 || all["good"] == nil {
		t.Fatalf("GetAll = %v, %v, want only good", all, err)
	}
	if cfg, err := cache.Get(ctx, "bad"); cfg != nil || err != nil {
		t.Fatalf("Get(bad) = %v, %v, want nil, nil", cfg, err)
	}
	rejected, err := cache.Rejected(ctx)
	want := []store.RejectedConfig{{PK: types.PipelinePK("bad"), PipelineID: "bad",
		Reason: "job.maxRetries 11 out of range [0,10]", Hash: shortSHA(invalid)}}
	if err != nil || !slices.Equal(rejected, want) {
		t.Fatalf("Rejected = %+v, %v, want %+v", rejected, err, want)
	}
	rejected[0].Reason = "mutated"
	if again, _ := cache.Rejected(ctx); again[0].Reason == "mutated" {
		t.Error("Rejected returned the cached slice, want a copy")
	}
}

// TestConfigCache_LogsRejectionsAndHoldsWithNoneAccepted: each refresh warns
// once per rejected row, so a skipped config is never silent, and a cache
// that accepted nothing still holds until its TTL instead of rescanning.
func TestConfigCache_LogsRejectionsAndHoldsWithNoneAccepted(t *testing.T) {
	fake, mem := storetest.NewMem()
	mem.Put("control", configRow(types.PipelinePK("broken"), sAttr(`{`)))
	mem.Put("control", configRow("ORPHAN", nil))
	s := storetest.NewStore(fake)
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	var buf bytes.Buffer
	cache := store.NewConfigCache(s, time.Minute)
	cache.Logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			return slog.Attr{}
		}
		return a
	}}))
	const msg = `level=WARN msg="pipeline config rejected, skipping" `
	perRefresh := msg + `pk=ORPHAN pipelineId="" reason="invalid PK \"ORPHAN\"" hash=""` + "\n" +
		msg + `pk=PIPELINE#broken pipelineId=broken reason="unmarshal config: unexpected end of JSON input" hash=` + shortSHA(`{`) + "\n"

	for _, step := range []struct {
		advance   time.Duration
		refreshes int
	}{{0, 1}, {30 * time.Second, 1}, {31 * time.Second, 2}} {
		now = now.Add(step.advance)
		if all, err := cache.GetAll(context.Background()); err != nil || len(all) != 0 {
			t.Fatalf("GetAll = %v, %v, want empty", all, err)
		}
		if got, want := buf.String(), strings.Repeat(perRefresh, step.refreshes); got != want {
			t.Fatalf("after +%s: logged\n%s\nwant\n%s", step.advance, got, want)
		}
	}
}

// TestScanConfigs_CutsLongReason: a rejection reason is cut to 1 KB on a rune
// boundary, so a huge config cannot make a huge log line or alert.
func TestScanConfigs_CutsLongReason(t *testing.T) {
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "long"},
		Schedule: types.ScheduleConfig{Include: &types.InclusionConfig{Dates: []string{"x" + strings.Repeat("é", 600)}}},
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	full := strings.Join(validation.ValidatePipelineConfig(&cfg), "; ")
	if len(full) <= 1024 || utf8.RuneStart(full[1024]) {
		t.Fatalf("fixture reason is %d bytes, want over 1024 with byte 1024 inside a rune", len(full))
	}
	fake, mem := storetest.NewMem()
	mem.Put("control", configRow(types.PipelinePK("long"), sAttr(string(data))))

	_, rejected, err := storetest.NewStore(fake).ScanConfigs(context.Background())
	if err != nil || len(rejected) != 1 {
		t.Fatalf("ScanConfigs = %+v, %v, want one rejection", rejected, err)
	}
	if want := full[:1023]; rejected[0].Reason != want {
		t.Fatalf("reason = %d bytes %q..., want the first 1023 bytes (the rune boundary before 1024)", len(rejected[0].Reason), rejected[0].Reason[:40])
	}
}
