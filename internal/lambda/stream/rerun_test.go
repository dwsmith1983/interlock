package stream

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestParseRerunRequestSK: the date of a RERUN_REQUEST# row goes into the
// execution name, so only a date the system writes itself passes.
func TestParseRerunRequestSK(t *testing.T) {
	for _, tt := range []struct{ schedule, date string }{
		{"stream", "2026-03-07"},
		{"cron", "2026-03-07T23"},
	} {
		schedule, date, err := parseRerunRequestSK(types.RerunRequestSK(tt.schedule, tt.date))
		if err != nil || schedule != tt.schedule || date != tt.date {
			t.Errorf("parse(%q, %q) = %q, %q, %v", tt.schedule, tt.date, schedule, date, err)
		}
	}
	for _, sk := range []string{
		"RERUN_REQUEST#stream",                   // no date
		"RERUN_REQUEST#stream#2026-3-7",          // date not YYYY-MM-DD
		"RERUN_REQUEST#stream#2026-03-07T24",     // hour out of range
		"RERUN_REQUEST#stream#2026-03-07 manual", // trailing text
		"RERUN_REQUEST#stream#2026-03-07#extra",  // extra segment
		"RERUN_REQUEST#stream#2026-03-07T10#x",   // hourly date with extra segment
	} {
		if s, d, err := parseRerunRequestSK(sk); err == nil {
			t.Errorf("parse(%q) = %q, %q, want an error", sk, s, d)
		}
	}
}

// TestHandleRerunRequest_SkipsBadRows: a RERUN_REQUEST# row with a bad SK or
// a schedule the pipeline does not use is skipped with a warning. Returning an
// error would retry the row, which can never succeed, and stall the shard.
func TestHandleRerunRequest_SkipsBadRows(t *testing.T) {
	for _, sk := range []string{
		"RERUN_REQUEST#stream",                     // no date
		"RERUN_REQUEST#stream#2026-03-07T24",       // bad date
		types.RerunRequestSK("cron", testDate),     // the pipeline has no cron
		types.RerunRequestSK("", testDate),         // empty schedule
		types.RerunRequestSK("stream/x", testDate), // forbidden character
	} {
		t.Run(sk, func(t *testing.T) {
			fake, mem := storetest.NewMem()
			mem.Put("control", configScanItem(t))
			s, sfnFake, logs := storetest.NewStore(fake), &countingSFN{}, &bytes.Buffer{}
			d := &lambda.Deps{Store: s, ConfigCache: store.NewConfigCache(s, 5*time.Minute), SFNClient: sfnFake,
				NowFunc: testNow, Logger: slog.New(slog.NewTextHandler(logs, nil))}
			record := events.DynamoDBEventRecord{EventName: "INSERT", Change: events.DynamoDBStreamRecord{
				NewImage: map[string]events.DynamoDBAttributeValue{"reason": events.NewStringAttribute("manual")},
			}}

			if err := handleRerunRequest(context.Background(), d, types.PipelinePK("p"), sk, record); err != nil {
				t.Fatalf("handleRerunRequest = %v, want nil", err)
			}
			if sfnFake.calls != 0 {
				t.Errorf("StartExecution called %d times, want 0", sfnFake.calls)
			}
			if !strings.Contains(logs.String(), "level=WARN") {
				t.Errorf("logged %q, want a warning", logs.String())
			}
		})
	}
}
