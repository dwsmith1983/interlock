package store_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// fixedClockStart is the injected "now" used by the clock tests. It is far
// from the wall clock so a stray time.Now read cannot pass by accident.
var fixedClockStart = time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

// numAttr returns the value of a DynamoDB number attribute or fails the test.
func numAttr(t *testing.T, av ddbtypes.AttributeValue, name string) string {
	t.Helper()
	n, ok := av.(*ddbtypes.AttributeValueMemberN)
	if !ok {
		t.Fatalf("%s: want number attribute, got %T", name, av)
	}
	return n.Value
}

// strAttr returns the value of a DynamoDB string attribute or fails the test.
func strAttr(t *testing.T, av ddbtypes.AttributeValue, name string) string {
	t.Helper()
	s, ok := av.(*ddbtypes.AttributeValueMemberS)
	if !ok {
		t.Fatalf("%s: want string attribute, got %T", name, av)
	}
	return s.Value
}

func TestWriteJobEvent_UsesInjectedClock(t *testing.T) {
	var put *dynamodb.PutItemInput
	fake := &storetest.FakeDynamo{
		PutItemFn: func(_ context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			put = in
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	s := storetest.NewStore(fake)
	now := fixedClockStart
	s.Now = func() time.Time { return now }

	if err := s.WriteJobEvent(context.Background(), "pipe-1", "daily", "2026-03-10", types.JobEventSuccess, "", 0, ""); err != nil {
		t.Fatalf("WriteJobEvent: %v", err)
	}
	if put == nil {
		t.Fatal("PutItem was not called")
	}

	sk := strAttr(t, put.Item["SK"], "SK")
	if !strings.HasSuffix(sk, "#1773144000000") {
		t.Errorf("SK = %q, want suffix #1773144000000", sk)
	}
	wantTTL := fmt.Sprintf("%d", int64(1773144000+30*24*3600))
	if got := numAttr(t, put.Item["ttl"], "ttl"); got != wantTTL {
		t.Errorf("ttl = %s, want %s", got, wantTTL)
	}
}

func TestWriters_UseInjectedClock(t *testing.T) {
	const lockTTL = 2 * time.Hour

	tests := []struct {
		name  string
		call  func(ctx context.Context, s *store.Store) error
		check func(t *testing.T, put *dynamodb.PutItemInput, upd *dynamodb.UpdateItemInput)
	}{
		{
			name: "WriteRerunRequest requestedAt",
			call: func(ctx context.Context, s *store.Store) error {
				return s.WriteRerunRequest(ctx, "pipe-1", "daily", "2026-03-10", "manual")
			},
			check: func(t *testing.T, put *dynamodb.PutItemInput, _ *dynamodb.UpdateItemInput) {
				want := fixedClockStart.UTC().Format(time.RFC3339)
				if got := strAttr(t, put.Item["requestedAt"], "requestedAt"); got != want {
					t.Errorf("requestedAt = %s, want %s", got, want)
				}
			},
		},
		{
			name: "WriteRerunRequest ttl",
			call: func(ctx context.Context, s *store.Store) error {
				return s.WriteRerunRequest(ctx, "pipe-1", "daily", "2026-03-10", "data-drift")
			},
			check: checkOwnRowTTL,
		},
		{
			name: "WriteSensor ttl",
			call: func(ctx context.Context, s *store.Store) error {
				return s.WriteSensor(ctx, "pipe-1", "schedule-missed-check#2026-03-10", map[string]interface{}{"alerted": "true"})
			},
			check: checkOwnRowTTL,
		},
		{
			name: "WriteSensorIfAbsent ttl",
			call: func(ctx context.Context, s *store.Store) error {
				_, err := s.WriteSensorIfAbsent(ctx, "pipe-1", "first-sensor-arrival#2026-03-10", map[string]interface{}{"arrivedAt": "x"})
				return err
			},
			check: func(t *testing.T, put *dynamodb.PutItemInput, upd *dynamodb.UpdateItemInput) {
				checkOwnRowTTL(t, put, upd)
				if got := aws.ToString(put.ConditionExpression); got != "attribute_not_exists(PK)" {
					t.Errorf("ConditionExpression = %q, want attribute_not_exists(PK)", got)
				}
			},
		},
		{
			name: "WriteDryRunMarker ttl",
			call: func(ctx context.Context, s *store.Store) error {
				_, err := s.WriteDryRunMarker(ctx, "pipe-1", "daily", "2026-03-10", fixedClockStart)
				return err
			},
			check: func(t *testing.T, put *dynamodb.PutItemInput, _ *dynamodb.UpdateItemInput) {
				want := fmt.Sprintf("%d", fixedClockStart.Add(7*24*time.Hour).Unix())
				if got := numAttr(t, put.Item["ttl"], "ttl"); got != want {
					t.Errorf("ttl = %s, want %s", got, want)
				}
			},
		},
		{
			name: "AcquireTriggerLock staleAt",
			call: func(ctx context.Context, s *store.Store) error {
				_, err := s.AcquireTriggerLock(ctx, "pipe-1", "daily", "2026-03-10", lockTTL)
				return err
			},
			check: func(t *testing.T, put *dynamodb.PutItemInput, _ *dynamodb.UpdateItemInput) {
				want := fmt.Sprintf("%d", fixedClockStart.Add(lockTTL).Unix())
				if got := numAttr(t, put.Item["staleAt"], "staleAt"); got != want {
					t.Errorf("staleAt = %s, want %s", got, want)
				}
			},
		},
		{
			name: "ResetTriggerLock staleAt",
			call: func(ctx context.Context, s *store.Store) error {
				_, err := s.ResetTriggerLock(ctx, "pipe-1", "daily", "2026-03-10", lockTTL)
				return err
			},
			check: func(t *testing.T, _ *dynamodb.PutItemInput, upd *dynamodb.UpdateItemInput) {
				if upd == nil {
					t.Fatal("UpdateItem was not called")
				}
				want := fmt.Sprintf("%d", fixedClockStart.Add(lockTTL).Unix())
				if got := numAttr(t, upd.ExpressionAttributeValues[":staleAt"], ":staleAt"); got != want {
					t.Errorf(":staleAt = %s, want %s", got, want)
				}
			},
		},
		{
			name: "WriteRerun triggeredAt and ttl",
			call: func(ctx context.Context, s *store.Store) error {
				_, err := s.WriteRerun(ctx, "pipe-1", "daily", "2026-03-10", "drift", "")
				return err
			},
			check: func(t *testing.T, put *dynamodb.PutItemInput, _ *dynamodb.UpdateItemInput) {
				var rec types.RerunRecord
				if err := attributevalue.UnmarshalMap(put.Item, &rec); err != nil {
					t.Fatalf("unmarshal rerun: %v", err)
				}
				if !rec.TriggeredAt.Equal(fixedClockStart) {
					t.Errorf("triggeredAt = %s, want %s", rec.TriggeredAt, fixedClockStart)
				}
				if want := fixedClockStart.Add(30 * 24 * time.Hour).Unix(); rec.TTL != want {
					t.Errorf("ttl = %d, want %d", rec.TTL, want)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var put *dynamodb.PutItemInput
			var upd *dynamodb.UpdateItemInput
			fake := &storetest.FakeDynamo{
				PutItemFn: func(_ context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
					put = in
					return &dynamodb.PutItemOutput{}, nil
				},
				UpdateItemFn: func(_ context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
					upd = in
					return &dynamodb.UpdateItemOutput{}, nil
				},
			}
			s := storetest.NewStore(fake)
			now := fixedClockStart
			s.Now = func() time.Time { return now }

			if err := tt.call(context.Background(), s); err != nil {
				t.Fatalf("call: %v", err)
			}
			if put == nil && upd == nil {
				t.Fatal("no write was issued")
			}
			tt.check(t, put, upd)
		})
	}
}

// checkOwnRowTTL fails the test unless the put carries the ttl of one of
// Interlock's own control rows, counted from fixedClockStart.
func checkOwnRowTTL(t *testing.T, put *dynamodb.PutItemInput, _ *dynamodb.UpdateItemInput) {
	t.Helper()
	if put == nil {
		t.Fatal("PutItem was not called")
	}
	want := fmt.Sprintf("%d", fixedClockStart.Add(validation.MaxPostRunWatchWindow+24*time.Hour).Unix())
	if got := numAttr(t, put.Item["ttl"], "ttl"); got != want {
		t.Errorf("ttl = %s, want %s", got, want)
	}
}

func TestConfigCache_UsesStoreClock(t *testing.T) {
	const cacheTTL = 5 * time.Minute

	var scans atomic.Int32
	fake := &storetest.FakeDynamo{
		ScanFn: func(_ context.Context, _ *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
			scans.Add(1)
			return &dynamodb.ScanOutput{
				Items: []map[string]ddbtypes.AttributeValue{{
					"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-1")},
					"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
					"config": &ddbtypes.AttributeValueMemberS{Value: `{"pipeline":{"id":"pipe-1"},"schedule":{"trigger":{"key":"upstream","check":"exists","cutoff":"23:59","cutoffDayOffset":7}}}`},
				}},
			}, nil
		},
	}
	s := storetest.NewStore(fake)
	now := fixedClockStart
	s.Now = func() time.Time { return now }
	cache := store.NewConfigCache(s, cacheTTL)
	ctx := context.Background()

	steps := []struct {
		name      string
		advance   time.Duration
		wantScans int32
	}{
		{name: "first Get loads", advance: 0, wantScans: 1},
		{name: "Get just inside ttl is cached", advance: cacheTTL - time.Second, wantScans: 1},
		{name: "Get past ttl reloads", advance: 2 * time.Second, wantScans: 2},
		{name: "Get right after reload is cached", advance: time.Second, wantScans: 2},
	}
	for _, st := range steps {
		now = now.Add(st.advance)
		cfg, err := cache.Get(ctx, "pipe-1")
		if err != nil {
			t.Fatalf("%s: Get: %v", st.name, err)
		}
		if cfg == nil {
			t.Fatalf("%s: Get returned nil config", st.name)
		}
		if got := scans.Load(); got != st.wantScans {
			t.Errorf("%s: scans = %d, want %d", st.name, got, st.wantScans)
		}
	}
}
