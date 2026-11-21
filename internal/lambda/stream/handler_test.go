package stream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// discardLogger keeps test output readable; every routing branch logs.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestHandleStreamEvent_BatchFailureUsesSequenceNumber pins the AWS
// ReportBatchItemFailures contract: Lambda matches each returned
// ItemIdentifier against the stream record's SequenceNumber. An EventID is not
// a recognised identifier, so Lambda treats the whole batch as failed and
// re-drives records whose side effects (WriteJobEvent, PublishEvent) are not
// idempotent.
func TestHandleStreamEvent_BatchFailureUsesSequenceNumber(t *testing.T) {
	s := storetest.NewStore(&storetest.FakeDynamo{})
	d := &lambda.Deps{
		Store:       s,
		ConfigCache: store.NewConfigCache(s, 5*time.Minute),
		Logger:      discardLogger(),
	}

	// No Keys at all -> handleRecord returns "record missing PK or SK".
	bad := events.DynamoDBEventRecord{
		EventID:   "evt-1",
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			SequenceNumber: "111",
			Keys:           map[string]events.DynamoDBAttributeValue{},
		},
	}
	// An unrouted SK prefix hits the default branch and returns nil without
	// touching the store.
	good := events.DynamoDBEventRecord{
		EventID:   "evt-2",
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			SequenceNumber: "222",
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK("p")),
				"SK": events.NewStringAttribute("UNROUTED#1"),
			},
		},
	}

	resp, err := HandleStreamEvent(context.Background(), d, lambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{bad, good},
	})
	if err != nil {
		t.Fatalf("HandleStreamEvent returned error: %v", err)
	}
	if len(resp.BatchItemFailures) != 1 {
		t.Fatalf("BatchItemFailures = %+v, want exactly 1 (only the keyless record)", resp.BatchItemFailures)
	}
	if got := resp.BatchItemFailures[0].ItemIdentifier; got != "111" {
		t.Errorf("ItemIdentifier = %q, want %q (the record SequenceNumber, not the EventID)", got, "111")
	}
}

// testDate is the execution date every REMOVE-guard case uses. It matches
// testNow so ResolveExecutionDate derives the same date from an absent
// NewImage (REMOVE records carry no NewImage).
const testDate = "2026-03-07"

func testNow() time.Time {
	return time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC)
}

// countingSFN is a lambda.SFNAPI double that counts started executions and
// keeps each one's input. While fail > 0, StartExecution fails without
// counting and counts fail down, so calls always means "started".
type countingSFN struct {
	calls  int
	fail   int
	inputs []string
}

func (c *countingSFN) StartExecution(_ context.Context, in *sfn.StartExecutionInput, _ ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	if c.fail > 0 {
		c.fail--
		return nil, errors.New("start execution failed")
	}
	c.calls++
	c.inputs = append(c.inputs, *in.Input)
	return &sfn.StartExecutionOutput{}, nil
}

// countingEventBridge is a lambda.EventBridgeAPI double that records PutEvents
// calls, so a test can prove no verdict was published.
type countingEventBridge struct{ calls int }

func (c *countingEventBridge) PutEvents(context.Context, *eventbridge.PutEventsInput, ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	c.calls++
	return &eventbridge.PutEventsOutput{}, nil
}

// removeGuardConfig is a pipeline with both a stream trigger and post-run
// rules, so a REMOVE on any routed SK prefix reaches a handler that would
// write or publish if the guard were missing.
func removeGuardConfig() types.PipelineConfig {
	return types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists},
		},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}},
		},
	}
}

// configScanItem builds the CONFIG row that store.ScanConfigs (and therefore
// ConfigCache) expects for pipeline "p".
func configScanItem(t *testing.T) map[string]ddbtypes.AttributeValue {
	t.Helper()
	data, err := json.Marshal(removeGuardConfig())
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	return map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	}
}

// newNoWriteFake serves the CONFIG row on Scan and, on GetItem, a COMPLETED
// TRIGGER row inside its post-run watch window at testNow (so a SENSOR#
// record reaches the post-run verdict path), and fails the test if the
// router issues any write.
func newNoWriteFake(t *testing.T) *storetest.FakeDynamo {
	t.Helper()
	return &storetest.FakeDynamo{
		ScanFn: func(context.Context, *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
			return &dynamodb.ScanOutput{
				Items: []map[string]ddbtypes.AttributeValue{configScanItem(t)},
			}, nil
		},
		GetItemFn: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			if skAttr, ok := in.Key["SK"].(*ddbtypes.AttributeValueMemberS); ok && strings.HasPrefix(skAttr.Value, "TRIGGER#") {
				return &dynamodb.GetItemOutput{Item: completedRow(testDate, "2026-03-07T11:00:00Z")}, nil
			}
			return &dynamodb.GetItemOutput{}, nil
		},
		PutItemFn: func(_ context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Errorf("PutItem called, want no writes: %v", in.Item)
			return &dynamodb.PutItemOutput{}, nil
		},
		UpdateItemFn: func(_ context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
			t.Errorf("UpdateItem called, want no writes: %v", in.Key)
			return &dynamodb.UpdateItemOutput{}, nil
		},
		DeleteItemFn: func(_ context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			t.Errorf("DeleteItem called, want no writes: %v", in.Key)
			return &dynamodb.DeleteItemOutput{}, nil
		},
	}
}

// TestHandleRecord_SkipsRemoveRecords pins H11: a deleted row carries no
// NewImage, so routing it makes downstream handlers act on absent data. A
// deleted RERUN_REQUEST# row starts a rerun with reason "manual"; a deleted
// SENSOR# row publishes a POST_RUN_* verdict. TTL expiries (30-day JOB# rows,
// trigger-lock rows) arrive as REMOVE with a Service user identity.
func TestHandleRecord_SkipsRemoveRecords(t *testing.T) {
	ttlIdentity := &events.DynamoDBUserIdentity{
		Type:        "Service",
		PrincipalID: "dynamodb.amazonaws.com",
	}

	tests := []struct {
		name         string
		sk           string
		userIdentity *events.DynamoDBUserIdentity
	}{
		{name: "operator deletes a rerun request", sk: types.RerunRequestSK("stream", testDate)},
		{name: "sensor row deleted", sk: types.SensorSK("row-count")},
		{name: "job row deleted", sk: types.JobSK("stream", testDate, "1772000000000")},
		{name: "job row expired by ttl", sk: types.JobSK("stream", testDate, "1772000000000"), userIdentity: ttlIdentity},
		{name: "trigger row expired by ttl", sk: types.TriggerSK("stream", testDate), userIdentity: ttlIdentity},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := storetest.NewStore(newNoWriteFake(t))
			sfnFake := &countingSFN{}
			ebFake := &countingEventBridge{}
			d := &lambda.Deps{
				Store:        s,
				ConfigCache:  store.NewConfigCache(s, 5*time.Minute),
				SFNClient:    sfnFake,
				EventBridge:  ebFake,
				EventBusName: "test-bus",
				NowFunc:      testNow,
				Logger:       discardLogger(),
			}

			record := events.DynamoDBEventRecord{
				EventID:      "evt-remove",
				EventName:    "REMOVE",
				UserIdentity: tt.userIdentity,
				Change: events.DynamoDBStreamRecord{
					SequenceNumber: "900",
					Keys: map[string]events.DynamoDBAttributeValue{
						"PK": events.NewStringAttribute(types.PipelinePK("p")),
						"SK": events.NewStringAttribute(tt.sk),
					},
					OldImage: map[string]events.DynamoDBAttributeValue{
						"reason": events.NewStringAttribute("data-drift"),
					},
				},
			}

			if err := handleRecord(context.Background(), d, record); err != nil {
				t.Fatalf("handleRecord returned error: %v", err)
			}
			if sfnFake.calls != 0 {
				t.Errorf("StartExecution called %d times, want 0 -- a REMOVE must never start a run", sfnFake.calls)
			}
			if ebFake.calls != 0 {
				t.Errorf("PutEvents called %d times, want 0 -- a REMOVE must never publish a verdict", ebFake.calls)
			}
		})
	}
}

// TestHandleRecord_RunRowIsIgnored: a RUN# row (an expected run date the
// user writes) is read by the watchdog only; inserting one starts nothing,
// publishes nothing and writes nothing.
func TestHandleRecord_RunRowIsIgnored(t *testing.T) {
	s := storetest.NewStore(newNoWriteFake(t))
	sfnFake, ebFake := &countingSFN{}, &countingEventBridge{}
	d := &lambda.Deps{
		Store: s, ConfigCache: store.NewConfigCache(s, 5*time.Minute), SFNClient: sfnFake,
		EventBridge: ebFake, EventBusName: "test-bus", NowFunc: testNow, Logger: discardLogger(),
	}
	record := events.DynamoDBEventRecord{
		EventID:   "evt-run",
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			SequenceNumber: "901",
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK("p")),
				"SK": events.NewStringAttribute(types.RunSK(testDate)),
			},
		},
	}

	if err := handleRecord(context.Background(), d, record); err != nil {
		t.Fatalf("handleRecord returned error: %v", err)
	}
	if sfnFake.calls != 0 || ebFake.calls != 0 {
		t.Errorf("StartExecution calls = %d, PutEvents calls = %d, want 0 and 0", sfnFake.calls, ebFake.calls)
	}
}

// TestIsTTLExpiry pins the UserIdentity shape DynamoDB's TTL deleter sets on
// records it removes itself, versus an operator- or application-driven
// delete (nil UserIdentity, or a non-Service identity such as an assumed
// role).
func TestIsTTLExpiry(t *testing.T) {
	tests := []struct {
		name         string
		userIdentity *events.DynamoDBUserIdentity
		want         bool
	}{
		{name: "nil user identity", userIdentity: nil, want: false},
		{
			name: "ttl service identity",
			userIdentity: &events.DynamoDBUserIdentity{
				Type:        "Service",
				PrincipalID: "dynamodb.amazonaws.com",
			},
			want: true,
		},
		{
			name:         "assumed role identity",
			userIdentity: &events.DynamoDBUserIdentity{Type: "AssumedRole"},
			want:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := events.DynamoDBEventRecord{UserIdentity: tt.userIdentity}
			if got := isTTLExpiry(record); got != tt.want {
				t.Errorf("isTTLExpiry() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestHandleRecord_RemoveWithoutKeysStillErrors pins the order of operations
// in handleRecord: key extraction happens before the REMOVE guard, so a
// REMOVE record with no PK/SK still returns the "missing PK or SK" error
// instead of being silently skipped.
func TestHandleRecord_RemoveWithoutKeysStillErrors(t *testing.T) {
	d := &lambda.Deps{Logger: discardLogger()}
	record := events.DynamoDBEventRecord{
		EventID:   "evt-remove-no-keys",
		EventName: "REMOVE",
		Change: events.DynamoDBStreamRecord{
			SequenceNumber: "1",
			Keys:           map[string]events.DynamoDBAttributeValue{},
		},
	}

	err := handleRecord(context.Background(), d, record)
	if err == nil {
		t.Fatal("handleRecord returned nil error, want an error for missing PK or SK")
	}
	if !strings.Contains(err.Error(), "missing PK or SK") {
		t.Errorf("handleRecord error = %q, want it to contain %q", err.Error(), "missing PK or SK")
	}
}

// TestHandleRecord_ConfigChangesInvalidateCache pins the one delete the router
// must still act on: a CONFIG row that disappears has to drop out of the
// cache, otherwise the router keeps triggering a pipeline whose config no
// longer exists for up to the cache TTL. INSERT and MODIFY must keep working.
// ConfigCache exposes no state, so invalidation is observed through the extra
// ScanConfigs call a stale cache would have skipped.
func TestHandleRecord_ConfigChangesInvalidateCache(t *testing.T) {
	tests := []struct {
		name      string
		eventName string
	}{
		{name: "config inserted", eventName: "INSERT"},
		{name: "config modified", eventName: "MODIFY"},
		{name: "config deleted", eventName: "REMOVE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scans := 0
			fake := &storetest.FakeDynamo{
				ScanFn: func(context.Context, *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
					scans++
					return &dynamodb.ScanOutput{
						Items: []map[string]ddbtypes.AttributeValue{configScanItem(t)},
					}, nil
				},
			}
			s := storetest.NewStore(fake)
			cache := store.NewConfigCache(s, 5*time.Minute)
			d := &lambda.Deps{Store: s, ConfigCache: cache, Logger: discardLogger()}

			// Prime the cache: a second GetAll is served from memory unless
			// the stream record invalidates it.
			if _, err := cache.GetAll(context.Background()); err != nil {
				t.Fatalf("prime cache: %v", err)
			}
			if scans != 1 {
				t.Fatalf("scans after priming = %d, want 1", scans)
			}

			record := events.DynamoDBEventRecord{
				EventID:   "evt-config",
				EventName: tt.eventName,
				Change: events.DynamoDBStreamRecord{
					SequenceNumber: "800",
					Keys: map[string]events.DynamoDBAttributeValue{
						"PK": events.NewStringAttribute(types.PipelinePK("p")),
						"SK": events.NewStringAttribute(types.ConfigSK),
					},
				},
			}
			if err := handleRecord(context.Background(), d, record); err != nil {
				t.Fatalf("handleRecord returned error: %v", err)
			}

			if _, err := cache.GetAll(context.Background()); err != nil {
				t.Fatalf("second GetAll: %v", err)
			}
			if scans != 2 {
				t.Errorf("scans after a %s CONFIG record = %d, want 2 -- the cache must be invalidated", tt.eventName, scans)
			}
		})
	}
}

// TestHandleRecord_SensorBesideBadConfig: another team's bad CONFIG row,
// malformed or failing validation, must not stop a good pipeline's trigger.
func TestHandleRecord_SensorBesideBadConfig(t *testing.T) {
	for name, sibling := range map[string]string{
		"JSON type mismatch": `{"pipeline":{"id":"q"},"job":{"maxRetries":"3"}}`,
		"fails validation":   `{"pipeline":{"id":"q"},"job":{"maxRetries":11}}`,
	} {
		t.Run(name, func(t *testing.T) {
			d, mem, sfnFake, _ := jobWorld(t, types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
			})
			mem.Put("control", map[string]ddbtypes.AttributeValue{
				"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("q")},
				"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
				"config": &ddbtypes.AttributeValueMemberS{Value: sibling},
			})
			record := events.DynamoDBEventRecord{
				EventName: "INSERT",
				Change: events.DynamoDBStreamRecord{
					SequenceNumber: "1",
					Keys: map[string]events.DynamoDBAttributeValue{
						"PK": events.NewStringAttribute(types.PipelinePK("p")),
						"SK": events.NewStringAttribute(types.SensorSK("upstream")),
					},
					NewImage: map[string]events.DynamoDBAttributeValue{
						"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{"status": events.NewStringAttribute("ready")}),
					},
				},
			}

			if err := handleRecord(context.Background(), d, record); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}
			if sfnFake.calls != 1 {
				t.Fatalf("StartExecution calls = %d, want 1", sfnFake.calls)
			}
		})
	}
}

// TestHandleRecord_MalformedKeys: a PK without the PIPELINE# prefix, or a JOB#
// SK without a date, is an error for every routed SK prefix. Each one fails
// before the config is read, so the Deps hold only a logger. A JOB# row with
// no event attribute is dropped before its SK is read, since a retry cannot
// fix it.
func TestHandleRecord_MalformedKeys(t *testing.T) {
	tests := []struct {
		name, pk, sk, wantErr string // wantErr empty means no error
		noEvent               bool
	}{
		{name: "sensor bad PK", pk: "BADFORMAT#p", sk: types.SensorSK("upstream"), wantErr: "unexpected PK format"},
		{name: "job bad PK", pk: "BADFORMAT#p", sk: types.JobSK("stream", testDate, "1772000000000"), wantErr: "unexpected PK format"},
		{name: "rerun bad PK", pk: "BADFORMAT#p", sk: types.RerunRequestSK("stream", testDate), wantErr: "unexpected PK format"},
		{name: "job SK without date", pk: types.PipelinePK("p"), sk: "JOB#malformed", wantErr: "invalid JOB SK format"},
		{name: "job SK without date, no event attribute", pk: types.PipelinePK("p"), sk: "JOB#malformed", noEvent: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &lambda.Deps{Logger: discardLogger()}
			record := events.DynamoDBEventRecord{EventName: "INSERT", Change: events.DynamoDBStreamRecord{
				SequenceNumber: "1",
				Keys: map[string]events.DynamoDBAttributeValue{
					"PK": events.NewStringAttribute(tt.pk),
					"SK": events.NewStringAttribute(tt.sk),
				},
				NewImage: map[string]events.DynamoDBAttributeValue{"event": events.NewStringAttribute(types.JobEventFail)},
			}}
			if tt.noEvent {
				delete(record.Change.NewImage, "event")
			}

			err := handleRecord(context.Background(), d, record)
			if (err != nil) != (tt.wantErr != "") || err != nil && !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("handleRecord error = %v, want one containing %q", err, tt.wantErr)
			}
		})
	}
}
