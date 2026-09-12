package stream

import (
	"context"
	"encoding/json"
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

// countingSFN is a lambda.SFNAPI double that records StartExecution calls, so
// a test can prove no run was started.
type countingSFN struct{ calls int }

func (c *countingSFN) StartExecution(context.Context, *sfn.StartExecutionInput, ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	c.calls++
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

// completedTriggerItem is a COMPLETED TRIGGER# row, the state that sends a
// SENSOR# record down the post-run verdict path.
func completedTriggerItem() map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", testDate)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusCompleted},
	}
}

// newNoWriteFake serves the CONFIG row on Scan and a COMPLETED TRIGGER row on
// GetItem, and fails the test if the router issues any write.
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
				return &dynamodb.GetItemOutput{Item: completedTriggerItem()}, nil
			}
			return &dynamodb.GetItemOutput{}, nil
		},
		PutItemFn: func(_ context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Errorf("PutItem called for a REMOVE record: %v", in.Item)
			return &dynamodb.PutItemOutput{}, nil
		},
		UpdateItemFn: func(_ context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
			t.Errorf("UpdateItem called for a REMOVE record: %v", in.Key)
			return &dynamodb.UpdateItemOutput{}, nil
		},
		DeleteItemFn: func(_ context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			t.Errorf("DeleteItem called for a REMOVE record: %v", in.Key)
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
