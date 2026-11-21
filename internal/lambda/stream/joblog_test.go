package stream

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

type publishedEvent struct {
	Type, Message string
	Detail        map[string]interface{}
}

// recordingEventBridge is a lambda.EventBridgeAPI double that keeps each
// event's detail-type and detail. While fail > 0, PutEvents fails and counts
// it down.
type recordingEventBridge struct {
	events []publishedEvent
	fail   int
}

func (r *recordingEventBridge) PutEvents(_ context.Context, in *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	if r.fail > 0 {
		r.fail--
		return nil, errors.New("put events failed")
	}
	for _, e := range in.Entries {
		var evt types.InterlockEvent
		if err := json.Unmarshal([]byte(*e.Detail), &evt); err != nil {
			return nil, err
		}
		r.events = append(r.events, publishedEvent{Type: *e.DetailType, Message: evt.Message, Detail: evt.Detail})
	}
	return &eventbridge.PutEventsOutput{}, nil
}

// jobWorld is a stream-router over an in-memory store holding cfg.
func jobWorld(t *testing.T, cfg types.PipelineConfig) (*lambda.Deps, *storetest.Mem, *countingSFN, *recordingEventBridge) {
	t.Helper()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	fake, mem := storetest.NewMem()
	mem.Put("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(cfg.Pipeline.ID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	})
	s := storetest.NewStore(fake)
	sfnFake, eb := &countingSFN{}, &recordingEventBridge{}
	return &lambda.Deps{
		Store: s, ConfigCache: store.NewConfigCache(s, 5*time.Minute),
		SFNClient: sfnFake, EventBridge: eb, EventBusName: "test-bus",
		NowFunc: testNow, Logger: discardLogger(),
	}, mem, sfnFake, eb
}

// jobRecord is the stream record of a JOB# row for run-a on testDate.
func jobRecord(eventName, date, event string, category types.FailureCategory) events.DynamoDBEventRecord {
	image := map[string]events.DynamoDBAttributeValue{
		"event": events.NewStringAttribute(event),
		"runId": events.NewStringAttribute("run-a"),
		"error": events.NewStringAttribute("boom"),
	}
	if category != "" {
		image["category"] = events.NewStringAttribute(string(category))
	}
	return events.DynamoDBEventRecord{
		EventName: eventName,
		Change: events.DynamoDBStreamRecord{
			SequenceNumber: "1",
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK("p")),
				"SK": events.NewStringAttribute(types.JobSK("stream", date, "1772000000000")),
			},
			NewImage: image,
		},
	}
}

// jobFailCase is a fail or timeout row for pipeline p and what it publishes.
type jobFailCase struct {
	name       string
	event      string
	dryRun     bool
	maxRetries int
	category   types.FailureCategory
	tweak      func(*types.PipelineConfig)
	wantType   types.EventDetailType
	wantDetail map[string]interface{}
}

// runJobFailCases checks that each row starts nothing and writes nothing, and
// publishes only wantType (with wantDetail), if set.
func runJobFailCases(t *testing.T, tests []jobFailCase) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				DryRun:   tt.dryRun,
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig, MaxRetries: tt.maxRetries},
			}
			if tt.tweak != nil {
				tt.tweak(&cfg)
			}
			d, mem, sfnFake, eb := jobWorld(t, cfg)
			// A newer row of another run with another category must not decide.
			mem.Put("joblog", map[string]ddbtypes.AttributeValue{
				"PK":       &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
				"SK":       &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", testDate, "1772000000001")},
				"event":    &ddbtypes.AttributeValueMemberS{Value: types.JobEventFail},
				"category": &ddbtypes.AttributeValueMemberS{Value: string(types.FailureTransient)},
			})
			if tt.dryRun { // the marker the stream writes when a dry run would have triggered
				mem.Put("control", map[string]ddbtypes.AttributeValue{
					"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
					"SK": &ddbtypes.AttributeValueMemberS{Value: types.DryRunSK("stream", testDate)},
				})
			}
			before := [3]int{len(mem.Items("control")), len(mem.Items("rerun")), len(mem.Items("joblog"))}

			if err := handleRecord(context.Background(), d, jobRecord("INSERT", testDate, tt.event, tt.category)); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}
			if after := [3]int{len(mem.Items("control")), len(mem.Items("rerun")), len(mem.Items("joblog"))}; sfnFake.calls != 0 || after != before {
				t.Errorf("StartExecution calls %d, control/rerun/joblog rows %v then %v: want no start and no writes",
					sfnFake.calls, before, after)
			}
			if tt.wantType == "" {
				if len(eb.events) != 0 {
					t.Errorf("events = %+v, want none", eb.events)
				}
				return
			}
			if len(eb.events) != 1 || eb.events[0].Type != string(tt.wantType) || !maps.Equal(eb.events[0].Detail, tt.wantDetail) {
				t.Fatalf("events = %+v, want one %s with detail %v", eb.events, tt.wantType, tt.wantDetail)
			}
		})
	}
}

// TestStreamDryRunJobFailure: a dry-run pipeline has no execution, so the
// stream says what CompleteTrigger would decide: exclusion first, the budget
// from the row's own category, no retries so far.
func TestStreamDryRunJobFailure(t *testing.T) {
	zero := 0
	permanent := func(c *types.PipelineConfig) { c.Job.MaxCodeRetries = &zero }
	excluded := func(c *types.PipelineConfig) { c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{testDate}} }
	runJobFailCases(t, []jobFailCase{
		{name: "retries left", event: types.JobEventFail, dryRun: true, maxRetries: 1,
			wantType: types.EventDryRunWouldRetry, wantDetail: map[string]interface{}{"retries": 0.0, "maxRetries": 1.0}},
		{name: "none left", event: types.JobEventFail, dryRun: true,
			wantType: types.EventDryRunRetryExhausted, wantDetail: map[string]interface{}{"retries": 0.0, "maxRetries": 0.0}},
		{name: "the row's PERMANENT category", event: types.JobEventFail, dryRun: true, maxRetries: 2,
			category: types.FailurePermanent, tweak: permanent,
			wantType: types.EventDryRunRetryExhausted, wantDetail: map[string]interface{}{"retries": 0.0, "maxRetries": 0.0}},
		{name: "excluded date before the budget", event: types.JobEventFail, dryRun: true, tweak: excluded,
			wantType:   types.EventDryRunRetryExhausted,
			wantDetail: map[string]interface{}{"retries": 0.0, "maxRetries": 0.0, "reason": "excluded by calendar"}},
	})
}
