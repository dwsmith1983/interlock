package store_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

type row = map[string]ddbtypes.AttributeValue

// jobRow is a JOB# row for pipeline "p", schedule "s", date "2026-03-01".
func jobRow(millis int, event, runID string) row {
	return row{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("s", "2026-03-01", strconv.Itoa(millis))},
		"event": &ddbtypes.AttributeValueMemberS{Value: event},
		"runId": &ddbtypes.AttributeValueMemberS{Value: runID},
		"error": &ddbtypes.AttributeValueMemberS{Value: event + " of " + runID},
	}
}

func pageKey(n int) row {
	return row{"page": &ddbtypes.AttributeValueMemberN{Value: strconv.Itoa(n)}}
}

// pagedQuery serves pages[n] on the nth call, newest first, chained through
// LastEvaluatedKey, and records every input. A call whose ExclusiveStartKey
// is not the key the previous page returned, or one more call than there are
// pages, fails, so a broken hand-off cannot loop forever.
func pagedQuery(t *testing.T, pages [][]row, failOn int, failErr error, seen *[]*dynamodb.QueryInput) func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	return func(_ context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		n := len(*seen)
		*seen = append(*seen, in)
		var wantKey row
		if n > 0 {
			wantKey = pageKey(n)
		}
		if !reflect.DeepEqual(in.ExclusiveStartKey, wantKey) {
			t.Errorf("call %d ExclusiveStartKey = %v, want %v", n, in.ExclusiveStartKey, wantKey)
			return nil, errors.New("bad ExclusiveStartKey")
		}
		if n >= max(len(pages), 1) {
			t.Errorf("call %d: only %d pages", n, len(pages))
			return nil, errors.New("too many Query calls")
		}
		if n == failOn {
			return nil, failErr
		}
		out := &dynamodb.QueryOutput{}
		if n < len(pages) {
			out.Items = pages[n]
		}
		if n+1 < len(pages) {
			out.LastEvaluatedKey = pageKey(n + 1)
		}
		return out, nil
	}
}

// checkInputs asserts the query shape on every recorded call.
func checkInputs(t *testing.T, seen []*dynamodb.QueryInput) {
	t.Helper()
	for _, in := range seen {
		if *in.TableName != "joblog" {
			t.Errorf("table = %q, want joblog", *in.TableName)
		}
		if got := *in.KeyConditionExpression; got != "PK = :pk AND begins_with(SK, :prefix)" {
			t.Errorf("KeyConditionExpression = %q", got)
		}
		if in.ConsistentRead == nil || !*in.ConsistentRead {
			t.Errorf("ConsistentRead = %v, want true (CheckJob reads right after another writer)", in.ConsistentRead)
		}
		if in.ScanIndexForward == nil || *in.ScanIndexForward {
			t.Errorf("ScanIndexForward = %v, want false (newest first)", in.ScanIndexForward)
		}
		if in.Limit != nil || in.FilterExpression != nil {
			t.Errorf("Limit = %v, FilterExpression = %v, want neither", in.Limit, in.FilterExpression)
		}
		pk := in.ExpressionAttributeValues[":pk"].(*ddbtypes.AttributeValueMemberS).Value
		prefix := in.ExpressionAttributeValues[":prefix"].(*ddbtypes.AttributeValueMemberS).Value
		if pk != types.PipelinePK("p") || prefix != "JOB#s#2026-03-01#" {
			t.Errorf(":pk = %q, :prefix = %q", pk, prefix)
		}
	}
}

func TestGetJobResult(t *testing.T) {
	tests := []struct {
		name       string
		runID      string
		pages      [][]row
		wantEvent  string // "" means nil result
		wantMillis int    // the matched row's SK timestamp
		wantCalls  int
	}{
		{
			name:      "matches the run by runId",
			runID:     "run-b",
			pages:     [][]row{{jobRow(3, types.JobEventSuccess, "run-b"), jobRow(1, types.JobEventFail, "run-a")}},
			wantEvent: types.JobEventSuccess, wantMillis: 3, wantCalls: 1,
		},
		{
			name:      "ignores a newer non-terminal row of the same run",
			runID:     "run-a",
			pages:     [][]row{{jobRow(5, types.JobEventInfraTriggerFailure, "run-a"), jobRow(4, types.JobEventFail, "run-a")}},
			wantEvent: types.JobEventFail, wantMillis: 4, wantCalls: 1,
		},
		{
			name:      "ignores terminal rows of other runs",
			runID:     "run-a",
			pages:     [][]row{{jobRow(9, types.JobEventFail, "run-x"), jobRow(8, types.JobEventTimeout, "run-y")}},
			wantCalls: 1,
		},
		{
			name:      "nil when there are no rows",
			runID:     "run-a",
			pages:     nil,
			wantCalls: 1,
		},
		{
			name:  "finds a match past the first page",
			runID: "run-a",
			pages: [][]row{
				{jobRow(9, types.JobEventFail, "run-x"), jobRow(8, types.JobEventInfraTriggerFailure, "run-a")},
				{jobRow(7, types.JobEventSuccess, "run-y")},
				{jobRow(6, types.JobEventTimeout, "run-a"), jobRow(2, types.JobEventSuccess, "run-a")},
			},
			wantEvent: types.JobEventTimeout, wantMillis: 6, wantCalls: 3,
		},
		{
			name:  "stops paging at the first match",
			runID: "run-a",
			pages: [][]row{
				{jobRow(9, types.JobEventSuccess, "run-a")},
				{jobRow(1, types.JobEventFail, "run-a")},
			},
			wantEvent: types.JobEventSuccess, wantMillis: 9, wantCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seen []*dynamodb.QueryInput
			s := storetest.NewStore(&storetest.FakeDynamo{QueryFn: pagedQuery(t, tt.pages, -1, nil, &seen)})

			rec, err := s.GetJobResult(context.Background(), "p", "s", "2026-03-01", tt.runID)
			if err != nil {
				t.Fatalf("GetJobResult: %v", err)
			}

			if tt.wantEvent == "" {
				if rec != nil {
					t.Fatalf("record = %+v, want nil", rec)
				}
			} else {
				wantSK := types.JobSK("s", "2026-03-01", strconv.Itoa(tt.wantMillis))
				if rec == nil || rec.Event != tt.wantEvent || rec.SK != wantSK || rec.RunID != tt.runID ||
					rec.Error != tt.wantEvent+" of "+tt.runID {
					t.Fatalf("record = %+v, want event %q SK %q runId %q", rec, tt.wantEvent, wantSK, tt.runID)
				}
			}

			if len(seen) != tt.wantCalls {
				t.Errorf("Query calls = %d, want %d", len(seen), tt.wantCalls)
			}
			checkInputs(t, seen)
		})
	}
}

func TestGetJobResult_Errors(t *testing.T) {
	injected := errors.New("dynamodb: throttled")
	noRunID := jobRow(5, types.JobEventFail, "")
	delete(noRunID, "runId")
	badEvent := jobRow(4, types.JobEventSuccess, "run-a")
	badEvent["event"] = &ddbtypes.AttributeValueMemberM{Value: row{}} // not a string

	tests := []struct {
		name      string
		runID     string
		pages     [][]row
		failOn    int   // call index that returns injected; -1 for none
		wantIs    error // nil: only an error is required
		wantCalls int
	}{
		{name: "empty runID is rejected before querying", runID: "", pages: [][]row{{noRunID}}, failOn: -1, wantCalls: 0},
		{name: "query error on the first page", runID: "run-a", pages: [][]row{{}}, failOn: 0, wantIs: injected, wantCalls: 1},
		{
			name: "query error on a later page", runID: "run-a", failOn: 1, wantIs: injected, wantCalls: 2,
			pages: [][]row{{jobRow(9, types.JobEventFail, "run-x")}, {}},
		},
		{name: "unmarshal error", runID: "run-a", pages: [][]row{{badEvent}}, failOn: -1, wantCalls: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seen []*dynamodb.QueryInput
			s := storetest.NewStore(&storetest.FakeDynamo{QueryFn: pagedQuery(t, tt.pages, tt.failOn, injected, &seen)})

			rec, err := s.GetJobResult(context.Background(), "p", "s", "2026-03-01", tt.runID)
			if err == nil || rec != nil {
				t.Fatalf("GetJobResult = (%+v, %v), want (nil, error)", rec, err)
			}
			if tt.wantIs != nil && !errors.Is(err, tt.wantIs) {
				t.Errorf("err = %v, want it to wrap %v", err, tt.wantIs)
			}
			if tt.runID != "" && !strings.Contains(err.Error(), fmt.Sprintf("%q", tt.runID)) {
				t.Errorf("err = %v, want it to name run %q", err, tt.runID)
			}
			if len(seen) != tt.wantCalls {
				t.Errorf("Query calls = %d, want %d", len(seen), tt.wantCalls)
			}
		})
	}
}
