package storetest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
)

func TestFakeDynamo_DefaultsReturnEmptyResponses(t *testing.T) {
	f := &storetest.FakeDynamo{}
	ctx := context.Background()

	out, err := f.GetItem(ctx, &dynamodb.GetItemInput{})
	if err != nil || out.Item != nil {
		t.Errorf("GetItem = (%v, %v), want (empty, nil)", out, err)
	}
	q, err := f.Query(ctx, &dynamodb.QueryInput{})
	if err != nil || len(q.Items) != 0 {
		t.Errorf("Query = (%v, %v), want (empty, nil)", q, err)
	}
	if _, err := f.PutItem(ctx, &dynamodb.PutItemInput{}); err != nil {
		t.Errorf("PutItem err = %v, want nil", err)
	}
	if _, err := f.UpdateItem(ctx, &dynamodb.UpdateItemInput{}); err != nil {
		t.Errorf("UpdateItem err = %v, want nil", err)
	}
	if _, err := f.DeleteItem(ctx, &dynamodb.DeleteItemInput{}); err != nil {
		t.Errorf("DeleteItem err = %v, want nil", err)
	}
	if _, err := f.Scan(ctx, &dynamodb.ScanInput{}); err != nil {
		t.Errorf("Scan err = %v, want nil", err)
	}
}

func TestFakeDynamo_HooksAreInvoked(t *testing.T) {
	want := errors.New("dynamodb: internal error")
	f := &storetest.FakeDynamo{
		GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return nil, want
		},
		QueryFn: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]ddbtypes.AttributeValue{
					{"SK": &ddbtypes.AttributeValueMemberS{Value: "JOB#daily#2026-03-01#1"}},
				},
			}, nil
		},
	}

	if _, err := f.GetItem(context.Background(), &dynamodb.GetItemInput{}); !errors.Is(err, want) {
		t.Errorf("GetItem err = %v, want %v", err, want)
	}
	q, err := f.Query(context.Background(), &dynamodb.QueryInput{})
	if err != nil || len(q.Items) != 1 {
		t.Errorf("Query = (%v, %v), want 1 item", q, err)
	}
}

func TestNewStore_SetsTableNames(t *testing.T) {
	s := storetest.NewStore(&storetest.FakeDynamo{})
	if s.ControlTable != "control" || s.JobLogTable != "joblog" || s.RerunTable != "rerun" || s.EventsTable != "events" {
		t.Errorf("NewStore tables = %q/%q/%q/%q", s.ControlTable, s.JobLogTable, s.RerunTable, s.EventsTable)
	}
}
