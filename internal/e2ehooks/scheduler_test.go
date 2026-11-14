//go:build e2e

package e2ehooks

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
)

// condDynamo is an in-memory DynamoAPI that evaluates the two condition
// expressions TableScheduler uses, keyed on the attribute that #n names.
type condDynamo struct {
	tables map[string]map[string]map[string]ddbtypes.AttributeValue
	err    error // returned by every call when set
}

func newCondDynamo() *condDynamo {
	return &condDynamo{tables: map[string]map[string]map[string]ddbtypes.AttributeValue{}}
}

func keyOf(item map[string]ddbtypes.AttributeValue, names map[string]string) (string, error) {
	attr, ok := names["#n"]
	if !ok {
		return "", errors.New("condDynamo: #n not in ExpressionAttributeNames")
	}
	s, ok := item[attr].(*ddbtypes.AttributeValueMemberS)
	if !ok {
		return "", fmt.Errorf("condDynamo: key attribute %q missing or not S", attr)
	}
	return s.Value, nil
}

func (f *condDynamo) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	if aws.ToString(in.ConditionExpression) != "attribute_not_exists(#n)" {
		return nil, fmt.Errorf("condDynamo: unexpected put condition %q", aws.ToString(in.ConditionExpression))
	}
	key, err := keyOf(in.Item, in.ExpressionAttributeNames)
	if err != nil {
		return nil, err
	}
	table := f.tables[aws.ToString(in.TableName)]
	if table == nil {
		table = map[string]map[string]ddbtypes.AttributeValue{}
		f.tables[aws.ToString(in.TableName)] = table
	}
	if _, exists := table[key]; exists {
		return nil, &ddbtypes.ConditionalCheckFailedException{Message: aws.String("conditional check failed")}
	}
	table[key] = in.Item
	return &dynamodb.PutItemOutput{}, nil
}

func (f *condDynamo) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	if aws.ToString(in.ConditionExpression) != "attribute_exists(#n)" {
		return nil, fmt.Errorf("condDynamo: unexpected delete condition %q", aws.ToString(in.ConditionExpression))
	}
	key, err := keyOf(in.Key, in.ExpressionAttributeNames)
	if err != nil {
		return nil, err
	}
	table := f.tables[aws.ToString(in.TableName)]
	if _, exists := table[key]; !exists {
		return nil, &ddbtypes.ConditionalCheckFailedException{Message: aws.String("conditional check failed")}
	}
	delete(table, key)
	return &dynamodb.DeleteItemOutput{}, nil
}

func (f *condDynamo) item(table, key string) (map[string]ddbtypes.AttributeValue, bool) {
	it, ok := f.tables[table][key]
	return it, ok
}

const testTable = "e2e-schedules"

func fullInput(name string) *scheduler.CreateScheduleInput {
	return &scheduler.CreateScheduleInput{
		Name:                       aws.String(name),
		GroupName:                  aws.String("interlock-sla"),
		ScheduleExpression:         aws.String("at(2026-03-10T12:30:00)"),
		ScheduleExpressionTimezone: aws.String("UTC"),
		State:                      schedulertypes.ScheduleStateEnabled,
		Target: &schedulertypes.Target{
			Arn:     aws.String("arn:aws:lambda:us-east-1:000000000000:function:sla-monitor"),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/scheduler"),
			Input:   aws.String(`{"mode":"fire-alert"}`),
		},
	}
}

func TestTableScheduler_ConflictOnDuplicate(t *testing.T) {
	ctx := context.Background()

	t.Run("second create with the same name conflicts", func(t *testing.T) {
		ts := NewTableScheduler(newCondDynamo(), testTable)
		if _, err := ts.CreateSchedule(ctx, fullInput("dup")); err != nil {
			t.Fatalf("first create: %v", err)
		}
		_, err := ts.CreateSchedule(ctx, fullInput("dup"))
		var conflict *schedulertypes.ConflictException
		if !errors.As(err, &conflict) {
			t.Fatalf("second create error = %v, want *ConflictException", err)
		}
		if conflict.ErrorMessage() == "" {
			t.Error("ConflictException has no message")
		}
	})

	t.Run("delete of a missing name is not found", func(t *testing.T) {
		ts := NewTableScheduler(newCondDynamo(), testTable)
		_, err := ts.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{Name: aws.String("missing")})
		var notFound *schedulertypes.ResourceNotFoundException
		if !errors.As(err, &notFound) {
			t.Fatalf("delete error = %v, want *ResourceNotFoundException", err)
		}
		if notFound.ErrorMessage() == "" {
			t.Error("ResourceNotFoundException has no message")
		}
	})

	t.Run("create stores every field then delete removes it", func(t *testing.T) {
		fake := newCondDynamo()
		ts := NewTableScheduler(fake, testTable)
		if _, err := ts.CreateSchedule(ctx, fullInput("sla-a")); err != nil {
			t.Fatalf("create: %v", err)
		}

		got, ok := fake.item(testTable, "sla-a")
		if !ok {
			t.Fatal("item not stored")
		}
		want := map[string]ddbtypes.AttributeValue{
			"name":       &ddbtypes.AttributeValueMemberS{Value: "sla-a"},
			"group":      &ddbtypes.AttributeValueMemberS{Value: "interlock-sla"},
			"expression": &ddbtypes.AttributeValueMemberS{Value: "at(2026-03-10T12:30:00)"},
			"timezone":   &ddbtypes.AttributeValueMemberS{Value: "UTC"},
			"targetArn":  &ddbtypes.AttributeValueMemberS{Value: "arn:aws:lambda:us-east-1:000000000000:function:sla-monitor"},
			"input":      &ddbtypes.AttributeValueMemberS{Value: `{"mode":"fire-alert"}`},
			"roleArn":    &ddbtypes.AttributeValueMemberS{Value: "arn:aws:iam::000000000000:role/scheduler"},
			"state":      &ddbtypes.AttributeValueMemberS{Value: "ENABLED"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("stored item = %#v, want %#v", got, want)
		}

		if _, err := ts.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{Name: aws.String("sla-a")}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if _, ok := fake.item(testTable, "sla-a"); ok {
			t.Error("item still present after delete")
		}
	})

	t.Run("minimal input omits timezone and keeps empty group", func(t *testing.T) {
		fake := newCondDynamo()
		ts := NewTableScheduler(fake, testTable)
		in := &scheduler.CreateScheduleInput{
			Name:               aws.String("bare"),
			ScheduleExpression: aws.String("at(2026-03-10T12:30:00)"),
		}
		if _, err := ts.CreateSchedule(ctx, in); err != nil {
			t.Fatalf("create: %v", err)
		}
		got, _ := fake.item(testTable, "bare")
		if _, has := got["timezone"]; has {
			t.Error("timezone stored although input had none")
		}
		if g, ok := got["group"].(*ddbtypes.AttributeValueMemberS); !ok || g.Value != "" {
			t.Errorf("group = %#v, want empty string", got["group"])
		}
	})
}

func TestTableScheduler_RejectsBadInput(t *testing.T) {
	ctx := context.Background()
	ts := NewTableScheduler(newCondDynamo(), testTable)

	tests := []struct {
		name string
		call func() error
	}{
		{"create nil input", func() error { _, err := ts.CreateSchedule(ctx, nil); return err }},
		{"create nil name", func() error {
			_, err := ts.CreateSchedule(ctx, &scheduler.CreateScheduleInput{ScheduleExpression: aws.String("at(2026-03-10T12:30:00)")})
			return err
		}},
		{"delete nil input", func() error { _, err := ts.DeleteSchedule(ctx, nil); return err }},
		{"delete nil name", func() error { _, err := ts.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{}); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); err == nil {
				t.Error("want an error, got nil")
			}
		})
	}
}

func TestTableScheduler_WrapsOtherErrors(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	fake := newCondDynamo()
	fake.err = boom
	ts := NewTableScheduler(fake, testTable)

	tests := []struct {
		name string
		call func() error
	}{
		{"create", func() error { _, err := ts.CreateSchedule(ctx, fullInput("x")); return err }},
		{"delete", func() error {
			_, err := ts.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{Name: aws.String("x")})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			if !errors.Is(err, boom) {
				t.Fatalf("error = %v, want it to wrap %v", err, boom)
			}
		})
	}
}
