//go:build e2e

package e2ehooks

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
)

// DynamoAPI is the subset of the DynamoDB client TableScheduler uses.
type DynamoAPI interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

// keyAttr is the table's partition key: the schedule name, as a string.
const keyAttr = "name"

// TableScheduler records schedules in a DynamoDB table instead of creating
// them in EventBridge Scheduler, so the e2e harness can fire them on its
// simulated clock. Its method set matches the scheduler client's
// CreateSchedule and DeleteSchedule, which is what the Lambdas depend on.
// The table is keyed on the schedule name alone: the group is stored but is
// not part of the key, which is fine while e2e runs use a single group.
type TableScheduler struct {
	client DynamoAPI
	table  string
}

// NewTableScheduler returns a TableScheduler writing to table through client.
func NewTableScheduler(client DynamoAPI, table string) *TableScheduler {
	return &TableScheduler{client: client, table: table}
}

// CreateSchedule stores the schedule under its name. A schedule with the same
// name already present yields *schedulertypes.ConflictException, as the real
// service does.
func (s *TableScheduler) CreateSchedule(ctx context.Context, in *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	if in == nil || in.Name == nil {
		return nil, errors.New("e2ehooks: CreateSchedule needs an input with a Name")
	}
	name := aws.ToString(in.Name)

	item := map[string]ddbtypes.AttributeValue{
		keyAttr:      str(name),
		"group":      str(aws.ToString(in.GroupName)),
		"expression": str(aws.ToString(in.ScheduleExpression)),
		"state":      str(string(in.State)),
	}
	if in.ScheduleExpressionTimezone != nil {
		item["timezone"] = str(aws.ToString(in.ScheduleExpressionTimezone))
	}
	if in.Target != nil {
		item["targetArn"] = str(aws.ToString(in.Target.Arn))
		item["input"] = str(aws.ToString(in.Target.Input))
		item["roleArn"] = str(aws.ToString(in.Target.RoleArn))
	}

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                aws.String(s.table),
		Item:                     item,
		ConditionExpression:      aws.String("attribute_not_exists(#n)"),
		ExpressionAttributeNames: map[string]string{"#n": keyAttr},
	})
	if err != nil {
		var ccf *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return nil, &schedulertypes.ConflictException{
				Message: aws.String(fmt.Sprintf("schedule %q already exists in %s", name, s.table)),
			}
		}
		return nil, fmt.Errorf("e2ehooks: put schedule %q into %s: %w", name, s.table, err)
	}
	return &scheduler.CreateScheduleOutput{}, nil
}

// DeleteSchedule removes the schedule with the given name. A missing name
// yields *schedulertypes.ResourceNotFoundException, as the real service does.
func (s *TableScheduler) DeleteSchedule(ctx context.Context, in *scheduler.DeleteScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	if in == nil || in.Name == nil {
		return nil, errors.New("e2ehooks: DeleteSchedule needs an input with a Name")
	}
	name := aws.ToString(in.Name)

	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:                aws.String(s.table),
		Key:                      map[string]ddbtypes.AttributeValue{keyAttr: str(name)},
		ConditionExpression:      aws.String("attribute_exists(#n)"),
		ExpressionAttributeNames: map[string]string{"#n": keyAttr},
	})
	if err != nil {
		var ccf *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return nil, &schedulertypes.ResourceNotFoundException{
				Message: aws.String(fmt.Sprintf("schedule %q not found in %s", name, s.table)),
			}
		}
		return nil, fmt.Errorf("e2ehooks: delete schedule %q from %s: %w", name, s.table, err)
	}
	return &scheduler.DeleteScheduleOutput{}, nil
}

func str(v string) *ddbtypes.AttributeValueMemberS {
	return &ddbtypes.AttributeValueMemberS{Value: v}
}
