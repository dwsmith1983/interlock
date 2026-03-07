package lambda_test

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	"github.com/dwsmith1983/interlock/internal/store"
)

// ---------------------------------------------------------------------------
// Mock SFN client
// ---------------------------------------------------------------------------

type mockSFN struct {
	mu         sync.Mutex
	executions []*sfn.StartExecutionInput
	err        error
}

func (m *mockSFN) StartExecution(_ context.Context, input *sfn.StartExecutionInput, _ ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	m.executions = append(m.executions, input)
	return &sfn.StartExecutionOutput{}, nil
}

// ---------------------------------------------------------------------------
// Mock EventBridge client
// ---------------------------------------------------------------------------

type mockEventBridge struct {
	mu     sync.Mutex
	events []*eventbridge.PutEventsInput
	err    error
}

func (m *mockEventBridge) PutEvents(_ context.Context, input *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	m.events = append(m.events, input)
	return &eventbridge.PutEventsOutput{}, nil
}

// ---------------------------------------------------------------------------
// Mock Scheduler client
// ---------------------------------------------------------------------------

type mockScheduler struct {
	mu        sync.Mutex
	created   []*scheduler.CreateScheduleInput
	deleted   []*scheduler.DeleteScheduleInput
	createErr error
	deleteErr error
}

func (m *mockScheduler) CreateSchedule(_ context.Context, input *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createErr != nil {
		return nil, m.createErr
	}
	m.created = append(m.created, input)
	return &scheduler.CreateScheduleOutput{}, nil
}

func (m *mockScheduler) DeleteSchedule(_ context.Context, input *scheduler.DeleteScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	m.deleted = append(m.deleted, input)
	return &scheduler.DeleteScheduleOutput{}, nil
}

// ---------------------------------------------------------------------------
// Mock DynamoDB client (re-implemented for this package's tests)
// ---------------------------------------------------------------------------

type mockDDB struct {
	mu    sync.Mutex
	items map[string]map[string]ddbtypes.AttributeValue
}

func newMockDDB() *mockDDB {
	return &mockDDB{
		items: make(map[string]map[string]ddbtypes.AttributeValue),
	}
}

// Compile-time check.
var _ store.DynamoAPI = (*mockDDB)(nil)

func ddbItemKey(table, pk, sk string) string {
	return table + "#" + pk + "#" + sk
}

func ddbExtractPKSK(item map[string]ddbtypes.AttributeValue) (pk, sk string) {
	pk = item["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk = item["SK"].(*ddbtypes.AttributeValueMemberS).Value
	return pk, sk
}

func ddbCopyItem(item map[string]ddbtypes.AttributeValue) map[string]ddbtypes.AttributeValue {
	out := make(map[string]ddbtypes.AttributeValue, len(item))
	for k, v := range item {
		out[k] = v
	}
	return out
}

func (m *mockDDB) GetItem(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pk := input.Key["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk := input.Key["SK"].(*ddbtypes.AttributeValueMemberS).Value
	key := ddbItemKey(*input.TableName, pk, sk)

	item, ok := m.items[key]
	if !ok {
		return &dynamodb.GetItemOutput{}, nil
	}
	return &dynamodb.GetItemOutput{Item: ddbCopyItem(item)}, nil
}

func (m *mockDDB) PutItem(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pk, sk := ddbExtractPKSK(input.Item)
	key := ddbItemKey(*input.TableName, pk, sk)

	// Support attribute_not_exists(PK) condition for lock acquisition.
	if input.ConditionExpression != nil && *input.ConditionExpression == "attribute_not_exists(PK)" {
		if _, exists := m.items[key]; exists {
			return nil, &ddbtypes.ConditionalCheckFailedException{
				Message: strPtr("The conditional request failed"),
			}
		}
	}

	m.items[key] = ddbCopyItem(input.Item)
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDDB) UpdateItem(_ context.Context, input *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pk := input.Key["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk := input.Key["SK"].(*ddbtypes.AttributeValueMemberS).Value
	key := ddbItemKey(*input.TableName, pk, sk)

	item, ok := m.items[key]
	if !ok {
		item = map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
			"SK": &ddbtypes.AttributeValueMemberS{Value: sk},
		}
	}

	// Parse UpdateExpression: supports SET and REMOVE clauses.
	if input.UpdateExpression != nil {
		expr := *input.UpdateExpression

		// Split into SET and REMOVE sections.
		setExpr, removeExpr := "", ""
		if idx := strings.Index(expr, " REMOVE "); idx >= 0 {
			setExpr = expr[:idx]
			removeExpr = expr[idx+len(" REMOVE "):]
		} else {
			setExpr = expr
		}

		// Apply SET assignments.
		setExpr = strings.TrimPrefix(setExpr, "SET ")
		for _, assignment := range strings.Split(setExpr, ",") {
			parts := strings.SplitN(strings.TrimSpace(assignment), "=", 2)
			if len(parts) != 2 {
				continue
			}
			nameRef := strings.TrimSpace(parts[0])
			valRef := strings.TrimSpace(parts[1])

			attrName := nameRef
			if input.ExpressionAttributeNames != nil {
				if resolved, ok := input.ExpressionAttributeNames[nameRef]; ok {
					attrName = resolved
				}
			}
			if val, ok := input.ExpressionAttributeValues[valRef]; ok {
				item[attrName] = val
			}
		}

		// Apply REMOVE deletions.
		if removeExpr != "" {
			for _, attr := range strings.Split(removeExpr, ",") {
				attr = strings.TrimSpace(attr)
				if input.ExpressionAttributeNames != nil {
					if resolved, ok := input.ExpressionAttributeNames[attr]; ok {
						attr = resolved
					}
				}
				delete(item, attr)
			}
		}
	}

	m.items[key] = item
	return &dynamodb.UpdateItemOutput{}, nil
}

func (m *mockDDB) DeleteItem(_ context.Context, input *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pk := input.Key["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk := input.Key["SK"].(*ddbtypes.AttributeValueMemberS).Value
	delete(m.items, ddbItemKey(*input.TableName, pk, sk))
	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *mockDDB) Query(_ context.Context, input *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pkVal := input.ExpressionAttributeValues[":pk"].(*ddbtypes.AttributeValueMemberS).Value

	// Prefer ":prefix" or ":skPrefix" for deterministic SK prefix matching.
	prefixVal := ""
	for _, candidate := range []string{":prefix", ":skPrefix"} {
		if v, ok := input.ExpressionAttributeValues[candidate]; ok {
			if s, ok := v.(*ddbtypes.AttributeValueMemberS); ok {
				prefixVal = s.Value
				break
			}
		}
	}
	if prefixVal == "" {
		for k, v := range input.ExpressionAttributeValues {
			if k == ":pk" {
				continue
			}
			if s, ok := v.(*ddbtypes.AttributeValueMemberS); ok {
				prefixVal = s.Value
				break
			}
		}
	}

	table := *input.TableName
	tablePrefix := table + "#" + pkVal + "#"

	var items []map[string]ddbtypes.AttributeValue
	for k, item := range m.items {
		if !strings.HasPrefix(k, tablePrefix) {
			continue
		}
		sk := item["SK"].(*ddbtypes.AttributeValueMemberS).Value
		if prefixVal != "" && !strings.HasPrefix(sk, prefixVal) {
			continue
		}
		// Apply FilterExpression if present.
		if input.FilterExpression != nil && !matchesQueryFilter(*input.FilterExpression, input.ExpressionAttributeValues, item) {
			continue
		}
		items = append(items, ddbCopyItem(item))
	}

	sort.Slice(items, func(i, j int) bool {
		skI := items[i]["SK"].(*ddbtypes.AttributeValueMemberS).Value
		skJ := items[j]["SK"].(*ddbtypes.AttributeValueMemberS).Value
		return skI < skJ
	})

	return &dynamodb.QueryOutput{Items: items, Count: int32(len(items))}, nil
}

// matchesQueryFilter evaluates a FilterExpression against an item.
// Supports "reason IN (:s0, :s1, ...)" pattern used by CountRerunsBySource.
func matchesQueryFilter(expr string, values, item map[string]ddbtypes.AttributeValue) bool {
	expr = strings.TrimSpace(expr)

	// Parse "field IN (:v0, :v1, ...)" pattern.
	if idx := strings.Index(expr, " IN ("); idx >= 0 {
		field := strings.TrimSpace(expr[:idx])
		inList := expr[idx+len(" IN ("):]
		inList = strings.TrimSuffix(inList, ")")

		actual, ok := item[field]
		if !ok {
			return false
		}
		actualStr, ok := actual.(*ddbtypes.AttributeValueMemberS)
		if !ok {
			return false
		}

		for _, ref := range strings.Split(inList, ",") {
			ref = strings.TrimSpace(ref)
			if v, ok := values[ref]; ok {
				if s, ok := v.(*ddbtypes.AttributeValueMemberS); ok {
					if s.Value == actualStr.Value {
						return true
					}
				}
			}
		}
		return false
	}

	// Unknown filter pattern — pass through.
	return true
}

func (m *mockDDB) Scan(_ context.Context, input *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	table := *input.TableName
	prefix := table + "#"

	var items []map[string]ddbtypes.AttributeValue
	for k, item := range m.items {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if input.FilterExpression != nil && !matchesScanFilter(*input.FilterExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues, item) {
			continue
		}
		items = append(items, ddbCopyItem(item))
	}

	return &dynamodb.ScanOutput{Items: items, Count: int32(len(items))}, nil
}

// matchesScanFilter evaluates a FilterExpression against an item.
// Supports simple "attr = :val", "begins_with(attr, :val)", and compound "... AND ..." clauses.
func matchesScanFilter(expr string, names map[string]string, values, item map[string]ddbtypes.AttributeValue) bool {
	clauses := strings.Split(expr, " AND ")
	for _, clause := range clauses {
		clause = strings.TrimSpace(clause)
		if !matchesScanClause(clause, names, values, item) {
			return false
		}
	}
	return true
}

func matchesScanClause(clause string, names map[string]string, values, item map[string]ddbtypes.AttributeValue) bool {
	// begins_with(attr, :val)
	if strings.HasPrefix(clause, "begins_with(") {
		inner := strings.TrimPrefix(clause, "begins_with(")
		inner = strings.TrimSuffix(inner, ")")
		parts := strings.SplitN(inner, ",", 2)
		if len(parts) != 2 {
			return true
		}
		attrName := strings.TrimSpace(parts[0])
		valRef := strings.TrimSpace(parts[1])

		expected, ok := values[valRef]
		if !ok {
			return true
		}
		actual, ok := item[attrName]
		if !ok {
			return false
		}
		expectedStr, ok1 := expected.(*ddbtypes.AttributeValueMemberS)
		actualStr, ok2 := actual.(*ddbtypes.AttributeValueMemberS)
		if ok1 && ok2 {
			return strings.HasPrefix(actualStr.Value, expectedStr.Value)
		}
		return true
	}

	// Simple "attr = :val" equality.
	parts := strings.SplitN(clause, " = ", 2)
	if len(parts) != 2 {
		return true
	}

	attrName := strings.TrimSpace(parts[0])
	valRef := strings.TrimSpace(parts[1])

	if names != nil {
		if resolved, ok := names[attrName]; ok {
			attrName = resolved
		}
	}

	expected, ok := values[valRef]
	if !ok {
		return true
	}

	actual, ok := item[attrName]
	if !ok {
		return false
	}

	expectedStr, ok1 := expected.(*ddbtypes.AttributeValueMemberS)
	actualStr, ok2 := actual.(*ddbtypes.AttributeValueMemberS)
	if ok1 && ok2 {
		return expectedStr.Value == actualStr.Value
	}
	return true
}

// putRaw inserts an item directly into the mock store.
func (m *mockDDB) putRaw(table string, item map[string]ddbtypes.AttributeValue) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pk, sk := ddbExtractPKSK(item)
	m.items[ddbItemKey(table, pk, sk)] = ddbCopyItem(item)
}
