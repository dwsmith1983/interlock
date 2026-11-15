package storetest_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func s(v string) *ddbtypes.AttributeValueMemberS { return &ddbtypes.AttributeValueMemberS{Value: v} }

// TestMem_TriggerLockThroughStore drives the conditional writes the trigger
// lock relies on through the real store methods.
func TestMem_TriggerLockThroughStore(t *testing.T) {
	fake, mem := storetest.NewMem()
	st := storetest.NewStore(fake)
	st.Now = func() time.Time { return time.Unix(1000, 0) }
	ctx := context.Background()
	status := func() (string, bool) {
		item := mem.Get("control", types.PipelinePK("p"), types.TriggerSK("s", "d"))
		_, hasTTL := item["ttl"]
		if item == nil {
			return "", false
		}
		return item["status"].(*ddbtypes.AttributeValueMemberS).Value, hasTTL
	}

	if ok, err := st.ResetTriggerLock(ctx, "p", "s", "d", time.Hour); ok || err != nil {
		t.Fatalf("reset of an absent row = %v, %v, want false, nil", ok, err)
	}
	for i, want := range []bool{true, false} {
		if ok, err := st.AcquireTriggerLock(ctx, "p", "s", "d", time.Hour); ok != want || err != nil {
			t.Fatalf("acquire %d = %v, %v, want %v, nil", i+1, ok, err, want)
		}
	}
	if err := st.SetTriggerStatus(ctx, "p", "s", "d", types.TriggerStatusFailedFinal); err != nil {
		t.Fatal(err)
	}
	if got, hasTTL := status(); got != types.TriggerStatusFailedFinal || hasTTL {
		t.Fatalf("after SetTriggerStatus = %q ttl %v, want FAILED_FINAL without ttl", got, hasTTL)
	}
	if ok, err := st.ResetTriggerLock(ctx, "p", "s", "d", time.Hour); !ok || err != nil {
		t.Fatalf("reset of a present row = %v, %v, want true, nil", ok, err)
	}
	if got, hasTTL := status(); got != types.TriggerStatusRunning || !hasTTL {
		t.Fatalf("after reset = %q ttl %v, want RUNNING with ttl", got, hasTTL)
	}
	if err := st.ReleaseTriggerLock(ctx, "p", "s", "d"); err != nil {
		t.Fatal(err)
	}
	if got, _ := status(); got != "" {
		t.Fatalf("after release = %q, want no row", got)
	}
}

func TestMem_QueryOrderLimitPagingAndCount(t *testing.T) {
	fake, mem := storetest.NewMem()
	for _, sk := range []string{"JOB#s#d#3", "JOB#s#d#1", "JOB#s#d#2", "JOB#s#e#9", "OTHER#1"} {
		mem.Put("joblog", map[string]ddbtypes.AttributeValue{"PK": s("PIPELINE#p"), "SK": s(sk)})
	}
	mem.Put("joblog", map[string]ddbtypes.AttributeValue{"PK": s("PIPELINE#q"), "SK": s("JOB#s#d#5")})
	query := func(forward bool, limit int32, start map[string]ddbtypes.AttributeValue, sel ddbtypes.Select) *dynamodb.QueryOutput {
		in := &dynamodb.QueryInput{
			TableName:              aws.String("joblog"),
			KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk": s("PIPELINE#p"), ":prefix": s("JOB#s#d#"),
			},
			ScanIndexForward: aws.Bool(forward), ExclusiveStartKey: start, Select: sel,
		}
		if limit > 0 {
			in.Limit = aws.Int32(limit)
		}
		out, err := fake.Query(context.Background(), in)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	sks := func(out *dynamodb.QueryOutput) string {
		var got []string
		for _, it := range out.Items {
			got = append(got, it["SK"].(*ddbtypes.AttributeValueMemberS).Value)
		}
		return strings.Join(got, ",")
	}

	first := query(false, 2, nil, "")
	if sks(first) != "JOB#s#d#3,JOB#s#d#2" || first.LastEvaluatedKey == nil {
		t.Fatalf("newest two = %s (last key %v), want 3,2 and a last key", sks(first), first.LastEvaluatedKey)
	}
	if rest := query(false, 2, first.LastEvaluatedKey, ""); sks(rest) != "JOB#s#d#1" || rest.LastEvaluatedKey != nil {
		t.Fatalf("next page = %s (last key %v), want 1 and no last key", sks(rest), rest.LastEvaluatedKey)
	}
	if fwd := query(true, 0, nil, ""); sks(fwd) != "JOB#s#d#1,JOB#s#d#2,JOB#s#d#3" {
		t.Fatalf("forward = %s", sks(fwd))
	}
	if c := query(true, 0, nil, ddbtypes.SelectCount); c.Count != 3 || c.Items != nil {
		t.Fatalf("count = %d items %v, want 3 and no items", c.Count, c.Items)
	}
}

func TestMem_ScanAndDelete(t *testing.T) {
	fake, mem := storetest.NewMem()
	mem.Put("control", map[string]ddbtypes.AttributeValue{"PK": s("PIPELINE#p"), "SK": s(types.ConfigSK), "config": s(`{"pipeline":{"id":"p"}}`)})
	mem.Put("control", map[string]ddbtypes.AttributeValue{"PK": s("PIPELINE#p"), "SK": s("TRIGGER#s#d")})
	configs, _, err := storetest.NewStore(fake).ScanConfigs(context.Background())
	if err != nil || len(configs) != 1 || configs["p"] == nil {
		t.Fatalf("ScanConfigs = %v, %v, want only p", configs, err)
	}
	if _, err := fake.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String("control"), Key: map[string]ddbtypes.AttributeValue{"PK": s("PIPELINE#p"), "SK": s("TRIGGER#s#d")},
	}); err != nil {
		t.Fatal(err)
	}
	if got := mem.Items("control"); len(got) != 1 {
		t.Fatalf("items after delete = %v, want the config row only", got)
	}
}

// TestMem_UnsupportedExpressionsFail: an expression the fake cannot evaluate
// must fail the call and name the expression, never match silently.
func TestMem_UnsupportedExpressionsFail(t *testing.T) {
	fake, _ := storetest.NewMem()
	ctx := context.Background()
	item := map[string]ddbtypes.AttributeValue{"PK": s("PIPELINE#p"), "SK": s("X")}
	tbl := aws.String("control")
	calls := map[string]func() error{
		"attribute_exists(SK)": func() error {
			_, err := fake.PutItem(ctx, &dynamodb.PutItemInput{TableName: tbl, Item: item, ConditionExpression: aws.String("attribute_exists(SK)")})
			return err
		},
		"ADD #n :one": func() error {
			_, err := fake.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: tbl, Key: item, UpdateExpression: aws.String("ADD #n :one")})
			return err
		},
		"attribute_exists(PK)": func() error {
			_, err := fake.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: tbl, Key: item, ConditionExpression: aws.String("attribute_exists(PK)")})
			return err
		},
		"reason IN (:s0)": func() error {
			_, err := fake.Query(ctx, &dynamodb.QueryInput{TableName: tbl, KeyConditionExpression: aws.String("PK = :pk"), FilterExpression: aws.String("reason IN (:s0)")})
			return err
		},
		"PK = :pk AND SK > :sk": func() error {
			_, err := fake.Query(ctx, &dynamodb.QueryInput{TableName: tbl, KeyConditionExpression: aws.String("PK = :pk AND SK > :sk")})
			return err
		},
		"#status = :running": func() error {
			_, err := fake.Scan(ctx, &dynamodb.ScanInput{TableName: tbl, FilterExpression: aws.String("#status = :running")})
			return err
		},
	}
	for expr, call := range calls {
		if err := call(); err == nil || !strings.Contains(err.Error(), expr) {
			t.Errorf("%s: err = %v, want an error naming it", expr, err)
		}
	}
}

// TestMem_Conditions: AND binds within an OR group; "=" and "<>" compare a
// named attribute; a missing row satisfies neither. A failed condition is a
// conditional check failure, and nothing is written.
func TestMem_Conditions(t *testing.T) {
	const running, notCompleted = "attribute_exists(PK) AND #status = :running", "attribute_not_exists(PK) OR #status <> :completed"
	for _, tc := range []struct {
		expr, status string // status "" means no row
		want         bool
	}{
		{running, types.TriggerStatusRunning, true}, {running, types.TriggerStatusCompleted, false}, {running, "", false},
		{notCompleted, "", true}, {notCompleted, types.TriggerStatusRunning, true},
		{notCompleted, types.TriggerStatusFailedFinal, true}, {notCompleted, types.TriggerStatusCompleted, false},
	} {
		fake, mem := storetest.NewMem()
		key := map[string]ddbtypes.AttributeValue{"PK": s("PIPELINE#p"), "SK": s("TRIGGER#s#d")}
		if tc.status != "" {
			mem.Put("control", map[string]ddbtypes.AttributeValue{"PK": key["PK"], "SK": key["SK"], "status": s(tc.status)})
		}
		_, err := fake.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
			TableName: aws.String("control"), Key: key, ConditionExpression: aws.String(tc.expr),
			UpdateExpression:         aws.String("SET #ttl = :ttl"),
			ExpressionAttributeNames: map[string]string{"#status": "status", "#ttl": "ttl"},
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":running": s(types.TriggerStatusRunning),
				":completed": s(types.TriggerStatusCompleted), ":ttl": &ddbtypes.AttributeValueMemberN{Value: "9"}},
		})
		var ccfe *ddbtypes.ConditionalCheckFailedException
		if got := err == nil; got != tc.want || (err != nil && !errors.As(err, &ccfe)) {
			t.Errorf("%s on %q: err = %v, want applied %v (else a conditional check failure)", tc.expr, tc.status, err, tc.want)
		}
		if _, hasTTL := mem.Get("control", "PIPELINE#p", "TRIGGER#s#d")["ttl"]; hasTTL != tc.want {
			t.Errorf("%s on %q: ttl written = %v, want %v", tc.expr, tc.status, hasTTL, tc.want)
		}
	}
}

// TestMem_ConditionNumberLess: "<" compares number attributes; an absent row
// or attribute satisfies it never, so "attribute_not_exists(PK) OR #ttl < :now"
// writes over an expired row only.
func TestMem_ConditionNumberLess(t *testing.T) {
	n := func(v string) *ddbtypes.AttributeValueMemberN { return &ddbtypes.AttributeValueMemberN{Value: v} }
	for _, tc := range []struct {
		ttl, now string // ttl "" means no row
		want     bool
	}{{"", "100", true}, {"100", "99", false}, {"100", "100", false}, {"100", "101", true}} {
		fake, mem := storetest.NewMem()
		item := map[string]ddbtypes.AttributeValue{"PK": s("PIPELINE#p"), "SK": s("M")}
		if tc.ttl != "" {
			mem.Put("control", map[string]ddbtypes.AttributeValue{"PK": item["PK"], "SK": item["SK"], "ttl": n(tc.ttl)})
		}
		_, err := fake.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String("control"), Item: item,
			ConditionExpression:       aws.String("attribute_not_exists(PK) OR #ttl < :now"),
			ExpressionAttributeNames:  map[string]string{"#ttl": "ttl"},
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":now": n(tc.now)},
		})
		var ccfe *ddbtypes.ConditionalCheckFailedException
		if got := err == nil; got != tc.want || (err != nil && !errors.As(err, &ccfe)) {
			t.Errorf("ttl %q now %s: err = %v, want written %v (else a conditional check failure)", tc.ttl, tc.now, err, tc.want)
		}
	}
}
