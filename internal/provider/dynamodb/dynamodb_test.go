//go:build integration

package dynamodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/interlock-systems/interlock/internal/provider/providertest"
	"github.com/interlock-systems/interlock/pkg/types"
)

func setupTestProvider(t *testing.T) *DynamoDBProvider {
	t.Helper()
	ctx := context.Background()
	tableName := fmt.Sprintf("interlock-test-%d", time.Now().UnixNano())
	cfg := &types.DynamoDBConfig{
		TableName:   tableName,
		Region:      "us-east-1",
		Endpoint:    "http://localhost:8000",
		CreateTable: true,
	}
	prov, err := New(cfg)
	if err != nil {
		t.Skipf("DynamoDB Local not available: %v", err)
	}
	if err := prov.Start(ctx); err != nil {
		t.Skipf("DynamoDB Local not available: %v", err)
	}
	t.Cleanup(func() {
		_, _ = prov.client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: &tableName,
		})
	})
	return prov
}

func TestConformance(t *testing.T) {
	prov := setupTestProvider(t)
	providertest.RunAll(t, prov)
}
