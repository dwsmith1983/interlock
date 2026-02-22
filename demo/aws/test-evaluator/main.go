package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	ddbClient *dynamodb.Client
	tableName string
	logger    *slog.Logger
)

func init() {
	logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		logger.Error("TABLE_NAME not set")
		os.Exit(1)
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		logger.Error("failed to load AWS config", "error", err)
		os.Exit(1)
	}
	ddbClient = dynamodb.NewFromConfig(cfg)
}

func handler(ctx context.Context, req events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	path := req.RawPath
	logger.Info("request", "method", req.RequestContext.HTTP.Method, "path", path)

	switch {
	case strings.HasSuffix(path, "/freshness"):
		return handleFreshness(ctx, req)
	case strings.HasSuffix(path, "/record-count"):
		return handleRecordCount(ctx, req)
	case strings.HasSuffix(path, "/upstream-check"):
		return handleUpstreamCheck(ctx, req)
	case strings.HasSuffix(path, "/trigger-endpoint"):
		return handleTrigger(ctx, req)
	default:
		return jsonResponse(http.StatusNotFound, map[string]string{
			"error": "unknown path: " + path,
		})
	}
}

// handleFreshness checks if data lag is within acceptable bounds.
// Sensor: SENSOR#freshness → {"lag": <seconds>}
// Config: {"maxLagSeconds": <threshold>}
func handleFreshness(ctx context.Context, req events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	sensor, err := getSensor(ctx, "freshness")
	if err != nil {
		return evalResponse("FAIL", map[string]any{"error": err.Error()})
	}

	cfg := extractConfig(req.Body)
	maxLag, _ := cfg["maxLagSeconds"].(float64)

	lag, _ := sensor["lag"].(float64)
	status := "FAIL"
	if maxLag > 0 && lag <= maxLag {
		status = "PASS"
	}

	return evalResponse(status, map[string]any{
		"lagSeconds": lag,
		"threshold":  maxLag,
	})
}

// handleRecordCount checks if record count meets minimum threshold.
// Sensor: SENSOR#record-count → {"count": <n>}
// Config: {"threshold": <min>}
func handleRecordCount(ctx context.Context, req events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	sensor, err := getSensor(ctx, "record-count")
	if err != nil {
		return evalResponse("FAIL", map[string]any{"error": err.Error()})
	}

	cfg := extractConfig(req.Body)
	threshold, _ := cfg["threshold"].(float64)

	count, _ := sensor["count"].(float64)
	status := "FAIL"
	if count >= threshold {
		status = "PASS"
	}

	return evalResponse(status, map[string]any{
		"count":     count,
		"threshold": threshold,
	})
}

// handleUpstreamCheck checks if upstream pipeline is complete.
// Sensor: SENSOR#upstream-check → {"complete": true/false}
// Config: {"expectComplete": true}
func handleUpstreamCheck(ctx context.Context, _ events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	sensor, err := getSensor(ctx, "upstream-check")
	if err != nil {
		return evalResponse("FAIL", map[string]any{"error": err.Error()})
	}

	complete, _ := sensor["complete"].(bool)
	status := "FAIL"
	if complete {
		status = "PASS"
	}

	return evalResponse(status, map[string]any{
		"complete": complete,
	})
}

// handleTrigger accepts trigger requests and returns success.
func handleTrigger(_ context.Context, _ events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	return jsonResponse(http.StatusOK, map[string]string{
		"status":  "success",
		"message": "triggered",
	})
}

// extractConfig parses the request body as EvaluatorInput and returns the config map.
// EvaluatorInput format: {"pipelineId":"...","traitType":"...","config":{...}}
func extractConfig(body string) map[string]any {
	var input struct {
		Config map[string]any `json:"config"`
	}
	if err := json.Unmarshal([]byte(body), &input); err != nil || input.Config == nil {
		return map[string]any{}
	}
	return input.Config
}

// getSensor reads sensor state from DynamoDB.
// Key: PK=SENSOR#{name}, SK=STATE
func getSensor(ctx context.Context, name string) (map[string]any, error) {
	out, err := ddbClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: "SENSOR#" + name},
			"SK": &ddbtypes.AttributeValueMemberS{Value: "STATE"},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb GetItem: %w", err)
	}
	if out.Item == nil {
		return nil, fmt.Errorf("sensor %q not found", name)
	}

	dataAttr, ok := out.Item["data"]
	if !ok {
		return nil, fmt.Errorf("sensor %q missing data attribute", name)
	}

	mapAttr, ok := dataAttr.(*ddbtypes.AttributeValueMemberM)
	if !ok {
		return nil, fmt.Errorf("sensor %q data is not a map", name)
	}

	result := make(map[string]any)
	for k, v := range mapAttr.Value {
		switch attr := v.(type) {
		case *ddbtypes.AttributeValueMemberN:
			var f float64
			fmt.Sscanf(attr.Value, "%f", &f)
			result[k] = f
		case *ddbtypes.AttributeValueMemberBOOL:
			result[k] = attr.Value
		case *ddbtypes.AttributeValueMemberS:
			result[k] = attr.Value
		}
	}
	return result, nil
}

func evalResponse(status string, value map[string]any) (events.APIGatewayV2HTTPResponse, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"status": status,
		"value":  value,
	})
}

func jsonResponse(statusCode int, body any) (events.APIGatewayV2HTTPResponse, error) {
	data, _ := json.Marshal(body)
	return events.APIGatewayV2HTTPResponse{
		StatusCode: statusCode,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       string(data),
	}, nil
}

func main() {
	lambda.Start(handler)
}
