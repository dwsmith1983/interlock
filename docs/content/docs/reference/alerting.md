---
title: Alerting
weight: 3
description: Sink interface, 5 implementations, Dispatcher, and AlertConfig schema.
---

Interlock dispatches alerts when SLA deadlines are breached, triggers fail, retries exhaust, or post-completion drift is detected. The alert system uses a pluggable sink architecture.

## Alert Type

```go
type Alert struct {
    Level      AlertLevel             // "error", "warning", "info"
    PipelineID string
    TraitType  string                 // empty for pipeline-level alerts
    Message    string
    Details    map[string]interface{}
    Timestamp  time.Time
}
```

### Alert Levels

| Level | Usage |
|---|---|
| `error` | SLA breach, trigger failure, retry exhaustion |
| `warning` | Approaching deadline, drift detected |
| `info` | Run completed, monitoring started |

## Sink Interface

Every alert destination implements the `Sink` interface:

```go
type Sink interface {
    Send(alert types.Alert) error
    Name() string
}
```

## Dispatcher

The `Dispatcher` routes alerts to all configured sinks:

```go
type Dispatcher struct {
    sinks  []Sink
    logger *slog.Logger
}

func NewDispatcher(configs []types.AlertConfig, logger *slog.Logger) (*Dispatcher, error)
func (d *Dispatcher) Dispatch(alert types.Alert)
func (d *Dispatcher) AddSink(s Sink)
func (d *Dispatcher) AlertFunc() func(types.Alert)
```

- `NewDispatcher` creates sinks from `[]AlertConfig` entries
- `Dispatch` sends to all sinks; errors are logged, not propagated
- `AddSink` adds a sink programmatically (used by Lambda init to add SNS)
- `AlertFunc` returns a `func(types.Alert)` suitable for passing to Engine and Watcher

## Sink Implementations

### Console

Prints alerts to stdout. Useful for development and debugging.

```yaml
alerts:
  - type: console
```

```go
func NewConsoleSink() *ConsoleSink
```

### Webhook

Sends alerts as JSON via HTTP POST to a configured URL.

```yaml
alerts:
  - type: webhook
    url: https://hooks.slack.com/services/T00/B00/xxx
```

```go
func NewWebhookSink(url string) *WebhookSink
```

**Payload**: The full `Alert` struct serialized as JSON.

### File

Appends alerts as JSON lines to a local file.

```yaml
alerts:
  - type: file
    path: /var/log/interlock/alerts.jsonl
```

```go
func NewFileSink(path string) *FileSink
```

### SNS

Publishes alerts to an AWS SNS topic. Used in the AWS Lambda deployment.

```yaml
alerts:
  - type: sns
    topicARN: arn:aws:sns:us-east-1:123456789:my-alerts
```

```go
func NewSNSSink(topicARN string) *SNSSink
```

**Message**: JSON-serialized `Alert`. The subject line includes the pipeline ID and alert level.

The SNS sink uses the `SNSAPI` interface for testability:

```go
type SNSAPI interface {
    Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
}
```

### S3

Writes alert records to S3 as JSON objects. Useful for long-term alert archival.

```yaml
alerts:
  - type: s3
    bucketName: my-alerts-bucket
    prefix: alerts/
```

```go
func NewS3Sink(bucketName, prefix string) *S3Sink
```

**Object key**: `{prefix}{pipelineID}/{timestamp}.json`

## AlertConfig

```go
type AlertConfig struct {
    Type       AlertType // "console", "webhook", "file", "sns", "s3"
    URL        string    // Webhook URL
    Path       string    // File path
    TopicARN   string    // SNS topic ARN
    BucketName string    // S3 bucket name
    Prefix     string    // S3 key prefix
}
```

### Configuration

Multiple sinks can be active simultaneously:

```yaml
alerts:
  - type: console
  - type: webhook
    url: https://hooks.slack.com/services/T00/B00/xxx
  - type: sns
    topicARN: arn:aws:sns:us-east-1:123456789:my-alerts
  - type: file
    path: /var/log/interlock/alerts.jsonl
  - type: s3
    bucketName: my-alerts-bucket
    prefix: alerts/
```

## Alert Events

Alerts are emitted at these points in the lifecycle:

| Event | Level | When |
|---|---|---|
| `SLA_BREACHED` | error | Evaluation or completion deadline exceeded |
| `TRIGGER_FAILED` | error | Trigger execution failed |
| `RETRY_EXHAUSTED` | error | All retry attempts consumed |
| `MONITORING_DRIFT_DETECTED` | warning | Post-completion trait regression |
| `RETRY_SCHEDULED` | info | Automatic retry queued |
| `MONITORING_STARTED` | info | Post-completion monitoring begins |

## Lambda Integration

In the AWS deployment, the Lambda `Init()` function configures alerting from environment variables:

```go
// internal/lambda/init.go
if topicARN := os.Getenv("SNS_TOPIC_ARN"); topicARN != "" {
    dispatcher.AddSink(alert.NewSNSSink(topicARN))
}
if bucket := os.Getenv("S3_ALERT_BUCKET"); bucket != "" {
    prefix := os.Getenv("S3_ALERT_PREFIX")
    dispatcher.AddSink(alert.NewS3Sink(bucket, prefix))
}
```

The alert-logger Lambda (in the medallion-pipeline project) subscribes to the SNS topic and writes structured JSON to CloudWatch Logs + persists `ALERT#` records in DynamoDB for queryability.
