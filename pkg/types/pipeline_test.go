package types_test

import (
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const samplePipelineYAML = `
pipeline:
  id: gold-revenue
  owner: revenue-team
schedule:
  cron: "0 7 * * *"
  timezone: America/New_York
  exclude:
    weekends: true
    holidays: us-federal
  evaluation:
    window: 1h
    interval: 5m
sla:
  deadline: "08:00"
  expectedDuration: 30m
  critical: true
validation:
  trigger: "ALL"
  rules:
    - key: SENSOR#upstream-complete
      check: equals
      field: status
      value: "COMPLETE"
    - key: SENSOR#record-count
      check: gte
      field: value
      value: 1000000
job:
  type: glue
  config:
    jobName: gold-revenue-etl
  maxRetries: 2
postRun:
  rules:
    - key: SENSOR#output-count
      check: gte
      field: value
      value: 500000
`

func TestPipelineConfig_UnmarshalYAML(t *testing.T) {
	var cfg types.PipelineConfig
	require.NoError(t, yaml.Unmarshal([]byte(samplePipelineYAML), &cfg))

	assert.Equal(t, "gold-revenue", cfg.Pipeline.ID)
	assert.Equal(t, "revenue-team", cfg.Pipeline.Owner)
	assert.Equal(t, "0 7 * * *", cfg.Schedule.Cron)
	assert.Equal(t, "America/New_York", cfg.Schedule.Timezone)
	assert.True(t, cfg.Schedule.Exclude.Weekends)
	assert.Equal(t, "us-federal", cfg.Schedule.Exclude.Holidays)
	assert.Equal(t, "1h", cfg.Schedule.Evaluation.Window)
	assert.Equal(t, "5m", cfg.Schedule.Evaluation.Interval)
	assert.Equal(t, "08:00", cfg.SLA.Deadline)
	assert.True(t, cfg.SLA.Critical)
	assert.Equal(t, "ALL", cfg.Validation.Trigger)
	assert.Len(t, cfg.Validation.Rules, 2)
	assert.Equal(t, types.CheckEquals, cfg.Validation.Rules[0].Check)
	assert.Equal(t, types.CheckGTE, cfg.Validation.Rules[1].Check)
	assert.Equal(t, "glue", string(cfg.Job.Type))
	assert.Equal(t, 2, cfg.Job.MaxRetries)
	require.NotNil(t, cfg.PostRun)
	assert.Len(t, cfg.PostRun.Rules, 1)
}

const streamTriggeredYAML = `
pipeline:
  id: silver-orders
  owner: data-eng
  description: Triggered by upstream DynamoDB stream write
schedule:
  trigger:
    key: SENSOR#raw-orders-landed
    check: equals
    field: status
    value: "COMPLETE"
  evaluation:
    window: 30m
    interval: 2m
validation:
  trigger: "ALL"
  rules:
    - key: SENSOR#raw-orders-landed
      check: equals
      field: status
      value: "COMPLETE"
    - key: SENSOR#schema-valid
      check: exists
      field: status
job:
  type: step-function
  config:
    stateMachineArn: arn:aws:states:us-east-1:123456789:stateMachine:silver-orders
`

func TestPipelineConfig_StreamTriggered(t *testing.T) {
	var cfg types.PipelineConfig
	require.NoError(t, yaml.Unmarshal([]byte(streamTriggeredYAML), &cfg))

	assert.Equal(t, "silver-orders", cfg.Pipeline.ID)
	assert.Equal(t, "data-eng", cfg.Pipeline.Owner)
	assert.Equal(t, "Triggered by upstream DynamoDB stream write", cfg.Pipeline.Description)

	// No cron — stream-triggered
	assert.Empty(t, cfg.Schedule.Cron)
	require.NotNil(t, cfg.Schedule.Trigger)
	assert.Equal(t, "SENSOR#raw-orders-landed", cfg.Schedule.Trigger.Key)
	assert.Equal(t, types.CheckEquals, cfg.Schedule.Trigger.Check)
	assert.Equal(t, "status", cfg.Schedule.Trigger.Field)
	assert.Equal(t, "COMPLETE", cfg.Schedule.Trigger.Value)

	assert.Equal(t, "30m", cfg.Schedule.Evaluation.Window)
	assert.Equal(t, "2m", cfg.Schedule.Evaluation.Interval)

	assert.Len(t, cfg.Validation.Rules, 2)
	assert.Equal(t, types.CheckExists, cfg.Validation.Rules[1].Check)

	assert.Equal(t, "step-function", string(cfg.Job.Type))
	assert.Nil(t, cfg.PostRun)
	assert.Nil(t, cfg.SLA)
}

const minimalPipelineYAML = `
pipeline:
  id: bronze-ingest
schedule:
  evaluation:
    window: 15m
    interval: 1m
validation:
  trigger: "ANY"
  rules:
    - key: SENSOR#file-arrived
      check: exists
      field: status
job:
  type: command
  config:
    command: /opt/scripts/ingest.sh
`

func TestPipelineConfig_Minimal(t *testing.T) {
	var cfg types.PipelineConfig
	require.NoError(t, yaml.Unmarshal([]byte(minimalPipelineYAML), &cfg))

	assert.Equal(t, "bronze-ingest", cfg.Pipeline.ID)
	assert.Empty(t, cfg.Pipeline.Owner)
	assert.Empty(t, cfg.Pipeline.Description)

	assert.Empty(t, cfg.Schedule.Cron)
	assert.Empty(t, cfg.Schedule.Timezone)
	assert.Nil(t, cfg.Schedule.Trigger)
	assert.Nil(t, cfg.Schedule.Exclude)
	assert.Empty(t, cfg.Schedule.Calendar)

	assert.Equal(t, "15m", cfg.Schedule.Evaluation.Window)
	assert.Equal(t, "1m", cfg.Schedule.Evaluation.Interval)

	assert.Nil(t, cfg.SLA)
	assert.Equal(t, "ANY", cfg.Validation.Trigger)
	assert.Len(t, cfg.Validation.Rules, 1)
	assert.Equal(t, types.CheckExists, cfg.Validation.Rules[0].Check)

	assert.Equal(t, "command", string(cfg.Job.Type))
	assert.Equal(t, 0, cfg.Job.MaxRetries)
	assert.Nil(t, cfg.PostRun)
}

const calendarScheduleYAML = `
pipeline:
  id: monthly-report
  owner: finance
schedule:
  calendar: us-business-days
  time: "06:00"
  timezone: America/Chicago
  exclude:
    dates:
      - "2026-01-01"
      - "2026-12-25"
  evaluation:
    window: 2h
    interval: 10m
sla:
  deadline: "09:00"
  expectedDuration: 1h
  timezone: America/Chicago
validation:
  trigger: "ALL"
  rules:
    - key: SENSOR#ledger-closed
      check: equals
      field: status
      value: "CLOSED"
    - key: SENSOR#record-count
      check: gt
      field: value
      value: 0
    - key: SENSOR#last-update
      check: age_lt
      field: updatedAt
      value: 30m
job:
  type: http
  config:
    url: https://internal.example.com/api/v1/reports/monthly
    method: POST
  maxRetries: 3
`

func TestPipelineConfig_CalendarSchedule(t *testing.T) {
	var cfg types.PipelineConfig
	require.NoError(t, yaml.Unmarshal([]byte(calendarScheduleYAML), &cfg))

	assert.Equal(t, "monthly-report", cfg.Pipeline.ID)
	assert.Equal(t, "finance", cfg.Pipeline.Owner)

	assert.Equal(t, "us-business-days", cfg.Schedule.Calendar)
	assert.Equal(t, "06:00", cfg.Schedule.Time)
	assert.Equal(t, "America/Chicago", cfg.Schedule.Timezone)
	assert.Empty(t, cfg.Schedule.Cron)
	assert.Nil(t, cfg.Schedule.Trigger)

	require.NotNil(t, cfg.Schedule.Exclude)
	assert.False(t, cfg.Schedule.Exclude.Weekends)
	assert.Empty(t, cfg.Schedule.Exclude.Holidays)
	assert.Equal(t, []string{"2026-01-01", "2026-12-25"}, cfg.Schedule.Exclude.Dates)

	assert.Equal(t, "2h", cfg.Schedule.Evaluation.Window)
	assert.Equal(t, "10m", cfg.Schedule.Evaluation.Interval)

	require.NotNil(t, cfg.SLA)
	assert.Equal(t, "09:00", cfg.SLA.Deadline)
	assert.Equal(t, "1h", cfg.SLA.ExpectedDuration)
	assert.Equal(t, "America/Chicago", cfg.SLA.Timezone)
	assert.False(t, cfg.SLA.Critical)

	assert.Equal(t, "ALL", cfg.Validation.Trigger)
	assert.Len(t, cfg.Validation.Rules, 3)
	assert.Equal(t, types.CheckEquals, cfg.Validation.Rules[0].Check)
	assert.Equal(t, types.CheckGT, cfg.Validation.Rules[1].Check)
	assert.Equal(t, types.CheckAgeLT, cfg.Validation.Rules[2].Check)
	assert.Equal(t, "updatedAt", cfg.Validation.Rules[2].Field)

	assert.Equal(t, "http", string(cfg.Job.Type))
	assert.Equal(t, 3, cfg.Job.MaxRetries)
	assert.Equal(t, "https://internal.example.com/api/v1/reports/monthly", cfg.Job.Config["url"])
	assert.Equal(t, "POST", cfg.Job.Config["method"])

	assert.Nil(t, cfg.PostRun)
}
