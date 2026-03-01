---
title: Pipelines
weight: 1
description: Pipeline YAML schema, validation rules DSL, and readiness evaluation.
---

## Pipeline Configuration

A pipeline is defined by a single YAML file that declares identity, schedule, SLA, validation rules, and job execution. Pipeline YAML files are loaded by the Terraform module from the `pipelines_path` directory and stored as CONFIG rows in the control DynamoDB table.

```yaml
pipeline:
  id: gold-revenue
  owner: analytics-team
  description: Gold-tier revenue aggregation pipeline

schedule:
  cron: "0 8 * * *"
  timezone: UTC
  trigger:
    key: upstream-complete
    check: equals
    field: status
    value: ready
  evaluation:
    window: 1h
    interval: 5m

sla:
  deadline: "10:00"
  expectedDuration: 30m

validation:
  trigger: "ALL"
  rules:
    - key: upstream-complete
      check: equals
      field: status
      value: ready
    - key: row-count
      check: gte
      field: count
      value: 1000
    - key: freshness
      check: age_lt
      field: updatedAt
      value: 2h

job:
  type: glue
  config:
    jobName: gold-revenue-etl
  maxRetries: 2
```

## Top-Level Sections

### `pipeline`

Identity and ownership metadata.

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | string | yes | Unique pipeline identifier (used as DynamoDB partition key) |
| `owner` | string | yes | Team or individual responsible for the pipeline |
| `description` | string | no | Human-readable description |

### `schedule`

Controls when and how often the pipeline is evaluated. See [Schedules](../schedules/) for full details.

### `sla`

Time-based constraints for pipeline completion. See [Schedules](../schedules/) for full details.

### `validation`

Declarative rules that determine pipeline readiness. See [Validation Rules](#validation-rules) below.

### `job`

Defines how to start the downstream job when validation passes. See [Triggers](../triggers/) for all 8 supported job types.

## Validation Rules

The `validation` block replaces the old archetype/trait system with a declarative DSL. External processes push sensor data into DynamoDB, and validation rules evaluate that data to determine readiness.

### How It Works

1. External systems write sensor data to the control table (e.g., `PK=PIPELINE#gold-revenue`, `SK=SENSOR#upstream-complete`)
2. The orchestrator Lambda reads sensor values and evaluates each rule against the current data
3. Rules are combined using the `trigger` mode to produce a readiness decision

### Trigger Modes

| Mode | Description |
|---|---|
| `ALL` | All rules must pass (logical AND) |
| `ANY` | At least one rule must pass (logical OR) |

### Rule Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `key` | string | yes | Sensor key to evaluate (matches the sensor record's SK suffix) |
| `check` | string | yes | Comparison operator (see [Supported Checks](#supported-checks)) |
| `field` | string | conditional | Field within the sensor data to compare (required for all checks except `exists`) |
| `value` | any | conditional | Expected value to compare against (required for all checks except `exists`) |

### Supported Checks

| Check | Description | Example |
|---|---|---|
| `exists` | Sensor key exists (any value) | `check: exists` |
| `equals` | Field equals the expected value | `check: equals`, `field: status`, `value: ready` |
| `gt` | Field is greater than the value (numeric) | `check: gt`, `field: count`, `value: 0` |
| `gte` | Field is greater than or equal to the value (numeric) | `check: gte`, `field: count`, `value: 1000` |
| `lt` | Field is less than the value (numeric) | `check: lt`, `field: errorRate`, `value: 5` |
| `lte` | Field is less than or equal to the value (numeric) | `check: lte`, `field: latency`, `value: 100` |
| `age_lt` | Field timestamp is newer than the duration | `check: age_lt`, `field: updatedAt`, `value: 2h` |
| `age_gt` | Field timestamp is older than the duration | `check: age_gt`, `field: createdAt`, `value: 24h` |

### Example: Event-Driven Pipeline

This pipeline triggers when an `orders-landed` sensor appears, regardless of value:

```yaml
validation:
  trigger: "ANY"
  rules:
    - key: orders-landed
      check: exists
    - key: orders-count
      check: gt
      field: count
      value: 0
```

### Example: Multi-Condition Pipeline

This pipeline requires all upstream checks to pass before triggering:

```yaml
validation:
  trigger: "ALL"
  rules:
    - key: upstream-complete
      check: equals
      field: status
      value: ready
    - key: row-count
      check: gte
      field: count
      value: 1000
    - key: freshness
      check: age_lt
      field: updatedAt
      value: 2h
```

## Retry Policy

Automatic retries on job failure are configured in the `job` block:

```yaml
job:
  type: glue
  config:
    jobName: gold-revenue-etl
  maxRetries: 2
```

| Field | Type | Default | Description |
|---|---|---|---|
| `maxRetries` | int | 0 | Maximum retry attempts after job failure |

Retries are managed by the Step Functions state machine, which re-invokes the orchestrator Lambda on failure up to `maxRetries` times.

## Pushing Sensor Data

The framework reads sensor data from DynamoDB but does not write it. External processes are responsible for pushing sensor values. Write a sensor record to the control table:

| Attribute | Value |
|---|---|
| `PK` | `PIPELINE#{pipeline-id}` |
| `SK` | `SENSOR#{key}` |
| Sensor fields | Any fields referenced by validation rules (e.g., `status`, `count`, `updatedAt`) |

When a sensor record is written, the DynamoDB stream triggers the stream-router Lambda, which starts the Step Functions execution for that pipeline.
