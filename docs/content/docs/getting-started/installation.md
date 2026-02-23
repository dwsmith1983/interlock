---
title: Installation
weight: 1
description: Prerequisites, building from source, and binary overview.
---

## Prerequisites

| Dependency | Version | Purpose |
|---|---|---|
| Go | 1.24+ | Build the CLI and Lambda handlers |
| Docker & Docker Compose | Latest | Local development (Redis + Postgres) |
| Redis | 7+ | Local provider backend |
| Terraform | 1.5+ | AWS deployment (optional) |
| AWS CLI v2 | Latest | AWS deployment (optional) |

## Build from Source

Clone the repository and build the CLI binary:

```bash
git clone https://github.com/dwsmith1983/interlock.git
cd interlock
make build
```

This produces the `interlock` binary in the project root.

### Build Targets

| Target | Description |
|---|---|
| `make build` | Build the `interlock` CLI |
| `make test` | Run all tests |
| `make test-unit` | Unit tests only |
| `make test-integration` | Integration tests (requires DynamoDB Local) |
| `make lint` | Run golangci-lint |
| `make fmt` | Format source code |
| `make dist` | Cross-compile for linux/amd64, linux/arm64, darwin/amd64, darwin/arm64 |
| `make build-lambda` | Build all 5 Lambda handler binaries |

### Lambda Binaries

The AWS deployment uses 5 Lambda handlers, each built as a separate binary:

```bash
make build-lambda
```

This runs `deploy/build.sh` and produces bootstrap binaries for:

| Handler | Path | Purpose |
|---|---|---|
| stream-router | `cmd/lambda/stream-router/` | DynamoDB Stream event routing |
| evaluator | `cmd/lambda/evaluator/` | Single trait evaluation |
| orchestrator | `cmd/lambda/orchestrator/` | Multi-action state machine dispatch |
| trigger | `cmd/lambda/trigger/` | Pipeline trigger execution |
| run-checker | `cmd/lambda/run-checker/` | Poll running job status |

## Project Layout

```
pkg/types/           Public domain types
internal/provider/   Storage interface + Redis/DynamoDB implementations
internal/engine/     Readiness evaluation engine
internal/evaluator/  Trait evaluator runners
internal/trigger/    Pipeline trigger execution
internal/alert/      Alert dispatching
internal/schedule/   Schedule, SLA, and retry utilities
internal/watcher/    Reactive evaluation loop (local mode)
internal/archiver/   Background Redis → Postgres archival
internal/lambda/     Shared Lambda initialization
cmd/lambda/          5 Lambda handler entry points
deploy/terraform/    AWS infrastructure as code
deploy/statemachine.asl.json  Step Function ASL definition
```
