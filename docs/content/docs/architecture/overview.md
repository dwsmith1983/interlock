---
title: Overview
weight: 1
description: STAMP safety model, three-level checks, and the run state machine.
---

## STAMP Model

Interlock applies [STAMP](https://psas.scripts.mit.edu/home/stamp/) (Systems-Theoretic Accident Model and Processes) to data pipeline operations. In STAMP, accidents result from inadequate enforcement of safety constraints — not just component failures. Interlock maps this to data pipelines:

| STAMP Concept | Interlock Mapping |
|---|---|
| Safety constraint | Trait (readiness check) |
| Control structure | Archetype (template of required checks) |
| Controller | Engine (evaluates traits, enforces rules) |
| Controlled process | Pipeline (data transformation job) |
| Feedback | Events, run state, SLA monitoring |
| Absence of control action | Watchdog (missed schedule detection) |

## Three-Level Check System

Interlock evaluates pipelines through three levels of safety checks:

### Level 1 — Trait Evaluation

Individual traits are evaluated by external programs (subprocess, HTTP, or builtin handlers). Each trait returns one of:

| Status | Meaning |
|---|---|
| `PASS` | Safety check satisfied |
| `FAIL` | Safety check not satisfied |
| `STALE` | Previous result expired (TTL exceeded) |

Traits have configurable TTL (time-to-live) so results are cached and re-evaluated only when stale.

### Level 2 — Readiness Rule

An archetype's `readinessRule` combines individual trait results into a pipeline-level decision. The default rule `all-required-pass` requires every required trait to have `PASS` status:

- **READY** — all required traits pass → trigger the pipeline
- **NOT_READY** — one or more required traits fail or are stale → wait and re-evaluate

### Level 3 — SLA Enforcement

Time-based constraints ensure pipelines complete within acceptable windows:

- **Evaluation SLA** — deadline by which all traits must reach READY
- **Completion SLA** — deadline by which the triggered job must finish
- **Validation timeout** — hard stop that prevents indefinite polling

Breaching an SLA fires an alert but does not cancel the pipeline.

## Run State Machine

Every pipeline run follows a deterministic state machine:

```
PENDING ──→ TRIGGERING ──→ RUNNING ──→ COMPLETED
                │              │            │
                │              │            ↓
                │              │    COMPLETED_MONITORING ──→ COMPLETED
                │              │
                ↓              ↓
             FAILED         FAILED / CANCELLED
```

| State | Description |
|---|---|
| `PENDING` | Run created, traits being evaluated |
| `TRIGGERING` | All traits passed, trigger executing |
| `RUNNING` | Trigger succeeded, job in progress |
| `COMPLETED` | Job finished successfully |
| `COMPLETED_MONITORING` | Job finished, post-completion drift monitoring active |
| `FAILED` | Job or trigger failed |
| `CANCELLED` | Run manually cancelled |

State transitions use **compare-and-swap** (CAS) with a version counter to prevent race conditions. Every transition increments the version; a concurrent update with a stale version fails cleanly.

## Concurrency Model

Interlock uses distributed locking to ensure exactly one evaluation loop runs per pipeline per schedule window:

- **Lock key format**: `eval:{pipelineID}:{scheduleID}`
- **Lock TTL**: computed from trait count and maximum evaluation timeout, plus a buffer
- **Mechanism**: `AcquireLock` (atomic set-if-not-exists) + `ReleaseLock`

On AWS, DynamoDB conditional writes provide the same semantics. Locally, Redis `SETNX` with TTL handles lock acquisition.

## Event Sourcing

All state changes emit immutable events to an append-only stream:

| Event Kind | Trigger |
|---|---|
| `TRAIT_EVALUATED` | Trait evaluation completes |
| `READINESS_CHECKED` | Readiness rule evaluated |
| `RUN_STATE_CHANGED` | State machine transition |
| `TRIGGER_FIRED` | Trigger execution starts |
| `TRIGGER_FAILED` | Trigger execution fails |
| `SLA_BREACHED` | SLA deadline exceeded |
| `RETRY_SCHEDULED` | Automatic retry queued |
| `RETRY_EXHAUSTED` | All retry attempts consumed |
| `MONITORING_DRIFT_DETECTED` | Post-completion trait regression |
| `SCHEDULE_MISSED` | Pipeline schedule passed without evaluation |

Events support the archiver (Redis → Postgres) and external observability integrations.
