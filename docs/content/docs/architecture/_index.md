---
title: Architecture
weight: 2
description: STAMP safety model, sensor-driven validation, and AWS event-driven infrastructure.
---

Interlock applies the STAMP (Systems-Theoretic Accident Model and Processes) safety model to data pipeline operations. This section explains the core concepts and the AWS deployment architecture.

{{< cards >}}
  {{< card link="overview" title="Overview" subtitle="STAMP model, declarative validation rules, sensor-driven evaluation, and the pipeline lifecycle." >}}
  {{< card link="aws" title="AWS" subtitle="3 DynamoDB tables, 4 Lambda functions, Step Functions, EventBridge, Terraform module." >}}
  {{< card link="watchdog" title="Watchdog" subtitle="Detects stale trigger executions and missed cron schedules." >}}
{{< /cards >}}
