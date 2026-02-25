---
title: Architecture
weight: 2
description: STAMP safety model, run state machine, and dual-backend design.
---

Interlock applies the STAMP (Systems-Theoretic Accident Model and Processes) safety model to data pipeline operations. This section explains the core concepts and both deployment architectures.

{{< cards >}}
  {{< card link="overview" title="Overview" subtitle="STAMP model, three-level checks, and the run state machine." >}}
  {{< card link="aws" title="AWS" subtitle="DynamoDB single-table, Step Functions, and 6 Lambda handlers." >}}
  {{< card link="local" title="Local" subtitle="Redis provider, watcher loop, Postgres archiver, Docker Compose." >}}
  {{< card link="watchdog" title="Watchdog" subtitle="Absence detection for silently missed pipeline schedules." >}}
{{< /cards >}}
