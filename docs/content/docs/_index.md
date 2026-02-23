---
title: Documentation
description: Interlock documentation — architecture, configuration, deployment, and API reference.
toc: false
---

Interlock is a STAMP-based safety framework for data pipeline reliability. It evaluates readiness traits before triggering pipelines, enforces SLA deadlines, and provides full lifecycle management with automatic retries and post-completion drift detection.

## Sections

{{< cards >}}
  {{< card link="getting-started" title="Getting Started" subtitle="Install, configure, and run your first readiness check." >}}
  {{< card link="architecture" title="Architecture" subtitle="STAMP model, run state machine, AWS and local backends." >}}
  {{< card link="configuration" title="Configuration" subtitle="Pipelines, schedules, triggers, and calendar exclusions." >}}
  {{< card link="deployment" title="Deployment" subtitle="Terraform for AWS, Docker Compose for local." >}}
  {{< card link="reference" title="Reference" subtitle="Provider interface, evaluator protocol, alert sinks." >}}
{{< /cards >}}
