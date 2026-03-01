---
title: Documentation
description: Interlock documentation -- architecture, configuration, deployment, and reference.
toc: false
---

Interlock is a STAMP-based safety framework for data pipeline reliability. It validates readiness using declarative rules against sensor data in DynamoDB, enforces SLA deadlines, and triggers pipelines automatically when all conditions pass. Deployed as a reusable Terraform module on AWS with Step Functions, Lambda, and EventBridge.

## Sections

{{< cards >}}
  {{< card link="getting-started" title="Getting Started" subtitle="Deploy the Terraform module and configure your first pipeline." >}}
  {{< card link="architecture" title="Architecture" subtitle="STAMP model, Step Functions state machine, DynamoDB tables." >}}
  {{< card link="configuration" title="Configuration" subtitle="Pipeline YAML configs, validation rules, and job triggers." >}}
  {{< card link="deployment" title="Deployment" subtitle="Deploy to AWS with the reusable Terraform module." >}}
  {{< card link="reference" title="Reference" subtitle="EventBridge event types and alerting patterns." >}}
{{< /cards >}}
