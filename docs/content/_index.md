---
title: Interlock
layout: hextra-home
toc: false
---

{{< hextra/hero-badge link="https://github.com/dwsmith1983/interlock" >}}
  <span>GitHub</span>
  {{< icon name="arrow-circle-right" attributes="height=14" >}}
{{< /hextra/hero-badge >}}

<div class="hx-mt-6 hx-mb-6">
{{< hextra/hero-headline >}}
  STAMP-Based Safety Framework&nbsp;<br class="sm:hx-block hx-hidden" />for Data Pipelines
{{< /hextra/hero-headline >}}
</div>

<div class="hx-mb-12">
{{< hextra/hero-subtitle >}}
  Evaluate readiness traits, enforce SLA deadlines, trigger pipelines&mdash;locally with Redis or on AWS with DynamoDB and Step Functions.
{{< /hextra/hero-subtitle >}}
</div>

<div class="hx-mb-6">
{{< hextra/hero-button text="Get Started" link="docs/getting-started/quickstart" >}}
</div>

<div class="hx-mt-6"></div>

{{< hextra/feature-grid >}}
  {{< hextra/feature-card
    title="Getting Started"
    subtitle="Install Interlock, configure your first pipeline, and run a readiness check in minutes."
    link="docs/getting-started"
    style="background: radial-gradient(ellipse at 50% 80%,rgba(59,130,246,0.15),hsla(0,0%,100%,0));"
  >}}
  {{< hextra/feature-card
    title="Architecture"
    subtitle="Understand the STAMP safety model, run state machine, and dual-backend design (Redis / DynamoDB)."
    link="docs/architecture"
    style="background: radial-gradient(ellipse at 50% 80%,rgba(16,185,129,0.15),hsla(0,0%,100%,0));"
  >}}
  {{< hextra/feature-card
    title="Configuration"
    subtitle="Pipelines, archetypes, multi-schedule, SLA deadlines, calendar exclusions, and 8 trigger types."
    link="docs/configuration"
    style="background: radial-gradient(ellipse at 50% 80%,rgba(245,158,11,0.15),hsla(0,0%,100%,0));"
  >}}
  {{< hextra/feature-card
    title="Deployment"
    subtitle="Deploy to AWS with Terraform or run locally with Docker Compose."
    link="docs/deployment"
    style="background: radial-gradient(ellipse at 50% 80%,rgba(139,92,246,0.15),hsla(0,0%,100%,0));"
  >}}
  {{< hextra/feature-card
    title="Reference"
    subtitle="Provider interface, evaluator protocol, alert sinks, and complete API surface."
    link="docs/reference"
    style="background: radial-gradient(ellipse at 50% 80%,rgba(236,72,153,0.15),hsla(0,0%,100%,0));"
  >}}
{{< /hextra/feature-grid >}}
