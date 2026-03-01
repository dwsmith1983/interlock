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
  Declarative validation rules, SLA enforcement, and automated pipeline triggering&mdash;deployed to AWS with DynamoDB, Step Functions, and EventBridge.
{{< /hextra/hero-subtitle >}}
</div>

<div class="hx-mb-6">
{{< hextra/hero-button text="Get Started" link="docs/getting-started/quickstart" >}}
</div>

<div class="hx-mt-6"></div>

{{< hextra/feature-grid >}}
  {{< hextra/feature-card
    title="Getting Started"
    subtitle="Deploy the Terraform module, write pipeline configs, and push sensor data."
    link="docs/getting-started"
    style="background: radial-gradient(ellipse at 50% 80%,rgba(59,130,246,0.15),hsla(0,0%,100%,0));"
  >}}
  {{< hextra/feature-card
    title="Architecture"
    subtitle="Understand the STAMP safety model, Step Functions state machine, and DynamoDB table design."
    link="docs/architecture"
    style="background: radial-gradient(ellipse at 50% 80%,rgba(16,185,129,0.15),hsla(0,0%,100%,0));"
  >}}
  {{< hextra/feature-card
    title="Configuration"
    subtitle="Pipeline YAML configs, validation rules, SLA deadlines, and job trigger types."
    link="docs/configuration"
    style="background: radial-gradient(ellipse at 50% 80%,rgba(245,158,11,0.15),hsla(0,0%,100%,0));"
  >}}
  {{< hextra/feature-card
    title="Deployment"
    subtitle="Deploy to AWS with the reusable Terraform module."
    link="docs/deployment"
    style="background: radial-gradient(ellipse at 50% 80%,rgba(139,92,246,0.15),hsla(0,0%,100%,0));"
  >}}
  {{< hextra/feature-card
    title="Reference"
    subtitle="EventBridge event types, alerting patterns, and observability integration."
    link="docs/reference"
    style="background: radial-gradient(ellipse at 50% 80%,rgba(236,72,153,0.15),hsla(0,0%,100%,0));"
  >}}
{{< /hextra/feature-grid >}}
