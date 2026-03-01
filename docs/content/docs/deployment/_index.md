---
title: Deployment
weight: 4
description: Deploy Interlock to AWS using the reusable Terraform module.
---

Interlock deploys as a fully serverless AWS stack using a reusable Terraform module. The module creates all required infrastructure -- DynamoDB tables, Lambda functions, Step Functions, EventBridge, and IAM roles. Your project provides pipeline YAML configuration files; no framework code runs in your repository.

{{< cards >}}
  {{< card link="terraform" title="Terraform" subtitle="Module variables, what gets created, and deployment steps." >}}
{{< /cards >}}
