---
name: Infrastructure Overview
description: Summary of all detected LinkedIn infrastructure systems and cross-cutting patterns in the oklahoma-managed-airflow workspace
---

# Infrastructure Systems Overview

This workspace (27 multiproducts) uses **13 LinkedIn infrastructure systems** across streaming, RPC, build, config, security, deployment, compute, observability, experimentation, and orchestration.

## Detected Systems

| System | Category | Key MPs |
|--------|----------|---------|
| [Kafka](kafka.md) | Streaming | lipy-airflow-providers, mufn-service |
| [gRPC](grpc.md) | RPC | mufn-service, rdev-api, bdp-artifact-metadata-service, li-productivity-agents |
| [LiGradle](ligradle.md) | Build | 20+ MPs with build.gradle |
| [Multiproduct](multiproduct.md) | Build | All 27 repos (product-spec.json) |
| [CFG2](cfg2.md) | Config | li-productivity-agents |
| [KMS](kms.md) | Security | lipy-airflow-providers |
| [CRT](crt.md) | Deployment | airflow-crt-action |
| [LCD](lcd.md) | Deployment | picli, tradewind, trails-tools, airflow-load-testing, orchestrator-tde, rdev-cli, roundup-workflows |
| [Grid](grid.md) | Compute | lipy-airflow-providers, mufn-service |
| [Spark](spark.md) | Compute | lipy-airflow-providers |
| [Observe/OTel](observe.md) | Observability | li-productivity-agents, mufn-service |
| [LiX](lix.md) | Experimentation | mufn-service |
| [Nephos Temporal](temporal.md) | Orchestration | li-productivity-agents |

## Cross-Cutting Patterns

### Build System
- All MPs use LiGradle with `product-spec.json` for MP metadata
- Java/Kotlin MPs use `build.gradle` + `settings.gradle` with `ligradle-core` plugin
- Python MPs use `lipy` tooling (lipy-cli, lipy-inops)

### Configuration
- CFG2 for application config (`config/app/**/*.src` files)
- Environment-specific overrides via CFG2 dimensions

### Deployment
- CRT (Change Request Tracker) for production deployments via `airflow-crt-action`
- LCD (LinkedIn Continuous Delivery) configs in `.linkedin/lcd/` for CLI tools and services

### Observability
- OpenTelemetry SDK for metrics and tracing
- Custom metric exporters in `li-productivity-agents`

### Security
- KMS for secret management and encryption
- gRPC with mTLS for service-to-service communication
