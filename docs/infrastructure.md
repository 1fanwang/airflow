# Infrastructure

> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

## Overview

Infrastructure systems used across this workspace. Detailed skill files with usage patterns and code-level conventions are in `.claude/skills/infrastructure/`. This document provides the cross-repo view.

For detailed per-system reference pages covering GGW, Spark, Kafka, and 12 other LinkedIn systems, see [Systems Reference](systems/README.md).

## Detected Systems

See `.claude/skills/infrastructure/SKILL.md` for the complete detection summary. Key systems:

| System | Used By | Skill File |
|--------|---------|------------|
| **Kafka** | lipy-airflow-providers, mufn-service, rdev-server, training-platform-agents | `kafka.md` |
| **gRPC** | mufn-service, rdev-api, bdp-artifact-metadata-service, orchestrator-tde, li-productivity-agents | `grpc.md` |
| **LiGradle** | 20+ repos | `ligradle.md` |
| **CFG2** | li-productivity-agents, tradewind, trails-tools, rdev-server, orchestrator-tde, roundup-workflows | `cfg2.md` |
| **KMS** | lipy-airflow-providers | `kms.md` |
| **CRT** | airflow-crt-action | `crt.md` |
| **LCD** | picli, tradewind, trails-tools, airflow-load-testing, orchestrator-tde, rdev-cli, roundup-workflows | `lcd.md` |
| **Grid** | lipy-airflow-providers, mufn-service | `grid.md` |
| **Spark** | lipy-airflow-providers | `spark.md` |
| **OpenTelemetry** | li-productivity-agents, mufn-service, orchestrator-tde | `observe.md` |
| **LiX** | mufn-service | `lix.md` |
| **Temporal** | li-productivity-agents | `temporal.md` |

## Deployment

### Docker Images
- **oklahoma-airflow-deployment**: Builds production and RDev Airflow images
- **rdev-base-image**: Base images for remote dev containers (Mariner2, RHEL8, CUDA variants)
- **airflow-oc-image**: Image for Flyte execution triggers from Grid Gateway

### Kubernetes
- Helm charts in `oklahoma-airflow-deployment/airflow-main/helm/`
- Kubernetes configs in `mufn-service/.linkedin/kube/`
- Multi-cluster deployment: holdem, war, faro, ei-ltx1 fabrics

### Databases
- **MySQL**: tradewind, trails-tools, rdev-server, orchestrator-tde, roundup-workflows (via SQLAlchemy + Alembic)
- **DuckDB**: trails-tools (local data warehouse)
- **H2**: mufn-service (test database)

## Configuration Management

- **CFG2**: Primary config system for services (`config/app/**/*.src` files)
- **product-spec.json**: MP metadata and dependency declarations
- **Helm values**: Kubernetes deployment configuration
