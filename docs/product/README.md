# Product

> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

## Overview

This directory contains product context for Oklahoma Managed Airflow — the what and why behind the platform.

## Product Purpose

Oklahoma is LinkedIn's managed Apache Airflow platform. It provides:

- **Workflow orchestration** for data pipelines, ML training, and batch processing
- **Custom operators and sensors** for LinkedIn infrastructure (Kafka, Grid, Spark, Dali, Flyte)
- **Multi-cluster deployment** across holdem, war, faro, and ei-ltx1 fabrics
- **Developer tooling** for DAG authoring, quality scoring, and deployment

## Users

- **Data Engineers**: Build and operate data pipelines using Airflow DAGs
- **ML Engineers**: Orchestrate training workflows, integrate with Flyte and Grid
- **Platform Engineers**: Maintain the Airflow infrastructure, deployment, and tooling
- **Data Scientists**: Run analytics workloads via DAGs and notebooks (gdp-sales-analytics)

## Domain Model

- **DAG**: A workflow definition with tasks and dependencies
- **Operator**: A task type performing a specific action
- **Sensor**: A task that waits for a condition
- **Provider**: A package extending Airflow with custom operators/hooks/sensors
- **Cluster**: A deployed Airflow instance (holdem, war, faro, ei-ltx1)
- **Fabric**: A deployment environment/datacenter

## Feature Areas

| Area | Key Repos |
|------|-----------|
| Core Scheduling | airflow, lipy-airflow-providers |
| Deployment | oklahoma-airflow-deployment, airflow-crt-action |
| Developer Experience | picli, airflow-workflow-gradle-plugin, airflow_starter_kit, airflow-docs |
| Quality & AI | airflow-autopilot, torch-autopilot |
| Federated UI | tradewind |
| Analytics & Observability | trails-tools, orchestrator-tde |
| Compute Integration | mufn-service, airflow-oc-image |
| Remote Development | rdev-api, rdev-server, rdev-cli, rdev-base-image |
