# Architecture

> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

## Overview

This document describes the system architecture of Oklahoma Managed Airflow — LinkedIn's managed Apache Airflow platform. It covers how the 27 repositories in this workspace relate to each other, their roles, data flow, and dependency graph.

## Workspace Purpose

Oklahoma is LinkedIn's managed Apache Airflow platform providing workflow orchestration for data pipelines, ML training, and batch processing. This workspace contains the core Airflow fork, custom providers and operators, deployment infrastructure, developer tooling, and supporting services.

## Repository Map

### Core Platform

| Repository | Purpose | Language |
|------------|---------|----------|
| **airflow** | LinkedIn fork of Apache Airflow 2.9.2 — the core scheduler, executor, web UI, and CLI | Python, Node.js |
| **lipy-airflow-providers** | Custom Airflow providers, operators, hooks, and sensors for LinkedIn infrastructure (Kafka, Grid, Spark, dbt, etc.) | Python |
| **oklahoma-airflow-deployment** | Production and RDev Docker images, Helm charts, and deployment scripts for Airflow clusters | Dockerfile, YAML |
| **oklahoma_system_dags** | System-level DAGs for regression testing, maintenance, and backfill operations | Python |

### Developer Tools

| Repository | Purpose | Language |
|------------|---------|----------|
| **picli** | Pipelines CLI — DAG policy enforcement and deployment tool | Python |
| **airflow-workflow-gradle-plugin** | Gradle plugin for DAG packaging, policy enforcement, and pre-deployment checks | Java, Groovy |
| **airflow-workflow** | MP template for scaffolding new Airflow workflow multiproducts | Gradle |
| **airflow_starter_kit** | Example DAG repository demonstrating operators, sensors, and best practices | Python |
| **airflow-crt-action** | CRT (Change Request Tracker) GitHub Action for DAG deployment workflows | Python |
| **airflow-docs** | Docusaurus documentation site for Airflow and Oklahoma platform | JavaScript, MDX |
| **airflow-load-testing** | Load testing and performance benchmarking for Airflow clusters | Python |

### AI and Automation

| Repository | Purpose | Language |
|------------|---------|----------|
| **airflow-autopilot** | Agentic DAG authoring and quality scoring — scores DAGs against quality dimensions | Python |
| **torch-autopilot** | TensorFlow to PyTorch conversion skill with iterative refinement and quality scoring | Python |
| **li-productivity-agents** | DPX platform for building developer productivity AI agents (go/lipa) | Python |
| **training-platform-agents** | AI agents for training failure analysis, workflow debugging, and optimization | Python |

### Infrastructure Services

| Repository | Purpose | Language |
|------------|---------|----------|
| **mufn-service** | Grid Gateway — unified control plane for batch jobs via gRPC API (Pekko actors, Kafka) | Scala |
| **bdp-artifact-metadata-service** | ARMS — artifact metadata service for dataset sensors (Dali, Hive, HDFS metadata) | Java |
| **tradewind** | Federated orchestration — unified Router API and React UI aggregating DAGs across clusters | Python, TypeScript |
| **trails-tools** | Data infrastructure analysis platform — DAG/Flyte/Trino analytics with AI failure analysis | Python, TypeScript |
| **orchestrator-tde** | Talent insights for orchestrator platforms — surfaces employee activity metrics via gRPC | Python |
| **gdp-sales-analytics** | GTM Data Platform analytics — dashboards and notebooks for sales data science | Python |
| **roundup-workflows** | Airflow DAGs for RoundUp risk detection and mitigation workflows | Python |

### Remote Development (RDev)

| Repository | Purpose | Language |
|------------|---------|----------|
| **rdev-api** | gRPC and REST API definitions for the rdev ecosystem | Java, Python |
| **rdev-server** | Backend service — Flask webapp, scheduler, and Kafka consumer for rdev environments | Python |
| **rdev-cli** | CLI tool for managing remote development environments | Python |
| **rdev-base-image** | Base Docker images for rdev containers (Mariner2, RHEL8, CUDA variants) | Dockerfile |

### ML Pipeline Integration

| Repository | Purpose | Language |
|------------|---------|----------|
| **airflow-oc-image** | CLI for triggering Flyte executions from Grid Gateway pods | Python |

## Data Flow

```
DAG Authors
    │
    ▼
picli / airflow-workflow-gradle-plugin (policy enforcement + packaging)
    │
    ▼
airflow-crt-action (CRT deployment)
    │
    ▼
oklahoma-airflow-deployment (Docker images + Helm charts)
    │
    ▼
airflow (core scheduler/executor) ◄── lipy-airflow-providers (custom operators)
    │                                       │
    ├── Grid Gateway (mufn-service)         ├── Kafka producers/consumers
    ├── Spark jobs via Grid                 ├── Flyte triggers (airflow-oc-image)
    ├── Dataset sensors (bdp-artifact-      └── dbt, Azkaban integrations
    │   metadata-service)
    │
    ▼
tradewind (federated UI) ◄── trails-tools (analytics + AI failure analysis)
```

### Key Integration Points

- **airflow → lipy-airflow-providers**: Core Airflow loads custom operators, hooks, and sensors at runtime
- **lipy-airflow-providers → mufn-service**: Grid Gateway operators submit batch jobs via gRPC
- **lipy-airflow-providers → bdp-artifact-metadata-service**: Dataset sensors query ARMS for partition metadata
- **picli → airflow-crt-action**: DAG policy enforcement triggers CRT deployment
- **tradewind → airflow**: Router API aggregates DAG metadata from multiple Airflow clusters
- **trails-tools → airflow/tradewind**: Analytics platform queries Airflow APIs and Trino for DAG insights
- **rdev-cli → rdev-server → rdev-api**: CLI communicates with server via gRPC definitions from rdev-api
- **airflow-autopilot → lipy-airflow-providers**: Scores DAGs using provider policy framework

## Dependency Graph (from product-spec.json)

Key internal dependencies:
- `lipy-airflow-providers` depends on: `airflow` (fork), `lipy-kafka`, `lipy-web`, `lipy-fabric`
- `oklahoma-airflow-deployment` depends on: `lipy-airflow-providers`, `python-image`
- `picli` depends on: `lipy-airflow-providers`, `airflow-workflow-gradle-plugin`
- `airflow-crt-action` depends on: `lipy-airflow-providers`, `lipy-orch-actions`
- `mufn-service` depends on: LinkedIn Kafka clients, gRPC infra, Azkaban
- `rdev-server` depends on: `rdev-api`, `lipy-kafka`, `lipy-flask-sqlalchemy`
- `tradewind` depends on: `lipy-config-base`, `lipy-datavault`, `lipy-web`

## Self-Documenting Repos

These repos have `.linkedin/ai-agent/` directories with their own implementation docs. For internal architecture, patterns, and testing details, read their docs directly:

| Repository | Topics Covered |
|------------|---------------|
| lipy-airflow-providers | MP rules, testing patterns |
| oklahoma-airflow-deployment | Version bumping procedures |
| airflow-docs | Content conventions |
| airflow-autopilot | Architecture, scoring, CLI, testing |
| picli | Code patterns, testing |
| orchestrator-tde | Architecture, testing, gRPC patterns |
| airflow-oc-image | Build and testing |
