# Glossary

> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

## Overview

Domain-specific terms, abbreviations, and internal jargon used across this workspace. Refer here when encountering unfamiliar terms in code, configs, or documentation.

## Platform and Product Terms

| Term | Definition |
|------|-----------|
| **Oklahoma** | LinkedIn's managed Apache Airflow platform. The name for the overall system this workspace builds. |
| **DAG** | Directed Acyclic Graph — an Airflow workflow definition that specifies tasks and their dependencies. |
| **Operator** | An Airflow task type that performs a specific action (e.g., SparkBatchOperator, KafkaProducerOperator). |
| **Sensor** | An Airflow operator that waits for a condition to be met before proceeding (e.g., dataset availability). |
| **Provider** | An Airflow package that extends functionality with custom operators, hooks, and sensors. `lipy-airflow-providers` is LinkedIn's provider package. |
| **Hook** | An Airflow interface for connecting to external systems (databases, APIs, file storage). |

## Infrastructure Terms

| Term | Definition |
|------|-----------|
| **MP / Multiproduct** | LinkedIn's unit of code organization — a repository with `product-spec.json` defining metadata, dependencies, and build configuration. |
| **LiGradle** | LinkedIn's customized Gradle build system. All Java/Kotlin MPs use `ligradle-core` plugin. |
| **CFG2** | LinkedIn's configuration management system. Config files use `.src` extension in `config/app/` directories with dimension-based overrides. |
| **CRT** | Change Request Tracker — gates production deployments, requiring approval before changes go live. |
| **LCD** | LinkedIn Continuous Delivery — CI/CD pipeline configuration stored in `.linkedin/lcd/` directories. |
| **Grid / Grid Gateway** | LinkedIn's compute platform for batch job submission. `mufn-service` is the unified control plane. |
| **KMS** | Key Management Service — LinkedIn's secret management and encryption service. |
| **Apollo** | LinkedIn's service lifecycle manager — used by Python webapps for controller registration and health checks. |
| **Fabric** | A deployment environment/datacenter (e.g., holdem, war, faro, ei-ltx1). |
| **LiX / T-REX** | LinkedIn's experimentation platform for feature flags and A/B testing. |
| **Nephos Temporal** | LinkedIn's hosted Temporal service for durable workflow orchestration. |

## Service and Tool Names

| Term | Definition |
|------|-----------|
| **MUFN** | Grid Gateway service (`mufn-service`) — unified control plane for batch jobs using Pekko actors and gRPC. |
| **ARMS** | Artifact Metadata Service (`bdp-artifact-metadata-service`) — fetches table/partition/snapshot metadata for dataset sensors. |
| **Tradewind** | Federated orchestration platform providing a unified Router API and React UI across multiple Airflow clusters. |
| **Trails** | Data infrastructure analysis platform (`trails-tools`) — DAG/Flyte/Trino analytics with AI-powered failure analysis. |
| **Picli** | Pipelines CLI — DAG policy enforcement and deployment tool. |
| **RDev** | Remote Development — LinkedIn's system for remote development environments (`rdev-api`, `rdev-server`, `rdev-cli`, `rdev-base-image`). |
| **RoundUp** | Risk detection and mitigation system with Airflow DAG workflows. |
| **Orchestrator-TDE** | Talent insights service surfacing employee activity metrics from orchestrator platforms. |
| **LIPA** | LinkedIn Productivity Agents (`li-productivity-agents`) — platform for developer AI agents at go/lipa. |
| **MAE** | Multi-Agent Engineering — CLI and framework within li-productivity-agents. |

## AI and Automation Terms

| Term | Definition |
|------|-----------|
| **Airflow Autopilot** | Quality scoring tool that grades DAGs against dimensions (correctness, performance, security, etc.) before deployment. |
| **Torch Autopilot** | Automated TensorFlow-to-PyTorch conversion tool using iterative refinement with 5-dimension quality scoring. |
| **Captain** | LinkedIn's MCP (Model Context Protocol) server providing tools and context to Claude Code. |
| **FastMCP** | Framework for building MCP servers used in `training-platform-agents`. |

## Data Terms

| Term | Definition |
|------|-----------|
| **Dali** | LinkedIn's data access library for reading datasets. Used by ARMS and Airflow operators. |
| **Darwin** | LinkedIn's notebook environment for data science. Used by `gdp-sales-analytics`. |
| **Trino** | Distributed SQL query engine used by trails-tools and gdp-sales-analytics for analytics queries. |
| **Flyte** | Workflow orchestration platform for ML pipelines. `airflow-oc-image` bridges Airflow and Flyte. |
| **KingKonG** | LinkedIn's training infrastructure. `training-platform-agents` analyzes KingKonG failures. |
