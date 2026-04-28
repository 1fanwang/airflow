> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

# Glossary

## Platform and Product Terms

| Term | Definition |
|------|-----------|
| **Oklahoma** | LinkedIn's managed Apache Airflow platform — the name for the overall system and team. |
| **DAG** | Directed Acyclic Graph — an Airflow workflow definition specifying tasks and their dependencies. |
| **Operator** | An Airflow task type that performs a specific action (e.g., `SparkBatchOperator`, `KafkaProducerOperator`). |
| **Sensor** | An Airflow operator that waits for a condition before proceeding (e.g., dataset partition availability). |
| **Provider** | An Airflow package extending functionality with custom operators, hooks, and sensors. `lipy-airflow-providers` is LinkedIn's provider package. |
| **Hook** | An Airflow interface for connecting to external systems (databases, APIs, file storage). |

---

## Infrastructure Terms

| Term | Definition |
|------|-----------|
| **MP / Multiproduct** | LinkedIn's unit of code organization — a repository with `product-spec.json` defining metadata, dependencies, and build configuration. |
| **LiGradle** | LinkedIn's customized Gradle build system. All Java/Kotlin MPs use the `ligradle-core` plugin. |
| **CFG2** | LinkedIn's configuration management system. Config files use `.src` extension in `config/app/` with dimension-based overrides. |
| **CRT** | Change Request Tracker — gates production deployments, requiring approval before changes go live. |
| **LCD** | LinkedIn Continuous Delivery — CI/CD pipeline configuration stored in `.linkedin/lcd/` directories. |
| **Grid / Grid Gateway** | LinkedIn's compute platform for batch job submission. `mufn-service` (GGW) is the unified control plane. |
| **KMS** | Key Management Service — LinkedIn's secret management and encryption service. |
| **Apollo** | LinkedIn's service lifecycle manager — used by Python webapps for controller registration and health checks. |
| **Fabric** | A deployment environment / datacenter (e.g., holdem, war, faro, ei-ltx1). Maps loosely to a cluster. |
| **LiX / T-REX** | LinkedIn's experimentation platform for feature flags and A/B testing. |
| **Nephos Temporal** | LinkedIn's hosted Temporal service for durable workflow orchestration. |
| **NKS / Nimbus** | LinkedIn's next-generation Kubernetes platform (Nimbus K8s Service), replacing legacy LKS. See [Deployment Changelog](codebase/deployment-changelog.md). |
| **DataVault (DV)** | LinkedIn's identity token service. DV tokens are used for API authentication across internal services. |
| **Grestin** | LinkedIn's certificate management system — issues internal TLS certs. Ambassador/cluster setups may or may not use Grestin-managed certs. |
| **D2 / Disco** | LinkedIn's service discovery system. Services are referenced as `d2://<ServiceName>`. |
| **Trust Bridge (TB)** | Auth proxy that forwards service identity along HTTP request paths. |

---

## Service and Tool Names

| Term | Definition |
|------|-----------|
| **MUFN / GGW** | Grid Gateway service (`mufn-service`) — unified control plane for batch jobs using Pekko actors and gRPC. |
| **ARMS** | Artifact Metadata Service (`bdp-artifact-metadata-service`) — fetches table/partition/snapshot metadata for dataset sensors. |
| **Tradewind** | Federated orchestration platform: unified Router API and React UI aggregating DAGs across clusters. |
| **Trails** | Data infrastructure analysis platform (`trails-tools`) — DAG/Flyte/Trino analytics with AI failure analysis. |
| **Picli** | Pipelines CLI — DAG policy enforcement and deployment tool. |
| **RDev** | Remote Development — LinkedIn's system for remote dev environments (`rdev-api`, `rdev-server`, `rdev-cli`, `rdev-base-image`). |
| **RoundUp** | Risk detection and mitigation system with Airflow DAG workflows (`roundup-workflows`). |
| **Orchestrator-TDE** | Talent insights service surfacing employee activity metrics from orchestrator platforms. |
| **LIPA** | LinkedIn Productivity Agents (`li-productivity-agents`) — platform for developer AI agents at go/lipa. |
| **MAE** | Multi-Agent Engineering — CLI and framework within `li-productivity-agents`. |
| **Captain** | LinkedIn's MCP (Model Context Protocol) server providing tools and context to Claude Code. |
| **IRIS** | Incident routing / alerting system. Airflow triggers IRIS callbacks on task/DAG failure. See [IRIS](systems/iris.md). |
| **PipelineMD** | Pipeline Metadata service — provides diagnostic URLs shown in Airflow task logs and error banners. |
| **InLogs / Kusto** | LinkedIn's log ingestion / analytics stack. Airflow task logs flow here; Kusto is the query layer. |

---

## AI and Automation Terms

| Term | Definition |
|------|-----------|
| **Airflow Autopilot** | Quality scoring tool that grades DAGs against dimensions (correctness, performance, security, etc.) before deployment. |
| **Torch Autopilot** | Automated TensorFlow-to-PyTorch conversion tool with iterative refinement and 5-dimension quality scoring. |
| **FastMCP** | Framework for building MCP servers (used in `training-platform-agents`). |

---

## Data Terms

| Term | Definition |
|------|-----------|
| **Dali** | LinkedIn's data access library for reading datasets. Used by ARMS and Airflow operators. |
| **Darwin** | LinkedIn's notebook environment for data science. Used by `gdp-sales-analytics` and the `DarwinOperator`. |
| **Trino** | Distributed SQL query engine used by trails-tools and gdp-sales-analytics for analytics queries. |
| **Flyte** | Workflow orchestration platform for ML pipelines. `airflow-oc-image` bridges Airflow and Flyte. |
| **KingKonG** | LinkedIn's training infrastructure. `training-platform-agents` analyzes KingKonG failures. |
| **UMP** | Unified Metadata Platform — provides dataset partition metadata used by sensors. |

---

## See Also
- [Systems](systems/README.md) — One-line summary of every LinkedIn system the wiki knows about
- [GGW](systems/ggw.md) — Grid Gateway reference
- [Trust Bridge](systems/trustbridge.md) — Trust Bridge / DataVault auth
- [D2](systems/d2.md) — D2 service discovery
- [IRIS](systems/iris.md) — IRIS alert routing
