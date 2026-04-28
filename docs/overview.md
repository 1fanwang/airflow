> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

# Airflow at LinkedIn — Overview

## What It Is

Oklahoma is LinkedIn's "Pipeline-as-a-Service" platform built on Apache Airflow. It provides fully-managed Airflow clusters with LinkedIn-specific extensions for workflow authoring and orchestration of ML/data pipelines. The platform handles scheduling, execution, monitoring, and DAG lifecycle management at LinkedIn scale.

Oklahoma covers:
- **Production ML/data pipeline orchestration** — Spark, Flink, Hadoop, Trino jobs submitted via Grid Gateway (GGW)
- **Feature computation** — Feature Cloud Push, Venice ingestion, Hosted Search pipelines
- **Data quality** — DQ assertion jobs via GridGateway hooks
- **Azkaban migration** — active effort to migrate Azkaban flows to Airflow DAGs

Team contact: `#ask_airflow` on Slack; `ask_airflow@linkedin.com`; oncall at `oncall.prod.linkedin.com/team/airflow`; office hours Mon-Thu 2-3pm.

---

## Docker Images

Three Docker images are built from `oklahoma-airflow` (`airflow-main/` subdir):

| Image | Dockerfile | Purpose |
|-------|-----------|---------|
| `airflow-main-airflow-oklahoma` | `airflow-oklahoma.Dockerfile` | Production scheduler, webserver, worker, dag-processor pods. Based on `linkedin-common-image/mariner2-common`. Runs as UID 50000. |
| `airflow-main-airflow-rdev` | `airflow-rdev.Dockerfile` | RDev (remote dev) environments for DAG authors. Based on `rdev-base-image/mariner2-rdev-mysql8`. Runs as `coder` user. Contains a local MySQL and startup scripts at `~/.okl_rdev/`. |
| `airflow-main-airflow-amf-stats-client` | `airflow-amf-stats-client.Dockerfile` | Metrics sidecar. Based on `python-image/python-mariner2`. Runs as UID 50000. Ships `lipy-metrics~=18.0.x`. |

All images use Python 3.10 (`/export/apps/python/3.10/bin/python3.10`) and are registry-hosted at `container-image-registry.corp.linkedin.com`.

The production image ships **two Airflow environments**:
- **Airflow 2.5.x** at the system Python path (legacy, being phased out)
- **Airflow 2.9.2** in a venv at `/opt/airflow/airflow2.9/`

Version 2.9.2 is used for all scheduler/webserver startup via `li_airflow_scheduler_2_9.py` / `li_airflow_webserver_2_9.py`.

Provider packages installed in both environments (current 2.9 version ~10.0.x):
- `apache-airflow-providers-lnkd` — GGW operators, sensors, event listeners
- `oklahoma-helpers` — Config loader, KafkaHelper, cert utilities
- `oklahoma-listener` — DAG/task lifecycle event capture
- `oklahoma-backfill` — Backfill management
- `in-dbt` / `lipy-indbt-providers` — dbt integration
- `kube-config-scripts` — KubernetesExecutor pod cert setup
- `airflow-policy-framework` — operator policy enforcement (2.9 only)
- `kms-python-api` — Key Management Service client (v26.2.55 for 2.9)

---

## Clusters

Deployment order for releases: `grid1-test` → `ei-ltx1 (faro)` → `corp-lva1 (corp)` → `grid1-prod (holdem/dbt)` → `grid2-prod (war)` → manually tag RDev stable image.

| Cluster | Fabric / K8s | Purpose | Executor | Notes |
|---------|-------------|---------|----------|-------|
| **Holdem** | `prod-ltx1` | Primary production cluster; general-purpose ML/data pipelines | KubernetesExecutor | Highest DAG volume; federated under Tradewind logical `holdem` |
| **War** | `prod-lva1` | Secondary production cluster | KubernetesExecutor | Federated under Tradewind; distinct from holdem physical shard `war-1` |
| **Faro** | `ei-ltx1` | Staging/EI cluster; used by DAG authors for testing before prod | KubernetesExecutor | Namespace `oklahoma` in `ei-ltx1-k8s-0` |
| **Corp** | `corp-lva1` | Internal tooling, corp-facing pipelines | KubernetesExecutor | Lower volume; corp-internal users |
| **RDev** | per-user | Individual developer environments via `picli`; local MySQL + Airflow | Local | Based on `airflow-rdev` image; stable tag updated after each War deploy |
| **Test** | `prod-ltx1` | Load-testing / regression cluster | KubernetesExecutor | Used with `airflow_load_test` DB for benchmarks |
| **Lasso** | `prod-ltx1` | Dedicated Lasso workflow cluster | KubernetesExecutor | PVC at `deployment/pvc/prod-ltx1/airflow-lasso-pvc.yaml` |
| **dbt / GHD** | `grid1` | Grid integration testing and dbt workflows | KubernetesExecutor | Namespace `grid-integration-testing` |

> Note: A `prod-ltx1` cluster exists but is currently unused ("please occasionally deploy it" per oncall runbook as of writing).

---

## Key Systems It Depends On

See [Systems](systems/README.md) for full detail.

| System | Role |
|--------|------|
| **Grid Gateway (GGW)** | gRPC job submission service. All Spark, Flink, Hadoop, Trino, Java jobs are submitted via GGW operators from `apache-airflow-providers-lnkd`. |
| **Tradewind** | Federated orchestration proxy sitting in front of Holdem and War. Provides DAG routing, transparent API proxy, and a unified React UI aggregating both clusters. |
| **Kafka** | Event emission from the `oklahoma-listener` plugin; DAG/task lifecycle events published to Kafka topics. Used by downstream monitoring. |
| **Venice** | Feature store. Feature Cloud Push DAGs write to Venice via SparkBatchOperator jobs. |
| **Espresso** | Internal NoSQL store. Used by some pipeline operators for metadata/state. |
| **Spark** | Most common compute engine. Jobs submitted via `SparkBatchOperator` through GGW; runs on YARN. |
| **Azkaban** | Legacy orchestrator being migrated to Airflow. `AzkabanFlowExecutionOperator` and sensors provide interop during transition. |
| **DataVault** | Authentication token service. Used for SSO credential exchange and DataVault identity tokens for inter-service auth. |
| **KMS** | Key Management Service. Installed as `kms-python-api` in all images for secret resolution. |
| **Ambry** | Blob store. Referenced in provider config for log/artifact storage. |
| **ARMS / Jasper** | Artifact metadata and partition sensors. `SnapshotSensorDefinition` and `PartitionSensorDefinition` poll dataset availability via gRPC. |

---

## How DAGs Get Deployed

DAG deployment to production clusters follows two paths:

**1. Upload Plugin + CRT (standard)**
- DAG authors author Python DAGs in their multiproduct (MP), define an `airflow-workflow` type MP.
- On merge to master, CRT (Continuous Release Tool) invokes the `airflow-crt-action` Docker action.
- `airflow-crt-action` calls the upload REST API: `POST /api/v1/plugins/upload/upload_dags`.
- The Upload Plugin (built into the `airflow-oklahoma` image at `$AIRFLOW_HOME/plugins/upload/`) validates the ZIP:
  - Python import validation via DagBag
  - Proxy user ACL checks (SparkOperator, DarwinOperator permissions)
  - Deduplication checks
- Valid DAGs land in `/dags/<mp_name>/<application_name>/` on the cluster.

**2. Direct API Call (emergency)**
- Engineers can POST directly to the upload endpoint using a DataVault identity token obtained via `id-tool grestin sign`.
- Used when CRT is down or for urgent unblocking. See `docs/docs/crt-dag-deployment.md`.

**Environment variables that control upload behavior:**
- `AIRFLOW_UPLOAD_PLUGIN_ENABLED=True` — enables the plugin
- `DAG_ROOT_UPLOAD_ALLOW_LIST` — allowlist for root-level DAG uploads (service accounts)
- `IGNORE_IMPORT_ERRORS_MP_ALLOW_LIST` — bypass import validation for specific MPs

---

## Team Ownership

- **Team**: Oklahoma (a.k.a. BDP Compute / Pipelines)
- **Multiproduct**: `oklahoma-airflow` (image builds), `oklahoma-airflow-deployment` (Helm/K8s configs)
- **On-call**: Primary/secondary listed at `oncall.prod.linkedin.com/team/airflow`
- **Slack**: `#ask_airflow`
- **Email**: `ask_airflow@linkedin.com`
- **Admin LDAP groups**: `SGP-ENG-oklahoma-dev` (Airflow Admin role), `SGP-ENG-azkaban-dev` (Airflow Admin role)
- **Viewer groups**: spark-dev, dali-team, Grid-Observability, venice-eng, and several DPP Crew LDAP groups

---

## Common Operations

- Authoring DAGs → [DAG Authoring](dag-authoring.md)
- Deploying → [Deployment](deployment.md)
- Debugging failures → [Troubleshooting](troubleshooting.md)
- Oncall → [Oncall](oncall/README.md)

## See Also

- [Architecture](architecture.md)
- [Clusters](clusters.md)
- [DAG Authoring](dag-authoring.md)
- [Codebase Overview](codebase/README.md)
- [Tradewind](systems/tradewind.md)
- [Spark](systems/spark.md)
- [lipy-airflow-providers](systems/lipy-airflow-providers.md)
