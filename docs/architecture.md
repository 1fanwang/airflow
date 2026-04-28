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

## Policy Enforcement Layer

DAG policy enforcement runs in two systems within `lipy-airflow-providers/airflow-policy-framework`:

### Old: @DagPolicy Framework

The original framework in `policies/lnkd/dag/validation.py` and `alerting.py` uses the `@DagPolicy` decorator. It currently has 6 registered checks:

1. `dag_id_naming_convention` -- naming conventions with exemption lists
2. `dagrun_timeout_validation` -- task timeout < dag timeout
3. `deprecated_operator_validation` -- blocks banned operators
4. `disallowed_dag_access_role_validation` -- blocks unsafe access roles
5. `airflow_prod_on_failure_alert` -- composite IRIS/FlowSentinel callback check
6. `airflow_prod_sla_miss_alert` -- composite dagrun_timeout/FlowSentinel SLA check

These checks raise `PolicyViolation` on failure (hard enforcement). `picli` loads and runs them at build time via DagBag.

### New: native/ Module

The `native/` module implements Airflow's built-in cluster policy hooks (`dag_policy`, `task_policy`). It currently has 6 soft checks that use `logger.warning()` for advisory enforcement rather than raising exceptions. This enables the same policy logic to run both locally via picli and server-side on the Airflow scheduler.

Reference PRs:
- lipy-airflow-providers #1182 (native Phase 1), #1185 (soft checks)
- picli #680 (adapter), #681 (wire soft checks)

The long-term plan is to converge both systems so that all checks run as native cluster policies (see DD-005 in [design-decisions/](design-decisions/README.md)).

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

## Runtime Architecture

### Component Map

All clusters run on Kubernetes. The Helm chart at `.linkedin/kube/airflow/` defines the following deployable components:

| Component | K8s Kind | Enabled by Default | Notes |
|-----------|----------|--------------------|-------|
| **scheduler** | Deployment | Yes | LinkedIn custom launcher (`li_airflow_scheduler_2_9.py`); 1 replica; 1 CPU / 2 Gi |
| **webserver** | Deployment | Yes | LinkedIn custom launcher (`li_airflow_webserver_2_9.py`) with SSO; 1 replica; 1 CPU / 2 Gi; port 8080 |
| **dag-processor** | Deployment | Yes | Standalone DAG parsing process (Airflow 2.9 architecture); 1 replica; 2 CPU limit / 2 Gi |
| **workers** | StatefulSet | Yes (CeleryExecutor) | Celery workers (prod); StatefulSet with 100 Gi PVC; 600s termination grace |
| **triggerer** | Deployment | No (disabled) | Handles deferred operators; available but not enabled in current configs |
| **flower** | Deployment | No (disabled) | Celery task monitoring UI; available but not enabled |
| **redis** | StatefulSet | No | Celery broker; chart-provisioned Redis 6 (buster); disabled — external Redis used in prod |
| **pgbouncer** | Deployment | No | PostgreSQL connection pooler; not used (clusters use MySQL + ProxySQL) |
| **amf-stats-client** | Sidecar/DaemonSet | Configurable | Legacy AMF metrics sidecar (`airflow-main-airflow-amf-stats-client:0.0.299`); disabled on holdem/war/faro/corp since Aug 2025 (replaced by OTEL). Still active on DBT cluster. |

#### Init Containers

Every scheduler, webserver, dag-processor, and worker pod runs a `k8s-lare` init container that fetches SPIFFE/MP/DAG certificates before the main container starts:
- `k8s-lare request-app-cert` for `airflow-main-airflow-oklahoma` (platform certs)
- `k8s-lare request-app-cert` for `${SELF_DAG_ID}` (per-DAG certs, workers only)
- `k8s-lare request-app-cert` for `${SELF_MP_NAME}` (per-MP certs, workers only)

Certs land in emptyDir volumes (`/var/cluster/oklahoma`, `/var/cluster`, `/var/cluster/mp`) and are passed to Grid Gateway for proxy-user auth.

---

### Executors Per Cluster

| Cluster | Executor | Rationale |
|---------|----------|-----------|
| Holdem | `KubernetesExecutor` | Each task gets its own pod; scales to workload; no persistent workers needed |
| War | `KubernetesExecutor` | Same |
| Faro (EI) | `KubernetesExecutor` | Default in `ei-ltx1/values.yaml` |
| Corp | `KubernetesExecutor` | Default in `corp-lva1/values.yaml` |
| Test / Lasso | `KubernetesExecutor` | Default in `prod-ltx1/values.yaml` |
| GHD / dbt | `KubernetesExecutor` | Explicit in `grid1/ghd/values.yaml`; namespace `grid-integration-testing` |
| RDev | `LocalExecutor` (implicit) | Single-user local Airflow inside an RDev container |

The base helm chart `values.yaml` defaults to `CeleryExecutor` (inherited from the OSS Airflow chart), but **all production fabrics override this to `KubernetesExecutor`** via their fabric-level `values.yaml`. There are no production clusters currently using CeleryExecutor.

---

### Database Layer

#### Metadata DB

- **Engine**: MySQL 8 (all clusters)
- **Connection**: `mysql+mysqldb://` via SQLAlchemy; SSL disabled in dev, configured per-cluster in prod
- **Connection pooling**: ProxySQL sits in front of the MySQL metadata DB in production environments to multiplex Airflow connections and reduce DB load. (Note: PgBouncer is present in the Helm chart config but disabled — it is a PostgreSQL pooler and not used since the DB is MySQL.)

#### Redis (Celery Broker)

Not currently used in production (all clusters use KubernetesExecutor). The chart includes Redis configuration for potential CeleryExecutor use.

#### Result Backend

Shared MySQL DB, accessed via `db+mysql+mysqldb://` scheme (Airflow 2.4+ auto-derives from `sql_alchemy_conn`).

---

### Helm Chart Structure

Source: `oklahoma-airflow-deployment/.linkedin/kube/airflow/`

```
.linkedin/kube/airflow/
├── Chart.yaml            # Chart metadata (name: airflow)
├── values.yaml           # Base defaults (CeleryExecutor, mysql, disabled optional components)
├── values/
│   ├── ei-ltx1/
│   │   ├── values.yaml         # Fabric override: KubernetesExecutor, ei-ltx1 k8s-lare certs
│   │   └── faro/values.yaml    # Cluster override for faro
│   ├── prod-ltx1/
│   │   ├── values.yaml         # Fabric override: KubernetesExecutor, prod-ltx1 k8s-lare certs
│   │   ├── holdem/values.yaml  # Cluster override (parallelism, db connection, etc.)
│   │   ├── lasso/values.yaml
│   │   ├── load-test/values.yaml
│   │   └── test/values.yaml
│   ├── prod-lva1/
│   │   ├── values.yaml         # Fabric override: KubernetesExecutor
│   │   └── war/values.yaml
│   ├── corp-lva1/
│   │   ├── values.yaml         # Fabric override: KubernetesExecutor
│   │   └── corp/values.yaml
│   └── grid1/
│       └── ghd/values.yaml     # namespaceOverride: grid-integration-testing, KubernetesExecutor
├── templates/            # K8s manifests (scheduler, webserver, worker, dag-processor, ...)
└── files/                # Pod template files, config maps
```

**Value hierarchy** (later overrides earlier):
`values.yaml` → `values/<fabric>/values.yaml` → `values/<fabric>/<cluster>/values.yaml`

Dev cluster values live separately in `deployment/helm-values/clusters/dev/<fabric>/values.yaml`.

---

### Data Flow: DAG Upload → Execution

```
1. Author commits DAG Python files to their MP repo
        |
        v
2. CRT triggers airflow-crt-action Docker job
        |
        v
3. airflow-crt-action POSTs ZIP to
   POST /api/v1/plugins/upload/upload_dags (webserver)
        |
        v
4. Upload Plugin validates:
   - Python import via DagBag
   - Proxy user ACL (SparkOperator, DarwinOperator)
   - Archive unpacking (ZIP/TAR.GZ)
   Stores DAGs at /opt/airflow/dags/<mp>/<app>/
        |
        v
5. dag-processor parses DAG files, serializes to metadata DB
   (AIRFLOW__SCHEDULER__PARSING_PROCESSES=4,
    MIN_FILE_PROCESS_INTERVAL=30s, DAG_DIR_LIST_INTERVAL=60s)
        |
        v
6. Scheduler reads serialized DAGs from DB, evaluates schedule,
   creates DagRuns and TaskInstances
   (PARALLELISM=512, MAX_ACTIVE_TASKS_PER_DAG=128,
    MAX_TIS_PER_QUERY=16)
        |
        v
7. KubernetesExecutor spawns a worker Pod per TaskInstance
   - k8s-lare init fetches certs for DAG MP + platform
   - Task runs (SparkBatchOperator → GGW gRPC → YARN job, etc.)
        |
        v
8. Task completes, pod terminates (DELETE_WORKER_PODS=True)
   TaskInstance state written back to metadata DB
        |
        v
9. oklahoma-listener plugin emits lifecycle events to Kafka
   (dag_run events, task_instance events, root-cause analysis)
```

---

### Key Configuration Values

Sourced from `deployment/helm-values/clusters/dev/prod-ltx1/dev/values.yaml` and fabric values files:

#### Scheduler

| Config | Value | Notes |
|--------|-------|-------|
| `AIRFLOW__CORE__PARALLELISM` | 512 | Global max concurrent task instances |
| `AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG` | 128 | Per-DAG parallelism cap |
| `AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG` | 128 | |
| `AIRFLOW__SCHEDULER__MAX_DAGRUNS_TO_CREATE_PER_LOOP` | 10 | |
| `AIRFLOW__SCHEDULER__MAX_DAGRUNS_PER_LOOP_TO_SCHEDULE` | 20 | |
| `AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY` | 16 | DB batch size for TI updates |
| `AIRFLOW__SCHEDULER__PARSING_PROCESSES` | 4 | DAG parsing parallelism |
| `AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL` | 30s | How often a DAG file is re-parsed |
| `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL` | 60s | How often the DAG directory is scanned |
| `AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT` | 50s | |
| `AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT` | False | |

#### KubernetesExecutor

| Config | Value | Notes |
|--------|-------|-------|
| `AIRFLOW__KUBERNETES_EXECUTOR__WORKER_PODS_CREATION_BATCH_SIZE` | 50 | Max pods created per scheduler loop |
| `AIRFLOW__KUBERNETES_EXECUTOR__DELETE_WORKER_PODS` | True | Pods deleted after task completion |
| `AIRFLOW__KUBERNETES_EXECUTOR__DELETE_WORKER_PODS_ON_FAILURE` | False | Failed pods retained for debugging |
| `AIRFLOW__KUBERNETES_EXECUTOR__MAX_ADOPTION_COMPLETED_PODS` | 80 | |

#### Webserver / Security

| Config | Value | Notes |
|--------|-------|-------|
| `PERMANENT_SESSION_LIFETIME` | 8 hours (28800s) | Flask session duration |
| `MAX_CONTENT_LENGTH` | 20 MB | DAG upload size limit |
| `SSO_PROVIDER` | `AAD` (Azure Active Directory) | SSO via `lnkdprod.com` tenant |
| `SSO_CLIENT_ID` | `82c36929-64e6-4d1d-b609-3c6e00abd720` | AAD application ID |
| `CSRF_PROTECTION_ENABLED` | False (TODO: enable) | CSRF protection disabled as of writing |
| `SESSION_COOKIE_SECURE` | True | HTTPS-only session cookies |
| `AUTH_TYPE` | `AUTH_DB` | FAB auth backed by Airflow DB (populated from LDAP/AAD at login) |
| `AUTH_ROLES_SYNC_AT_LOGIN` | True | Roles refreshed from LDAP groups on each login |

---

### SSO and Authentication

Authentication stack: Azure AD SSO via `linkedin.websso` → LinkedIn Flask session manager → Airflow FAB security manager (`LinkedInAirflowSecurityManager`).

- Login flow: user hits webserver → redirected to `/_tools/sso/login` → AAD OAuth → callback at `/_tools/sso/callback` → session established
- Proxy: SSO calls go through Kraken proxy at `http://ltx1-kraken-vip-1.prod.linkedin.com:10339`
- DataVault identity tokens used for service-to-service auth (upload plugin, GGW, KMS)
- SPIFFE/grestin certificates used for inter-service mTLS; fetched by `k8s-lare` init container

---

### Certificate Architecture

Each pod type receives certificates via the `k8s-lare` init container:

| Volume | Mount | Contents | Used for |
|--------|-------|---------|----------|
| `airflow-main-airflow-oklahoma-app-certs` | `/var/cluster/oklahoma` | Platform MP cert | Grid Gateway auth as oklahoma-airflow |
| `dag-certs` | `/var/cluster` | Per-DAG MP cert (workers only) | Grid Gateway proxy-user auth |
| `mp-certs` | `/var/cluster/mp` | Per-MP cert (workers only) | DataVault token exchange |
| `riddler-certs` | `/etc/riddler` | CA bundle | TLS verification |
| `lipki-certs` | `/etc/lipki` | lipki PKI certs | |
| `pki-agent-socket` | `/var/run/lipki/pki-agent.sock` | Socket for cert requests | Used by k8s-lare during init |

---

### Resource Limits (Default)

| Component | CPU Request | CPU Limit | Memory Request | Memory Limit |
|-----------|------------|-----------|----------------|-------------|
| scheduler | 1 | 1 | 2 Gi | 2 Gi |
| webserver | 1 | 1 | 2 Gi | 2 Gi |
| dag-processor | 1 | 2 | 1 Gi | 2 Gi |
| workers (Celery) | 200m | 500m | 500 Mi | 1 Gi |
| triggerer | 200m | 500m | 500 Mi | 1 Gi |
| worker pod (KubeExec) | 200m | 500m | 500 Mi | 1 Gi |

---

### Nimbus K8s Migration Context

Production clusters were migrated to Nimbus Kubernetes (LinkedIn's managed K8s platform) in the Dec 2025 -- Feb 2026 timeframe. Prior to this, clusters ran on legacy Grid1/Grid2 Hadoop infrastructure. The move to Nimbus brought:

- Standardized K8s namespace layout
- `k8s-lare` cert provisioning replacing older grestin sidecar patterns
- SPIFFE-based certificate ramp-up (feature-flagged via `AIRFLOW__OKLAHOMA__SPIFFE_RAMP_UP_PERCENTAGE`)
- PVC-based DAG storage replaced with in-image DAG uploads (the "no-nfs" initiative)
- Removal of NFS mounts that previously served DAG files to workers

Legacy NFS PVC manifests remain at `deployment/legacy/pvc/` for reference but are no longer active.

---

### Observability

- **Metrics**: OpenTelemetry → FluentBit (host port 22784) → Geneva (MDM) → Grafana. Primary transport since Aug 2025 (PR #700). MDM account `LNKD-MP-OKLAHOMA-AIRFLOW`, namespace `airflow-${CLUSTER_NAME}`. StatsD/AMF sidecar is legacy — disabled on holdem/war/faro/corp; active only on DBT. See [Metrics](metrics.md) for full metric catalog and query guide.
- **Dashboards**: OTEL-based Grafana dashboards (Operational, Alerts) on observe.prod.linkedin.com. InGraphs dashboards generated by `dashboards/generate_dashboards.py` from templates (legacy).
- **Logs**: Pod logs in Kubernetes; queryable via Kusto with cluster/namespace/pod filters.
- **Events**: `oklahoma-listener` publishes DAG run and task instance lifecycle events to Kafka. Downstream consumers include root-cause analysis and failure classification pipelines.
- **E2E Tests**: Checkly Playwright tests in `e2e-tests/` verify cluster health.

---

### Runtime Architecture — See Also

- [Overview](overview.md)
- [Clusters](clusters.md)
- [DAG Authoring](dag-authoring.md)
- [Deployment](deployment.md)
- [Tradewind](systems/tradewind.md)
- [Spark](systems/spark.md)
- [lipy-airflow-providers](systems/lipy-airflow-providers.md)
- [Codebase Overview](codebase/README.md)
- [Patterns](patterns.md)
- [Gotchas](codebase/gotchas.md)

### K8s Pod Specification Storage

K8s pod YAML specifications are persisted in the `rendered_task_instance_fields` table under the `k8s_pod_yaml` column. This allows retrieval of rendered pod definitions after execution for debugging and audit purposes. Use this column when investigating pod configuration issues or analyzing K8s resource allocations.

### Task Instance Rendering Schema

The `rendered_task_instance_fields` table stores rendered Airflow task context:

```sql
CREATE TABLE rendered_task_instance_fields (
  dag_id VARCHAR(...),
  task_id VARCHAR(...),
  run_id VARCHAR(...),
  map_index INT,
  rendered_fields LONGTEXT,  -- serialized templates
  k8s_pod_yaml LONGTEXT,     -- K8s pod spec (K8sExecutor only)
  PRIMARY KEY (dag_id, task_id, run_id, map_index)
);
```

Inserts use `ON DUPLICATE KEY UPDATE` to handle task retries. The `k8s_pod_yaml` column is populated only on K8s executor clusters (War, Faro) and can grow large with complex pod specs; monitor for write contention during high task volume DAG runs.
