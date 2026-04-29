> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

# Airflow — Clusters

## Cluster Overview

| Cluster | Environment | Datacenter | Fabric | Purpose | URL |
|---------|-------------|-----------|--------|---------|-----|
| **Holdem** | prod | prod-ltx1 | prod-ltx1 | Primary offline/batch workloads | holdem.oklahoma-airflow.grid.linkedin.com |
| **War** | prod | prod-lva1 | prod-lva1 | Online use cases, faster DAG scheduling | war.oklahoma-airflow.grid.linkedin.com |
| **Faro** | staging | ei-ltx1 | ei (staging) | Staging environment for testing | faro.oklahoma-airflow.stg.linkedin.com |
| **Corp** | corp | corp-lva1 | corp-lva1 | Corporate/online use cases | corp.airflow.corp.linkedin.com |
| **rdev** | development | corp-lva1 | corp-lva1 | Local development via Darwin RDev instances | localhost:8080 (per-developer) |

Additional clusters: Test, Lasso, Load-test (all prod-ltx1), GHD (grid1 - integration testing)

---

## Holdem

**Production primary cluster** | offline/batch workloads | TX data center

### Environment
- **Environment**: Production (`prod`)
- **Datacenter**: prod-ltx1
- **Airflow version**: 2.9.2
- **Executor**: Kubernetes (KubernetesExecutor)

### Resource Sizing
- **Scheduler replicas**: 24 (high-performance for large DAG parsing)
- **Webserver replicas**: 10
- **DAG Processor replicas**: 12
- **Scheduler CPU**: 2-4 cores (request-limit)
- **Scheduler memory**: 4Gi-8Gi
- **Webserver CPU**: 2-3 cores
- **Webserver memory**: 6Gi-8Gi

### Configuration
- **Base URL**: `https://holdem.oklahoma-airflow.grid.linkedin.com`
- **Parallelism**: 1024 (AIRFLOW__CORE__PARALLELISM)
- **Max active tasks per DAG**: 128
- **Max active runs per DAG**: 128
- **Parsing processes**: 8
- **Min file process interval**: 1800s (30min - less frequent parsing)
- **DAG dir list interval**: 86400s (24hr)
- **Scheduler health check threshold**: 180s
- **Worker creation batch size**: 50 pods at a time
- **Navbar color**: #e7c39c (tan)

### Features & Configuration
- Multi-product DAG uploads enabled
- SPIFFE certificate support ramping up (0% initially)
- Pod mutation hooks for automatic MP tagging
- Grestin cert support enabled
- Proxy user ACL validation enabled (30s timeout)
- Remote logging via Elasticsearch
- Upload plugin enabled

### Metadata & Secrets
- **Database**: airflow-holdem-database-secret
- **Fernet key**: airflow-holdem-fernet-key
- **Webserver secret**: airflow-holdem-webserver-secret
- **DAG PVC**: airflow-holdem-dags-pvc (1Gi)
- **Logs PVC**: airflow-holdem-logs-pvc

### Pod Topology Spreading (since Apr 2026)

All Airflow pods (schedulers, dag-processors, webservers) are spread across **maintenance zones** by default via `topologySpreadConstraints` with `topology.kubernetes.io/zone` key, `maxSkew: 1`, and `whenUnsatisfiable: ScheduleAnyway`. This prevents a single zone maintenance event from taking down all instances of a component simultaneously.

Source: deployment PR #1062 (2026-04-28).

### Purpose
Primary offline and batch workload processing. Highest resource allocation reflects the volume and complexity of batch DAGs. Less frequent DAG reloading (1800s, 24hr) compared to Faro/War optimizes for stable, long-running batch jobs.

---

## War

**Production secondary cluster** | online/real-time workloads | Virginia data center

### Environment
- **Environment**: Production (`prod`)
- **Datacenter**: prod-lva1
- **Airflow version**: 2.9.2
- **Executor**: Kubernetes

### Resource Sizing
- **Scheduler replicas**: 12 (moderate for online/real-time)
- **Webserver replicas**: 7
- **DAG Processor replicas**: 12
- **Scheduler CPU**: 4-6 cores
- **Scheduler memory**: 3Gi-6Gi
- **Webserver CPU**: unspecified (uses defaults)
- **Webserver memory**: unspecified

### Configuration
- **Base URL**: `https://war.oklahoma-airflow.grid.linkedin.com`
- **Parallelism**: 1024
- **Max active tasks per DAG**: 128
- **Max active runs per DAG**: 128
- **Parsing processes**: 8
- **Min file process interval**: 900s (15min - more responsive)
- **DAG dir list interval**: 360s (6min - more responsive)
- **Scheduler health check threshold**: 180s
- **Worker creation batch size**: 50
- **Worker refresh**: 1 worker every 6000s
- **Navbar color**: #cedcc3 (sage green)
- **Sensitive vars**: session_id marked as sensitive

### Features & Configuration
- Multi-product DAG uploads enabled (limited allow list)
- SPIFFE certificate support disabled
- Grestin cert support enabled
- Proxy user ACL validation enabled (30s timeout)
- Pod mutation hooks enabled
- Remote logging via Elasticsearch
- Faster DAG refresh (900s vs 1800s) for responsive deployments

### Metadata & Secrets
- **Database**: airflow-war-database-secret
- **Fernet key**: airflow-war-fernet-key
- **Webserver secret**: airflow-war-webserver-secret
- **DAG PVC**: airflow-war-dags-pvc (1Gi)
- **Logs PVC**: airflow-war-logs-pvc

### Purpose
Online and real-time workloads, faster scheduling. More responsive DAG parsing (900s, 6min intervals) vs Holdem's batch-optimized intervals. Suitable for time-sensitive, lower-latency workflows.

---

## Faro

**Staging cluster** | testing before prod | TX data center (same as Holdem but staging fabric)

### Environment
- **Environment**: Staging (`ei`)
- **Datacenter**: ei-ltx1
- **Fabric group**: ei
- **Airflow version**: 2.9.2
- **Executor**: Kubernetes

### Resource Sizing
- **Webserver replicas**: 3 (minimal)
- **DAG Processor replicas**: 2 (minimal)
- **DAG Processor CPU**: 2-4 cores
- **DAG Processor memory**: 2Gi
- **amfStatsdClient CPU**: 2000m
- **amfStatsdClient memory**: 24Gi (measurement/metrics collection)

### Configuration
- **Base URL**: `https://faro.oklahoma-airflow.stg.linkedin.com`
- **Parallelism**: 512 (staging scale)
- **Parsing processes**: 4 (staging scale)
- **Min file process interval**: 900s (15min)
- **DAG dir list interval**: 360s (6min)
- **Scheduler health check threshold**: 30s (aggressive, testing environment)
- **Max TIs per query**: 16 (staging scale)
- **Max DAG runs to create per loop**: 10
- **Backfill permitted roles**: Admin (restricted for staging)
- **Proxy user ACL validation**: DISABLED (proxy_user_acl_validation_timeout_sec: 0)
- **Upload plugin**: disabled
- **Navbar color**: #ccbad4 (lavender)
- **Service port**: 31115

### Features & Configuration
- SPIFFE certificate support enabled (ramp up 0%)
- Grestin cert support enabled
- Proxy user ACL validation DISABLED because EI network cannot reach corp DataVault service
- Remote logging via Elasticsearch
- Sensitive var names: session_id
- DAG retention: shorter than prod
- Logs PVC: 100Gi (generous for testing/debugging)

### Metadata & Secrets
- **Database**: airflow-faro-database-secret
- **Fernet key**: airflow-faro-fernet-key
- **Webserver secret**: airflow-faro-webserver-secret
- **DAG PVC**: airflow-faro-dags-pvc (1Gi)
- **Logs PVC**: airflow-faro-logs-pvc (100Gi - large for debugging)

### Known Limitations
- **No tracking data**: The tracking team does not support tracking tables on Faro. All tracking event data (e.g., `tracking.*` tables) is produced to prod Kafka topics and dumped to **Holdem only**. DAGs that depend on tracking data cannot be tested on Faro. (APA-144366, APA-144405 — 2026-04-14)
- **Proxy user ACL validation disabled**: EI fabric cannot reach corp DataVault service.

### Purpose
Staging environment. Lower resource allocation than prod clusters. Disabled proxy user ACL validation due to network isolation (EI fabric cannot reach corp DataVault). Larger logs volume for development troubleshooting. Used for validating DAGs and configurations before promotion to prod.

---

## Corp

**Corporate/online production cluster** | isolated corporate network | Virginia data center

### Environment
- **Environment**: Corporate (`corp`)
- **Datacenter**: corp-lva1
- **Fabric group**: corp
- **Airflow version**: 2.9.2
- **Executor**: Kubernetes

### Resource Sizing
- **Webserver replicas**: unspecified
- **Scheduler replicas**: unspecified
- **DAG Processor**: unspecified (using defaults)

### Configuration
- **Base URL**: `https://corp.airflow.corp.linkedin.com`
- **Parallelism**: 512 (corp scale)
- **Parsing processes**: 4 (corp scale)
- **Min file process interval**: 900s (15min)
- **DAG dir list interval**: 360s (6min)
- **Worker creation batch size**: 50
- **Worker refresh**: 1 worker every 6000s
- **Navbar color**: default (not specified)
- **Service port**: 44623
- **Upload plugin**: disabled
- **Backfill permitted roles**: Admin (restricted)
- **Proxy user ACL validation**: enabled (30s timeout)

### Features & Configuration
- SPIFFE certificate support disabled
- Grestin cert support enabled
- Pod mutation hooks enabled
- Remote logging via Elasticsearch
- DAG uploads: limited to service accounts only
- Azkaban key mounting: commented out (not used)
- Logs PVC: 100Gi

### Metadata & Secrets
- **Database**: airflow-corp-database-secret
- **Fernet key**: airflow-corp-fernet-key
- **Webserver secret**: airflow-corp-webserver-secret
- **DAG PVC**: airflow-corp-dags-pvc (1Gi)
- **Logs PVC**: airflow-corp-logs-pvc (100Gi)

### Purpose
Corporate network online use cases. Isolated from main prod. Minimal resource configuration suggests it may be smaller-scale or in an isolated corporate network. Lacks Azkaban integration (commented out).

---

## rdev

**Development cluster** | per-developer local environments | Darwin platform

### Environment
- **Environment**: Development/local
- **Fabric group**: corp
- **Fabric**: corp-lva1
- **Cluster ID**: rdev
- **Airflow version**: 2.9.2
- **Executor**: LocalExecutor (single-machine)

### Local Configuration
- **Airflow home**: `/opt/airflow`
- **Base URL**: `http://localhost:8080` (per developer)
- **Instance name**: "RDev Airflow - {PRODUCT_NAME}/{RDEV_NAME}"
- **Navbar color**: #b3d3ec (tropical blue)
- **Database**: MySQL local (`mysql+mysqldb://airflow:airflow_password@localhost:3306/airflow_db`)
- **Scheduler settings**:
  - `DAG_DIR_LIST_INTERVAL`: 60s (very responsive)
  - `MIN_FILE_PROCESS_INTERVAL`: 15s (very responsive)
  - `USE_JOB_SCHEDULE`: False (blocks scheduled executions, manual only)
  - `LIMIT_DAGS_TO_DELETE`: 0 (no retention)
- **Min DAG retention time**: 0 (no retention)
- **Fernet key**: RdevFernetKeyRdevFernetKeyRdevFernetKeyRdev=
- **API auth backends**: LinkedIn API auth + basic auth

### SMTP Configuration (recent addition, commit 7de2a54)
- **SMTP host**: mail-gw.corp.linkedin.com
- **SMTP port**: 25
- **SMTP starttls**: enabled
- **SMTP mail from**: rdev-airflow@linkedin.com

### Features
- Logging fallback to file (for local MySQL)
- Show trigger form if no params (better UX for manual testing)
- Load examples: False
- No job scheduling (developer must trigger manually)
- Remote logging disabled (local file fallback)
- Proxy user validation enabled (30s)
- **Customizable navbar header** (deployment PR #1043, 2026-04-14): Downstream rdev images (e.g., `darwin-rdev-airflow-image`) can now customize the Airflow nav header to visually distinguish their modified version from the base Oklahoma rdev. Addresses incident-10325 / ACTIONITEM-16458.

### Purpose
Per-developer Airflow instances running locally or in Darwin RDev containers. Used for:
- Testing DAGs before submitting to staging/prod
- Rapid iteration and debugging
- Local development without impacting shared environments
- Recently added SMTP support to enable EmailOperator and failure notifications

---

## Additional Clusters

### Test
- **Location**: prod-ltx1
- **Purpose**: Testing cluster in prod-ltx1 fabric
- **Scheduler replicas**: 2 (minimal)
- **Webserver replicas**: 2
- **Unique features**: 
  - Upload plugin enabled
  - Debug logging enabled (AIRFLOW__LOGGING__LOGGING_LEVEL: DEBUG)
  - Worker timeout: 600s (shorter than prod)
  - Can DAG updates allow list extended for testing
  - Parallelism: 1024 (prod-like)
- **Metadata DB**: airflow-test-database-secret

### Lasso
- **Location**: prod-ltx1 (being tested as new image)
- **Purpose**: Test cluster for oklahoma-airflow-deployment image overrides
- **Parallelism**: 1024
- **Crew sync MP denylist**: oklahoma-airflow (limit scope)
- **Trusted users**: restricted (only oklahoma-airflow-test-app)
- **Unique features**: oklahoma-airflow-deployment image override being tested

### Load-test
- **Location**: prod-ltx1
- **Purpose**: Performance/load testing cluster
- **Execution balancer**: disabled (post-Nimbus migration)

### GHD (Groundhog CI)
- **Location**: grid1
- **Namespace**: grid-integration-testing
- **Purpose**: Integration testing cluster for continuous deployment validation
- **Cluster name**: groundhog-ci
- **IPv6**: Enabled (PR #1028, 2026-04-14) — applies the same pattern as PR #894
- **Post-merge CI**: GHA invokes `grid-integration-testing-cli` for automated integration tests
- **Image path gotcha**: After the oklahoma-airflow repo split (March 2026), `defaultAirflowRepository` was incorrectly changed to `oklahoma-airflow-deployment` (the deployment repo). Fix: PR #1037 (2026-04-13) restored the correct image path. Pods on GHD showed `InvalidImageName` / `ImagePullBackOff` between commit 10efffecc3 (Mar 18) and the fix.

---

## Environment Matrix

| Feature | Holdem | War | Faro | Corp | rdev |
|---------|--------|-----|------|------|------|
| **Environment** | prod | prod | staging | corp | dev |
| **Datacenter** | ltx1 | lva1 | ltx1 (EI) | lva1 | local |
| **Executor** | K8s | K8s | K8s | K8s | Local |
| **Parallelism** | 1024 | 1024 | 512 | 512 | N/A |
| **Scheduler replicas** | 24 | 12 | 0 (default) | 0 | 1 |
| **Webserver replicas** | 10 | 7 | 3 | 0 | 1 |
| **DAG parse interval** | 1800s | 900s | 900s | 900s | 15s |
| **DAG list interval** | 86400s | 360s | 360s | 360s | 60s |
| **Upload plugin** | enabled | enabled | disabled | disabled | N/A |
| **Proxy ACL validation** | enabled | enabled | **disabled** | enabled | enabled |
| **SPIFFE certs** | ramp (0%) | disabled | enabled | disabled | N/A |
| **Grestin certs** | enabled | enabled | enabled | enabled | N/A |
| **Remote logging** | Elasticsearch | Elasticsearch | Elasticsearch | Elasticsearch | File-based |
| **Logs PVC size** | default | default | 100Gi | 100Gi | N/A |
| **DAG retention** | 3600s | 0s | 0s | 0s | 0s |

---

## Key Differences

### Holdem vs War
- **Scheduling**: Holdem optimized for high volume batch (1800s, 24hr DAG refresh); War optimized for responsiveness (900s, 6min)
- **Scheduler resources**: Holdem: 24 replicas, 2-4 cores; War: 12 replicas, 4-6 cores
- **Use case**: Holdem = offline/batch; War = online/real-time
- **Datacenter**: Holdem = ltx1; War = lva1 (geographic distribution)

### Faro vs Prod
- **Proxy user ACL validation**: Disabled on Faro (EI fabric cannot reach corp DataVault)
- **Resource scale**: Faro 3 webservers, 2 DAG processors; Prod 7-10+ webservers, 12+ DAG processors
- **URL domain**: `.stg.linkedin.com` vs `.grid.linkedin.com` or `.corp.linkedin.com`
- **Logs volume**: Faro 100Gi (large for debugging) vs prod defaults (smaller)

### rdev vs Prod Clusters
- **Executor**: LocalExecutor (single-machine) vs KubernetesExecutor (distributed)
- **Scheduling**: Disabled (USE_JOB_SCHEDULE=False) — manual trigger only
- **Database**: Local MySQL vs managed database
- **DAG retention**: Zero (no cleanup) vs 3600s+
- **URL**: localhost:8080 per developer vs public HTTPS
- **Network**: Local or Darwin container vs corporate networks
- **Purpose**: Development/testing vs production workloads

---

## Recent Changes

[Source: git log -60 oklahoma-airflow-deployment]

### Latest (Apr 2026)
- **7de2a54** (Apr 10): Add SMTP configuration to rdev environment
  - Added SMTP env vars to rdev setup script (host, port, starttls, mail_from)
  - Enables EmailOperator and email_on_failure notifications in rdev
  - Tested on rdev-aks-wus3-8

- **49e14ef** (#1009): Show MP name and RDev name in RDev Airflow webserver title
  - RDev instances now display "{PRODUCT_NAME}/{RDEV_NAME}" in webserver instance_name

### Recent Cluster Updates (Mar-Apr 2026)
- **a4dab72**: Update fsGroup for worker and webserver
- **6f438a8** (#1035): Fix Airflow pod fsGroup to prevent NFS mount hangs on new nodes
- **1e8afe4** (#983): Disable proxy user ACL validation on faro (EI) cluster
  - Due to network isolation, EI cannot reach corp DataVault service
  - Set proxy_user_acl_validation_timeout_sec: 0
- **13aabb8** (#953): Clear dags_on_old_version config for holdem cluster

### Historical Cluster Work (2025-2026)
- **434c27a** (#997): Fix invocation of install_external_deps.sh for rdev startup modes
- **bd30ced** (#962): Rollback oklahoma-airflow-deployment image override for lasso cluster
- **513b19a** (#959): Move oklahoma-airflow-deployment image override from test to lasso cluster
- **8aec367** (#955): Add nodeAffinity to prefer nodes without upcoming maintenance
- **fc204ac** (#850): Disable execution balancer post-Nimbus migration in Holdem and War
- **7c0674a** (#915): Remove Airflow DBT Helm chart (cluster being decommissioned)

---

## SSO Proxy URLs by Fabric

All clusters use Azure AD (`spi-oklahoma-airflow-prod`, tenant `lnkdprod.com`) for SSO. The SSO proxy endpoint is fabric-specific:

| Fabric | Proxy URL |
|--------|-----------|
| PROD grid1 (ltx1) | `ltx1-kraken-vip-1.prod.linkedin.com:10339` |
| PROD grid2 (lva1) | `lva1-kraken-vip-1.prod.linkedin.com:10339` |
| EI (staging) | `ltx1-kraken-vip-1.stg.linkedin.com:10339` |
| CORP | `lva1-kraken-vip-8.corp.linkedin.com:10339` |

When adding a new cluster, add its redirect URI (`https://{cluster-url}/oauth-authorized/azure`) to the `spi-oklahoma-airflow-prod` App Registration in Azure AD.

---

## NKS Topology-Based Discovery

> DNS Range is deprecated. All clusters use NKS topology-based disco hostnames.

Hostname format: `{product-tag}.{application}.{fabric}.atd.disco.linkedin.com:{port}`

Example (Lasso, prod-ltx1): `lasso.airflow-main-airflow-oklahoma.prod-ltx1.atd.disco.linkedin.com:31119`

---

## API Access

All clusters support the Airflow v1 REST API:

```
https://<cluster-host>/api/v1/
```

**Authentication**: DataVault identity token (via `curli --dv-auth SELF`)

**Endpoints**: 
- DAG management: `/dags`
- Executions: `/dags/{dag_id}/dagRuns`
- Task instances: `/dags/{dag_id}/dagRuns/{run_id}/taskInstances`

---

## Database & Metadata

Each cluster has isolated metadata:
- **Database secret**: `airflow-{cluster}-database-secret`
- **Fernet key secret**: `airflow-{cluster}-fernet-key` (for sensitive variable encryption)
- **Webserver secret**: `airflow-{cluster}-webserver-secret` (Flask session key)

Secrets are managed via Kubernetes and populated by infrastructure team.

---

## See Also
- [Overview](overview.md) — high-level Airflow architecture
- [Deployment](deployment.md) — CRT flow, DAG deployment, promotion process
- [Architecture](architecture.md) — components, data flow, dependencies
- [DAG Authoring](dag-authoring.md) — how to write DAGs, naming, operators

## Getting rdev Information and Owner

To retrieve information about a remote development environment:

```bash
rdev info [RDEV_ENV_NAME]
```

Where `RDEV_ENV_NAME` format is `<MP_NAME>/<RDEV_NAME>`.

To find the owner/creator of a specific rdev host:

```bash
rdev debug find-owner <HOST>
```

Example:
```bash
rdev debug find-owner voyager-web-98jx2-ppp6b.corp.rdev.svc.cluster.local
```

This is useful for identifying who created or manages a specific rdev environment.
