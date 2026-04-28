> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# System — lipy-airflow-providers

> LinkedIn's custom Airflow provider package: GridGateway operators, event listeners, Iris integration, hooks for internal systems (Azkaban, Ambry, KMS, BDP services)

## Purpose

`lipy-airflow-providers` is the primary distribution vehicle for LinkedIn's internal Airflow extensions. It packages:

1. **GridGateway (GGW)** operators for distributed job execution across multiple platforms (Spark, Flink, Trino SQL, Hadoop, Java, etc.)
2. **Event listeners** that capture task and DAG lifecycle events for observability, analytics, and incident management
3. **Integration hooks** for internal systems: Azkaban, Ambry, IRIS incident management, KMS, BDP (ARMS/Jasper/Featurecloud)
4. **Airflow policy framework** for DAG access control, operator deprecation, and deployment governance
5. **Utility plugins**: upload-dags, macros (date utilities for data triggers), notifications

The repo is multi-module: `apache-airflow-providers-lnkd` is the main provider package; supporting modules (`oklahoma-listener`, `oklahoma-backfill`, `oklahoma-helpers`) are distributed alongside.

---

## Package Layout

```
lipy-airflow-providers/
├── apache-airflow-providers-lnkd/       Main provider package (published to Artifactory)
│   ├── src/airflow/providers/lnkd/
│   │   ├── gridgateway/                 ~30 operators for GGW job execution
│   │   ├── operators/                   TriggerDagRunOperator, misc
│   │   ├── sensors/                     SensorArray sensor
│   │   ├── hooks/                       Base hook classes
│   │   ├── azkaban/                     Azkaban hooks & operators
│   │   ├── ambry/                       Ambry push operator & hooks
│   │   ├── bdp/                         BDP services: ARMS, Jasper, Featurecloud
│   │   ├── iris/                        IRIS incident creation, callbacks, context parsing
│   │   ├── kms/                         KMS (Key Management Service)
│   │   ├── dex/                         DAG execution context enrichment
│   │   ├── log/                         Logging handler plugins
│   │   ├── notifications/               Custom Slack/email formatters
│   │   ├── oncall/                      On-call integration
│   │   ├── upload_dags/                 DAG upload plugin (synchronizes DAG bundles)
│   │   ├── macros/                      Date utility macros for data triggers
│   │   ├── documentstore/               Document storage integration
│   │   ├── featurecloud/                Featurecloud hooks
│   │   ├── airflow_version_compatibility/  Compatibility shims for Airflow versions
│   │   ├── utils/                       Shared utilities (URL, datetime, context, DRY run)
│   │   ├── get_provider_info.py         Provider registration entry point
│   │   └── exceptions.py                Custom exception classes
│   ├── setup.py, setup.cfg              Package metadata
│   └── build.gradle                     Gradle build config (depends on oklahoma-* modules)
│
├── oklahoma-listener/                  DAG & task lifecycle event capture, root cause analysis
│   ├── src/linkedin/airflow/plugins/
│   │   ├── listener/
│   │   │   ├── dag_listener.py
│   │   │   ├── task_listener.py
│   │   │   ├── dag_upload_listener.py
│   │   │   ├── event_schemas/          DAG/Task/Upload event schemas
│   │   │   ├── root_cause_analyzer/    Failure classification logic
│   │   │   └── listener_plugin.py      Plugin registration
│   │   └── utils/
│
├── oklahoma-helpers/                   Shared utilities & helper functions
│   └── src/linkedin/oklahoma/
│
├── oklahoma-backfill/                  Backward compat stub (real backfill plugin is open-source)
│
├── airflow-policy-framework/           DAG/operator policy enforcement engine
│   ├── Policies for ACLs, deprecation, operator validation
│   └── Integrates with Airflow's policy system
│
├── gradle.properties, settings.gradle  Multi-module Gradle build config
├── build.gradle                        "li-python-product" plugin
└── README.md                           Dev testing guide (snapshot builds, e2e testing)
```

### Key Entry Points

- **Provider registration**: `apache-airflow-providers-lnkd/src/airflow/providers/lnkd/get_provider_info.py`
  - Declares all hooks, connection types, extra-links for Airflow discovery
  - Hook classes: SparkServiceHook, AzkabanHook, MufnHook, GridGatewayHook, ArmsHook, JasperHook, IrisHook, AmbryHook

- **Airflow plugins**: `setup.cfg` [options.entry_points]
  - `upload_dags_plugin`: UploadDAGsPlugin (DAG synchronization)
  - `macro_plugin`: MacrosPlugin (date utilities)
  - `airflow_version_compatibility`: AirflowVersionCompatibilityPlugin

---

## Providers

### GridGateway (GGW)

Primary execution framework for distributed jobs. Operators in `gridgateway/operators/`:

| Operator | Purpose |
|----------|---------|
| **SparkBatchOperator** | Execute Spark jobs via GridGateway; returns Spark log URLs via XCom |
| **FlinkBatchOperator** | Apache Flink batch jobs |
| **TrinoSqlOperator** / **SqlOperator** | SQL execution (Trino/various DBs) |
| **HadoopJavaProcessOperator** | Generic Hadoop Java process execution |
| **JavaOperator** | Standalone Java applications |
| **CommandOperator** | Shell command execution |
| **KafkaPushOperator** | Push data to Kafka |
| **PinotPushOperator** | Push to Pinot OLAP DB |
| **WormholePushOperator** | Push to Wormhole (LinkedIn's data mesh) |
| **VenicePushOperator** | Push to Venice key-value store |
| **AmbryPushOperator** | Push to Ambry (LinkedIn's blob store) |
| **CarbonOperator** | Carbon framework integration |
| **DataQualityJobOperator** | Data quality checks |
| **FlyteOperator** / **FlyteClusterOperator** | Flyte workflow execution |
| **In-DBT** operators | dbt integration with Grid Gateway |

All inherit from GridGatewayBaseOperator; support disruption readiness, external job checkpointing, auth via Trustbridge.

### Core Operators & Sensors

| Module | Item | Purpose |
|--------|------|---------|
| **operators** | TriggerDagRunOperator | Trigger downstream DAGs; supports external job checkpointing |
| **sensors** | SensorArray | Array sensor for task mapping |
| **hooks** | Base hook classes | HTTP, connection mgmt |

### System Integrations

| System | Module | Purpose |
|--------|--------|---------|
| **Azkaban** | `azkaban/` | Legacy job scheduler; hooks for metadata retrieval |
| **Ambry** | `ambry/` | Distributed blob store; push operators & hooks |
| **IRIS** | `iris/` | LinkedIn's incident management; callbacks for task failures, context enrichment |
| **KMS** | `kms/` | Key Management Service for secrets |
| **BDP Services** | `bdp/` | ARMS (metadata), Jasper (lineage), Featurecloud (feature store) |
| **Featurecloud** | `featurecloud/` | Feature store integration |
| **Documentstore** | `documentstore/` | Internal doc storage |
| **On-call** | `oncall/` | Alert routing to on-call engineers |

### Listeners & Observability

| Module | Purpose |
|--------|---------|
| **oklahoma-listener** (dag_listener) | Captures DAG start/complete events; publishes to Kafka/metrics |
| **oklahoma-listener** (task_listener) | Task lifecycle events (start, success, fail, skip); task classification |
| **oklahoma-listener** (root_cause_analyzer) | Analyzes failures, populates failure classification fields (e.g., GGW timeout vs. auth) |
| **oklahoma-listener** (dag_upload_listener) | Tracks DAG bundle uploads |
| **log/** | Custom log handlers (e.g., NFS unavailable checks, batch logging to Kusto) |
| **notifications/** | Custom formatters for Slack/email (includes PipelineMD URLs, Darwin notebook links) |

### Utilities & Policy

| Module | Purpose |
|--------|---------|
| **upload_dags/** | DAG upload/sync plugin; deletes removed DAGs |
| **macros/** | Date utility macros for data-triggered jobs |
| **airflow_version_compatibility/** | Shims for Airflow version differences |
| **utils/** | URL parsing, datetime handling, context enrichment, dry-run utilities, shared resource manager |
| **dex/** | DAG execution context (Dex) integration |
| **airflow-policy-framework/** | Policy enforcement: ACL validation, operator deprecation, mutation rules |

---

## Versioning

Versioning is managed via LinkedIn's internal build system (Gradle + mint):

- **Version scheme**: Semantic versioning, currently at 10.x.x (bumped in [#1038](https://github.com/linkedin/lipy-airflow-providers/commit/ee434715))
- **Release types**:
  - **SNAPSHOT**: Development builds (e.g., `8.0.77-SNAPSHOT`)
  - **Release**: Published to Corp Artifactory
- **Artifacts**: Multiple tar.gz packages published:
  - `apache-airflow-providers-lnkd-X.X.X.tar.gz` (main provider)
  - `oklahoma-listener-X.X.X.tar.gz`
  - `oklahoma-helpers-X.X.X.tar.gz`
  - `oklahoma-backfill-X.X.X.tar.gz`

**Build process** (from README):
```bash
mint build       # Compile & run tests
mint snapshot    # Create snapshot version
mint release     # Tag release
mint publish ... # Publish to Artifactory
```

---

## Recent Evolution (git log themes)

### GridGateway & Job Execution (Jan-Apr 2026)
- External job checkpointing for Kubernetes Pod Disruption Handling ([#901](https://github.com/linkedin/lipy-airflow-providers/commit/87d6db65))
- Disruption readiness ramped to SparkBatch, Trino SQL job types ([#931](https://github.com/linkedin/lipy-airflow-providers/commit/7e24ad26), [#955](https://github.com/linkedin/lipy-airflow-providers/commit/9e36a936))
- GridGatewayDataVaultTokenException for DataVault auth failures ([#1121](https://github.com/linkedin/lipy-airflow-providers/commit/b7998347))
- Standardize GGW & ARMS error messages ([#951](https://github.com/linkedin/lipy-airflow-providers/commit/fa160002))
- Improve GGW error banner wording, add Nimbus diagnostics URL ([#1116](https://github.com/linkedin/lipy-airflow-providers/commit/24a1afcf))
- Trustbridge integration: log request headers ([#1](https://github.com/linkedin/lipy-airflow-providers/commit/1e6beace))

### Event Listeners & Observability (ongoing)
- Task lifecycle event enrichment: map index, failure classification fields ([#944](https://github.com/linkedin/lipy-airflow-providers/commit/65be3561), [#947](https://github.com/linkedin/lipy-airflow-providers/commit/94bc1a31))
- Batch logging to Kusto, read only after job completion ([#983](https://github.com/linkedin/lipy-airflow-providers/commit/037b8391))
- Iris integration: PipelineMD diagnostic URLs, SLA alerting ([#1110](https://github.com/linkedin/lipy-airflow-providers/commit/38e89704), [#1030](https://github.com/linkedin/lipy-airflow-providers/commit/e1af3219))
- DAG mutation policy for auto-injecting MP/application tags ([#1018](https://github.com/linkedin/lipy-airflow-providers/commit/27fb3d5b))

### Policy & ACLs (ongoing)
- DAG access role policy enforcement ([#1112](https://github.com/linkedin/lipy-airflow-providers/commit/528d54a4), [#1115](https://github.com/linkedin/lipy-airflow-providers/commit/d1291f4d))
- Proxy user ACL validation during bundle deployment ([#1113](https://github.com/linkedin/lipy-airflow-providers/commit/637b4115))
- Deprecated operator policy ([#1044](https://github.com/linkedin/lipy-airflow-providers/commit/297fb71a))
- Policy exemptions for specific DAGs ([#1045](https://github.com/linkedin/lipy-airflow-providers/commit/4a5edad0))

### Dependency & Version Upgrades
- Airflow core 2.9.2.114 ([#1011](https://github.com/linkedin/lipy-airflow-providers/commit/e452d7a1))
- Upgrade lipy-kafka, lipy-datavault, lipy-fabric, lipy-oklahoma-airflow ([#1109](https://github.com/linkedin/lipy-airflow-providers/commit/050cc469))
- Bump airflow-oc-image versions ([#1122](https://github.com/linkedin/lipy-airflow-providers/commit/c890992d))

### DAG Upload & Sync
- Delete removed DAGs on upload ([#1085](https://github.com/linkedin/lipy-airflow-providers/commit/dbcdd871))
- Block patch uploads to non-symlink directories ([#934](https://github.com/linkedin/lipy-airflow-providers/commit/fb86875e))
- Batch upload endpoint support ([#903](https://github.com/linkedin/lipy-airflow-providers/commit/0f0da6e1))

### Config Override Support (Mar-Apr 2026)
- Add overridable attrs to all GGW operators with `get_overridable_attrs()` using `super()` inheritance ([#1156](https://github.com/linkedin/lipy-airflow-providers/pull/1156))
- Add `override.destinationConnectionString: metrics` to metadata kafka push job ([#1170](https://github.com/linkedin/lipy-airflow-providers/pull/1170))

### PipelineMD Global Extra Link Plugin (Apr 2026)
- PipelineMD diagnosis button was only visible for tasks with operator-level extra links. Created a proper Airflow plugin (`pipelinemd_plugin.py`) registered via `global_operator_extra_links` so PMD button appears for every task ([#1171](https://github.com/linkedin/lipy-airflow-providers/pull/1171))

### Bug Fixes & Hardening
- Fix stale NFS file handle failures ([#1065](https://github.com/linkedin/lipy-airflow-providers/commit/0578347f))
- DAG timeout handling for Iris context ([#1024](https://github.com/linkedin/lipy-airflow-providers/commit/90565bd9))
- RDEV config recursive merge fix ([#1008](https://github.com/linkedin/lipy-airflow-providers/commit/5577898d))
- Cross-MP artifact resolution in RDEV ([#1124](https://github.com/linkedin/lipy-airflow-providers/commit/a31714f8))
- Fix off-by-one in Iris alert try number: `ti.try_number` returns next attempt when task is not running; fix uses `try_number - 1` ([#1164](https://github.com/linkedin/lipy-airflow-providers/pull/1164))
- Fix guava extraClassPath overwriting user-supplied spark configs in result_generator: use `dict.copy()` and append instead of overwrite ([#1157](https://github.com/linkedin/lipy-airflow-providers/pull/1157), [#1159](https://github.com/linkedin/lipy-airflow-providers/pull/1159))
- Support `use_hourly_for_daily` in hourly partition sensor for daily datasets reading from hourly-partitioned sources ([#1158](https://github.com/linkedin/lipy-airflow-providers/pull/1158))
- Darwin notification email reformatted with structured HTML table layout and color-coded status banner ([#1155](https://github.com/linkedin/lipy-airflow-providers/pull/1155))
- Lowercase `cluster_id` in PipelineMD URL to match server expectations ([#1161](https://github.com/linkedin/lipy-airflow-providers/pull/1161))
- Support v1 schedule DAG IDs in darwin email notification "View resource" link ([#1188](https://github.com/linkedin/lipy-airflow-providers/pull/1188), [#1189](https://github.com/linkedin/lipy-airflow-providers/pull/1189)) — regex updated to match v1 schedule DAG naming pattern

---

## Entry Points & Airflow Discovery

Airflow discovers providers via the `get_provider_info()` function in `get_provider_info.py`:

```python
def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-lnkd",
        "versions": ["1.0.0"],
        "name": "LinkedIn Airflow Providers",
        "hook-class-names": [
            "airflow.providers.lnkd.lispark.hooks.spark_as_service.SparkServiceHook",
            "airflow.providers.lnkd.azkaban.hooks.azkaban_hook.AzkabanHook",
            # ... 6 more hooks
        ],
        "connection-types": [...],
        "extra-links": [...],  # 19 GGW operator links
    }
```

**Plugins** (registered in `setup.cfg`):
- `upload_dags_plugin` -> UploadDAGsPlugin
- `macro_plugin` -> MacrosPlugin
- `airflow_version_compatibility` -> AirflowVersionCompatibilityPlugin

**Dependencies**:
- `apache-airflow >= 2.5.3`
- `apache-airflow-providers-http`
- Internal: lipy-kafka, lipy-datavault, lipy-mp, lipy-crew, iris-client, mufn-control-api-client, grpc-python-client, etc.

---

## Development

See README.md for:
- **Snapshot testing**: `mint snapshot` to test changes across rdev instances
- **Local installation**: Copy tar files from rdev or local build to test airflow instance
- **Unit tests**: `cd <module> && source activate && pytest ./test`

Code quality: mypy type checking (disabled for most modules), black/isort formatting, >63% test coverage.

---

## See Also

- [GGW](ggw.md) — GridGateway detailed architecture & failure modes
- [Kafka](kafka.md) — Event publisher for DAG/task lifecycle
- [DAG Authoring](../dag-authoring.md) — How to author DAGs using these operators
- [Deployment](../deployment.md) — DAG upload/bundle promotion flow
- [Codebase Overview](../codebase/README.md) — Repo structure & local setup
