# Codebase

> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

This section covers codebase-level details: internal implementation, changelogs, gotchas, and development practices. For the high-level repository map and data flow, see [Architecture](../architecture.md).

## Working Branches

These are the active working branches for each repo (confirmed via GitHub API 2026-04-15):

| Repo | Default Branch | Notes |
|------|---------------|-------|
| `airflow` | `BR_REL-li.2.9.2` | LinkedIn's Airflow fork — track release branch, not master |
| `lipy-airflow-providers` | `BR_REL-li.2.9.2` | LinkedIn's custom providers — track release branch, not master |
| `oklahoma-airflow-deployment` | `master` | Helm/K8s deployment config |
| `oklahoma_system_dags` | `master` | Oklahoma team's system DAGs |
| `picli` | `master` | LinkedIn's Airflow CLI tool |
| `tradewind` | `master` | Transparent proxy for Airflow API requests |
| `airflow-crt-action` | `master` | CRT GitHub Actions for Airflow deployments |

> **Important**: `airflow` and `lipy-airflow-providers` use `BR_REL-li.2.9.2`, NOT `master`. PRs and local dev should target this branch.

### PR Guidelines

- **Always base feature branches off the repo's default branch** (see table above). Never branch from `master` if the default branch is a release branch (e.g., `BR_REL-li.2.9.2`).
- Before creating a branch, ensure your local default branch is up to date:
  ```bash
  git checkout <default-branch>
  git fetch origin
  git merge --ff-only origin/<default-branch>
  git checkout -b <your-feature-branch>
  ```
- When opening a PR, confirm the **base branch** in GitHub matches the default branch in the table — GitHub may default to `master` even when the working branch is different.
- If your PR base is wrong, update it before requesting review: `gh pr edit <number> --base <default-branch>`

### Keeping Default Branches in Sync

- Regularly pull from `origin` to avoid drift:
  ```bash
  git fetch origin
  git checkout <default-branch>
  git merge --ff-only origin/<default-branch>
  ```
- Use `--ff-only` — if it fails, your local branch has diverged and should not be rebased; investigate before proceeding.
- Never push directly to the default branch; always go through a PR.
- For repos with `BR_REL-li.2.9.2`: upstream Airflow changes from `master` are cherry-picked by the platform team; do not merge `master` into `BR_REL-li.2.9.2` manually.

---

## Entry Points

### Key Repos

| Repo | Purpose | Entry Point | Owner |
|------|---------|-------------|-------|
| **lipy-airflow-providers** | LinkedIn's custom operators, listeners, hooks, policies | `apache-airflow-providers-lnkd/src/airflow/providers/lnkd/get_provider_info.py` | lipy-airflow team |
| **oklahoma-listener** | DAG/task lifecycle event capture & analysis (part of lipy-airflow-providers) | `src/linkedin/airflow/plugins/oklahoma/listener/listener_plugin.py` | lipy-airflow team |
| **airflow-policy-framework** | DAG/operator policy enforcement (part of lipy-airflow-providers) | Policy classes in `src/` | lipy-airflow team |

### Package Layout: lipy-airflow-providers

```
lipy-airflow-providers/
├── apache-airflow-providers-lnkd/    Main provider package
│   ├── src/airflow/providers/lnkd/
│   │   ├── gridgateway/              ~30 GridGateway operators (SparkBatch, Flink, Trino, Java, etc.)
│   │   ├── operators/                TriggerDagRunOperator
│   │   ├── sensors/                  SensorArray
│   │   ├── hooks/                    Base hook classes
│   │   ├── azkaban/                  Azkaban integration
│   │   ├── ambry/                    Ambry blob store integration
│   │   ├── bdp/                      BDP services (ARMS, Jasper, Featurecloud)
│   │   ├── iris/                     IRIS incident management
│   │   ├── kms/                      Key Management Service
│   │   ├── log/                      Custom logging handlers
│   │   ├── notifications/            Custom formatters (Slack/email)
│   │   ├── oncall/                   On-call integration
│   │   ├── upload_dags/              DAG sync/upload plugin
│   │   ├── macros/                   Date utility macros
│   │   ├── utils/                    Shared utilities
│   │   ├── get_provider_info.py      Airflow discovery entry point
│   │   └── exceptions.py             Custom exceptions
│   ├── setup.py, setup.cfg
│   └── build.gradle
│
├── oklahoma-listener/                Event listeners & root cause analysis
│   ├── dag_listener.py, task_listener.py, dag_upload_listener.py
│   ├── event_schemas/                Task/DAG/Upload event types
│   ├── root_cause_analyzer/          Failure classification
│   └── listener_plugin.py            Airflow plugin registration
│
├── oklahoma-helpers/                 Shared utilities
├── oklahoma-backfill/                Backward compat stub
├── airflow-policy-framework/         Policy enforcement engine
│
├── gradle.properties                 Gradle config
├── settings.gradle                   Multi-module config
├── build.gradle                      li-python-product plugin
└── README.md                         Dev testing guide
```

### Airflow Provider Discovery

**File**: `apache-airflow-providers-lnkd/src/airflow/providers/lnkd/get_provider_info.py`

This function registers all hooks, connections, and extra-links with Airflow:

```python
def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-lnkd",
        "hook-class-names": [
            "airflow.providers.lnkd.lispark.hooks.spark_as_service.SparkServiceHook",
            "airflow.providers.lnkd.azkaban.hooks.azkaban_hook.AzkabanHook",
            # ... 8 hooks total
        ],
        "connection-types": [...],
        "extra-links": [...],  # 19 GGW operator links
    }
```

### Airflow Plugins

**File**: `apache-airflow-providers-lnkd/setup.cfg` [options.entry_points]

```
apache_airflow_provider=
    provider_info=airflow.providers.lnkd.get_provider_info:get_provider_info
airflow.plugins =
    upload_dags_plugin=airflow.providers.lnkd.upload_dags.upload_dags_plugin:UploadDAGsPlugin
    macro_plugin=airflow.providers.lnkd.macros.macros_plugin:MacrosPlugin
    airflow_version_compatibility=airflow.providers.lnkd.airflow_version_compatibility.airflow_version_compatibility:AirflowVersionCompatibilityPlugin
```

### How to Navigate the Code

**Finding an Operator**:
1. Start at `apache-airflow-providers-lnkd/src/airflow/providers/lnkd/gridgateway/operators/`
2. Look for `<operator_name>.py`
3. Operators inherit from `GridGatewayBaseOperator` (in `grid_gateway_base.py`)
4. Extra-links defined as nested `BaseOperatorLink` classes

**Finding a Hook**:
1. `get_provider_info.py` lists all hook class paths
2. Navigate to module (e.g., `azkaban/hooks/azkaban_hook.py`)
3. Hooks manage connection auth, API calls, context

**Finding Listener Code**:
1. `oklahoma-listener/src/linkedin/airflow/plugins/oklahoma/listener/`
2. `dag_listener.py` — DAG start/complete events
3. `task_listener.py` — Task lifecycle events
4. `root_cause_analyzer/analyzer.py` — Failure classification
5. `event_schemas/` — Event type definitions (DAG, Task, Upload)

**Finding Policy Rules**:
1. `airflow-policy-framework/src/` — Policy classes
2. Policies enforce ACLs, operator deprecation, DAG mutation
3. Integrate with Airflow's `policies.py` system

**Finding Utils**:
- URL parsing, datetime: `utils/url_util.py`, `utils/datetime_util.py`
- Context enrichment: `utils/context_util.py`
- DRY run logic: `utils/dry_run_utils.py`

---

## Versioning & Local Dev

**Scheme**: Semantic versioning, currently at 10.x.x

**Release process**:
```bash
mint build              # Compile & run tests
mint snapshot           # Create SNAPSHOT version
mint release            # Tag release
mint publish --branch=master  # Publish to Corp Artifactory
```

**Local Testing Against DAG Repo**:
1. In lipy-airflow-providers: `mint build && mint snapshot`
2. Note the snapshot version (e.g., `8.0.77-SNAPSHOT`)
3. In DAG repo, update `mint.yaml` to use snapshot version
4. DAG repo: `mint build` — includes snapshot provider

**Code Quality Standards**:
- Type checking: mypy (disabled for most modules for pragmatism)
- Formatting: black (line length 160), isort
- Tests: pytest, minimum 63% coverage
- Build checks: flake8 (excluded: generated code in `mufn/stubs/`)

---

## Pages in This Section

| Page | Summary |
|------|---------|
| [Gotchas](gotchas.md) | Non-obvious behaviors, footguns, known bugs — the things that waste time |
| [Airflow Fork Internals](airflow-fork-internals.md) | Deep internals: reschedule lock, executor batching, SKIP LOCKED, RTIF gap lock fixes |
| [Deployment Changelog](deployment-changelog.md) | Infrastructure history: Nimbus migration, cluster timeline, version 2.5 to 2.9.2.170 |
| [lipy-airflow-providers Changelog](lipy-changelog.md) | 6-month evolution: GGW reliability, checkpointing, policy, v8 to v10 |
| [System DAGs](system-dags.md) | Oklahoma team's system DAGs and the rdev Airflow Docker image |
| [Performance](performance.md) | Load testing, benchmarks, and performance characteristics |
| [PR Standards](pr-standards.md) | PR description template requirements |

---

## See Also
- [Architecture](../architecture.md) — workspace repository map, data flow, dependency graph
- [Build and Test](../build-and-test.md) — build, test, and lint commands for all repos
- [Patterns](../patterns.md) — code conventions and architectural patterns
- [Gotchas](gotchas.md) — known issues and footguns
