# Codebase — System DAGs + RDev Image

> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

## System DAGs

The `oklahoma_system_dags` multiproduct contains three categories of DAGs:

### Backfill DAGs (5 DAGs)
| DAG | Purpose | Schedule |
|-----|---------|----------|
| `backfill_creation__oklahoma_system_dags` | Process submitted backfills and force-sync newly created DAGs to database immediately | Every 3 minutes |
| `backfill_deletion__oklahoma_system_dags` | Delete expired backfill DAGs | Scheduled |
| `backfill_state_update__oklahoma_system_dags` | Update backfill state | Scheduled |
| `backfill_state_update_backlog__oklahoma_system_dags` | Process backlog of state updates | Scheduled |
| `backfill_dag_execution_alwaysok__oklahoma_system_dags` | Test backfill execution | Scheduled |

### Maintenance DAGs (8 DAGs)
| DAG | Purpose | Schedule |
|-----|---------|----------|
| `cleanup_logs__oklahoma_system_dags` | Clean up task and scheduler logs with retention policies (7 days by default) | Daily |
| `cleanup_inactive_dags__oklahoma_system_dags` | Remove inactive DAGs and associated FAB permissions | Scheduled |
| `cleanup_orphaned_fab_permissions__oklahoma_system_dags` | Clean up orphaned Airflow FAB (Flask App Builder) permission entries and backup for recovery | Scheduled |
| `restore_fab_permissions__oklahoma_system_dags` | Manual-only DAG to restore FAB permissions from NFS backup CSVs | Manual trigger |
| `cleanup_patch_deployments__oklahoma_system_dags` | Clean up patch deployment artifacts | Scheduled |
| `cleanup_scale_test_dags__oklahoma_system_dags` | Remove scale testing DAGs | Scheduled |
| `async_clean_rendered_task_instance_field__oklahoma_system_dags` | Asynchronously delete rendered task instance fields with worker parallelization | Scheduled |
| `auto_unlock__oklahoma_system_dags` *(or similar)* | Automatically unlock stuck/deadlocked Airflow resources (DAG import locks, task instance locks, or Hive locks). **Failing since ~Apr 14** — APA-144481 (In Progress, Major, Harsh Shah). Exact unlock target TBD. | Scheduled |

> **Note**: The `auto_unlock` DAG was added after the Apr 12 system-dags snapshot. Its exact DAG ID and implementation are not yet in the codebase snapshot. APA-144481 was filed Apr 14 and is still In Progress as of Apr 20.

### Regression Test DAGs (14 DAGs)

**Generic Tests (6 DAGs):**
| DAG | Purpose |
|-----|---------|
| `oklahoma_test_regression_alwaysOk__oklahoma_system_dags` | Basic sanity check that Oklahoma Airflow cluster is running (runs every minute) |
| `oklahoma_test_regression_failDag__oklahoma_system_dags` | Test failure handling and alerting |
| `oklahoma_test_regression_validationDag__oklahoma_system_dags` | Validate DAG structure and configurations |
| `oklahoma_test_regression_executeHooks__oklahoma_system_dags` | Test pre_execute and post_execute hook exception handling |
| `oklahoma_test_email_operator__oklahoma_system_dags` | Test EmailOperator functionality (sends test email) |
| `rtif_lock_contention_test__oklahoma_system_dags` | Test for RTIF (Rendered Task Instance Field) cleanup performance and lock contention |

**Grid Tests (8 DAGs):**
| DAG | Purpose |
|-----|---------|
| `oklahoma_test_regression_operator_command__oklahoma_system_dags` | Test Command operators on Grid Gateway |
| `oklahoma_test_regression_operator_sql__oklahoma_system_dags` | Test SQL operators (queries Trino via Grid Gateway) |
| `oklahoma_test_regression_operator_sparkbatch__oklahoma_system_dags` | Test Spark batch jobs |
| `oklahoma_test_regression_operator_hadoop_shell__oklahoma_system_dags` | Test Hadoop shell operations |
| `oklahoma_test_regression_operator_java__oklahoma_system_dags` | Test Java process execution |
| `oklahoma_test_regression_operator_java_process__oklahoma_system_dags` | Test Java process operators |
| `oklahoma_test_regression_operator_flyte__oklahoma_system_dags` | Test Flyte integration |
| `oklahoma_test_regression_sensor_dataset_sensor_array__oklahoma_system_dags` | Test dataset sensors |

---

## How System DAGs Differ from User DAGs

1. **Purpose**: System DAGs perform infrastructure/cluster maintenance and validation. User DAGs execute business logic and data pipelines.

2. **Naming Convention**: System DAGs use the suffix `__oklahoma_system_dags` to distinguish them from user DAGs.

3. **Access Control**: System DAGs are restricted to `SGP-CREW-1090-MEMBERS` (infrastructure team) or `SGP-ENG-oklahoma-dev`. User DAGs have service-specific permissions.

4. **Scheduling**: 
   - System DAGs run on fixed intervals or manually (not data-driven)
   - User DAGs typically respond to data availability events

5. **Configuration**: System DAGs use centralized config files under `config/` (global.jsonc, grid1/, grid2/, corp-lva1/, ei-ltx1/) to differ by Airflow cluster. User DAGs define their own configs.

6. **Deployment**: System DAGs are part of the `oklahoma_system_dags` multiproduct and deployed as a unit. User DAGs are independently deployed via CRT.

---

## Testing System DAGs

### Unit Testing
- **Location**: `/oklahoma_system_dags/test/`
- **Test framework**: pytest with mocking (unittest.mock)
- **Examples**:
  - `test_async_rti_deletion.py`: 15+ test classes covering the async RTI cleanup utility with batching, worker splitting, and window filtering
  - `test_cleanup_orphaned_fab_permissions.py`: Tests FAB permission deletion logic
  - `test_restore_fab_permissions.py`: Tests backup restoration

### Integration Testing
- **Rdev-first approach**: Developers test most operators within their Airflow rdev instance before PR review
- **Grid tests**: Verify operator functionality across Grid Gateway by running on actual Grid clusters (holdem)
- **Regression tests**: Auto-run regression DAGs on all Airflow clusters to catch regressions

### How to Test
1. **Build locally**: `mint build && mint release` generates a local zip artifact on rdev
2. **Trigger in rdev**: Manually trigger DAGs in rdev Airflow UI to verify behavior
3. **PR review**: Include rdev test results in PR description
4. **Deploy**: After approval, deploy via CRT to production cluster
5. **Babysit**: Monitor initial execution in production (optional but recommended)

---

## EmailOperator Regression Test DAG

**DAG ID**: `oklahoma_test_email_operator__oklahoma_system_dags`  
**Added**: Commit `299d2c4` (most recent)  
**Purpose**: Regression test to verify EmailOperator sends emails correctly

**Configuration**:
- **Schedule**: Manual trigger only (`schedule_interval=None`)
- **Start date**: 2026-01-01
- **Paused by default**: Yes (`is_paused_upon_creation=True`)
- **Access**: `SGP-ENG-oklahoma-dev`

**Task Structure**:
```python
EmailOperator(
    task_id="send_test_email",
    to="viagarwa@linkedin.com",
    subject="[Oklahoma] EmailOperator test - {{ ds }}",
    html_content="""
    <h3>EmailOperator Regression Test</h3>
    <p>DAG: <b>{{ dag.dag_id }}</b></p>
    <p>Execution date: {{ ds }}</p>
    <p>Run ID: {{ run_id }}</p>
    """
)
```

**What it tests**:
- Email configuration in Airflow cluster
- SMTP connectivity (relies on cluster's SMTP config)
- Templating of Airflow context variables (ds, run_id, dag_id)
- Email delivery to recipients

---

## RDev Airflow Image

**Repository**: `darwin-rdev-airflow-image`  
**Registry**: `container-image-registry.corp.linkedin.com/lps-image/linkedin/darwin-rdev-airflow-image/darwin-rdev`

### What's in the Image

The Darwin rdev image extends the base Oklahoma rdev image (`oklahoma-airflow/airflow-main-airflow-rdev:stable`) with:

1. **DBT Environment**: Creates isolated Python venv at `/opt/airflow/dags/dbt/venv/indbt-venv` for in-dbt (LinkedIn's internal DBT fork)
2. **Credentials & Certs**: Copies Datavault tokens and Grestin SSL certs into the image
3. **Environment Variables**: Sets platform-specific configs for Darwin development
4. **Dependencies**: Installs DBT packages (in-dbt, in-dbt-core, in-dbt-spark)
5. **DAG Files**: Mounts airflow-ignore files and Airflow configuration

### RDev vs Production Differences

| Aspect | RDev | Production |
|--------|------|-----------|
| **Base Image** | LinkedIn rdev base with development tools | Lightweight production-optimized image |
| **Platform** | Set to `DARWIN` for Darwin-specific behavior | Set to production fabric (corp-lva1, ei-ltx1, etc.) |
| **Grid Gateway** | `USE_TB_GRID_GATEWAY=True` for testing | Uses standard Grid Gateway config |
| **Credentials** | Datavault token and certs baked into image | Loaded from secure credential stores at runtime |
| **Logging** | More verbose for debugging | Production-level logging |
| **Permissions** | Loose for developer testing | Strict, based on cluster-level RBAC |
| **DBT Environment** | Full isolated venv for experimentation | Can be provisioned on-demand or pre-loaded |

### Key Configuration Differences

**Environment Variables Set in Dockerfile**:
```bash
LI_PLATFORM="DARWIN"                          # Platform identifier
DATAVAULT_TOKEN_PATH="/usr/tmp/dvtoken.txt"  # Token location for data access
GRESTIN_CERTS_DIR="/usr/tmp/"                # SSL certs for Grestin
USE_TB_GRID_GATEWAY="True"                   # Use Grid Gateway for jobs
```

**Note on SMTP**: No SMTP config is specifically mentioned in the rdev image; SMTP is configured at the Airflow cluster level via the base image or cluster configuration. The EmailOperator test DAG validates that SMTP is working in the cluster.

### How Developers Use RDev for Testing

1. **Create rdev instance**: Spin up a Darwin rdev Airflow instance using this image
2. **Edit DAGs**: Code DAGs locally and sync to rdev via mounted volumes
3. **Trigger & debug**: Use rdev's Airflow UI to trigger DAGs and see logs in real-time
4. **Test operators**: Verify operators work on Grid Gateway, Spark, SQL, etc. against dev clusters
5. **Iterate**: Quickly rebuild image (`docker build`) and restart Airflow if needed
6. **Graduate to production**: Once validated, deploy to production clusters via CRT

---

## Recent Changes

**Last 60 commits (oklahoma_system_dags)**:

- **2026-04-10**: Added EmailOperator regression test DAG
- **2026-03-31**: Tuned RTIF cleanup DAG defaults and improved logging
- **2026-03-26**: Rewrote RTIF cleanup DAG with fully-indexed approach for performance
- **2026-03-26**: Optimized RTIF batch delete to use single DELETE per batch instead of row-by-row
- **2026-02-14**: Added regression DAG for pre_execute/post_execute hook exception handling
- **2026-02-05**: Add FAB permission cleanup to cleanup_inactive_dags DAG
- **2025-12-20**: Optimize RTIF batch delete with bounded SELECT + row-by-row deletes with batched commits
- **2025-11-30**: Bump SparkBatch regression DAG timeouts to handle slow GGW bootstrap
- **2025-11-15**: Remove LI_BASE_USER role from Airflow DAGs access control

Key themes:
- RTIF (Rendered Task Instance Field) cleanup improvements (performance, batching)
- FAB permission management (cleanup, restoration, orphan handling)
- Regression test additions (hooks, email)
- Dependency updates via automated dependency upgrader (ADU)

---

## See Also
- [DAG Authoring](../references/dag-authoring.md)
- [Deployment](../references/deployment.md)
- [Patterns](../patterns.md)
