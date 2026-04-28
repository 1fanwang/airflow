> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

# Airflow — DAG Authoring

## Operators Reference

### Grid Gateway Operators

Grid Gateway operators enable distributed execution of jobs on LinkedIn's Grid infrastructure. All inherit from `GridGatewayBaseOperator` and support external job checkpointing for pod disruption recovery.

| Operator | Purpose | Key Parameters | Notes |
|----------|---------|-----------------|-------|
| **SparkBatchOperator** | Execute Spark jobs on Hadoop clusters | `proxy_user`, `execution_target` (jar), `driver_memory`, `executor_memory`, `executor_num`, `executor_cores`, `spark_confs` (Dict), `app_params` (List), `job_queue`, `enable_job_checkpoint` | Replaces Azkaban Spark jobtype. Supports custom Spark configs via `conf.{key}` params. Links to Spark Job Log via XCom. Min polling interval: 30s. |
| **FlinkBatchOperator** | Execute Flink batch jobs | `proxy_user`, `grid_gateway_params`, `grid_gateway_dependencies`, `enable_job_checkpoint` | Minimal config required; most params via `grid_gateway_params` dict. Links to Flink Job Log. |
| **HadoopJavaOperator** | Run Java jobs that access HDFS via Hadoop tokens | `proxy_user`, `job_class` (fully qualified), `dependency_ivy` (Ivy coords), `grid_gateway_dependencies`, `enable_job_checkpoint` | Maps `job_class` to `grid_gateway_params["job.class"]`. Secure cluster access via Kerberos/Hadoop tokens. |
| **JavaOperator** | Legacy: plain Java execution (no HDFS access) | `proxy_user`, `job_class` | **DEPRECATED**. Use `HadoopJavaOperator` (HDFS access) or `JavaProcessOperator` (plain Java). Raises `DeprecationWarning`. |
| **JavaProcessOperator** | Execute plain Java code without Hadoop access | `proxy_user`, `job_class`, `enable_job_checkpoint` | Modern replacement for `JavaOperator`. No Hadoop token setup overhead. |
| **KafkaPushOperator** | Push data to Kafka topics | `proxy_user`, `topic` (required), `input_path` (HDFS), `name_node`, `batch_size` (bytes), `kafka_url`, `disable_schema_registration`, `enable_job_checkpoint` | Auto-registers schema unless `disable_schema_registration=True`. Validates input schema matches destination topic. Supports Dali input via `input.dali.resource` in `grid_gateway_params`. |
| **CommandOperator** | Execute shell commands via Grid Gateway | `proxy_user`, `command` (str or Iterable[str]) | Maps multi-part commands: `command[0]` to `grid_gateway_params["command"]`, `command[1]` to `grid_gateway_params["command.1"]`, etc. Supports disruption readiness. |
| **SQLOperator** | Execute SQL queries via JDBC | `proxy_user`, `jdbc_url`, `jdbc_user_id`, `jdbc_encrypted_credential`, `jdbc_crypto_key_path` | Params map to `jdbc.*` keys. Credentials always required (can be placeholder "foo" for disabled auth). |
| **DataQualityJobOperator** | Run DSSV3 data quality assertions | `proxy_user`, `cluster`, `assertions` (DSSV3 definition), `assertion_variables` (Dict), `persist` (bool), `job_queue`, `ml_context` (Dict), `wap_id`, `enable_rules_repository`, `retry_on_assertion_failure` | Assertions base64+zlib encoded. Assertion failures: raises `AirflowFailException` (no retries) by default; set `retry_on_assertion_failure=True` to allow Airflow retries. ML context for spark config with `dataset_options` / `dali_options` support. |
| **HadoopShellOperator** | Execute shell scripts on Hadoop cluster | `proxy_user`, grid_gateway_params | Maps to `hadoopShell` function. Supports disruption readiness. |
| **HadoopJavaProcessOperator** | Base for Java-based Grid Gateway jobs | `proxy_user`, common Grid Gateway params | Base class for SparkBatchOperator, HadoopJavaOperator, etc. Handles polling & XCom pushes. |
| **CarbonOperator** | Carbon data platform integration | `proxy_user`, grid_gateway_params | Maps to Carbon function on Grid. |
| **PinotPushOperator** | Push data to Pinot search index | `proxy_user`, topic (KAFKA), grid_gateway_params | Dali input support. Table/column mapping via params. |
| **AmbryPushOperator** | Push blobs to Ambry storage | `proxy_user`, grid_gateway_params | LinkedIn blob store integration. |
| **VenicePushOperator** | Push data to Venice KV store | `proxy_user`, grid_gateway_params | Streaming store integration. |
| **WormholePushOperator** | Push to Wormhole data platform | `proxy_user`, grid_gateway_params | Generic push operator. |
| **DarwinOperator** | Darwin ML platform execution | `proxy_user`, grid_gateway_params | ML job orchestration on Grid. |
| **FlyteOperator** | Flyte workflow execution | `proxy_user`, grid_gateway_params | Flyte task submission via Grid Gateway. |
| **InDBTJobTypeOperator** | Execute dbt models in-database | `proxy_user`, grid_gateway_params | Distributed dbt execution. |
| **GridGatewayOperator** | Generic Grid Gateway job (fallback) | `proxy_user`, `function_name`, `grid_gateway_params` | Direct function invocation. Use specific operators when available. |

### Other Operators

| Operator | Purpose | Key Parameters | Notes |
|----------|---------|-----------------|-------|
| **LnkdTriggerDagRunOperator** | Trigger & monitor DAG runs with checkpoint support | `trigger_dag_id`, `wait_for_completion=True`, `poke_interval`, `allowed_states`, `failed_states`, `enable_job_checkpoint=True` | Extends Airflow's `TriggerDagRunOperator`. On pod disruption: checkpoints triggered `run_id` in XCom, resumes polling on restart instead of re-triggering. Only checkpoints when `wait_for_completion=True`. |
| **EmailOperator** | Send emails via LinkedIn SMTP | `to` (str or list), `subject`, `html_content`, `files` (list of attachment paths), `cc`, `bcc`, `mime_subtype` (default "mixed"), `mime_charset` (default "utf-8") | Uses LinkedIn's SMTP infrastructure (`smtp_host`, `smtp_port`, `smtp_mail_from` in airflow.cfg). Supports HTML content, multiple recipients, and file attachments. Available on rdev since 2026-04-11 (deployment PR #1038 added SMTP config). **RDev redirect (since 2026-04-18)**: `EmailOperatorPatchPlugin` (lipy-airflow-providers PR #1187) intercepts all EmailOperator sends in rdev and redirects both `to` and `from` to `$USER@linkedin.com` — prevents spamming team DLs during testing. See [airflow-docs EmailOperator page](https://airflow-docs.corp.linkedin.com) for examples. |
| **AzkabanFlowExecutionOperator** | Trigger Azkaban flows | `azkaban_conn_id`, `project_name`, `flow_name`, `disabled_jobs` (List[str]), `concurrent_option` ("skip"/"ignore"), `runtime_properties` (Dict), `wait_for_completion=True`, `poke_interval` (default 60s), `wait_timeout` (default 3600s) | Embeds AzkabanFlowExecutionSensor when `wait_for_completion=True`. Stores execution ID in XCom. Links to Azkaban UI via `XCOM_KEY_AZKABAN_FLOW_EXECUTION`. |

---

## Sensors Reference

| Sensor | Purpose | Mode | Poke Interval | Notes |
|--------|---------|------|---------------|-------|
| **AzkabanFlowExecutionSensor** | Monitor Azkaban flow until completion | `poke` (polling) | default 60s (configurable) | Tracks consecutive connection failures (default max 3). Fetches failed job logs on failure. Returns `PokeReturnValue(is_done=True/False)`. State persisted in XCom. Timeout default: 3600s. |
| **AzkabanFlowExecutionJobLogsSensor** | Wait for specific job logs in Azkaban flow | `poke` | default 60s | Monitors job-level log output. Supports offset tracking for incremental log retrieval. |
| **AzkabanSensor** (base) | Abstract base for Azkaban sensors | N/A | N/A | Provides hook initialization, connection management. Subclass and implement `poke(context)`. |
| **SnapshotSensorDefinition** | Wait for dataset snapshot update (ARMS) | `poke` | No built-in; use SensorArrayOperator | Compares dataset update timestamp vs baseline (default: `dag_run.start_date`). Supports watermark field names for column-level tracking. Grabs timestamp via ARMS gRPC. Returns True if dataset updated after baseline. |
| **PartitionSensorDefinition** | Wait for dataset partition (ARMS) | `poke` | No built-in; use SensorArrayOperator | Checks partition existence via ARMS. Supports multi-column partitions, granularity (DAILY/HOURLY), fallback mode (STRICT/TOLERANT). `partitions_required`: "all" (all required) or "any" (at least one). Returns True if partitions exist. |

### Sensor Characteristics

- **Polling-based**: All sensors use `poke()` method returning `bool` or `PokeReturnValue`.
- **Disruption Readiness**: Azkaban sensors support pod disruption checkpointing (state persisted in XCom).
- **No reschedule mode**: ARMS sensors (Snapshot/Partition) don't support reschedule; use polling via `SensorArrayOperator`.
- **XCom-based state**: Azkaban sensors track state (connection failures, offsets) in XCom for resume on pod restart.

---

## Grid Gateway Configuration

### Common Parameters (all Grid Gateway operators)

- **`grid_gateway_conn_id`** (default: "grid_gateway_service_default"): Airflow connection for Grid Gateway service endpoint.
- **`polling_interval`** (default: 30s, min enforced: 30s): Polling frequency during job execution.
- **`grid_gateway_params`** (Dict[str, str]): Job-specific parameters passed to Grid Gateway function.
  - Syntax: `{"key": "value"}` maps to function parameter `key=value`.
  - Operator-specific prefixes: `spark_*`, `conf.*`, `dq.job.*`, `dq.ml.*`, etc.
- **`grid_gateway_dependencies`** (Sequence[str]): Archive URLs for extra dependencies (Grid Gateway format).
- **`dependency_ivy`** (Sequence[str]): Maven/Ivy coordinates for dependencies (e.g., `"org.example:lib:1.0"`).
- **`grid_gateway_function_overrides`** (List[FunctionOverride]): Override function implementations at runtime.
- **`target_grid_cluster`** (str, optional): Route job to specific Grid cluster (default: inferred from environment).
- **`enable_sts_token`** (bool, default: False): Use STS token for DataVault authentication.
- **`image_url`** (str, optional): Custom container image URL for job execution.
- **`disruption_ready`** (bool, default: False): Enable disruption-ready retry policy for ENVIRONMENT_* errors.
  - Supported functions: `hadoopJava`, `java`, `javaprocess`, `command`, `hadoopShell`.
  - Applies: `maxRetries: 3` on ENVIRONMENT_* error codes.
- **`allow_rdev_runs`** (bool, default: True): Skip execution in rdev to preserve prod data stores.
- **`enable_job_checkpoint`** (bool, default: True): Persist external job state on pod disruption.

### XCom Keys (Operator-Specific)

Operators push job logs to XCom for clickable "Job Log" links in Airflow UI:

| Operator | XCom Key | Value |
|----------|----------|-------|
| SparkBatchOperator | `spark.log.url` | Spark driver/executor logs URL |
| FlinkBatchOperator | `flink.log.url` | Flink job manager URL |
| HadoopJavaOperator | `hadoop_java.log.url` | Hadoop job tracker URL |
| CommandOperator | `command.log.url` | Command execution log |
| SQLOperator | `sql.log.url` | SQL query execution log |
| DataQualityJobOperator | `data_quality_job.log.url` | DQ job execution log |
| KafkaPushOperator | `kafka_push.log.url` | Kafka push job log |
| All Grid Gateway | `mufn.log.url` | Generic Grid Gateway log |

---

## Naming Conventions

### Task IDs
- **Lowercase with underscores**: `extract_data`, `validate_schema`, `load_warehouse`.
- **Avoid reserved words**: Don't use `task`, `operator`, `sensor`, `trigger`.
- **Be descriptive**: Include action + target, e.g., `spark_process_events`, `wait_for_partition`.

### Operator Parameters
- **Snake_case**: `proxy_user`, `job_class`, `driver_memory`, `enable_job_checkpoint`.
- **Prefixes for grouped params**:
  - Spark: `spark_confs`, `executor_num`, `driver_memory` (top-level); `conf.spark.*` (in `grid_gateway_params`).
  - Kafka: `topic`, `input_path`, `name_node` (top-level); `name.node`, `batch.num.bytes`, `kafka.url` (in `grid_gateway_params`).
  - DQ: `assertions`, `assertion_variables`, `persist` (top-level); `conf.spark.dq.job.*` (in `grid_gateway_params`).

### Connection IDs
- Default: `grid_gateway_service_default` for Grid Gateway.
- Azkaban: `azkaban_default` or custom `azkaban_conn_id`.

---

## Deprecated Operators — Do Not Use

### JavaOperator
- **Status**: Deprecated since Airflow 2.9+.
- **Replacement**: 
  - Use **HadoopJavaOperator** if job accesses HDFS (Hadoop tokens required).
  - Use **JavaProcessOperator** if plain Java code (no Hadoop access).
- **Symptom**: Raises `DeprecationWarning` on instantiation.

### skip_if_specified() (in task_execution_util)
- **Status**: Deprecated.
- **Replacement**: Use `skip_specified_tasks()` for conditional task skipping.

---

## Recent Changes & Fixes (last 40 commits)

Key improvements from recent commits:

1. **External Job Checkpointing** (BDP-39842, commit bb308be2):
   - Added `enable_job_checkpoint` to GridGatewayBaseOperator & LnkdTriggerDagRunOperator.
   - Survives Kubernetes pod disruptions by checkpointing external job IDs (run_id, execution URN) in XCom.
   - On restart: resumes monitoring instead of re-triggering/re-executing.

2. **Disruption Readiness** (BDP-60120, commit 9789d8d3):
   - New `disruption_ready` parameter for Grid Gateway operators.
   - Applies automatic retry policy (`maxRetries: 3`) on ENVIRONMENT_* errors.
   - Supported functions: `hadoopJava`, `java`, `javaprocess`, `command`, `hadoopShell`.

3. **DataVault Token Support** (commit b7998347):
   - New `GridGatewayDataVaultTokenException` for DataVault auth failures.
   - `enable_sts_token=True` for STS token authentication.

4. **DQ Assertion Retry Control** (commit 2757819a):
   - New `retry_on_assertion_failure` parameter for DataQualityJobOperator.
   - Default: False (fails immediately without retry).
   - Set True to allow Airflow's native retry mechanism on DQ assertion failures.

5. **Grid Gateway Error Banners** (commit 24a1afcf):
   - Enhanced error messages with Nimbus GGW logs dashboard URL.
   - Improved Airflow error visibility and support redirects.

6. **Rules Repository** (commit d37c1b96):
   - New `enable_rules_repository` parameter for DataQualityJobOperator.
   - Enables DQ rule repository usage for assertion evaluation.

7. **Config Override for All GGW Operators** (lipy PR #1156, 2026-03-31):
   - All GGW operators now declare `get_overridable_attrs()` using `super()` inheritance.
   - Users can override operator configs (e.g., `executor_num`, `driver_memory`) at runtime from the DAG trigger page without code changes.
   - Supported via `_overridable_attrs_map` on each operator.
   - See [Config Override docs](https://airflow-docs.corp.linkedin.com) for supported operators and fields.

8. **`use_hourly_for_daily` Partition Sensor Support** (lipy PR #1158, 2026-04-09):
   - Daily datasets reading from hourly-partitioned source tables can now use `use_hourly_for_daily=True` in the hourly partition sensor.
   - Uses UMP calendar-day boundaries (`00`-`23`) instead of hour-anchored sliding windows for partition checks.

9. **EmailOperator** (airflow-docs PR #252, 2026-04-11):
   - New documentation for `EmailOperator` with usage examples (basic, multiple recipients, attachments, HTML content).
   - SMTP config added to rdev environment (deployment PR #1038).

---

## Common Patterns & Best Practices

### Handling Grid Gateway Jobs with Checkpointing

```python
from airflow.providers.lnkd.gridgateway.operators.spark_batch import SparkBatchOperator

spark_task = SparkBatchOperator(
    task_id="process_events",
    proxy_user="{{ var.value.proxy_user }}",
    execution_target="/path/to/job.jar",
    driver_memory="2g",
    executor_memory="4g",
    executor_num=10,
    job_queue="default",
    enable_job_checkpoint=True,  # Enables pod disruption recovery
    grid_gateway_conn_id="grid_gateway_service_default",
    retries=3,
)
```

### Using Data Quality Assertions

```python
from airflow.providers.lnkd.gridgateway.operators.data_quality_job import DataQualityJobOperator

dq_task = DataQualityJobOperator(
    task_id="validate_events",
    proxy_user="{{ var.value.proxy_user }}",
    cluster="holdem",
    assertions="""
    {
        "dataset": "table_name",
        "columns": [
            { "name": "col1", "type": "string", "checks": ["notNull"] }
        ]
    }
    """,
    assertion_variables={"threshold": "0.99"},
    persist=True,
    retry_on_assertion_failure=True,  # Allow Airflow retries on DQ failure
)
```

### Triggering & Monitoring External DAGs

```python
from airflow.providers.lnkd.operators.trigger_dagrun import LnkdTriggerDagRunOperator

trigger_task = LnkdTriggerDagRunOperator(
    task_id="trigger_downstream_dag",
    trigger_dag_id="downstream_dag_id",
    wait_for_completion=True,
    poke_interval=60,
    enable_job_checkpoint=True,  # Survives pod disruption
)
```

### Disruption-Ready Jobs

```python
from airflow.providers.lnkd.gridgateway.operators.command import CommandOperator

cmd_task = CommandOperator(
    task_id="run_command",
    proxy_user="{{ var.value.proxy_user }}",
    command="/usr/bin/my_script.sh --arg1 value1",
    disruption_ready=True,  # Applies retry policy on ENVIRONMENT_* errors
    grid_gateway_conn_id="grid_gateway_service_default",
)
```

---

## Integration with Related Systems

### Grid Gateway (GGW)
- Core execution engine for Grid operators.
- Provides distributed job execution, resource management, Hadoop token handling.
- Connection: Airflow -> `grid_gateway_service_default` -> Grid Gateway service endpoint.

### Azkaban
- Legacy job scheduler at LinkedIn.
- AzkabanFlowExecutionOperator triggers flows; AzkabanFlowExecutionSensor monitors.
- Use for legacy workflow migration; prefer Grid Gateway for new DAGs.

### Data Trigger (ARMS)
- Metadata service for dataset/partition tracking.
- SnapshotSensorDefinition & PartitionSensorDefinition monitor dataset readiness.
- Grpc-based communication; default connection: `arms_default`.

### OklahomaEnvironment
- LinkedIn-specific environment utilities.
- Detects cluster, RDEV mode, provides identity context.
- Used internally by Grid Gateway operators for execution tagging.

### Spark / Flink
- Batch job engines.
- SparkBatchOperator & FlinkBatchOperator wrap Grid Gateway execution.
- Config: `spark_confs`, `executor_*`, `driver_memory` -> `grid_gateway_params`.

### dBT (via InDBTJobTypeOperator)
- Data transformation framework.
- Distributed dbt execution via Grid Gateway.
- Model compilation & execution delegated to Grid; Airflow orchestrates.

---

## Email Sending

LinkedIn's internal SMTP gateway. Use `EmailOperator` for simple cases; use `smtplib` in a `PythonOperator` for custom formatting or attachments.

- **Gateway**: `mail-gw.corp.linkedin.com` port 25
- **Protocol**: SMTP with STARTTLS
- **Auth**: Not required for `@linkedin.com` addresses
- **RDev**: `EmailOperatorPatchPlugin` redirects all sends to `$USER@linkedin.com` — prevents spam during testing
- **Reference**: [How To Send Email Messages On Our Network](https://linkedin.atlassian.net/wiki/spaces/IST/pages/607259230)

```python
import smtplib
from email.mime.text import MIMEText
from airflow.operators.python import PythonOperator

def send_email(**context):
    msg = MIMEText("Email body")
    msg["Subject"] = "Subject"
    msg["From"] = "myname@linkedin.com"
    msg["To"] = "recipient@linkedin.com"
    with smtplib.SMTP("mail-gw.corp.linkedin.com", 25) as server:
        server.starttls()
        server.sendmail(msg["From"], [msg["To"]], msg.as_string())

send_task = PythonOperator(task_id="send_email", python_callable=send_email)
```

Test email DAGs on the `airflow-load-test` cluster before deploying to production.

---

## Proxy User Configuration

The default proxy user for Grid Gateway DAGs in the starter kit is `dppfoundations`. HDFS output paths follow the pattern `/jobs/<proxy_user>`.

When changing the proxy user (e.g., to `airflowstarter`), update the HDFS path consistently in the same change. Config lives in `starter_dags/src/airflow_starter_kit/starter_dags/holdem/`.

---

## See Also
- [lipy-airflow-providers](systems/lipy-airflow-providers.md) — Operator & sensor source code
- [Overview](overview.md) — DAG structure & basics
- [GGW](systems/ggw.md) — Grid Gateway documentation
- [Azkaban](systems/azkaban.md) — Legacy scheduler (reference)
