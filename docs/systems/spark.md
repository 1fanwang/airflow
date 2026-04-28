> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# System — Spark at LinkedIn

> How Airflow submits and manages Spark jobs at LinkedIn via Grid Gateway

## What It Is

Spark job execution at LinkedIn is brokered through **Grid Gateway**, a unified job submission platform. Airflow does not call Spark directly; instead, tasks use the `SparkBatchOperator` to submit jobs to Grid Gateway, which handles provisioning, resource allocation, YARN queue management, and log aggregation. This architecture:

- Decouples Airflow from cluster topology (YARN, Kubernetes, cloud-specific)
- Centralizes security (Kerberos tokens, DataVault secrets, SPIFFE certificates)
- Provides unified logging, metrics, and job tracking across job types
- Supports multiple execution targets: Scala JARs, PySpark scripts, mixed workloads

Grid Gateway is internal LinkedIn infrastructure. Public Apache Spark operators (e.g., `SparkSubmitOperator`) are **deprecated** in favor of `SparkBatchOperator`.

---

## LipySparkOperator

The operator class is **`SparkBatchOperator`** (not "LipySpark"). It inherits from `HadoopJavaProcessOperator` and submits Spark jobs to Grid Gateway.

Location: `/airflow/providers/lnkd/gridgateway/operators/spark_batch.py`

### Constructor Parameters

| Parameter | Type | Default | Notes |
|-----------|------|---------|-------|
| `proxy_user` | `str` | **Required** | User to impersonate for job execution. Must have Grid User Manager approval. |
| `execution_target` | `str` | None | Main execution artifact: `.jar` file path or `.py` file path (PySpark). Replaces deprecated `execution_jar`. |
| `job_class` | `str` | None | Fully qualified class name (e.g., `com.linkedin.spark.SparkCount`). Required for Java/Scala JARs. |
| `spark_version` | `str` | None | Spark version (e.g., `3.1.1`). Passed to Grid Gateway for runtime selection. |
| `driver_memory` | `str` | None | Driver heap size (e.g., `2G`, `4G`). Maps to `--driver-memory` in spark-submit. |
| `executor_memory` | `str` | None | Executor heap size (e.g., `4G`, `8G`). Maps to `--executor-memory`. |
| `executor_num` | `int` | None | Number of Spark executors (e.g., 2, 10, 100). Maps to `--num-executors`. |
| `executor_cores` | `int` | None | CPU cores per executor (e.g., 4, 8). Maps to `--executor-cores`. |
| `classpath_jars` | `str` | None | Colon-separated JAR paths to add to classpath. Maps to `--jars`. |
| `flags` | `List[str]` | None | Spark flags (e.g., `["verbose"]`). Each flag `f` becomes `flag.{f}=true`. |
| `spark_confs` | `Dict[str, str]` | None | Custom Spark config (e.g., `{"spark.authenticate": "true"}`, `{"spark.rightsizing.enabled": "true"}`). Prefixed with `conf.` when sent to Grid Gateway. |
| `app_params` | `List[str]` | None | Application arguments (e.g., input/output file paths). Space-joined and passed as `params` to job. |
| `job_queue` | `str` | None | YARN queue name (e.g., `spark_queue`, `priority.production`). Maps to `--queue`. |
| `dependency_ivy` | `Sequence[str]` | None | Ivy coordinates for JAR dependencies (e.g., `["org.example:lib:1.0"]`). Grid Gateway resolves from Artifactory. |
| `grid_gateway_dependencies` | `Sequence[str]` | None | Pre-built multiproduct artifacts (e.g., `["@Artifact(foo,bar,baz)"]`). Preferred over `dependency_ivy` for large/complex dependencies. |
| `grid_gateway_params` | `Dict[str, str]` | None | Raw Grid Gateway parameters (escape hatch for undocumented features). |
| `grid_gateway_function_overrides` | `List[FunctionOverride]` | None | Override Grid Gateway function behavior (advanced). |
| `polling_interval` | `int` | 30 (min) | Seconds between status polls. Clamped to >=30 by Grid Gateway base operator. |
| `enable_job_checkpoint` | `bool` | True | Enable pod disruption recovery via external job checkpointing (BDP-20471). |
| `disruption_ready` | `bool` | False | Enable ENVIRONMENT_* error retry policy for pod disruption resilience. **Not yet supported for Spark** (only hadoopJava, java, javaprocess, command, hadoopShell). |

### Execution Validation

The operator requires **either** `execution_target` or `execution_jars` (deprecated):

```python
if self.execution_target is None and self.execution_jars is None:
    raise ValueError("One of execution_target and execution_jars must be defined")
```

If only `execution_jars` is provided, it is copied to `execution_target` for backward compatibility.

### Parameter Mapping to Grid Gateway

Parameters are transformed into a flat dictionary (`grid_gateway_params`) sent to Grid Gateway:

| Operator Param | Grid Gateway Key | Example Value |
|---|---|---|
| `execution_target` | `execution-jar` | `lib/my-spark-app.jar` or `myscript.py` |
| `job_class` | `class` | `com.linkedin.analysis.Main` |
| `spark_version` | `spark-version` | `3.1.1` |
| `driver_memory` | `driver-memory` | `2G` |
| `executor_memory` | `executor-memory` | `4G` |
| `executor_cores` | `executor-cores` | `4` (stringified) |
| `executor_num` | `num-executors` | `10` (stringified) |
| `classpath_jars` | `jars` | `lib/foo.jar:lib/bar.jar` |
| `job_queue` | `queue` | `spark_queue` |
| `app_params` | `params` | `/input/path /output/path` (space-joined) |
| `spark_confs['key']` | `conf.key` | `conf.spark.authenticate=true` |
| `flags[0]` | `flag.{flag}` | `flag.verbose=true` |

---

## Spark Submission Flow

### 1. DAG Definition

```python
from airflow.providers.lnkd.gridgateway.operators.spark_batch import SparkBatchOperator

spark_task = SparkBatchOperator(
    task_id="spark_wordcount",
    proxy_user="spark_team",
    execution_target="lib/acceptance-test_2.12-0.0.119.jar",
    job_class="com.linkedin.spark.SparkCount",
    spark_version="3.1.1",
    driver_memory="2G",
    executor_memory="4G",
    executor_num=4,
    executor_cores=2,
    classpath_jars="lib/spark-common_2.12-3.0.97.jar",
    app_params=["/input/wordcount", "/output/wordcount"],
    job_queue="spark_queue",
    grid_gateway_dependencies=["@Artifact(acceptance-test_2.12,azkabanProd)"],
    target_grid_cluster="holdem",
    polling_interval=10,
)
```

### 2. Execution (Airflow Task Instance)

When the task runs:

1. **Authentication**: Airflow retrieves Grid Gateway connection and constructs auth context (identity + proxy user).
2. **Execution Tag**: Generates a unique execution tag (e.g., `dag_id-task_id-execution_date`).
3. **Hook Initialization**: `GridGatewayHook` establishes gRPC connection to Grid Gateway control plane.
4. **Start Execution**: Hook calls `start_execution()` with serialized parameters, Ivy/multiproduct dependencies, and retry policy.
5. **Grid Gateway Processing**: 
   - Resolves dependencies from Artifactory
   - Validates proxy user permissions
   - Allocates YARN resources from specified queue
   - Generates spark-submit command
6. **Polling**: Airflow polls Grid Gateway every `polling_interval` seconds (minimum 30) for job state updates.
7. **Terminal State**: Job reaches `SUCCEEDED`, `FAILED`, or `STOPPED`.
8. **Log URL**: Grid Gateway returns Spark application log URL; stored in XCom as `spark.log.url` for UI link.

### 3. Output Handling

If `do_xcom_push=True` (default in base operator):
- Grid Gateway execution result dict is serialized (pickle or JSON) and stored in XCom
- Includes log URL, state, error code/message, and any custom output fields

### 4. Failure Handling

Grid Gateway communicates execution state via gRPC:

| State | Airflow Action |
|-------|---|
| `SUCCEEDED` | Task succeeds; XCom logged. |
| `FAILED` | Task fails with `GridGatewayExecutionError` and Grid Gateway error banner. |
| `STOPPED` | Task fails; user or system initiated cancellation. |
| Timeout (polling > max retries) | Task fails with `GridGatewayTimeoutException`. |
| Connection error | Task fails with `GridGatewayConnectionException`. |
| Permission denied | Task fails with `GridGatewayProxyUserPermissionException`. |

---

## Configuration

### Spark Configurations (`spark_confs`)

Custom Spark settings are passed as a dictionary and transformed into `conf.{key}={value}` format:

```python
spark_confs={
    "spark.authenticate": "true",           # Enable Kerberos auth
    "spark.rightsizing.enabled": "true",    # LinkedIn-specific resource optimization
    "spark.yarn.stagingDir": "/tmp/spark",  # Staging directory for job files
    "spark.driver.extraClassPath": "lib/commons-lang3.jar",  # Extra driver classpath
    "spark.executor.extraClassPath": "lib/commons-lang3.jar",  # Extra executor classpath
}
```

**Known LinkedIn-specific configs:**
- `spark.authenticate=true` — Enables Kerberos authentication for secure clusters
- `spark.rightsizing.enabled=true` — Automatic resource right-sizing (may adjust `executor_memory`, `executor_cores` based on workload)
- `spark.yarn.stagingDir` — Scratch directory on HDFS for Spark job files (usually auto-set)
- `spark.driver.extraClassPath`, `spark.executor.extraClassPath` — Additional classpath entries for both driver and executors

**Gotcha: extraClassPath Mutation** [code/lipy-airflow-providers#8c1d7b73]
- When Feature Cloud Push assigns `spark.driver.extraClassPath="guava-25.0-jre.jar"`, it must **append** (not overwrite) any user-supplied classpath.
- Fixed in commit 8c1d7b73: use `+=` to append guava + commons-lang3, not `=` to replace.
- Same applies to `spark.executor.extraClassPath`.

### Grid Gateway Connection (`grid_gateway_conn_id`)

Default: `"grid_gateway_service_default"`

Connection must be configured in Airflow with:
- **Conn Type**: `grid_gateway_service`
- **Host**: Grid Gateway control plane endpoint (e.g., `mufn-control-service:443`)
- **Extras**: `{"cert_path": "/var/cluster/oklahoma/identity.cert", "key_path": "/var/cluster/oklahoma/identity.key"}`

### YARN Queue (`job_queue`)

Maps to `--queue` in spark-submit. Common queues at LinkedIn:
- `spark_queue` — Default queue for on-demand Spark jobs
- `priority.production` — Priority queue for production workloads
- `interactive` — For Jupyter/Spark shell sessions

### Polling Interval

Clamped to minimum 30 seconds by Grid Gateway base operator (BDP-21492):

```python
self._polling_interval = max(polling_interval, 30)
```

Rationale: Prevents Airflow scheduler from hammering Grid Gateway with too-frequent status checks.

---

## Common Failure Modes

### 1. Missing or Invalid Proxy User

**Symptom**: `GridGatewayProxyUserPermissionException`

**Cause**: Proxy user not registered in Grid User Manager or insufficient permissions.

**Fix**: Contact Grid User Manager team; ensure proxy user is authorized for the target Grid cluster.

### 2. Execution Target Not Found

**Symptom**: Grid Gateway returns error about missing JAR/script.

**Cause**: `execution_target` path incorrect or file not in expected location (Artifactory, HDFS, local cluster filesystem).

**Fix**: Verify artifact was built and published; check `target_grid_cluster` parameter matches cluster where artifact is available.

### 3. Job Class Not Found

**Symptom**: ClassNotFoundException at runtime.

**Cause**: `job_class` typo, or JAR missing from classpath.

**Fix**: Double-check class name spelling; ensure `execution_target` JAR includes the class; verify `classpath_jars` dependencies.

### 4. Out of Memory (Driver or Executors)

**Symptom**: `java.lang.OutOfMemoryError` in Spark logs.

**Cause**: Insufficient `driver_memory` or `executor_memory` for workload.

**Fix**: Increase heap sizes (e.g., `driver_memory="4G"`, `executor_memory="8G"`); profile job to estimate memory needs.

### 5. YARN Queue Not Found or No Capacity

**Symptom**: Grid Gateway returns error about queue rejection.

**Cause**: Queue name invalid or queue has no available resources.

**Fix**: Verify queue name with cluster admin; reduce `executor_num` or wait for queue capacity.

### 6. Dependency Resolution Failure

**Symptom**: `@Artifact` resolution fails or Ivy coordinates not found.

**Cause**: Artifact not in Artifactory; typo in multiproduct name or version.

**Fix**: Check artifact exists in Artifactory; use exact multiproduct name (case-sensitive); verify version published.

### 7. Kerberos / Authentication Failure

**Symptom**: `GSSException` or `AuthenticationException` in Grid Gateway logs.

**Cause**: Missing or expired Kerberos ticket; identity certificate not provisioned.

**Fix**: Verify `spark.authenticate=true` is set; check identity certificate at `/var/cluster/oklahoma/identity.cert`; restart if expired.

### 8. Guava ClassConflict / Serialization Mismatch

**Symptom**: Deserialization error involving guava classes or Apache Commons serialization.

**Cause**: Multiple guava JAR versions in classpath; extraClassPath overwritten by framework.

**Fix**: Ensure guava version pinned to 25.0 in Feature Cloud Push; use append pattern for `spark.driver.extraClassPath` and `spark.executor.extraClassPath` (not replace).

### 9. Pod Disruption During Job Execution

**Symptom**: Task marked as FAILED with brief error, but Grid Gateway job continued running.

**Cause**: Airflow pod evicted mid-polling; external job not cleaned up.

**Fix**: Enable `enable_job_checkpoint=True` (default). Restart Airflow pod; task resumes polling from checkpoint instead of re-submitting job.

### 10. Timeout / Polling Exhaustion

**Symptom**: Task exceeds `max_tries` before Grid Gateway job completes.

**Cause**: Job legitimately takes longer than Airflow polling window; increase polling retry budget.

**Fix**: Increase Airflow task `retries` parameter; increase `GRID_GATEWAY_POLLING_RETRIES` constant (default 5); or set longer expected runtime in Grid Gateway job definition.

---

## Spark Versions and Variants

### Supported Versions

LinkedIn uses Spark 3.x:
- `3.1.1` — Common version in lipy-airflow-providers codebase
- `3.0.x` — Earlier version, may be deprecated
- `3.2.x`, `3.3.x` — Newer versions, support depends on Grid Gateway release

Pass `spark_version="3.1.1"` to SparkBatchOperator; Grid Gateway resolves the binary.

### Variants

Spark execution runs on YARN (Hadoop clusters). No direct Kubernetes support in SparkBatchOperator; K8s-native Spark jobs use Grid Gateway's Kubernetes operator (`KubernetesOperator`), not Spark.

**No Li-Spark variant** documented in codebase; LinkedIn uses Apache Spark with custom configs (`spark.authenticate`, `spark.rightsizing.enabled`).

---

## External Job Checkpointing

**Enabled by default** (`enable_job_checkpoint=True`).

When Airflow pod is evicted due to cluster disruption:

1. **Before eviction**: Airflow saves external job ID (Grid Gateway execution URN) to persistent checkpoint store.
2. **Pod restart**: Task instance detects checkpoint; resumes polling for original job instead of re-submitting.
3. **Recovery**: No duplicate jobs; job state reloaded from Grid Gateway; task completes normally.

Supports pod disruption budgets and graceful node drains. See `ExternalJobCheckpointMixin` in base operator.

---

## Disruption Readiness

**Status**: Ramped to SparkBatchOperator in commit 7e24ad26 (feature flag `disruption_ready`).

**Current behavior**: Flag present but **NOT yet functional** for Spark (supported only for hadoopJava, java, javaprocess, command, hadoopShell).

When enabled (future):
- Grid Gateway applies default retry policy: `{"maxRetries": 3, "rules": [{"onErrorCode": "ENVIRONMENT_.*"}]}`
- Spark jobs failing with ENVIRONMENT_* errors (cluster, resource issues) are automatically retried up to 3 times
- User-recoverable errors (e.g., bad input data) are not retried

---

## Feature Cloud Push Integration

Feature Cloud Push (ingestion framework) heavily uses SparkBatchOperator for:

- **Venice Push Preprocessing**: `com.linkedin.featurecloud.ingestion.VenicePushPreparation`
- **Hosted Search Exporter**: `com.linkedin.featurecloud.ingestion.FdsL1HostedSearchWrapper`
- **Hosted Search Result Generator**: Same class, different Spark config
- **Hosted Search Compliance Annotator**: Same class, different Spark config
- **OpenHouse Push**: `com.linkedin.featurecloud.ingestion.OpenHousePush`
- **Feature Group Info Write**: `com.linkedin.featurecloud.ingestion.FeatureGroupInfoWrite`

**Key configs:**

```python
SPARK_VERSION = "3.1.1"
EXECUTION_JARS = "lib/feature-cloud-ingestion-impl-*.jar"
CLASSPATH_JARS = "./lib/*"
DEFAULT_GRID_GATEWAY_PARAMS = {"azkaban.job.enable.ssl": "true", "obtain.hcat.token": "true"}

# Data Quality job (Feature Cloud)
DATA_QUALITY_GRID_PARAMS = {
    "conf.spark.driver.cores": "2",
    "conf.spark.executor.cores": "4",
    "conf.spark.executor.memory": "8G",
}
```

---

## Logging and Debugging

### Log URLs

SparkBatchOperator stores log URL in XCom:

```python
XCOM_SPARK_LOG_URL = "spark.log.url"
```

Airflow UI displays link to Spark application logs via `SparkBatchOperatorLink` (operator_extra_links).

### Grid Gateway Logs

Grid Gateway base operator also provides log link:

```python
XCOM_GRID_GATEWAY_LOG_URL = "mufn.log.url"
```

### Error Formatting

On failure, Grid Gateway hook formats errors with banner:

```
+=======================================================================+
|                   GRID GATEWAY EXECUTION FAILURE                      |
+=======================================================================+

This failure occurred during execution on Grid Gateway infrastructure.
Please reach out to the Grid Gateway team for triaging and create a
Grid Gateway support ticket if you need further assistance.

SUPPORT:
   - Create Ticket: https://engx.corp.linkedin.com/products/100/support
   - Oncall: https://oncall.prod.linkedin.com/team/team/Grid%20Jobs%20Platform
   - Owning Crew: https://engx.corp.linkedin.com/crews/1095

DOCS:
   - Grid Gateway: https://congenial-adventure-r4qn544.pages.github.io/docs/user/onboarding
   - Airflow: https://didactic-umbrella-wlp475l.pages.github.io/docs/users/dag-authoring/write-run-dags

=========================================================================
```

---

## Spark on KJP — Debugging Startup Delays

> LinkedIn's Kubernetes Job Platform (KJP) runs Spark on dedicated K8s clusters (e.g., `prod-ltx1-k8s-19`). This differs from YARN-based Spark. Driver and executor pods run in namespaces like `namespace-for-sgp-crew-XXXX`, scheduled by Volcano gang scheduler.

### Architecture: What Runs Where

| Component | Cluster | Namespace | Pod Name Pattern |
|---|---|---|---|
| Airflow worker | prod-ltx1-k8s-59 (holdem) | `airflow` | `airflow-worker-*` |
| Spark driver | prod-ltx1-k8s-19 (KJP) | `namespace-for-sgp-crew-XXXX` | `<app-id>` (e.g., `spark-2f110320-7f03-...`) |
| Spark executors | prod-ltx1-k8s-19 (KJP) | `namespace-for-sgp-crew-XXXX` | `<app-id>-exec-N` |

The Airflow worker polls GGW -> GGW launches the driver pod -> driver requests executor pods via K8s API -> Volcano schedules as PodGroup.

### KJP Startup Phases (Before Any Work Runs)

The KJP startup sequence has multiple blocking phases that **all count against `execution_timeout` and `dagrun_timeout`**:

1. **GGW queuing** (seconds to minutes): GGW accepts the request and schedules driver pod creation
2. **Driver scheduling** (seconds): Kubernetes schedules the driver pod on a node
3. **DriverResourceLocalizer init container** (minutes): Downloads HDFS tarballs (`hive-libjars-*.tar.gz`, `hadoop-*.tar.gz`, `spark-conf-*.tar.gz`) to `/opt/spark/work-dir/`
4. **spark-submit** (seconds): Driver JVM starts, SparkContext initializes
5. **SparkContext JVM downloads** (minutes — the double-download): SparkContext downloads the **same HDFS tarballs again** to `/tmp/userFiles-*/` for the JVM classpath
6. **Executor pod scheduling** (seconds to minutes): Volcano PodGroup scheduling; may show `FailedScheduling` events
7. **ExecutorResourceLocalizer init container** (minutes): **Each** executor pod downloads the same HDFS tarballs again to its `/opt/spark/work-dir/`
8. **CoarseGrainedExecutorBackend** starts: Executor registers with driver; job can now actually run

### The Double-Download Problem

This is a known architectural behavior in LinkedIn's KJP Spark setup — the same HDFS files are downloaded three separate times:
- **DriverResourceLocalizer** (init container) -> `/opt/spark/work-dir/`
- **SparkContext** (driver JVM, during spark-submit startup) -> `/tmp/userFiles-*/` (for JVM classpath)
- **ExecutorResourceLocalizer** (init container on **every** executor) -> each pod's `/opt/spark/work-dir/`

Under normal conditions each phase takes 30-60s. Under HDFS degradation, a single large tarball (e.g., `hive-libjars-1.1.0.232.tar.gz` at ~1.2 GB) can take 8+ minutes per phase, and this is multiplied across driver + N executors.

**Key bottleneck file**: `hive-libjars-1.1.0.232.tar.gz` — the largest tarball; downloading it is the dominant delay signal.

### "Initial job has not accepted any resources" — What It Actually Means

This log line from the Spark driver does **not** mean executor pods were rejected or failed to schedule. It means:
- Executor pods may be `Running` as K8s pods
- But `CoarseGrainedExecutorBackend` has not yet registered with the driver
- The executors are still in `ExecutorResourceLocalizer` init (blocked on HDFS downloads)

**Distinguish "no pods scheduled" from "pods stuck in init"**: check K8s events first for Volcano errors, then check executor pod logs directly.

### Timeout Budget Math

When investigating a KJP timeout, map the startup phases to actual time:

```
Budget consumed before any work = queueing_delay + driver_init_container + SparkContext_downloads + executor_scheduling + executor_init_containers
Remaining work budget = dagrun_timeout - budget_consumed
```

Example (APA-144XXX, holdem, Apr 20, 2026):
- `dagrun_timeout=55min`, `max_active_runs=1`, schedule=`"8,38 * * * *"`
- Prior run still in flight -> queuing delay: **30min**
- Remaining budget when Attempt 2 started: 25min
- DriverResourceLocalizer + scheduling: **12min** (HDFS degraded)
- SparkContext downloads (double-download): **8min** (same HDFS degradation)
- Executor init containers: **~3min** (HDFS degraded)
- **Remaining budget when executors would be ready: ~2min -> zero tasks ran**

### Diagnosing KJP Startup Failures — Tool Order

Follow this investigation sequence:

**Step 1 — Airflow task logs** -> Get GGW execution URN and driver pod name
- URN format: `urn:li:mu:prod-ltx1:function(spark):execution(<uuid>)`
- Pod name: in task logs as `spark-<uuid>` (no `-exec-N` suffix for the driver)

**Step 2 — Driver pod logs** (Spark KJP Debugging Grafana dashboard or Kusto `kube_grid_logs`)
- Grafana: `https://observe.prod.linkedin.com/g/d/spark-on-kjp-logs/spark-on-kjp-debugging?var-namespace=<ns>&var-pod_name=<pod>`
- Look for: DriverResourceLocalizer timing, `hive-libjars` download duration, SparkContext startup, "no resources" warnings, `SchedulerBackend is ready` timestamp

**Step 3 — K8s events** (`kubeEvents2` Kusto table, or via observe-agent)
- Look for: `FailedScheduling`, `PodGroupNotEnoughResources`, Volcano `PodGroupNotReady`
- These distinguish "pods couldn't be scheduled" from "pods scheduled but stuck in init"

**Step 4 — Executor pod logs** (`kube_grid_logs`, pod names = `<app-id>-exec-N`)
- Look for: `ExecutorResourceLocalizer` download progress, timing from pod start to `CoarseGrainedExecutorBackend` ready
- Last line of container log = SIGPWR (`Power failure`) if killed by driver shutdown

### Kusto Query Patterns

**Driver logs** (KubernetesGridLogs database, `kube_grid_logs` table):
```kql
kube_grid_logs
| where PodName == "spark-<app-id>"
| where ContainerName == "spark-kubernetes-driver"
| order by Timestamp asc
```

**Executor logs** (same table, executor pods):
```kql
kube_grid_logs
| where PodName startswith "spark-<app-id>-exec"
| where ContainerName == "spark-app-container"
| order by Timestamp asc
```

**K8s events for Volcano scheduling** (`kubeEvents2` table):
```kql
kubeEvents2
| where Namespace == "namespace-for-sgp-crew-<crew-id>"
| where Timestamp between (datetime(<job-start>) .. datetime(<job-end>))
| where Message contains "FailedScheduling" or Message contains "PodGroup" or Message contains "Unschedulable"
| order by Timestamp asc
```

**Via observe-agent CLI** (recommended — handles LinkedIn auth automatically):
```bash
observe agent "query kube_grid_logs for executor logs of pod spark-<app-id> between <start> and <end>"
observe agent --session <id> "query kubeEvents2 for Volcano scheduling events in namespace namespace-for-sgp-crew-<id>"
```

### SIGPWR — The Driver-Sent Kill Signal

When the Spark driver's timeout fires or `dagrun_timeout` expires, the driver shuts down and sends **SIGPWR** (signal 30) to all running executor processes. In executor container logs, this appears as the **last line**:

```
/opt/entrypoint.sh: line 148:    27 Power failure    ${JAVA_HOME}/bin/java ... org.apache.spark.executor.ExecutorResourceLocalizer ...
```

Key facts:
- `Power failure` = bash's human-readable name for POSIX signal 30 (SIGPWR)
- It is NOT an actual power failure — it is the Kubernetes kill signal from the driver shutdown hook
- All executors receive SIGPWR simultaneously (within milliseconds of each other)
- If the executor was still in `ExecutorResourceLocalizer` when killed -> it never ran any tasks; `CoarseGrainedExecutorBackend` never launched

**Where to find it**: Kusto `kube_grid_logs`, `spark-app-container`, sort by Timestamp desc -> the last entry before the container exit.

### Note on `spark.kubernetes.executor.apiPollingInterval`

The driver polls the K8s API at this interval (default: 2 minutes) to discover executor pod state. When the remaining timeout budget is only a few minutes, the driver may only have 1-2 poll cycles before shutdown. This means the driver log may show executor pods as "not found" even if they were running — it just never polled during the window they were alive.

### Common KJP Failure Patterns

| Pattern | Key Signature | Root Cause | Fix |
|---|---|---|---|
| All executors killed before any task runs | SIGPWR in every executor's last log line; driver shows "no resources" right before shutdown | Timeout budget exhausted during HDFS downloads | Extend `execution_timeout` + `dagrun_timeout`; investigate HDFS degradation |
| Executors stuck in init, never register | `ExecutorResourceLocalizer` downloading HDFS slowly in exec pod logs; driver shows "waiting for executors" | HDFS degradation; `hive-libjars` tarball bottleneck | Wait for HDFS recovery; retry; monitor HDFS NameNode health |
| Volcano gang scheduling failure | `FailedScheduling: pod group not ready, N Unschedulable` in `kubeEvents2` | Insufficient cluster resources for all executor pods simultaneously | Reduce `executor_num`; retry during off-peak; check KJP cluster resource utilization |
| exec-N pod Running but zero log output | Pod `Running` in K8s; `kube_grid_logs` returns zero rows for `spark-app-container` | Pod scheduled but its init container hasn't finished yet | Check init container logs (`ExecutorResourceLocalizer`); compare pod start time to other executors |
| dagrun_timeout cascade from max_active_runs queuing | Attempt 2 fails shortly after starting despite correct timeouts | Prior run held the slot under `max_active_runs=1`; `dagrun_timeout` clock started 30min before job ran | Increase `dagrun_timeout` to `schedule_interval * 2 + actual_job_duration`; audit `max_active_runs` vs schedule frequency |

---

## See Also

- [lipy-airflow-providers](lipy-airflow-providers.md) — Grid Gateway operators and infrastructure
- [DAG Authoring](../dag-authoring.md) — SparkBatchOperator usage and operator reference
- [Troubleshooting](../troubleshooting.md) — Grid Gateway failure modes and debugging

---

## Spark Performance Guide

> Source: LinkedIn internal Spark Performance Guide (https://musical-spork-l5v46ze.pages.github.io/)
> Maintained by the Spark team. Also available as `spark_domain_knowledge.md` in `linkedin-multiproduct/resource-agent-cli` and `li-productivity-agents`.

---

### Diagnostic Methodology

Apply this systematic approach when analyzing a slow Spark job:

1. **Identify slowest stages** by `executorRunTime` — highest value = bottleneck stage
2. **Check shuffle volumes** — `shuffleReadBytes`/`shuffleWriteBytes` > 1 GB per stage warrants investigation
3. **Check join strategies** — look for `SortMergeJoin` where one side is small enough for `BroadcastHashJoin`
4. **Check data skew** — if `max task time >> median task time` within a stage, data is skewed on the join/group key
5. **Check spill** — any non-zero `memoryBytesSpilled` or `diskBytesSpilled` = tasks ran out of execution memory -> massive slowdown from disk I/O
6. **Review Spark config** vs workload — compare `spark.sql.shuffle.partitions`, executor memory/cores against actual data volumes
7. **Check API usage** — typed Dataset lambdas (`.map()`, `.filter()`, `.flatMap()`, `groupByKey`) block Catalyst optimizations

---

### Dataset API vs DataFrame API

Typed `Dataset[T]` lambdas are **opaque to the Catalyst optimizer**. This disables:

- **Predicate pushdown** — `ds.filter(_.age > 30)` will NOT push down to Parquet/ORC
- **Column pruning** — `ds.map(_.name)` forces full-row deserialization of ALL columns
- **Auto-broadcast join** — typed ops lose size statistics; Spark cannot auto-broadcast
- **Whole-stage codegen** — lambdas break the codegen pipeline
- Each typed op also incurs per-row encoder **serde overhead** (decode Tungsten binary -> JVM object -> re-encode)

**Fix order of priority:**

1. Convert simple typed ops to column expressions:
   ```scala
   ds.filter(_.age > 30)      // BAD — no pushdown
   ds.filter($"age" > 30)     // GOOD
   ds.map(_.name)             // BAD — no pruning
   ds.select($"name").as[String]  // GOOD
   ```
2. Add explicit broadcast hints (auto-broadcast won't fire through typed API):
   ```scala
   // LinkedIn spark-lms typed joins:
   import com.linkedin.spark.lms.broadcast.BroadcastHint
   large.leftOuterJoinWith(small).on(_.key, _.key, BroadcastHint.BroadcastRight)
   // Or use DataFrame hint:
   large.join(small.hint("broadcast"), Seq("key"))
   ```
3. Move filters before typed operations (minimize data before serde)
4. Select columns before typed operations (enable pruning)
5. Keep complex business logic in typed lambdas but keep them as late in the pipeline as possible

---

### Key Execution Plan Operators

| Operator | Meaning | What to look for |
|---|---|---|
| `Exchange` | Shuffle (network I/O) | Check partitioning type: `hashpartitioning`, `rangepartitioning`, `SinglePartition` |
| `SortMergeJoin` | Shuffle + sort on both sides | Most common optimization target — can one side be broadcast? |
| `BroadcastHashJoin` | Broadcast join (no shuffle) | Verify broadcast side is actually small |
| `ShuffledHashJoin` | Hash join with shuffle, no sort | Faster than SMJ when build side fits in memory; AQE can convert SMJ -> SHJ |
| `BroadcastExchange` | Broadcasts table to all executors | Too large = OOM risk |
| `FileScan parquet/orc` | File scan | Check `PushedFilters`, `PartitionFilters`, `ReadSchema` — missing = full scan |
| `SortAggregate` | Sort-based aggregation | Falls back from hash agg when data doesn't fit in memory — indicates memory pressure |
| `DeserializeToObject` / `SerializeFromObject` | Typed Dataset encode/decode | Multiple adjacent pairs = chained typed ops with repeated serde overhead |
| `WholeStageCodegen` | Fused operators for CPU efficiency | Good — breaks in codegen chain = slower operators |

---

### Key Stage Metrics

| Field | Concerning threshold |
|---|---|
| `executorRunTime` | Highest value = bottleneck stage |
| `shuffleReadBytes` / `shuffleWriteBytes` | > 1 GB per stage = investigate |
| `memoryBytesSpilled` / `diskBytesSpilled` | **Any non-zero value** = major slowdown |
| `jvmGcTime` | > 10% of `executorRunTime` = GC pressure |
| `peakExecutionMemory` | If close to executor memory allocation = spill risk |

**Skew detection (task_summaries):** `max / median > 5x` for `executorRunTime` = skew signal; `> 10x` = extreme skew.

---

### Top Optimizations

#### Broadcast Joins (highest impact)

Tables up to ~200 MB can safely be broadcast (default threshold is only 10 MB — very conservative).

**Critical**: typed Dataset API blocks auto-broadcast — must add explicit `BroadcastHint`.

```sql
SELECT /*+ BROADCAST(small_table) */ * FROM large JOIN small_table ON ...
```
```scala
large.join(broadcast(small), Seq("key"))
large.join(small.hint("broadcast"), Seq("key"))
```

#### Adaptive Query Execution (AQE)

`spark.sql.adaptive.enabled=true` (default on in Spark 3.2+):
- Auto-coalesces small post-shuffle partitions
- Converts `SortMergeJoin` -> `BroadcastHashJoin` at runtime when actual data is small
- Handles skew join automatically (splits oversized partitions when > 5x median AND > 256 MB)

#### Partition Tuning

- Target ~128 MB per partition
- Default `spark.sql.shuffle.partitions=200` is often wrong
- **Best practice with AQE**: set high (1000-2000), let AQE coalesce small partitions at runtime via `spark.sql.adaptive.advisoryPartitionSizeInBytes` (default 64 MB)

#### Skew Handling (without AQE)

- Manual salting of the skewed key
- Isolate skewed keys (e.g., nulls) into a separate broadcast join, then `union`
- Pre-filter to remove unnecessary skewed values

#### Coalesce vs Repartition

- `coalesce(n)` — reduces partitions **without shuffle** — only use when reducing partition count AND current partitions are small
- `repartition(n)` — full shuffle to exactly `n` partitions — use when you need even distribution
- `REBALANCE` hint (Spark 3.2+) — AQE-aware rebalancing for output files

#### Other

- **Kryo serializer**: `spark.serializer=org.apache.spark.serializer.KryoSerializer` (~10x faster, more compact)
- **GC pressure**: use `MEMORY_AND_DISK_SER` for cached data; `jvmGcTime > 10%` of executorRunTime = problem
- **Dynamic Partition Pruning (DPP)**: enabled by default in Spark 3.0+; very effective for star-schema joins when fact table is partitioned and dimension table is small
- **Predicate pushdown failures**: caused by UDFs in filter conditions, typed Dataset lambdas, filters on computed columns, non-deterministic expressions

---

### Common Anti-Patterns

| Anti-pattern | Problem | Fix |
|---|---|---|
| `collect()` on large datasets | OOM on driver | Write to storage; use `take(n)` |
| `.count() > 0` to check emptiness | Scans all data | Use `.head(1)` or `.isEmpty` (Spark 3.3+) |
| `groupByKey` on RDD or Dataset | Materializes all values per key in memory | Use `reduceByKey` / `aggregateByKey` (RDD) or `df.groupBy.agg` |
| Excessive caching | Competes with execution memory | Only cache DataFrames reused multiple times; always `unpersist()` when done |
| Chained typed ops `.filter().map().filter().map()` | Repeated serde per op | Batch into single `.map()`; convert to column expressions |
| `repartition()` when `coalesce()` suffices | Unnecessary full shuffle | Use `coalesce` when only reducing count and partitions are small |
| Not broadcasting small lookup tables | Unnecessary SortMergeJoin shuffle | Broadcast up to ~200 MB tables |
| UDFs in filter conditions | Blocks predicate pushdown and codegen | Replace with native Spark SQL functions |
| `rdd.groupByKey()` | All values materialized before processing | Use `reduceByKey(_ + _)` for map-side partial aggregation |

---

### LinkedIn-Specific Rules

These rules prevent common incorrect recommendations in the LinkedIn Spark environment:

1. **Shuffle compression is normal** — LZ4/Zstd compression is default. Serialized shuffle size < deserialized size is expected behavior, NOT a problem. Do not flag this.

2. **Celeborn context matters**:
   - App NOT on Celeborn + FetchFailedException / shuffle fetch failures -> recommend Celeborn onboarding (not app-level code changes)
   - App already on Celeborn + shuffle errors -> tune Celeborn config (worker exclusion, slot exhaustion, push data failures); do NOT recommend onboarding again

3. **Repartition before OpenHouse writes has a purpose** — OH/Iceberg does not honor Dali's `SPLIT_SIZE`. Do not blindly recommend removing `repartition` before OH writes — it's intentional for output file count control.

4. **Coalesce != always better than repartition** — only recommend coalesce when partition sizes are small. If reducer partitions are already large, coalesce won't help.

5. **Classify fix location** for every recommendation:
   - `app_code` — anti-pattern the user can fix in their code
   - `infra_onboarding` — e.g., Celeborn adoption, config tuning
   - `platform_limitation` — e.g., OH not honoring SPLIT_SIZE (the fix is upstream, not app-level)

6. **Do not fabricate metrics** — only report metrics directly present in SHS data. Do not compute derived ratios unless they are well-defined.

---

### Spark Queue Resource Lookup

To check available resources for a YARN queue:

| Method | How |
|---|---|
| Self-serve portal | `go/sparkselfserve` — live queue view, escalation |
| Spark History Server (Holdem) | `go/holdemshs <app-id>` or `https://shs-ltx1-holdem.grid.linkedin.com` — Environment tab shows queue + config for any past job |
| Spark History Server (War) | `https://shs-lva1-war.grid.linkedin.com` |
| Job resource usage | `gridbench resource -a <application_id>` |
| Raw YARN (grid node access) | `yarn queue -status <queue-name>` |

Known YARN queues: `spark_queue` (default), `priority.production`, `interactive`, `dm_dataquality`, `default`

Queue capacity error: `YARN queue 'spark_queue' has insufficient capacity` -> reduce `executor_num` or escalate via `go/sparkselfserve`

---

### OpenHouse + Kafka Pipeline Patterns

#### HadoopShellOperator cannot read OpenHouse

`HadoopShellOperator` has no Spark session and cannot acquire the OpenHouse REST catalog token (`LiOpenHouseSparkCatalog`). This is **not a supported pattern**.

#### Option A — KafkaPushOperator reading OpenHouse directly (simplest)

```python
from airflow.providers.lnkd.gridgateway.operators.kafka_push import KafkaPushOperator

KafkaPushOperator(
    task_id="oh_push_to_kafka",
    topic="MyOutputTopic",
    proxy_user="myheadless",
    target_grid_cluster="holdem",
    enable_new_prop_values=True,
    polling_interval=10,
    grid_gateway_params={
        "launch.spark.kafka.push.job.using.grid.gateway": "true",
        "input.dali.resource": "dalids:///openhouse.mydb.my_oh_table",
        "input.dali.filter": "datepartition = '{{ ds }}'",
        "obtain.openhouse.token": "true",
        "conf.spark.sql.viewshift.enabled": "true",
    }
)
```

#### Option B — SparkBatchOperator -> KafkaPushOperator (with processing)

```python
process = SparkBatchOperator(
    task_id="read_openhouse_and_process",
    proxy_user="myheadless",
    target_grid_cluster="holdem",
    # ... job config ...
    app_params=["--input_table", "openhouse.mydb.my_oh_table", "--output_path", "/jobs/myteam/output/{{ ds }}/"],
)

push = KafkaPushOperator(
    task_id="push_to_kafka",
    topic="MyOutputTopic",
    proxy_user="myheadless",
    target_grid_cluster="holdem",
    enable_new_prop_values=True,
    polling_interval=10,
    grid_gateway_params={
        "launch.spark.kafka.push.job.using.grid.gateway": "true",
        "input.dali.resource": "dalids:///mydb.processed_output",
        "input.dali.filter": "datepartition = '{{ ds }}'",
    }
)

process >> push
```

**Key KafkaPushOperator params for OpenHouse:**
- `"obtain.openhouse.token": "true"` — required for OpenHouse access
- `"conf.spark.sql.viewshift.enabled": "true"` — required for OpenHouse/Groot views
- `"input.dali.resource": "dalids:///openhouse.<db>.<table>"` — OpenHouse table via Dali
- `"input.dali.partition.latest": "true"` — auto-pick latest partition (mutually exclusive with `input.dali.filter`)

Resources: `go/kafkapushoperator` - `go/kpjsettings` - Slack: `#ask_kafka_push_job`
