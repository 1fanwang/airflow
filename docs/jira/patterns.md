> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# Jira — Patterns

> Recurring issue types seen in Airflow at LinkedIn: how to recognize them, what triggers them, and how often they appear

## Pattern Format

Each pattern below uses this structure:

- **Name**: short identifier used in playbooks and Jira titles
- **Trigger**: what causes the issue to surface
- **Signature**: what the error or symptom looks like in logs/UI
- **Frequency**: how commonly this class of ticket appears
- **Related systems**: which components are involved
- **See**: link to resolution playbook

---

## P1 — GGW ENVIRONMENT_* Errors (Pod Disruption)

**Trigger**: Kubernetes pod running Airflow worker or scheduler is evicted mid-task while a Grid Gateway job is running. The pod dies, Airflow marks the task failed, but the GGW job continued running independently.

**Signature**:
```
GridGatewayExecutionError: ENVIRONMENT_CLUSTER_UNAVAILABLE
GridGatewayExecutionError: ENVIRONMENT_PREEMPTION
GridGatewayExecutionError: ENVIRONMENT_NODE_FAILURE
```
Or: task shows FAILED in Airflow UI but Grid Gateway logs show the job actually succeeded or is still running.

**Root cause**: Airflow's worker pod lost connectivity to Grid Gateway mid-poll. With `enable_job_checkpoint=True` (default), the external job checkpoint persists the GGW execution URN; on restart the task resumes polling instead of re-submitting. Without it, the job is abandoned and may run twice if retried.

**Frequency**: Medium — occurs regularly during cluster maintenance windows, node drains, or resource pressure spikes. Higher frequency on holdem (larger cluster, more disruptions).

**Related systems**: GGW (Grid Gateway), Kubernetes, YARN, Airflow scheduler

**Disruption-ready job types** (auto-retry on `ENVIRONMENT_.*`): `hadoopJava`, `java`, `javaprocess`, `command`, `hadoopShell`, **`darwin`** (added Apr 2026, lipy-airflow-providers PR #1193). Spark is NOT yet disruption-ready despite the flag existing.

**Spark-specific variant — Shuffle Recompute Loop** (APA-144875, Resolved Apr 2026): When Spark executors are preempted mid-shuffle, shuffle data is lost. The stage reruns to regenerate shuffle output, but executors may be preempted again, creating a cycle where the DAG appears stuck with runs repeatedly failing and retrying. Distinguished from a one-off ENVIRONMENT_* failure by the *sustained loop* pattern (days, not one attempt). `disruption_ready=True` does NOT help because it is not yet supported for Spark. Mitigation: increase executor resources to reduce preemption likelihood, schedule during off-peak windows, or request priority queue placement from Grid team.

**See**: [Playbooks - GGW Environment Error](playbooks.md#ggw-environment-error)

---

## P2 — ProxyUser ACL Validation Failures

**Trigger**: A DAG task using `SparkBatchOperator` (or any GGW operator) specifies a `proxy_user` that is not authorized for the calling Oklahoma identity in Grid User Manager. Also surfaces during DAG upload validation if the Upload Plugin checks ACLs at upload time.

**Signature**:
```
GridGatewayProxyUserPermissionException: Grid Gateway start execution permission denied.
Details: urn:li:servicePrincipal:(identity,...) is not allowed to impersonate as proxy_user
```
Or during upload: `DAG validation failed: proxy user 'myuser' not authorized for identity 'oklahoma-mp-mymp'`

**Root cause**: The Oklahoma DAG/MP identity principal (e.g., `oklahoma-mp-mymp`) has not been granted impersonation rights for the `proxy_user` in the Grid User Manager ACL system. Common after:
- New DAG being created without setting up ACLs
- MP rename without updating ACL rules
- Proxy user account being rotated or renamed
- Running in EI environment where ACLs are not propagated

**Frequency**: High — one of the most common new-DAG onboarding failures. Every new DAG that uses Grid must set up proxy user ACLs.

**Related systems**: GGW (Grid Gateway), Grid User Manager, DataVault, picli, Oklahoma identity (SPIFFE certs)

**See**: [Playbooks - Proxy User ACL](playbooks.md#proxy-user-acl)

---

## P3 — Spark OOM / YARN Queue Capacity

**Trigger**: Spark job exceeds allocated executor or driver memory, or the YARN queue has no available resources for the job.

**Signature**:
```
java.lang.OutOfMemoryError: Java heap space
java.lang.OutOfMemoryError: GC overhead limit exceeded
```
Or Grid Gateway error: `YARN queue 'spark_queue' has insufficient capacity` / `Application rejected by ResourceManager`

**Root cause (OOM)**: `executor_memory` or `driver_memory` too low for workload. Common for jobs with large shuffles, wide joins, or growing input data volumes.

**Root cause (queue)**: `job_queue` is full — too many concurrent jobs, resource contention during peak hours. Also: `executor_num` too high for available queue capacity.

**Frequency**: Medium-high — Spark OOM is a steady stream of tickets especially as data volumes grow. Queue capacity issues spike during peak batch windows (morning/evening).

**Related systems**: Spark, GGW, YARN

**Note on rightsizing**: `spark_confs={"spark.rightsizing.enabled": "true"}` enables LinkedIn's automatic resource adjustment. This may alter `executor_memory` / `executor_cores` at runtime.

**See**: [Playbooks - Spark OOM](playbooks.md#spark-oom)

---

## P4 — DAG Import Errors (Circular Imports, Missing Deps)

**Trigger**: Airflow scheduler cannot parse a DAG file. Causes: circular import in `airflow_local_settings` / policy framework, missing Python dependency in the DAG application venv, or a cross-MP import where the library MP hasn't been deployed yet.

**Signature**: DagBag errors in scheduler logs:
```
broken_dags:
  /dags/mymp/my_dag.py: ImportError: cannot import name 'X' from 'airflow.providers...'
  /dags/mymp/my_dag.py: ModuleNotFoundError: No module named 'metis_ingestion_offline_airflow'
  /dags/mymp/my_dag.py: ImportError: circular import
```
In Airflow UI: DAG shows as "Import Error" with red indicator.

**Root cause (circular import)**: Top-level `from airflow.*` imports in `airflow_local_settings` or policy framework modules cause Airflow to re-initialize during import, creating a cycle. Fix: lazy imports inside functions.

**Root cause (missing dep)**: Library MP not deployed before consumer DAG MP. Or dependency not declared in `product-spec.json` + `.okl_setup.json`.

**Root cause (missing module)**: Package available locally but not installed in cluster venv. Or version mismatch between local and cluster.

**Root cause (intermittent on programmatic trigger)**: DAG shows no import errors in Airflow UI, but programmatic triggers via REST API intermittently return `HTTP 400: DAG has import errors`. The dag-processor may briefly mark the DAG as broken during a parse cycle (e.g., transient file lock, NFS lag), and a trigger request arriving during that window fails. Frequency increased when dag-processor concurrency or parse interval changed. See APA-144169 (Closed — Fixed, Apr 2026).

**Frequency**: Medium — common during new DAG onboarding, after dependency upgrades, or when library MPs are deployed out of order. Intermittent variant (programmatic trigger) is lower frequency but harder to diagnose.

**Related systems**: Airflow scheduler, DagBag, oklahoma-helpers, CRT deployment, dag-processor

**See**: [Playbooks - DAG Import Error](playbooks.md#dag-import-error)

---

## P5 — Sensor Timeout (Poke Mode vs Reschedule Mode)

**Trigger**: A sensor (DatasetSensorArray, PythonSensor, AzkabanSensor) exhausts its `timeout` before the condition is satisfied, or holds a worker slot indefinitely in `poke` mode causing slot starvation.

**Signature**:
```
airflow.exceptions.AirflowSensorTimeout: Sensor has timed out; run duration of X > timeout of Y
```
Or: tasks piling up in `queued` state because all worker slots are consumed by long-running sensors in `poke` mode.

**Root cause (timeout)**: Data not available within expected window. Upstream pipeline delayed, partition not written, snapshot watermark not updated.

**Root cause (slot starvation)**: `mode='poke'` (default) holds a worker slot for the entire wait duration. With many concurrent sensors, slots fill up. Fix: use `mode='reschedule'` — sensor releases slot between checks.

**Frequency**: Medium — sensor timeouts are common during upstream pipeline delays. Slot starvation is less frequent but high-impact.

**Related systems**: ARMS (Artifact Metadata Service), Dali, Airflow executor, upstream pipelines

**Key parameters**:
- `poke_interval`: seconds between checks (default 30s for DatasetSensorArray)
- `timeout`: max seconds to wait before raising `AirflowSensorTimeout`
- `mode`: `'poke'` (holds slot) vs `'reschedule'` (releases slot between checks)

**See**: [Playbooks - Sensor Timeout](playbooks.md#sensor-timeout)

---

## P6 — Fernet Key Length Failures (RDev Specifically)

**Trigger**: `AIRFLOW__CORE__FERNET_KEY` set to a value that is not exactly 32 bytes when base64-decoded. Occurs primarily in RDev environment setup.

**Signature**: No immediate error at startup. Fails later with cryptic decryption errors:
```
cryptography.fernet.InvalidToken
ValueError: Fernet key must be 32 url-safe base64-encoded bytes
```
Or: DAG params, task state, or connections cannot be decrypted; scheduler behaves erratically.

**Root cause**: Fernet encryption requires exactly 32-byte keys. Short keys like `RDevFernetKey` appear to "work" until an actual decryption is attempted. Discovered in commit `a889dd1` (`setup_oklahoma_rdev_env.sh`).

**Frequency**: Low-medium — mainly affects new RDev environment setups and automated test environments. Rare in production.

**Related systems**: RDev, Fernet/cryptography library, Airflow scheduler

**See**: [Playbooks - Fernet Key](playbooks.md#fernet-key)

---

## P7 — Config Merge Not Applying User Overrides (RDev)

**Trigger**: User sets nested config overrides in RDev with `enable_merge=True`, but top-level keys overwrite entire nested dicts instead of merging.

**Signature**: Silently loses config values. No error thrown; user config appears to apply but nested keys under the top-level key are gone. Only detectable by inspecting runtime config.

**Root cause**: `_update_dicts()` in `oklahoma-helpers/config.py` was called without `is_recursive=self.enable_merge` on the user override path. Top-level dict keys replaced nested dicts entirely. Fixed in commit `5577898d`.

**Frequency**: Low — affects only RDev users customizing nested config. May be fixed in newer versions.

**Related systems**: RDev, oklahoma-helpers, `linkedin.config.base`

**See**: [Playbooks - Config Merge](playbooks.md#config-merge)

---

## P8 — LDAP Case Sensitivity

**Trigger**: User LDAP username is mixed-case (e.g., `JohnDoe`) but Airflow stores or looks it up in a different case, causing authentication failures or duplicate user records.

**Signature**:
```
User 'johndoe' not found
PermissionError: User does not have access
```
Or: user can log in but cannot see their DAGs; `access_control` lookup fails because group membership is case-sensitive.

**Root cause**: LDAP usernames are case-sensitive on the server but could be extracted in mixed case by the security manager. Normalized to uppercase in commit `b777e3ec` (`security/manager.py`).

**Frequency**: Low — mainly affects users with non-lowercase LDAP usernames, or old user records created before normalization was in place.

**Related systems**: LDAP, Airflow security manager, `access_control` DAG param

**See**: [Playbooks - LDAP Case](playbooks.md#ldap-case)

---

## P9 — SSL Verification Disabled Warnings

**Trigger**: HTTPS requests to Grid User Manager (GUM) or other internal services made with `verify=False`. Code continues to work but emits `InsecureRequestWarning` and is a security risk.

**Signature**:
```
InsecureRequestWarning: Unverified HTTPS request is being made to host '...'. Adding certificate verification is strongly advised.
```

**Root cause**: `dag_validations.py` uses `verify=False` for SSL. The constant `TRUSTSTORE_FILEPATH` exists but is not used. Marked as TODO.

**Frequency**: Low severity, but appears in every DAG upload validation that hits GUM. Not actionable for end users — requires platform fix.

**Related systems**: DAG validation, Grid User Manager, truststore

**See**: [Playbooks - SSL Verify](playbooks.md#ssl-verify)

---

## P10 — Scheduler Critical Section Contention (High DAG Count)

**Trigger**: Airflow scheduler becomes slow or unresponsive when the DAG count on a cluster is very high (thousands of DAGs). Critical section duration metric spikes.

**Signature**:
```
critical_section_duration: 45.2s (threshold: 10s)
```
In Airflow UI: tasks not being scheduled despite being ready; large backlog of `scheduled` tasks not transitioning to `queued`. Scheduler logs show long lock acquisition times.

**Root cause**: Airflow scheduler uses a database lock (critical section) to serialize DAG scheduling decisions. With many DAGs, lock hold time increases. Also affected by `parallelism`, `max_active_tasks_per_dag`, and `ti_per_loop` settings. Load testing at LinkedIn shows contention starts becoming visible at 10,000+ DAGs; serious above 20,000.

**Frequency**: Low but high-impact — affects shared clusters (holdem) with many teams' DAGs. Spikes during mass DAG deployments or DagBag parsing delays.

**Related systems**: Airflow scheduler, MySQL (DAG database), load testing infrastructure

**Key metrics**: `critical_section_duration`, `schedule_delay`, `loop_duration`, `task_count`

**See**: [Playbooks - Critical Section](playbooks.md#critical-section)

---

## P11 — DAG Not Showing in Tradewind UI

**Trigger**: DAG is deployed to an Airflow cluster but does not appear in the Tradewind federated UI, or appears on the wrong cluster's view.

**Signature**: DAG visible directly on cluster URL (e.g., `holdem.oklahoma-airflow.grid.linkedin.com`) but absent from Tradewind UI at `tradewind.corp.linkedin.com`. Or DAG shows under wrong logical cluster.

**Root cause**: Tradewind's router database has not received the DAG registration event, or shard placement is incorrect. Tradewind routes by `DAG_ID -> cluster -> shard` stored in its MySQL registry. Missing entry = DAG invisible. Possible causes: CRT deployment didn't trigger registration, DAG naming convention mismatch (`<DAG_NAME>__<MP_NAME>` required), or Tradewind worker backlog.

**Frequency**: Low-medium — appears after new DAG deployments, cluster migrations, or when Tradewind worker falls behind.

**Related systems**: Tradewind, CRT deployment, oklahoma-airflow DAG syncer

**See**: [Playbooks - Tradewind Missing](playbooks.md#tradewind-missing)

---

## P12 — NFS Mount Hangs on New NKS Nodes (fsGroup Mismatch)

**Trigger**: New NKS node joins the cluster. Airflow pods scheduled to it hang during NFS mount because the pod's `securityContext.fsGroup` doesn't match the NFS export's expected GID.

**Signature**: Pod stuck in `ContainerCreating` or processes in `D` (uninterruptible sleep) state. `kubectl describe pod` shows NFS mount timeout. Scheduler/webserver/worker become unresponsive on affected node.

**Root cause**: `fsGroup` in the Helm values was incorrect for the NFS partition. On new nodes (no cached mounts), the kernel attempts to set group ownership on the NFS mount — if the GID doesn't match, the mount hangs.

**Frequency**: Low — only triggers when pods land on new/drained nodes. Not visible on existing nodes with cached mounts.

**Related systems**: Kubernetes (NKS), NFS (sdnas-csi-driver), Helm chart

**Fix**: Deployment PRs #1035/#1036 (2026-04-10) corrected `fsGroup` for scheduler, webserver, and worker pods.

---

## P13 — RDev Setup and Cert Issues

**Trigger**: `picli test login` times out, `Cannot fetch user cert`, or Airflow UI shows "Invalid login" in the rdev environment.

**Signature**:
```
picli test login: timed out waiting for response
Cannot fetch user cert: ...
airflow.exceptions.AirflowConfigException: Invalid login
```
Or: DAGs not visible in rdev UI despite upload succeeding.

**Root cause**:
- `Cannot fetch user cert` / login timeout: ssh-ca-cli process stale. Fix from your local mac (not inside the rdev): `pkill -f ssh-ca-cli && ssh-ca-cli refresh`.
- rdev image too old: rdevs created before March 4, 2026 must be deleted and recreated — base image changed for GGW connectivity and libstdc++ TLS fix (APA-140407, APA-140417).
- Image pinned to old version: `devcontainer.json` pins `airflow-main-airflow-rdev:0.0.xyz` — change to `airflow-main-airflow-rdev:stable`.
- Airflow UI "Invalid login": admin user not created inside rdev. Run: `airflow users create --role Admin --username <ldap> --firstname X --lastname Y --email z@linkedin.com --password <ldap>` inside the rdev, then `~/.okl_rdev/restart_airflow.sh`.
- `grid_gateway_service_default` connection missing: run `~/.okl_rdev/grid_setup.sh -g holdem` inside rdev.

**Frequency**: **Very High** — the single largest category in #ask_airflow (~40% of questions). Affects everyone setting up or upgrading rdev.

**Related systems**: RDev, picli, ssh-ca-cli, Grid Gateway, oklahoma devcontainer

**See**: [Playbooks - RDev Cert](playbooks.md#rdev-cert)

---

## P14 — RDev DAGs Not Loading

**Trigger**: rdev starts successfully but no DAGs appear in the UI; Airflow UI shows 0 DAGs or `ImportError` for all DAGs.

**Signature**:
```
Broken DAG: [/opt/airflow/dags/...]: No module named '...'
```
Or: empty DAG list, no symlinks under `/opt/airflow/dags/`.

**Root cause**:
- `rdev-init.sh` was not run after rdev creation, or was run without the `-r` (reset) flag when changing source directory.
- The `/opt/airflow/dags/<mp_name>` symlink was not created.
- Common after `rdev recreate` or `rdev rebuild`.

**Frequency**: High — follows every rdev creation or major rebuild.

**Related systems**: RDev, okl-rdev-init.sh, DAG symlinks

**Fix**:
```bash
okl-rdev-init.sh -d <path_to_your_dag_source_dir> -r
# -r flag resets and re-creates symlinks
```
If still failing: open a support ticket with MP name and branch — the Oklahoma team can diagnose DAG loading failures remotely.

**See**: [Playbooks - RDev DAGs](playbooks.md#rdev-dags)

---

## P15 — Ambry / DataVault Certificate Failures (AmbCacheError)

**Trigger**: Tasks fail with certificate-related errors mentioning Ambry, DataVault, or k8s-lare during normal production runs.

**Signature**:
```
AmbCacheError: Failed to get certificates for Ambry
BinaryCacheError: ...
```
Or: GGW DataVaultTokenException with Ambry-specific context.

**Root cause**: DataVault certificates are populated by the `k8s-lare` Kubernetes operator. When a deployment regression occurs, `k8s-lare` may fail to inject updated certs into pods. The issue is often cluster-specific (e.g., holdem affected, war not). Rollback of the offending deployment restores cert availability.

**Frequency**: Low — appears during deployment incidents. Not triggered by DAG code changes.

**Related systems**: DataVault, k8s-lare, Ambry, GGW

**Debug**:
1. Check if the error is holdem-specific or also on war.
2. Check recent deployments to the affected cluster around the failure time.
3. If holdem-only: rollback the latest oklahoma-airflow deployment.
4. Escalate to Oklahoma oncall via go/ask-airflow with cluster name + timestamp.

**See**: [Playbooks](playbooks.md) (no dedicated playbook — escalate to oncall)

---

## P16 — OSOS / Non-Spark Jobs Stuck in Queue

**Trigger**: OSOS (or other non-Spark GGW) jobs remain in `QUEUED` state indefinitely; GGW shows the execution URN but the job never transitions to RUNNING.

**Signature**:
Task stays `running` in Airflow. Grid Gateway shows execution in QUEUED state with no error. Job does not start after several minutes.

**Root cause**: Resource saturation on the target grid cluster, or OSOS cluster maintenance. Not an Airflow bug — the job is accepted by GGW but not yet dispatched.

**Frequency**: Low-medium — tracked as APA-143794.

**Related systems**: GGW, OSOS, grid cluster resource management

**Fix**:
1. Check grid cluster capacity via Grid Gateway UI.
2. If cluster is saturated: wait, or reschedule the DAG run to off-peak.
3. If stuck > 1 hour with no progress: escalate to Grid Gateway oncall with the execution URN.

---

## P17 — Tracking Data Only Available on Holdem (Not Faro)

**Trigger**: User onboards a DAG on Faro that requires tracking event tables (e.g., `tracking.SalesChooserPageViewEvent`) and expects them to be available on the Faro (staging/EI) cluster.

**Signature**: Sensor timeout or query returns no data on Faro. DAG works on Holdem but fails on Faro. User files a ticket requesting tracking table onboarding to Faro.

**Root cause**: The LinkedIn tracking team **does not support tracking tables on Faro**. All tracking data is produced to prod Kafka topics and dumped into the **Holdem cluster only**. This is a platform limitation, not a configuration issue.

**Frequency**: Low-medium — appears when teams attempt to test tracking-dependent DAGs on Faro before promoting to Holdem. Two duplicate tickets filed in April 2026 (APA-144366, APA-144405).

**Related systems**: Tracking infra, Kafka, Faro cluster, Holdem cluster

**Fix**: Test tracking-dependent DAGs directly on Holdem (or use rdev with `target_grid_cluster=holdem`). There is no workaround to get tracking data on Faro.

**Source**: APA-144366, APA-144405 (both Closed — Won't Do)

---

## P18 — Hive View Schema Mismatch with Espresso (Recurring)

**Trigger**: Airflow DAG queries a Hive view over an Espresso table, and the Hive view definition is out of sync with the underlying Espresso table schema (e.g., union type wrapping with `tag`/`field0` fields vs direct access).

**Signature**:
```
AnalysisException: cannot resolve 'col_name' given input columns: [tag, field0, ...]
```
Or: query returns nulls / wrong data because field access paths changed after a schema migration.

**Root cause**: Espresso tables use Avro union types. Hive views are generated from Espresso schemas, but schema migrations (e.g., Kyoto migration) change the field access paths. The Hive view definition must be regenerated after each schema change. This has been recurring for **4+ months** as of April 2026 — the schema "flip-flops" between representations.

**Frequency**: Low but recurring — APA-144168 notes this pattern has been going on for 4+ months with repeated flip-flopping. Holdem and War may be affected at different times depending on migration progress.

**Related systems**: Hive, Espresso, Kyoto migration, Trino

**Fix**: Run query via Trino (`SELECT * FROM ...`) which handles schema evolution more gracefully. For Hive, regenerate the view definition after Kyoto migration completes on the target cluster. Monitor for recurrence.

**Source**: APA-144168 (Closed — Fixed, but recurring)

---

## P19 — Trino Iceberg StackOverflowError on Complex Predicate Pushdown

**Trigger**: A Trino query in an Airflow DAG references a large Iceberg table (many partition files/splits) via a CTE without a partition filter, and the main WHERE clause has deeply nested boolean conditions. Trino's `IcebergSplitSource.partitionMatchesConstraint` recurses too deeply evaluating the pushed-down predicates.

**Signature**:
```
java.lang.StackOverflowError
  at io.trino.plugin.iceberg.IcebergSplitSource.partitionMatchesConstraint(...)
```
Occurs during Iceberg partition pruning, not during query execution. The query plan looks valid but fails during split generation.

**Root cause**: Trino's Iceberg connector pushes WHERE clause predicates down as partition constraints. When the predicate tree is deeply nested (multiple `AND`/`OR` with `BETWEEN`, `IN`, boolean columns) and the target Iceberg table has many splits, the recursive evaluation of `partitionMatchesConstraint` overflows the JVM stack.

**Frequency**: Low — but HIGH risk for flows migrating from `fact_talent_report` to `tracking.pageViewEvent` (or any migration to large Iceberg tables with complex join predicates).

**Fix**:
1. Move filter conditions (e.g., `is_active`, date ranges) into the CTE's own WHERE clause to reduce predicate complexity at the join level.
2. Move BETWEEN clauses into JOIN conditions instead of the outer WHERE clause, preventing Trino from pushing them down as Iceberg partition filters.
3. Add explicit partition filters to CTEs referencing large Iceberg tables.

**Source**: APA-143248 (Closed), lts-reporting-insights-offline PR #909

---

## P20 — HDFS DataNode High Disk Utilization (Holdem Cluster)

**Trigger**: DataNode disk utilization on a Holdem HDFS cluster exceeds the 20% `DfsUsedHighUtilizationFraction` threshold. Triggers auto-generated Iris alert tickets.

**Signature**: Iris alert tickets with `DfsUsedHighUtilizationFraction` metric exceeding threshold. Example: `ltx1-holdem-cluster07` at 41.7% (2x threshold).

**Root cause**: Not yet documented. Possible causes: data growth, failed decommission, HDFS trash not being emptied, or large DAG outputs accumulating.

**Frequency**: Low-medium — ~30 alert tickets generated for `ltx1-holdem-cluster07` over Apr 12-13 2026. No documented resolution.

**Impact on Airflow**: Holdem DAGs writing to HDFS (Spark outputs, feature computations) may fail or slow down if disk capacity is exhausted on the affected cluster.

**Related systems**: HDFS, Holdem Airflow cluster, YARN

**Source**: APA-144235 through APA-144300 (all Closed — no comments, Apr 2026)

---

## P21 — Partition Sensor Permanent False (Trino DCE Gap)

**Trigger**: A DAG uses a `PartitionSensorDefinition` (or any ARMS-based partition sensor) to wait for data written by a Trino `SQLOperator` or Trino-based write path. The sensor runs indefinitely and never triggers.

**Signature**:
```
airflow.exceptions.AirflowSensorTimeout: Sensor has timed out; run duration of X > timeout of Y
```
Sensor pokes return False every cycle despite data being present in the target table. No ARMS error — the sensor simply never sees the partition.

**Root cause**: Trino/SQLOperator writes do **not** emit Data Change Events (DCE) to ARMS/Jasper. Partition sensors rely on DCE to detect new data. Since no DCE is emitted, ARMS has no record of the partition, and the sensor returns False forever. Known gap tracked as APA-137978.

**Frequency**: Low-medium — affects DAGs migrating from Spark-based writes to Trino, or new DAGs using Trino for data production with downstream partition sensors.

**Related systems**: ARMS, Jasper, Trino, SQLOperator, Airflow sensors

**Fix**:
1. Switch from Trino writes to **Spark** writes (which emit DCE).
2. Use **time-based sensors** (`TimeDeltaSensor`, `TimeSensor`) instead of partition sensors.
3. Do NOT rely on `PartitionSensorDefinition` for Trino-written datasets.

**Source**: APA-141942, APA-137978

---

## P22 — YARN "Unknown Job" Error in GGW Flows

**Trigger**: A GGW-submitted Hadoop/MapReduce job (commonly UMP union_merge steps) completes or is evicted, and the YARN ResourceManager loses track of the job ID before the GGW polling cycle can retrieve final status.

**Signature**:
```
Caused by: org.apache.hadoop.ipc.RemoteException(java.io.IOException): Unknown Job job_XXXX_XXXXXX
```
Error surfaces at task steps that query YARN for job status (e.g., `union_merge` in UMP pipelines).

**Root cause**: The YARN ResourceManager no longer has the job in its active or completed-job history. Possible causes:
- YARN RM restart or failover (in-memory job state lost)
- Job exceeded YARN's completed-job retention period before status was fetched
- Grid cluster instability causing RM to drop job records

**Frequency**: Low — appears sporadically, often correlated with grid cluster maintenance or RM instability. Multiple UMP flows affected simultaneously suggests a cluster-wide event.

**Related systems**: GGW, YARN ResourceManager, Hadoop IPC, UMP (Unified Merge Pipeline)

**Fix**:
1. Retry the failed task — the job will be resubmitted to YARN.
2. If multiple flows fail simultaneously with `Unknown Job`, check YARN RM health on the target grid cluster.
3. Escalate to Grid/YARN oncall if the pattern persists across multiple executions.

**Source**: APA-142535 (Closed — Fixed, Apr 2026)

---

## P23 — AirflowSkipException Regression (pre_execute FAIL Instead of SKIP)

**Trigger**: DAG uses `AirflowSkipException` in `pre_execute` (including operators that internally skip via this mechanism) and `lipy-airflow-providers` was upgraded through version v0.0.881.

**Signature**:
Task shows `FAILED` with `AirflowExecuteHookException` in logs when the expected outcome was `SKIPPED`. The task body never executed; the failure originates in `pre_execute`. No obvious application logic error.

**Root cause**: Breaking regression in v0.0.881: skip exceptions raised inside `pre_execute` were no longer translated to task `SKIP` state. Instead they propagated as `AirflowExecuteHookException`, causing `FAILED` state. Fixed by rollback to v0.0.867 at the time of incident.

**Frequency**: Low — specific to the v0.0.881 version window (Feb 2026). May reappear if a similar change is re-introduced during future refactors.

**Related systems**: lipy-airflow-providers (apache-airflow-providers-lnkd), Airflow task lifecycle

**Note**: The `allow_rdev_runs=False` intentional-SKIP behavior in RDev is a **separate** mechanism and was not affected by this regression.

**See**: [lipy-airflow-providers](../systems/lipy-airflow-providers.md)

---

## P24 — AIRFLOW::TASK_EXEC::ABORTED

**Trigger**: Airflow worker pod receives SIGTERM during task execution. GGW or ARMS is mid-operation when the pod is disrupted.

**Signature**:
```
AIRFLOW::TASK_EXEC::ABORTED
```
Task ends with FAILED or UPSTREAM_FAILED. In logs: pod disruption event, or failed GGW "adoption" (picking up an in-progress job after scheduler restart).

**Root cause**:
- Pod eviction/drain during active task execution
- GGW adoption failure: the task tried to re-adopt a GGW execution after scheduler restart, but adoption fails (connection refused, auth timeout)
- SIGTERM during ARMS partition-sensor evaluation

**Frequency**: Low-medium — spikes during cluster maintenance or resource pressure.

**Related systems**: GGW, ARMS, Kubernetes (pod lifecycle)

**See**: [Playbooks - GGW Environment Error](playbooks.md#ggw-environment-error) (ABORTED is often related to P1 pattern)

---

## P25 — AIRFLOW::TASK_EXEC::HEARTBEAT_LOST

**Trigger**: Worker pod dies (OOM, eviction, node failure) while a task is RUNNING. Task becomes a zombie.

**Signature**:
```
AIRFLOW::TASK_EXEC::HEARTBEAT_LOST
```
Task stuck in `running` state with no recent log output. No heartbeat update for >N minutes.

**Root cause**:
- Worker pod OOM-killed or evicted (see codebase/gotchas for GGW log size OOM)
- Stale logging recursion crash — task's log buffer causes infinite logging loop that eventually crashes the process
- Node failure removes the pod without graceful termination

**Frequency**: Low-medium — higher on Holdem (larger cluster, more workers, more disruptions).

**Related systems**: Kubernetes (pod lifecycle), GGW log emission, Airflow zombie detector

**Debug**:
1. Check if worker pod still exists: `kubectl get pods -n airflow -l component=worker`
2. Check Kusto for OOM events around the task failure time
3. If GGW log size OOM suspected: look for 376KB+ `exec-status` log lines in pod logs

**See**: [Playbooks - Task Stuck Running](playbooks.md#task-stuck-running)

---

## P26 — AIRFLOW::TASK_EXEC::PRE_EXEC_FAILURE

**Trigger**: DAG file is not found or `FileNotFoundError` occurs during task setup before execution body runs.

**Signature**:
```
AIRFLOW::TASK_EXEC::PRE_EXEC_FAILURE
FileNotFoundError: /opt/airflow/dags/<mp>/<dag>.py
```
Task fails immediately at start, before any application logic runs.

**Root cause**:
- DAG file removed from NFS but scheduler still has the DagBag entry cached
- Worker node's NFS mount is stale or lagging
- Race between DAG upload and task execution (task started before new DAG file was fully written to NFS)
- MP was undeployed mid-run

**Frequency**: Low — spikes during MP undeployments or NFS rebalancing events.

**Related systems**: NFS, Airflow worker, DagBag, CRT deployment

**Fix**: Retry the task once the DAG file is restored. If recurring, check NFS mount health on affected nodes.

---

## P27 — AIRFLOW::MISSING::NON_GGW::EMPTY_FAILURE_MSG

**Trigger**: Task fails with no error message in Airflow UI or logs.

**Signature**:
```
AIRFLOW::MISSING::NON_GGW::EMPTY_FAILURE_MSG
```
Task FAILED state but empty error field. No exception traceback in task logs.

**Root cause** (two distinct causes as of April 2026):
1. **DBT cluster (Airflow 2.5.3)** — older Airflow version does not propagate exception messages in all code paths; empty failure messages are common on the DBT-specific cluster.
2. **ExternalPythonOperator with legacy venv** — 69+ DAGs in Corp/Holdem use `ExternalPythonOperator` pointing to an old Python environment. The failure message capture mechanism differs in the legacy env and produces empty messages.

**Frequency**: Medium within DBT cluster scope; Low-medium in Corp/Holdem ExternalPythonOperator flows.

**Related systems**: DBT Airflow cluster (Airflow 2.5.3), ExternalPythonOperator, legacy Python venvs

**Debug**:
1. Check which cluster the DAG is on — DBT cluster uses Airflow 2.5.3
2. Look for `ExternalPythonOperator` in the DAG definition
3. Check the actual Python subprocess output in raw pod logs (not Airflow UI log viewer)

---

## P28 — UMP Schema Incompatibility After Dimension Changes

**Trigger**: A PR to `metric-defs` adds or removes dimensions in a UMP (Unified Metrics Platform) flow. The existing schema for `*_union` and `*_union_staged` tables in `u_metrics` is incompatible with the changed dimensions.

**Signature**:
```
Schema incompatibility error writing to `u_metrics`.<flow_name>_union_staged
```
Or: flow fails at the ORC/Dali publish step with schema mismatch between the changed dimension columns and the existing table definition. Also: `Union column type mismatch error` when a dimension column is removed but the existing schema still expects it.

**Root cause**: UMP flows write to staged union tables (`*_union_staged`, `*_union`). When dimensions are added or removed via a `metric-defs` PR, the existing Hive table schema doesn't match the new column set, causing a schema incompatibility at write time. The fix is to **drop only the schema** (NOT the data) of the affected tables, allowing the next run to recreate them with the updated schema.

**Frequency**: **High** — occurs each time dimensions are added to or removed from a UMP metric definition. **Seven confirmed instances** in April 2026: APA-144496, APA-144369, APA-144528, APA-144539 (`payments_approval_v3`), APA-144610 (`capi_adoption_metrics_agg`), APA-144631 (`lms_advertiser_quality_actions_daily` — dimension removal variant), APA-144645 (`rsc_candidates_dq`/`rsc_applications_dq_v2`, Apr 20). Pattern is accelerating — 7 instances in 3 weeks.

**Related systems**: UMP, metric-defs repo, Hive, ORC, Dali

**Fix**:
1. Identify affected tables from the error message (e.g., `u_metrics.annotation_quiz_metrics_union`, `u_metrics.conversion_tracking_v2_cpa_union_staged`, `u_metrics.conversion_tracking_v2_plus_union`)
2. Drop schema only (NOT data): `ALTER TABLE u_metrics.<table_name> REPLACE COLUMNS (...)` or recreate the table definition
3. Request schema drop via Slack or ticket to the UMP team
4. Re-run the flow after schema is updated

**Note**: APA-144528 (`conversion_tracking_v2_plus`) was a follow-on from APA-144369 (`conversion_tracking_v2_cpa`) — the same `metric-defs` PR caused type casting errors in a sibling flow. Multiple downstream union tables may need schema drops from a single dimension PR. APA-144631 is the first confirmed instance triggered by dimension *removal* (removing `datepartition` from dimensions), not just addition.

**Source**: APA-144496 (annotation_quiz_metrics), APA-144369 (conversion_tracking_v2_cpa), APA-144528 (conversion_tracking_v2_plus), APA-144631 (lms_advertiser_quality_actions_daily, **Closed Apr 20**) — all Closed/Fixed; APA-144539 (payments_approval_v3), APA-144610 (capi_adoption_metrics_agg), APA-144645 (rsc_candidates_dq / rsc_applications_dq_v2, opened 2026-04-20) — Open, Apr 2026. **7 confirmed instances** in April 2026.

---

## P29 — ARMS PartitionSensor False Negative (Existing Partitions)

**Trigger**: A `PartitionSensor` (or `DatasetSensorArray` with `partition_check_fallback_mode: STRICT`) consistently returns `poke result: False` for a dataset partition that is confirmed present in the underlying Hive/HDFS table. No data is actually missing; ARMS is returning incorrect metadata. Onset: approximately 2026-04-13 on Holdem cluster.

**Signature**:
```
poke result: False, description:
```
Empty `description` field (no error detail, no "partition not found" message). The partition timestamp window in the sensor config may also shift unexpectedly (e.g., sensor checking 2025-11-01-2025-11-10 instead of the expected current date range).

**Root cause**: `bdp-artifact-metadata-service` (the ARMS backend) uses `dali-data-sdk` internally to resolve Hive partition metadata via gRPC calls. The bug specifically affects datasets whose underlying table is a **Hive view backed by an OpenHouse table** (`DESCRIBE FORMATTED` -> `Table Type: VIRTUAL_VIEW`). Two events around Apr 9-12 are the suspected regression triggers:
1. `bdp-artifact-metadata-service` v0.0.200 and v0.0.201 deployed **2026-04-09** — may have pinned an older or buggy `dali-data-sdk`
2. `dali-data-sdk` **v2.9.18** released **2026-04-12** — may have reintroduced a Hive-view resolution bug fixed in v2.8.60

**Prior history**: This is a recurring bug class — APA-140631 (same symptom on holdem), APA-142622 (root cause: `dali-data-sdk` 2.7.11 broken for Hive views), APA-143191 (fix deployed Mar 26: bumped to 2.8.60). The Apr 13 regression is a re-emergence after the Mar 26 fix.

**Distinction from similar patterns**:
- **P21** (Trino DCE gap): Data was never emitted by Trino's DCE pipeline — ARMS metadata is correct, data truly isn't there. Fix: wait or backfill.
- **P5** (Sensor timeout): Data is genuinely absent or late. Fix: investigate upstream producer.
- **P29** (this pattern): Data IS present; ARMS/dali-data-sdk returns False for Hive views over OpenHouse tables. Fix: ARMS team bumps/pins `dali-data-sdk` version.

**Frequency**: Low — 2 confirmed instances as of 2026-04-20. May be underreported if teams mark sensors success manually without filing tickets.

**Related systems**: ARMS (`bdp-artifact-metadata-service`), `dali-data-sdk`, PartitionSensor, DatasetSensorArray, OpenHouse, Holdem, War (lva1 cluster)

**Workaround (unblock now)**:
1. Confirm the partition exists via Trino/Hive: `SELECT * FROM u_lmssalesops.dim_lms_country_region WHERE datepartition='2026-04-16-00' LIMIT 1`
2. In Airflow UI -> Task Instance -> Actions -> **Mark Success** on the stuck sensor task
3. Verify table type: `DESCRIBE FORMATTED <table>` — if `Table Type: VIRTUAL_VIEW`, this pattern applies

**Diagnostic (distinguish ARMS vs Dali layer)**:
```scala
// Run in spark-shell on holdem
import com.linkedin.dali.data.sdk.DaliDataSdk
DaliDataSdk.jasperCheckAllPartitionsExist(
  "u_lmssalesops.dim_lms_country_region",
  Seq("datepartition"), Seq(Seq("2026-04-16-00")), "DAILY", "STRICT")
// false -> Dali SDK bug; true -> ARMS service layer bug
```

**Long-term fix**: ARMS team (`bdp-artifact-metadata-service`) verifies `dali-data-sdk` version in `product-spec.json` and rolls back to `2.8.60` or patches the regression in `2.9.x`. Escalate to BDP Data Triggers / ARMS oncall via #ask_airflow.

**Source**: APA-144621, APA-144646, APA-144609, APA-144545, APA-144431, APA-144592; prior: APA-140631, APA-142622, APA-143191

---

## P30 — Spark KJP Startup Timeout (HDFS Degradation + Timeout Budget Exhaustion)

**Trigger**: A `SparkBatchOperator` task on KJP (Kubernetes Job Platform) fails with `AirflowTaskTimeout` or the DAG-level `dagrun_timeout` fires. The Spark driver launched, but zero tasks ever executed. Executor pods were either killed before `CoarseGrainedExecutorBackend` could register, or were never scheduled due to Volcano gang scheduling failures.

**Signature**:
- Driver log: `Initial job has not accepted any resources` immediately before the driver exits
- Executor log: last line is `Power failure` (SIGPWR signal 30)
  ```
  /opt/entrypoint.sh: line 148:    27 Power failure    ${JAVA_HOME}/bin/java ... org.apache.spark.executor.ExecutorResourceLocalizer ...
  ```
- Airflow task log: `AirflowTaskTimeout` or DAG run state transitions to `failed` with reason `dagrun_timeout`
- K8s events (`kubeEvents2`): may contain `FailedScheduling: pod group not ready, N Unschedulable` from Volcano scheduler

**Root cause**: The KJP Spark startup pipeline downloads large HDFS tarballs at three separate layers (DriverResourceLocalizer init container -> SparkContext JVM -> ExecutorResourceLocalizer init container). This "triple download" of the same files (notably `hive-libjars-*.tar.gz`, ~1.2 GB) is fast under normal HDFS conditions but becomes the bottleneck under HDFS degradation. Combined with `max_active_runs=1` queueing delay consuming part of `dagrun_timeout` before the attempt even starts, the timeout budget is exhausted before executors can register.

**Frequency**: Low normally; spikes during HDFS NameNode degradation events (seen Apr 20, 2026 on holdem).

**Related systems**: KJP (Kubernetes Job Platform), HDFS (for tarball downloads), Volcano (gang scheduler), GGW, Airflow `dagrun_timeout` / `execution_timeout`

**Mitigation**:
- Short-term: increase `dagrun_timeout` to `schedule_interval x 2 + actual_job_runtime + startup_overhead` (account for queueing + triple HDFS download)
- Check `max_active_runs` vs schedule frequency; `max_active_runs=1` with a 30-min schedule can burn up to 30min of `dagrun_timeout` before the run starts
- Long-term: monitor HDFS NameNode health during slowdowns; investigate whether `hive-libjars` tarball caching can reduce triple-download overhead

**See**: [Spark - KJP Debugging](../systems/spark.md#spark-on-kjp--debugging-startup-delays)

**Source**: APA-144XXX (holdem, Apr 20, 2026)

---

## P31 — DaliException Schema Nullability Mismatch (APA-144721)

**Trigger**: A Spark job writes a dataframe to a Dali-managed dataset, but the dataframe's inferred schema includes non-nullable fields that conflict with the target Avro schema's nullable union types (or vice versa).

**Signature**:
```
DaliException: Schema incompatible — dataframe schema has non-nullable field 'X' but Avro schema expects ['null', 'string']
```
Or: schema validation fails at write time with nullability mismatch between the Spark dataframe schema and the registered Avro schema.

**Root cause**: Dali's schema compatibility check enforces strict nullability matching between the Spark dataframe's inferred schema and the Hive/Avro table schema. When a Spark dataframe column is inferred as non-nullable (e.g., from a `filter(col.isNotNull)` or a literal) but the target schema defines the field as a union type `["null", "string"]`, Dali rejects the write. This also surfaces after schema migrations that change nullability semantics.

**Frequency**: Low — 1 confirmed instance (APA-144721, Apr 2026).

**Related systems**: Dali, Avro, Spark dataframe schema inference, Hive metastore

**Fix**:
1. Explicitly cast columns to nullable types before writing: `.withColumn("col", col("col").cast(StringType()))` — Spark `cast()` produces nullable output by default.
2. Or update the target Avro schema to make the field non-nullable if the data guarantees non-null values.
3. Check `DESCRIBE FORMATTED <table>` for the exact Avro schema and compare with `df.printSchema()`.

**Source**: APA-144721 (Apr 2026)

---

## P32 — Spark 3.1 Iceberg Sort Injection Gap (write.distribution.mode=hash)

**Trigger**: A Spark 3.1 job writes to an OpenHouse/Iceberg table configured with `write.distribution.mode=hash` and `write ordered by` properties. The write fails with `IllegalStateException` because Spark 3.1 does not inject a sort node before `AppendData`, unlike Spark 3.5 which handles this correctly.

**Signature**:
```
IllegalStateException: Incoming records violate the writer assumption that records are clustered by spec and by partition
```
Task fails at write time with Iceberg partition violation. The Spark job may have appeared to run correctly up to the write phase.

**Root cause**: In Spark 3.1, the Iceberg integration does not automatically inject a `Sort` physical plan node before `AppendData` when `write.distribution.mode=hash` and `write ordered by` are specified on the table. Spark 3.5 correctly adds this sort. This is a version-specific gap in the Spark-Iceberg integration — it is unclear whether this is intentional or a bug in the older Spark version.

**Frequency**: Low — affects DAGs running on Spark 3.1 that write to Iceberg tables with hash distribution mode. Will become less common as teams migrate to Spark 3.5.

**Related systems**: Spark (3.1 specifically), Iceberg, OpenHouse, Grid Gateway

**Fix**:
1. **Preferred**: Upgrade to Spark 3.5 where the sort injection is automatic.
2. **Workaround**: Manually add `.sortWithinPartitions(...)` or `.repartition(...)` before the Iceberg write to ensure records are properly ordered.
3. Remove `write.distribution.mode=hash` from the table properties if hash distribution is not required.

**Source**: APA-145082 (Closed — Fixed, Apr 2026)

---

## P33 — Dali/Jasper Partition Check False Failure on Holdem (Recurring)

**Trigger**: A Spark job using Dali SDK or Jasper to check partition existence for a tracking table (e.g., `tracking.profileeditevent`) on the Holdem cluster falsely reports that partitions do not exist, despite the data being present. Similar to P29 (ARMS PartitionSensor False Negative) but occurs within Spark job code rather than Airflow sensors.

**Signature**:
```
DaliException: Partition check failed for tracking.profileeditevent
```
Or: Jasper `checkAllPartitionsExist` returns false for partitions confirmed present via direct Trino/Hive query.

**Root cause**: The Dali SDK or Jasper client within the Spark job is using a stale or buggy version that fails to resolve partitions correctly for certain table types (particularly Hive views over OpenHouse tables or tracking tables). This is the same class of bug as P29 — related to `dali-data-sdk` version regressions.

**Frequency**: Low — 1 confirmed instance (APA-144143, Closed — Fixed, Apr 2026).

**Related systems**: Dali SDK, Jasper, Spark, Holdem cluster, OpenHouse

**Fix**:
1. Verify the `dali-data-sdk` version used by the Spark job matches the latest known-good version.
2. Check if the partition exists via direct Hive/Trino query as a workaround.
3. Escalate to Data Triggers / ARMS team if the SDK version is correct and the issue persists.

**Source**: APA-144143 (Closed — Fixed, Apr 2026)

---

## Quick Reference Table

| # | Pattern | Key Signature | Frequency | Systems |
|---|---------|--------------|-----------|---------|
| P1 | GGW ENVIRONMENT_* | `ENVIRONMENT_CLUSTER_UNAVAILABLE` / task failed but GGW job ran | Medium | GGW, K8s, YARN |
| P2 | ProxyUser ACL | `not allowed to impersonate as proxy_user` | High | GGW, Grid User Manager |
| P3 | Spark OOM / Queue | `OutOfMemoryError` / queue capacity error | Medium-High | Spark, YARN |
| P4 | DAG import error | `ImportError`, `ModuleNotFoundError` in DagBag | Medium | Scheduler, DagBag |
| P5 | Sensor timeout | `AirflowSensorTimeout` / worker slot starvation | Medium | ARMS, sensors |
| P6 | Fernet key length | `InvalidToken` / `must be 32 url-safe base64-encoded bytes` | Low-Med | RDev, Fernet |
| P7 | Config merge | Nested config silently lost in RDev | Low | RDev, oklahoma-helpers |
| P8 | LDAP case sensitivity | `User not found` / permission errors | Low | LDAP, security manager |
| P9 | SSL verify disabled | `InsecureRequestWarning` | Low (platform) | DAG validation, GUM |
| P10 | Scheduler contention | `critical_section_duration` spike | Low, high-impact | Scheduler, MySQL |
| P11 | DAG missing in Tradewind | DAG on cluster but not in federated UI | Low-Med | Tradewind, CRT |
| P12 | NFS mount hang (fsGroup) | Pod stuck in `ContainerCreating`, D-state processes | Low | NKS, NFS, Helm |
| P13 | RDev cert / login issues | `Cannot fetch user cert`, picli timeout, "Invalid login" | **Very High** | RDev, picli, ssh-ca-cli |
| P14 | RDev DAGs not loading | Empty DAG list, missing symlinks | High | RDev, okl-rdev-init.sh |
| P15 | AmbCacheError | `Failed to get certificates for Ambry` | Low | DataVault, k8s-lare |
| P16 | OSOS stuck in queue | GGW execution QUEUED indefinitely | Low-Med | GGW, OSOS |
| P17 | Tracking data Holdem-only | Data missing on Faro; tracking tables not on staging | Low-Med | Tracking, Kafka, Faro |
| P18 | Hive-Espresso schema mismatch | `AnalysisException` / wrong field paths after Kyoto migration | Low (recurring) | Hive, Espresso, Kyoto |
| P19 | Trino Iceberg StackOverflow | `StackOverflowError` in `IcebergSplitSource.partitionMatchesConstraint` | Low (high risk for migrations) | Trino, Iceberg, Spark |
| P20 | HDFS DataNode high disk util | `DfsUsedHighUtilizationFraction` alerts, 30+ tickets | Low-Med | HDFS, Holdem |
| P21 | Partition sensor Trino DCE gap | Sensor times out; data exists but ARMS never sees it | Low-Med | ARMS, Trino, sensors |
| P22 | YARN Unknown Job (UMP) | `Unknown Job job_XXXX` from Hadoop IPC at union_merge | Low | GGW, YARN RM, UMP |
| P23 | AirflowSkipException regression | Task `FAILED` with `AirflowExecuteHookException` when SKIPPED expected | Low (v0.0.881 regression) | lipy-airflow-providers |
| P24 | ABORTED (pod disruption) | `AIRFLOW::TASK_EXEC::ABORTED` | Low-Med | GGW, ARMS, K8s |
| P25 | HEARTBEAT_LOST (zombie task) | `AIRFLOW::TASK_EXEC::HEARTBEAT_LOST` | Low-Med | K8s, GGW logs, zombie detector |
| P26 | PRE_EXEC_FAILURE (DAG file missing) | `AIRFLOW::TASK_EXEC::PRE_EXEC_FAILURE` / `FileNotFoundError` | Low | NFS, DagBag, CRT |
| P27 | EMPTY_FAILURE_MSG (DBT/ExternalPython) | `AIRFLOW::MISSING::NON_GGW::EMPTY_FAILURE_MSG` | Med (DBT) / Low-Med (Corp) | DBT cluster, ExternalPythonOperator |
| P28 | UMP schema incompatibility | Schema incompatibility on `u_metrics.*_union_staged` after dimension changes | **High** (7 instances Apr 2026) | UMP, metric-defs, Hive |
| P29 | ARMS PartitionSensor false negative | `poke result: False, description:` (empty) for partitions confirmed present | Low (2 instances, onset 4/13) | ARMS, PartitionSensor, Holdem/War |
| P30 | Spark KJP startup timeout (HDFS degradation) | `Power failure` (SIGPWR) in executor logs; zero tasks run; driver: "no resources" before exit | Low (spikes during HDFS events) | KJP, HDFS, Volcano, GGW, Airflow timeouts |
| P31 | DaliException schema nullability mismatch | `DaliException` / Avro schema nullability check fails on dataframe schema | Low (1 instance Apr 2026) | Dali, Avro, Spark, Hive |
| P32 | Spark 3.1 Iceberg sort injection gap | `IllegalStateException: Incoming records violate the writer assumption` with `write.distribution.mode=hash` | Low (Spark 3.1 only) | Spark 3.1, Iceberg, OpenHouse |
| P33 | Dali/Jasper partition check false failure | Dali SDK `checkAllPartitionsExist` returns false for existing partitions on Holdem | Low (1 instance) | Dali SDK, Jasper, Holdem |

---

## External State Override

**Pattern**: Task state manually set to `success` via Airflow UI/API while the task is still running.

**Trigger**: User accidentally marks task as success, or automated process sets state without checking if task is actively executing.

**Symptoms**:
- Log: `State of this instance has been externally set to success. Terminating instance.`
- Task runner killed mid-execution
- Expected output files never written
- Downstream DAGs/Flyte executions fail due to missing outputs

**Key insight** (confirmed APA-144575): This log line is **definitive proof of a manual action or REST API call** — it is never triggered automatically by Airflow's own internals (not a timeout, not zombie detection, not scheduler logic). The actor is always traceable.

**Investigation path**:
1. In Airflow UI -> **Browse -> Audit Logs**
2. Filter by `dag_id`, `task_id`, and the run timestamp (+/-5 min around the log line)
3. Look for a `mark_success` or `set_task_instance_state` event — the actor (user LDAP or service principal) will be recorded
4. Confirm with that person whether it was intentional

**Distinction from APA-144177 / Lakeshift silent success**: That ticket involved a platform-level aggregation bug where the parent DAG reports SUCCESS despite a child failing — no external state override involved. **APA-144177 was Closed Apr 20 (Vikram Bohra).** This pattern requires the `externally set` log line.

**Recovery**:
1. Do **not** mark the task success again — let it run to natural completion
2. Clear the failed task in Airflow UI to re-queue it
3. Watch logs to confirm output is written before task finishes
4. Re-trigger any blocked downstream Flyte/DAG executions once the missing partition/output is confirmed present

**Source**: APA-144575 (Airflow Premium inference flow, holdem, Apr 16 — On Hold/resolved as user action; root cause triage by Vinayak)

## See Also
- [Playbooks](playbooks.md) — step-by-step resolution for each pattern
- [Troubleshooting](../troubleshooting.md) — failure taxonomy and debug paths
- [GGW](../systems/ggw.md) — Grid Gateway architecture and failure modes
- [Spark](../systems/spark.md) — Spark failure modes and operator reference
- [DAG Authoring](../dag-authoring.md) — DAG authoring best practices
