> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

# Airflow — Troubleshooting

## Quick Debug Checklist

When something is broken, run through these steps in order:

1. **Locate the failed DAG run** — Open the DAG in Airflow UI, use Grid View to find the red run. Note: the data interval end is when the run actually starts.
2. **Check for PipelineMD** — If you see the `PipelineMD` button on the DAG run page, click it first. It provides automated root cause analysis and actionable insights.
3. **Check for "View Root Cause"** — Click it. Routes to either the Observe UI (Grid Gateway jobs) or Airflow task log page (sensors, Python tasks).
4. **If no root cause button** — Check for DAG run timeout, manual failure (audit log), or upstream dependency failures.
5. **For GGW jobs** — Read the "Job Failure Message" in the Grid Gateway log page. If unclear, use the `Spark Job Log` button (top right) to dig into the underlying job.
6. **For sensors** — Check if upstream data is available; search for the dataset owner at go/datahub.
7. **Check task logs directly** — via Airflow UI, kubectl, or airflow CLI (see Log Access section below).
8. **Look up the error class** in the Failure Taxonomy table.
9. **Escalate if needed** — see escalation paths at the bottom of this page.

---

## Failure Taxonomy

### 1. Grid Gateway (GGW) Failures

| Failure Type | Signature | Debug Path | Fix / Mitigation | Gotcha |
|---|---|---|---|---|
| **GridGatewayExecutionError** | `Execution {urn} did not succeed! Error code: ... Error message: ...` + GGW banner in logs | Click "Grid Gateway Log" button in task UI. Check errorCode and errorMessage fields. | Depends on underlying error — see Spark/Java failure sub-types. | GGW banner is logged at ERROR level in main task log before the exception is raised. |
| **GridGatewayTimeoutException** | `gRPC call to Grid Gateway exceeds its deadline` | Check if GGW service is down. Check polling retries (`GRID_GATEWAY_POLLING_RETRIES=5`). Check if `polling_interval` is too short. | Increase `retries` or `GRID_GATEWAY_POLLING_RETRIES`; verify GGW is healthy. | Minimum polling interval clamped to 30s (BDP-21492). Passing `polling_interval=10` still becomes 30. |
| **GridGatewayConnectionException** | `Grid Gateway gRPC service is unavailable` | Check GGW service health; check network/cert connectivity from Airflow pod. | Retry; escalate to Grid Gateway team if persistent. | |
| **GridGatewayConnectionSettingNotFound** | `Grid Gateway connection error: ... AirflowNotFoundException` | Check that `grid_gateway_service_default` connection is configured in Airflow connections. | Create or fix the Airflow connection with correct host, cert, and key paths. | |
| **GridGatewayProxyUserPermissionException** | `DAG/MP identity does not have permission to impersonate as proxy user` | Check proxy_user value; verify registration in Grid User Manager. | Contact Grid User Manager team; ensure proxy user is authorized for the target cluster. | See also OklahomaAirflowProxyUserACLException for upload-time ACL validation. |
| **GridGatewayDataVaultTokenException** | `DataVault token acquisition fails due to service unavailability or timeouts` | Check DataVault service health; verify identity certificate is provisioned at `/var/cluster/oklahoma/identity.cert`. | Restart pod to refresh token; escalate to DataVault/identity team if persistent. | `enable_sts_token=True` flag controls STS token usage (commit b7998347). |
| **GridGatewayInitializationException** | `Grid Gateway hook initialization error: ...` or `Grid Gateway operator initialization error: ...` | Check operator constructor params; check conn_id; check `disruption_ready` param type (must be bool not string). | Fix param types; verify conn_id exists. | `disruption_ready` must be a Python bool. Passing a string like `"True"` raises `TypeError`. |
| **GridGatewayCertificateProvisioningError** | `Grid Gateway certificate provisioning process error` | Check identity cert at configured path. | Reprovision identity certificate. | |
| **GridGatewayUnexpectedGrpcException** | Catch-all for unanticipated gRPC errors | Check GGW logs for detailed error. | Escalate to Grid Gateway team with execution URN. | |
| **ENVIRONMENT_* errors** | `errorCode: ENVIRONMENT_CLUSTER_FAILURE` or similar `ENVIRONMENT_*` pattern | Cluster or resource infrastructure issue, not user code. | Enable `disruption_ready=True` on supported functions (hadoopJava, java, javaprocess, command, hadoopShell, **darwin** [added Apr 2026, PR #1193]) to auto-retry up to 3 times. **Not yet supported for Spark.** | Disruption-ready retry policy: `{"maxRetries": 3, "rules": [{"onErrorCode": "ENVIRONMENT_.*"}]}` |

---

### 2. Spark Failures

All Spark failures surface as `GridGatewayExecutionError`. Use the Spark Job Log button in the GGW log page to get the underlying Spark application logs.

| Failure Type | Signature | Debug Path | Fix / Mitigation | Gotcha |
|---|---|---|---|---|
| **OOM (Driver)** | `java.lang.OutOfMemoryError` in Spark driver logs | Check driver heap usage via Spark UI / Observe logs. | Increase `driver_memory` (e.g., `"4G"`). Profile job to estimate needs. | |
| **OOM (Executor)** | `java.lang.OutOfMemoryError` in executor logs | Check executor heap via Spark UI / Observe logs. | Increase `executor_memory`; reduce executor payload per partition. | |
| **YARN Queue Capacity** | `Queue not found` or queue rejection error from GGW | Verify queue name; check queue utilization in YARN UI. | Reduce `executor_num` or wait for queue capacity. Use `priority.production` for high-priority jobs. | |
| **Dependency Resolution Failure** | `@Artifact resolution fails` or Ivy coordinates not found | Check artifact exists in Artifactory; verify exact multiproduct name and version. | Fix `grid_gateway_dependencies` coordinates; re-publish artifact to Artifactory if needed. | Multiproduct name is case-sensitive. |
| **Kerberos / Auth Failure** | `GSSException` or `AuthenticationException` in Spark/GGW logs | Check identity cert; check `spark.authenticate=true` is set in `spark_confs`. | Verify identity cert at `/var/cluster/oklahoma/identity.cert`; restart pod to refresh Kerberos ticket. | |
| **Guava Classpath Conflict** | Deserialization error involving guava or commons classes | Check `spark.driver.extraClassPath` and `spark.executor.extraClassPath` values in GGW params. | Ensure guava 25.0 is pinned; use APPEND pattern for extraClassPath, not replace. [code/lipy-airflow-providers#8c1d7b73] | Feature Cloud Push injects guava via framework code. If you set `spark.driver.extraClassPath`, the framework must APPEND to it (`+=`), not overwrite it. Fixed in commit 8c1d7b73. |
| **Pod Disruption During Job** | Task FAILED briefly; GGW job was still running (check GGW log URL) | Check GGW execution URN in task logs. Query GGW to see if job is still running. | Enable `enable_job_checkpoint=True` (default). On pod restart, task resumes polling from checkpoint instead of re-submitting. | Checkpoint resume logic is in `ExternalJobCheckpointMixin`. If GGW job succeeded during eviction, task will complete successfully on resume. |
| **Execution Target Not Found** | GGW error about missing JAR or script file | Check `execution_target` path; verify artifact was published to Artifactory/HDFS. | Fix path; verify `target_grid_cluster` matches cluster where artifact is deployed. | |
| **Job Class Not Found** | `ClassNotFoundException` at runtime | Check `job_class` fully-qualified name; verify it's in the JAR. | Fix class name spelling; verify JAR includes the class; check `classpath_jars`. | |
| **Polling Exhaustion** | Task exceeds max tries before GGW job completes | Check `GRID_GATEWAY_POLLING_RETRIES` (default 5); check expected job runtime. | Increase `retries` on operator; increase `polling_interval` for very long jobs. | |
| **RDev Execution Skipped** | `EXECUTION SKIPPED ... allow_rdev_runs` message in logs | Intentional — operator has `allow_rdev_runs=False` which skips in RDev to prevent touching prod data stores. | Set `allow_rdev_runs=True` to override; understand why it was disabled first. | |
| **Spark KJP Startup Timeout (HDFS degradation)** | Task fails with `AirflowTaskTimeout` or `dagrun_timeout` fires; driver log shows "Initial job has not accepted any resources"; executor logs end with `Power failure` (SIGPWR); zero tasks completed | HDFS download slowness during DriverResourceLocalizer + SparkContext double-download + ExecutorResourceLocalizer phases consumes the entire timeout budget before any executor registers. Compounded by `max_active_runs=1` queuing delay eating into `dagrun_timeout`. | Extend `execution_timeout` and `dagrun_timeout`; audit `max_active_runs` vs schedule frequency (budget = queueing + 3x HDFS download phases + actual job). Check HDFS NameNode health. For investigation: Airflow logs -> driver pod logs (`kube_grid_logs`, `spark-kubernetes-driver`) -> K8s events (`kubeEvents2`, Volcano `FailedScheduling`) -> executor pod logs (`kube_grid_logs`, `spark-app-container`, pod `<app-id>-exec-N`). See [Spark](systems/spark.md). | `"Initial job has not accepted any resources"` does NOT mean pods weren't scheduled — pods may be `Running` but stuck in `ExecutorResourceLocalizer` init container. Always check executor logs separately. `Power failure` in executor logs = SIGPWR signal 30 from driver shutdown hook, not an actual power failure. |

---

### 3. Sensor Timeouts

| Failure Type | Signature | Debug Path | Fix / Mitigation |
|---|---|---|---|
| **Data Partition Not Available** | Sensor exits with `ArmsDataUnavailableException` or times out waiting for partition | Check upstream job that produces the dataset; search go/datahub for dataset owner. | Contact upstream data owner to investigate why partition is late. |
| **Sensor poke_interval Too Short** | Airflow worker slot exhaustion — too many sensors running concurrently | Check active task count per DAG; note: sensors hold a worker slot in POKE mode. | Use `reschedule` mode (`mode="reschedule"`) to free worker slots between pokes. Use `DatasetSensorArray` to check multiple datasets in a single task. |
| **Sensor Timeout (task-level)** | Task moves to FAILED after `timeout` seconds | Check `timeout` parameter; check sensor start time vs expected data arrival. | Set `timeout` to 2-3x typical data arrival lag. Set `dagrun_timeout` at DAG level as safety net. |
| **ARMS Connection Failure** | `ArmsConnectionException: ARMS gRPC service is unavailable` | Check ARMS service health; check network connectivity from Airflow pod. | Retry; escalate to Oklahoma team via go/ask-airflow if persistent. |
| **Snapshot Sensor False Negative** | Sensor does not satisfy even though data exists | Verify `baseline_datetime` value; check watermark fields in dataset metadata. | Adjust `watermark_field_names` to match the dataset's actual watermark field. |
| **Partition Sensor — Trino DCE Gap** | Sensor times out but data IS present in table | Upstream writes via Trino/SQLOperator — check if write path emits DCE to ARMS. | Trino writes do NOT emit DCE (APA-137978). Switch to Spark writes or use time-based sensors. See [Jira Patterns](jira/patterns.md). |

---

### 4. Scheduler Issues

| Failure Type | Signature | Debug Path | Fix / Mitigation | Gotcha |
|---|---|---|---|---|
| **Critical Section Lock Contention** | High `critical_section_duration` metric; scheduler loop slow | Check MDM metrics for `critical_section_duration` and `loop_duration`. | Reduce `ti_per_loop`; add scheduler replicas (contention increases non-linearly with replicas). | Critical section contention is the primary scheduler bottleneck under load. |
| **DAG Serialization Slow** | Long `schedule_delay`; DAG bag parse time high | Check DAG count; check import times in DAG processor logs. | Reduce DAG complexity; minimize top-level imports; use lazy imports in policy framework. |  Circular imports in `airflow_local_settings` and policy framework cause import failures at startup — use lazy imports inside functions [gotchas.md]. |
| **Pod Disruption (Scheduler)** | Scheduler pod evicted mid-cycle; tasks in `running` state orphaned | Check k8s events: `kubectl describe pod -n airflow-test <scheduler-pod>` | External job checkpointing (`enable_job_checkpoint=True`) handles GGW tasks. For others, tasks are re-queued on next scheduler cycle. |
| **Scheduler Fails to Start** | `Airflow scheduler failed to start -` in pod logs | Check pod logs; check security manager monkey-patch is applied. | Check `start_scheduler_2_9.py`; ensure `ApplessLinkedInAirflowSecurityManager` is patched. Known limitation (DAAS-80068). |
| **Log Server Port Conflict** | `Exception when creating server for worker logs` in scheduler log | Port `worker_log_server_port` already in use. | This is caught and does not abort scheduler startup. Investigate what is using the port. |
| **MySQL Transaction Kills Stuck Schedulers** | All jobs stuck in `queued` state; no new tasks dispatched | MySQL DBA killed long-running transactions; scheduler exception handling did not recover cleanly. | Restart scheduler pods. Coordinate with MySQL DBA team. APA-143794. Source: #ask_airflow Apr 2026. |

---

### 5. Upload / Deployment Failures

| Failure Type | Signature | Debug Path | Fix / Mitigation |
|---|---|---|---|
| **DAG Parse Failure** | `OklahomaAirflowDAGParsingException: Failed to parse DAG` / `FAILED TO PARSE DAGS...` in CRT | Check stacktrace in error message. Load DAGs in RDev and check import errors at top of Airflow UI. | Fix Python syntax errors, missing imports, or incorrect DAG structure. |
| **Import Error (missing module)** | `ModuleNotFoundError: No module named 'pandas'` or similar | DAG code uses an external library not available in the Airflow image or the application directory. | Only use libraries from the Airflow image or included in your application directory. Do not import from other MP modules. |
| **DAG Sync Failure** | `OklahomaAirflowDAGSyncingException: found import error after sync_to_db` | Race condition between DAG processor and upload sync. Usually transient. | Retry upload. If persistent, check for circular imports or broken access_control definitions. |
| **Proxy User ACL Missing** | `OklahomaAirflowProxyUserACLException: proxy_user ACL rules are missing` | Check that the uploading user is a member of all proxy user headless accounts in Grid User Manager. | Add user to the proxy user headless account in Grid User Manager; or remove the proxy_user parameter if not needed. |
| **DAG Not Visible After Deployment** | CRT says success but DAG not in Airflow UI | Check `access_control` in DAG definition; ensure you are a member of the specified group (not just admin). | Add `access_control={"SGP-ENG-<your-group>": {"can_read", "can_edit"}}`. Note: admins are NOT members of `SGP-CREW-<ID>-MEMBERS`. |
| **Conflicting Upload in Progress** | `Upload of {file} blocked due to conflicting upload in progress` | Another upload for the same MP is in progress. | Wait and retry. If upload is stale, investigate lock state. |
| **Archive Extraction Failure** | `OklahomaAirflowException: Failed to unzip file` | Check that uploaded zip is valid and not corrupted. | Re-build and re-upload the zip artifact. |
| **Too Many Files in Upload** | `Upload contains {N} files which exceeds the limit of 500` | Zip contains too many Python files. | Reduce files in upload to under 500. Split into multiple applications. |
| **NFS Move Failure** | `OklahomaAirflowException: Failed to move files to NFS directory` | NFS mount point unavailable or permission error. | Check NFS mount health; check file system permissions. |

---

### 6. DAG Import Errors

| Failure Type | Signature | Debug Path | Fix / Mitigation | Gotcha |
|---|---|---|---|---|
| **Circular Import** | `ImportError` or `AttributeError` during DAG parsing | Identify the import chain in the traceback; look for airflow module imports at module level in policy framework code. | Move `from airflow.*` imports inside functions (lazy imports). [codebase/gotchas.md] | Circular import in `airflow_local_settings` <-> `airflow_policy_framework.policies.lnkd.dag.mutation`. Affects any module that imports from `airflow.*` at module level. |
| **Missing Dependency** | `ModuleNotFoundError` on a library installed in a different MP module | DAG code imports from a different module in the same MP — not allowed. | Consolidate shared code into the Airflow application directory; don't cross-import between modules. |
| **DAG ID Naming Mismatch** | DAG not associated with correct crew asset; silent failure during sync | Check DAG ID follows `<dag_name>__<mp_name>` convention. | Fix DAG ID; use `picli validate-dags` to pre-validate naming before upload. [codebase/gotchas.md] |

---

### 7. Config Failures

| Failure Type | Signature | Debug Path | Fix / Mitigation | Gotcha |
|---|---|---|---|---|
| **Missing cfg2 Key** | Service crashes at runtime when accessing a config key that does not exist in deployment config | Check the servicemesh config template against required keys; enable verbose logging. | Add missing cfg2 key to deployment config template **before** tagging image stable. Check within the built Docker image. [codebase/gotchas.md] |
| **Fernet Key Length Error** | Scheduler fails to decrypt DAG params, connections, or task state; mysterious decryption failures | Validate the Fernet key: `python -c "from cryptography.fernet import Fernet; Fernet(your_key)"` | Use a properly padded 32-byte key (e.g., `RdevFernetKeyRdevFernetKeyRdevFernetKeyRdev=`). [codebase/gotchas.md] |
| **Config Merge Not Applied** | User override values silently lost; nested dict keys overwritten instead of merged | Check `enable_merge=True` is set in `Config` constructor; verify user config file has correct keys. | Ensure `is_recursive=self.enable_merge` is passed when `_update_dicts()` is called for user overrides. [codebase/gotchas.md] |
| **Grid Config Not Found (Fabric Migration)** | DAG reads wrong config after cluster migrated from `grid1-k8s-0` to `prod-ltx1-k8s-59` | Confirm config files are still under `grid1/holdem.jsonc` (not `prod-ltx1/holdem.jsonc`). | Do NOT create `prod-*` config paths. Oklahoma's config system auto-routes grid clusters to grid configs. |

---

### 8. Auth Failures

| Failure Type | Signature | Debug Path | Fix / Mitigation | Gotcha |
|---|---|---|---|---|
| **Proxy User ACL (at upload)** | `OklahomaAirflowProxyUserACLException` during upload | Check that the uploading user is in the headless account group in Grid User Manager. | Add user to the headless account via Grid User Manager. |
| **DataVault Token Timeout** | `GridGatewayDataVaultTokenException` / `TokenClient` timeout | Check DataVault service health at `DATAVAULT_TOKEN_FABRIC`. Check SPIFFE cert paths. | Retry; ensure cert paths are correct (`CLIENT_CERTIFICATE_FILEPATH`, `CLIENT_KEY_FILEPATH`); escalate to identity team. |
| **LDAP Case Sensitivity** | User can log in but sees wrong role or duplicate user created | Check if username is uppercase in LDAP; the Airflow security manager normalizes to uppercase. | Ensure all LDAP user lookups use uppercase normalization. Migrate any existing lowercase user records. [codebase/gotchas.md] |
| **Service Account Auth Inconsistency** | Service account sees stale role after LDAP role change | Service account auth always hits LDAP (caching removed as premature optimization). | If roles appear stale, check LDAP directly; caching of service account roles was reverted (commit 2bbdbf64). [codebase/gotchas.md] |
| **SSL Verification Disabled** | MITM possible on Grid User Manager API calls during validation | `verify=False` in `dag_validations.py` line 142 — known TODO. | Known security limitation. Avoid exposing Airflow webserver to untrusted networks. [codebase/gotchas.md] |

---

### 9. CRT / Deployment Failures

| Failure Type | Signature | Debug Path | Fix / Mitigation |
|---|---|---|---|
| **Promotion Failure** | CRT deployment fails during HOLDEM -> WAR promotion | Check CRT deployment logs; check if `dagrun_timeout` and `on_failure_callback` are set (required by alerting policy). | Fix policy enforcement failures (see Alerting Enforcement section). Retry promotion after fixes. |
| **Alerting Policy Enforcement** | Build or deployment fails: missing `dagrun_timeout` or `on_failure_callback` | Check DAG definition for required alerting (required for WAR, and HOLDEM unless tagged `policy-enforcement=alerting-opt-out`). | Add `dagrun_timeout=timedelta(hours=X)` and `on_failure_callback=create_iris_incident_callback(plan="...", context_parser=oklahoma_airflow_parser)`. |
| **Airflow Version Incompatibility** | Provider API breakage after core version bump | Check provider changelogs; check for reverts in git history (pattern: `revert of revert` commits). | Test provider against new core version in staging before promoting; avoid bumping core version without full integration testing. [codebase/gotchas.md] |
| **Rollback Needed** | Bad deployment; DAGs failing in production | Use CRT to revert to previous stable tag. | Use `picli airflow crt-sync` to help manage CRT workflow changes; contact Oklahoma team via go/ask-airflow. |

---

### 10. Database Issues

| Failure Type | Signature | Debug Path | Fix / Mitigation | Gotcha |
|---|---|---|---|---|
| **Connection Pool Exhaustion** | `OperationalError: (pymysql.err.OperationalError) (2003, "Can't connect to MySQL")`; task instances stuck in queued state | Check DB connection pool settings; check active connections to MySQL. | Increase `SQLALCHEMY_POOL_SIZE` and `SQLALCHEMY_MAX_OVERFLOW`; reduce concurrent task load. |
| **Lock Contention** | `PendingRollbackError` during DAG bag sync; slow scheduler | During concurrent DAG bag sync, race condition between upload and DAG processor causes rollback. | The upload code falls back to serial sync automatically (`_serial_dag_sync`). If persistent, stagger uploads. |
| **mysqlclient TLS glibc Bug** | Process hangs or crashes with cryptic memory allocation errors when connecting to MySQL | Check for glibc TLS allocation error; check `LD_PRELOAD` setting. | Set `ENV LD_PRELOAD=/lib/libstdc++.so.6` in Dockerfile AND add `RUN echo 'export LD_PRELOAD=/lib/libstdc++.so.6' > /etc/profile.d/ld_preload.sh` for login shells. [codebase/gotchas.md] |
| **Root Cause Analyzer Overload** | Incident detection delayed; RCA query times out for high-frequency DAGs | Check DAG run count; check RCA query in incident detection service. | Implement pagination or time-window-based lookups. Known bug for DAGs with too many runs. [codebase/gotchas.md] |

---

### 11. RDev / Local Testing Issues

These issues appear exclusively in the rdev (remote dev) environment. They represent the highest-volume support category in #ask_airflow.

| Failure Type | Signature | Debug Path | Fix |
|---|---|---|---|
| **picli login timeout / cert failure** | `picli test login` hangs or `Cannot fetch user cert` | ssh-ca-cli stale on local mac | From **local mac** (not rdev): `pkill -f ssh-ca-cli && ssh-ca-cli refresh` |
| **rdev image too old** | `ImportError: /lib/libstdc++.so.6: cannot allocate memory in static TLS block` or `ImportError: libmariadb.so.3` | rdev created before March 4, 2026 | Delete and recreate rdev; then run `~/.okl_rdev/grid_setup.sh -g holdem`. APA-140407, APA-140417. **Quick fix** (if you can't recreate): `pip install --force-reinstall --no-binary mysqlclient mysqlclient==2.2.4` or `yum install -y mariadb-connector-c`, then restart. |
| **rdev image pinned** | Recurring import/cert issues despite recent creation | `devcontainer.json` pins `airflow-main-airflow-rdev:0.0.xyz` | Change to `airflow-main-airflow-rdev:stable` and rebuild |
| **rdev image references deprecated repo name** | RDev silently uses stale image; imports fail or certs mismatch | `devcontainer.json` references old `oklahoma-airflow/airflow-main-airflow-rdev` instead of `oklahoma-airflow-deployment` | Update image path in `.devcontainer/devcontainer.json` to use `oklahoma-airflow-deployment`. A build-time warning was added in deployment PR #1052 (2026-04-22). |
| **No DAGs in UI** | Empty DAG list, 0 DAGs after starting rdev | `okl-rdev-init.sh` not run or run without `-r` | `okl-rdev-init.sh -d <dag_source_dir> -r` then `~/.okl_rdev/restart_airflow.sh` |
| **grid_gateway_service_default missing** | `GridGatewayConnectionSettingNotFound` on first GGW task | Grid setup not run after rdev creation | `~/.okl_rdev/grid_setup.sh -g holdem` |
| **INVALID_ARGUMENT: Tag(pool,...) not allowed** | GGW rejects submission with tag error | `target_grid_cluster` not set in operator despite `-g faro` in picli login | Add `target_grid_cluster="faro"` to SparkBatchOperator |
| **Invalid login on rdev Airflow UI** | Login page rejects credentials | Admin user not created in rdev DB | Inside rdev: `airflow users create --role Admin --username <ldap> ... --password <ldap>` then `~/.okl_rdev/restart_airflow.sh` |
| **rdev tasks hitting prod** | Jobs unexpectedly appear in production clusters | No built-in safeguard in rdev | Use oklahoma-config-system to separate rdev vs. prod config. APA-143327. |
| **WEAVER_URLS override removed** | Fabric/Weaver calls in rdev may behave differently after PR #1063 | `WEAVER_URLS` was removed from rdev env setup because it caused Fabric/Weaver to use a stale endpoint | This is expected — the override was removed (deployment PR #1063, 2026-04-24) because it was only used internally by `lipy-fabric` and the hardcoded value caused failures (DEPEND-102101). If your DAG breaks after updating rdev, check if it relied on the old Weaver endpoint. |

---

## Log Access

### Via Airflow UI

1. Navigate to the DAG -> click the failed task instance -> click "Log" tab.
2. For GGW tasks: click "Grid Gateway Log" button (links to Observe UI via `mufn.log.url` XCom).
3. For Spark tasks: click "Spark Job Log" button (links via `spark.log.url` XCom).
4. "View Root Cause" button on the DAG run page routes to the most relevant log automatically.

### Via kubectl (airflow-test namespace)

```bash
# List scheduler pods
kubectl get pods -n airflow-test -l component=scheduler

# Scheduler logs (live)
kubectl logs -n airflow-test -l component=scheduler -f

# Worker pod logs
kubectl logs -n airflow-test <worker-pod-name> -f

# Describe a pod (check eviction events)
kubectl describe pod -n airflow-test <pod-name>

# Get events for a namespace (detect pod disruptions)
kubectl get events -n airflow-test --sort-by='.metadata.creationTimestamp' | tail -30

# Exec into a pod for ad-hoc inspection
kubectl exec -n airflow-test -it <pod-name> -- /bin/bash

# Check all pods and their status
kubectl get pods -n airflow-test -o wide

# Check resource usage
kubectl top pods -n airflow-test
```

### Via airflow CLI

```bash
# List task instances for a DAG run
airflow tasks list <dag_id>

# Get task instance state
airflow tasks state <dag_id> <task_id> <execution_date>

# View task logs
airflow tasks logs <dag_id> <task_id> <execution_date>

# Check DAG import errors
airflow dags list-import-errors

# List DAGs and their pause status
airflow dags list
```

### Key Log Locations (in pod)

| Log Type | Path |
|---|---|
| Scheduler log | `/opt/airflow/logs/scheduler/` |
| Task logs | `/opt/airflow/logs/dag_id=.../run_id=.../task_id=.../` |
| DAG processor logs | `/opt/airflow/logs/dag_processor_manager/` |
| GGW execution URN | In task log lines: `Execution {urn}` |

---

## PipelineMD

**PipelineMD** is a LinkedIn-internal diagnostic tool integrated into the Airflow UI. When a DAG run fails, the `PipelineMD` button appears on the DAG run page.

### What it does

- Automatically analyzes the failed DAG run and identifies the root cause task.
- Provides actionable diagnostic insights and links to relevant logs.
- Aggregates context from multiple data sources (Grid Gateway state, sensor status, etc.).

### How to use it

1. Navigate to the failed DAG run in the Airflow UI.
2. Look for the `PipelineMD` button next to the run status indicator.
3. Click it — you will be redirected to the PipelineMD analysis page.
4. Read the root cause summary and follow the recommended actions.

### When there is no PipelineMD button

- The DAG run may have failed for structural reasons (timeout, dependency, manual mark).
- Fall back to the "View Root Cause" button or inspect task logs directly.

### Relationship to "View Root Cause"

- "View Root Cause" links to the raw log (Observe UI for GGW, Airflow log page for sensors).
- PipelineMD provides higher-level synthesized analysis on top of those raw logs.
- Check PipelineMD first; use "View Root Cause" for raw log access.

---

## Escalation Paths

| Job / Issue Type | Where to File |
|---|---|
| Spark jobs | go/supportal -> Spark team |
| Data sensors | go/ask-airflow -> Oklahoma team |
| Flyte jobs | go/supportal -> Flyte team |
| All push jobs (Kafka, Venice, Pinot, Ambry) | go/supportal -> target system team |
| Grid Gateway issues (other) | go/supportal -> Grid Gateway team; also: https://engx.corp.linkedin.com/products/100/support |
| Oklahoma / scheduler / upload issues | go/ask-airflow |
| DAG authoring questions | go/ask-airflow |

**Grid Gateway oncall**: https://oncall.prod.linkedin.com/team/team/Grid%20Jobs%20Platform  
**GGW crew**: https://engx.corp.linkedin.com/crews/1095  
**GGW docs**: https://congenial-adventure-r4qn544.pages.github.io/docs/user/onboarding  

---

## Key Exception Classes Reference

From `airflow.providers.lnkd.exceptions`:

| Exception Class | Meaning |
|---|---|
| `GridGatewayExecutionError` | GGW execution terminal state was not SUCCEEDED |
| `GridGatewayTimeoutException` | gRPC call exceeded deadline |
| `GridGatewayConnectionException` | gRPC service unreachable |
| `GridGatewayConnectionSettingNotFound` | Airflow connection `grid_gateway_service_default` missing |
| `GridGatewayProxyUserPermissionException` | MP identity cannot impersonate proxy_user |
| `GridGatewayDataVaultTokenException` | DataVault token acquisition failed |
| `GridGatewayCertificateProvisioningError` | Identity cert provisioning error |
| `GridGatewayConfigurationException` | User configuration error (bad param) |
| `GridGatewayInitializationException` | Hook or operator init error (wraps other exceptions) |
| `GridGatewayUnexpectedGrpcException` | Catch-all for unexpected gRPC errors |
| `ArmsDataUnavailableException` | Data partition/snapshot not available (sensor) |
| `ArmsConnectionException` | ARMS gRPC unreachable (sensor) |
| `OklahomaAirflowProxyUserACLException` | Proxy user ACL missing at upload time |
| `OklahomaAirflowDAGParsingException` | DAG file failed to parse |
| `OklahomaAirflowDAGSyncingException` | DAG failed to sync to DB |
| `OklahomaAirflowDAGZipException` | DAG zip has wrong structure |
| `OklahomaAirflowInternalSQLException` | SQLAlchemy error in internal Airflow code |

---

### 12. Webserver Rate-Limit Crash (Fixed Apr 2026)

| Failure Type | Signature | Debug Path | Fix / Mitigation |
|---|---|---|---|
| **metrics_stop 500 on rate-limited request** | HTTP 500 from webserver; `AttributeError: 'g' has no attribute 'start_time'` in webserver logs | Check if the request was rate-limited by Flask-Limiter (429 status) | Fixed in deployment PR #1068 (2026-04-27). `metrics_stop` now guards against missing `g.start_time`. |

**Root cause**: Flask-Limiter raises `RateLimitExceeded` during `before_request`, which aborts the rest of the `before_request` chain. This means `metrics_start` never runs and `g.start_time` is never set. `metrics_stop` then crashes with `AttributeError` when trying to access `g.start_time`, converting the 429 rate-limit response into a 500 server error.

---

### 13. DAG Run Deadlock Detection (Added Apr 2026)

| Failure Type | Signature | Debug Path | Fix / Mitigation |
|---|---|---|---|
| **dagrun.deadlocked** | `dagrun.deadlocked` metric incremented; DAG run state transitions to `failed` | Check `dagrun.deadlocked` metric in MDM/Grafana with `dag_id` tag. Inspect task dependencies for circular waits or all-unfinished-but-none-schedulable states. | Fix the DAG's task dependency graph to eliminate deadlock conditions. |

**Root cause**: All tasks in a DAG run are unfinished but none are schedulable — a task deadlock. Previously, Airflow failed the DAG run silently with no metric emission, making deadlock-induced failures invisible in error-rate dashboards.

**New metric**: `Stats.incr("dagrun.deadlocked", tags=self.stats_tags)` — emitted when the scheduler detects the deadlock condition. Use this to alert on DAGs with structural dependency issues.

Source: airflow fork PR #120 (2026-04-23).

---

### Kusto Airflow Logs Reference

Airflow pod logs are queryable via Azure Data Explorer (Kusto). Four cluster/database combos hold Airflow logs:

| Cluster | Database | Covers |
|---------|----------|--------|
| `inlogseiplatform` | `Kubernetes` | EI/staging (Faro) Kubernetes pod logs |
| `inlogseiplatform` | `Oklahoma` | EI/staging (Faro) Airflow application logs |
| `inlogsliprod` | `Kubernetes` | Production (Holdem, War) Kubernetes pod logs |
| `inlogsliprod` | `Oklahoma` | Production (Holdem, War) Airflow application logs |
| `inlogscorpplatform` | `Kubernetes` | Corp cluster Kubernetes pod logs |

**Corp cluster**: The corp Airflow webserver pods log to the `inlogscorpplatform` cluster under the `Kubernetes` database. Added 2026-04-27 (deployment PR #1069).

Source: deployment PR #1056 (2026-04-22), deployment PR #1069 (2026-04-27).

---

## Common Patterns That Are Not Failures

- **`AirflowSkipException` with EXECUTION SKIPPED banner** — Operator has `allow_rdev_runs=False` and you are in RDev. Intentional. **Exception**: if the task shows `FAILED` (not `SKIPPED`) with `AirflowExecuteHookException`, this was a regression in `lipy-airflow-providers` v0.0.881 where pre_execute skips were incorrectly raised as exceptions. Fixed in post-rollback versions. See [Jira Patterns](jira/patterns.md) and [Gotchas](codebase/gotchas.md).
- **`PendingRollbackError` in upload logs** — Transient race between DAG processor and upload sync. Auto-recovered via serial sync fallback.
- **Sensor still running** — Expected if upstream data is late. Check `dagrun_timeout` to ensure it eventually fails rather than running forever.
- **Tasks in SKIPPED state when `dagrun_timeout` fires** — When the DAG-level timeout fires, running tasks go to SKIPPED (not FAILED). Only the DAG-level `on_failure_callback` executes, not task-level callbacks.

---

## See Also

- [Oncall](oncall/README.md) — oncall patterns, escalation paths, SLAs
- [DAG Authoring](dag-authoring.md) — operators, sensors, naming, config, ACLs
- [Deployment](deployment.md) — CRT flow, promotion, RDev testing
- [Spark](systems/spark.md) — Spark failure modes and SparkBatchOperator details
- [Gotchas](codebase/gotchas.md) — non-obvious behaviors cross-referenced throughout this page
- [Jira Patterns](jira/patterns.md) — recurring issue types and their signatures
- [Jira Playbooks](jira/playbooks.md) — step-by-step solutions for known issue types

### Database Error 1062: Duplicate Key in DAG Serialization

**Symptom:** IntegrityError with code 1062 during DAG serialization

**Root Cause:** Concurrent processors attempting to serialize and write the same DAG simultaneously

**Understanding the Error:**
- DAG serialization is wrapped in a SAVEPOINT for isolation
- When 1062 (duplicate key) occurs, it means another processor has already successfully written the serialized DAG to the database
- This is not a failure — it's evidence of concurrent success

**Resolution:**
- Treat as success rather than retry
- Return empty result (skip the DAG) since another processor already committed the correct state
- Retry would be redundant and cause unnecessary delays

**Related Code:** `airflow/models/dagbag.py` — DAG serialization and concurrent processing handling

## Error 1062 - Duplicate Key (DAG Serialization)

**Symptom**: IntegrityError with code 1062 during DAG serialization in dagbag.py.

**Cause**: Concurrent processors attempting to serialize and write the same DAG simultaneously. The first processor wins; subsequent attempts fail with a duplicate-key constraint violation.

**Resolution**: Skip retry entirely. Return gracefully. When 1062 occurs during serialization, another processor has already successfully written valid serialized DAG data. Do not retry — just treat it as success and continue. Retrying wastes resources since the outcome is already achieved.

**Code Location**: `airflow/models/dagbag.py` - serialize() method exception handler (circa line 659 in LinkedIn fork).

**Context**: This is a race condition artifact in multi-processor scheduling environments. With multiple schedulers or processors writing DAGs concurrently, the SAVEPOINT isolation strategy catches duplicate writes. Since the concurrent processor's write was successful, our process should accept the outcome.

## Serialized DAG Duplicate Key (1062) Race Condition

**Symptom:** IntegrityError 1062 (duplicate key) during DAG serialization in `dagbag.py`

**Root Cause:** Multiple processors attempt to serialize and insert the same DAG concurrently. One succeeds; the others fail with 1062 (duplicate key constraint violation).

**Resolution:** Treat 1062 as success, not a retryable error. When 1062 occurs, another concurrent processor already wrote the serialized DAG to the DB, so retry is unnecessary and wasteful.

**Implementation:** In `airflow/models/dagbag.py` error handling, detect 1062 and return early (treat as normal completion) rather than rolling back and retrying. Pattern: `if isinstance(e, IntegrityError) and e.orig is not None and e.orig.args[0] == 1062: return []`

## Error 1062: Duplicate Key During DAG Serialization

**Symptom:** IntegrityError with code 1062 during DAG serialization in `airflow.models.dagbag`.

**Root Cause:** Race condition when multiple Airflow processors attempt to serialize and insert the same DAG into the database concurrently. The first processor wins; subsequent processors hit duplicate-key constraint violations.

**Resolution:** This is not a failure — it indicates successful concurrent DAG serialization. The Airflow fork skips retry on 1062 errors because another processor already wrote the DAG to the database. The error is caught, logged as debug, and treated as success (returns empty list).

**Context:** This is expected behavior in multi-processor Airflow deployments (Holdem, War, Faro) and does not require manual intervention.

## DAG Serialization Race Conditions

When multiple Airflow processors attempt to serialize and store the same DAG concurrently, race conditions can occur. The first processor to write succeeds; subsequent processors may encounter database constraints.

**IntegrityError 1062 (duplicate key) in DAG serialization:**
- Indicates another processor has already successfully serialized and stored the DAG
- Do not retry — the work is already done by the competing processor
- Correct handling: log, rollback the savepoint, and return success (no error)
- This is expected behavior in multi-processor deployments

**Location:** `airflow/models/dagbag.py`, serialized DAG insert logic (near line 659)

**Why skip retry:** Retrying will continue to fail with 1062. The concurrent processor has already completed the serialization; accepting its work avoids wasted retry cycles and reduces contention on the database.

### Finding RDev Owner

To find the username/email of the user who created or owns a specific rdev environment:

```bash
rdev debug find-owner <HOST>
```

Example:
```bash
rdev debug find-owner voyager-web-98jx2-ppp6b.corp.rdev.svc.cluster.local
```

Host format: `<name>-<hash>-<pod>.corp.rdev.svc.cluster.local`

## DAG-Processor File Stats Race Condition

**Status**: Known bug affecting prod-ltx1-k8s-59 holdem cluster (2026-04-09+)

**Symptom**: Pod crashes with `KeyError` in `manager.py:1026` during `set_file_paths()` call:
```
File "/opt/airflow/airflow2.9/lib/python3.10/site-packages/airflow/dag_processing/manager.py", line 1026, in set_file_paths
  self._file_stats.pop(file_path)
KeyError: '/opt/airflow/dags/<path>/<dag>.py'
```

**Affected paths** (examples):
- `/opt/airflow/dags/darwin_airflow_dags__darwin_airflow_dags/darwin_15471358__darwin_airflow_dags.py`
- `/opt/airflow/dags/kyoto_cdc_spark/incr*`

**Pattern**: Multiple pods in same deployment hit the same crash (not isolated). Exit code 1 (not OOMKill).

**Root cause hypothesis**: Race condition in dag-processor's file stats tracking. When a newly-appearing NFS file triggers `set_file_paths()`, the code attempts to `pop()` a file_path that was never added to `self._file_stats`, or was removed by a concurrent operation.

**Workaround**: Pod restarts recover. Currently stable after ~11h, but will crash again on next NFS file appearance event.

**Investigation needed**: 
- Concurrent write/deletion race in `_file_stats` dict updates
- NFS file sync delays causing stale file list handling

### dag-processor Manager.py KeyError (race condition)

**Symptom**: Pod crashes with exit code 1, logs show `KeyError: '/opt/airflow/dags/...'` in `manager.py:1026` during `set_file_paths()` call.

**Root Cause**: Race condition in dag-processor's file stat tracking. When NFS-mounted DAG files appear/disappear between stat collection and cleanup, `self._file_stats.pop(file_path)` fails because the file entry doesn't exist in the snapshot.

**Affected**: Multiple dag-processor pods across holdem cluster (shared NFS issue, not pod-specific).

**Diagnosis**:
```bash
# Check previous logs for KeyError pattern
kubectl -n airflow logs <pod-name> --previous --tail=100 | grep -E "(KeyError|set_file_paths|manager\.py:1026)"

# Check if multiple pods affected
kubectl -n airflow logs <other-pod> --previous --tail=30 | grep -E "(KeyError|set_file_paths)"
```

**Example Paths**:
- `/opt/airflow/dags/darwin_airflow_dags__darwin_airflow_dags/darwin_15471358__darwin_airflow_dags.py`
- `/opt/airflow/dags/kyoto_cdc_spark/incr...`

**Recovery**: Pod restarts temporarily stabilize but will crash again when new files trigger the race. Requires airflow/dag-processor code fix or NFS sync adjustment.

## Database Query Performance Analysis

### Slow Query Patterns (5s+ threshold on PROD/CORP)

Regular slow-query audits reveal performance issues in production (`airflow_war`) and staging (`airflow`) systems:

**Common Bottlenecks:**
- **Serialized DAG lookups**: `SELECT ? FROM serialized_dag WHERE dag_id = ?` — frequent idle transactions
- **Scheduler writes**: `UPDATE dag_run SET last_scheduling_decision=?, updated_at=?` — blocking writes (max 8s duration, RUNNING state)
- **Task instance aggregations**: Complex COUNT(*) queries with joins on `task_instance` table
- **XCom/rendered field inserts**: Bulk `INSERT...ON DUPLICATE KEY UPDATE` into `rendered_task_instance_fields` (stores K8s pod specs in `k8s_pod_yaml` column)

### Debugging Slow Writes

To analyze XCom row size distribution without triggering new slow queries:

```sql
SELECT
  CASE
    WHEN LENGTH(value) < 1024 THEN '< 1 KB'
    WHEN LENGTH(value) < 102400 THEN '1-100 KB'
    WHEN LENGTH(value) < 1048576 THEN '100 KB - 1 MB'
    ELSE '> 1 MB'
  END AS size_bucket,
  COUNT(*) AS cnt,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM xcom_value), 1) AS pct
FROM xcom_value
GROUP BY size_bucket
ORDER BY size_bucket;
```

**Note**: Avoid full-table scans and expensive aggregations on audit tables — these themselves become slow queries. Use sampling or pre-aggregated metrics instead.

## Slow Query Patterns: RTIF and XCom Analysis

When diagnosing Airflow database performance, watch for slow queries on `rendered_task_instance_fields` and `xcom` tables. Full table scans with histogram analysis become prohibitively expensive on large tables. Specific patterns to monitor:

- `rendered_task_instance_fields` stores K8s pod specs in the `k8s_pod_yaml` column
- `rendered_fields` and `k8s_pod_yaml` columns in RTIF are common INSERT/UPDATE bottlenecks during task execution
- XCom value histogram queries should use sampling or time-window filters to avoid timeout

For row-size analysis on these tables, use indexed lookups or recent data partitions instead of full scans.

## Database Slow Query Diagnostics

Slow-query audits from PROD (`airflow_war`) and CORP (`airflow`) databases (Apr 2026) revealed write-intensive bottlenecks exceeding 5-second threshold:

- **dag_run table**: `UPDATE dag_run SET last_scheduling_decision=?, updated_at=?` — up to 8s, caused by scheduler decision writes (CORP, 2 occurrences)
- **rendered_task_instance_fields table**: `INSERT...ON DUPLICATE KEY UPDATE` for K8s pod specs and rendered fields — common on PROD during task instance updates
- **serialized_dag table**: Repeated SELECT queries on dag_id lookups in the scheduler critical path

Root causes: High scheduler concurrency, missing or inefficient indexes on `dag_id` + `update_at` composite, or contention during bulk task instance updates. Check slow_log via `kubectl exec` on scheduler pods or query `information_schema.tables` for table size analysis.
