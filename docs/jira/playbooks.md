> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# Jira — Playbooks

> Step-by-step resolution guides for the most common Airflow issue types at LinkedIn

## How to Use This Page

Each playbook corresponds to a pattern in [Patterns](patterns.md). Find the matching pattern, follow the steps in order. Steps are ordered from "cheapest to check first" to "most invasive last."

---

## PB1 — Task Stuck in Running State {#task-stuck-running}

**When to use**: Task has been in `running` state far longer than expected. No recent log output. Worker may have died.

**Steps**:

1. **Check task logs** in Airflow UI -> DAG -> Task Instance -> Log. Look for the last timestamp. If logs stopped updating > 5 min ago, the worker likely died.

2. **Check if a GGW job is still running** — If the task uses any GGW operator (SparkBatch, HadoopJava, etc.), the job may still be executing on Grid Gateway independently. Go to the log URL in XCom or Grid Gateway UI.
   - If GGW job is running: wait for it to finish. Airflow will resume polling if checkpointing is enabled (`enable_job_checkpoint=True`).
   - If GGW job is not found: the checkpoint may have failed; proceed to step 3.

3. **Check the worker pod** (if you have cluster access):
   ```bash
   kubectl get pods -n airflow -l component=worker
   kubectl logs <pod-name> -n airflow --tail=100
   ```
   If the pod is in `CrashLoopBackOff` or `Evicted`, the task died mid-execution.

4. **Mark the task failed manually** via Airflow UI -> Task Instance -> Actions -> "Mark Failed". This unblocks the DAG run and allows retries or downstream tasks to proceed.
   - Use "Mark Failed" not "Mark Success" unless you are certain the underlying work completed.

5. **Retry the task** via Airflow UI -> Task Instance -> Actions -> "Clear" (which re-queues the task). If the task uses GGW with `enable_job_checkpoint=True`, clearing will re-poll the existing GGW job if still running, not re-submit.

6. **If task keeps getting stuck**: check if the worker is running out of memory, check DAG `execution_timeout`, and check if the task holds a worker slot in sensor mode (see [Sensor Timeout](#sensor-timeout)).

**Notes**:
- `execution_timeout` on a task does NOT kill GGW jobs — it only marks Airflow's task failed. The GGW job continues independently.
- Zombie tasks (stuck `running` after pod death) are automatically detected by the Airflow scheduler's "zombie detector" but it may take several minutes.

---

## PB2 — DAG Not Being Scheduled {#dag-not-scheduled}

**When to use**: A DAG has a schedule but is not creating DAG runs at expected times, or existing runs are not progressing.

**Steps**:

1. **Check if the DAG is paused**:
   - Airflow UI -> DAGs list -> look for the toggle on the left of the DAG row. A gray toggle means paused.
   - Unpause via UI or API: `PATCH /api/v1/dags/{dag_id}` with `{"is_paused": false}`
   - New DAGs default to paused on first deployment. Must be manually unpaused.

2. **Check for DAG import errors**:
   - Airflow UI -> Browse -> DAG Import Errors (or look for red indicators in DAG list).
   - If present, the scheduler cannot parse the DAG file — go to [DAG Import Error](#dag-import-error).

3. **Check `start_date` and `catchup` settings**:
   - If `catchup=False` and `start_date` is in the past, only future runs will be created.
   - If `catchup=True` and `start_date` is far in the past, many backfill runs may be queued — check DAG run list.
   - `schedule_interval=None` means manual trigger only — not a bug.

4. **Check DagBag parse time**:
   - In scheduler logs: look for `dag_processing.last_duration` metrics.
   - If DagBag parse takes too long (> 30s), the scheduler may skip runs. Reduce complex top-level imports.

5. **Check scheduler health**:
   - Airflow UI -> Admin -> Configurations -> look for `scheduler.heartbeat_timeout`.
   - If scheduler is unhealthy (heartbeat stale), all scheduling stops. Restart scheduler pod if needed:
     ```bash
     kubectl rollout restart deployment/airflow-scheduler -n airflow
     ```

6. **Check `max_active_runs`** — if `max_active_runs` (default 16) is reached, no new runs are created until existing ones complete/fail.

7. **Check `depends_on_past=True`** — if set, each run waits for the previous run's same task to succeed. A single old failure blocks everything.

**Notes**:
- The Tradewind UI may show stale DAG state — always verify against the actual cluster UI directly.
- Scheduler critical section contention can delay scheduling without stopping it; see [Critical Section](#critical-section).

---

## PB3 — Proxy User ACL Failure {#proxy-user-acl}

**When to use**: Task fails with `GridGatewayProxyUserPermissionException` or DAG upload fails with proxy user validation error.

**Steps**:

1. **Identify the identity and proxy user**:
   - Error message will contain the full URN: `urn:li:servicePrincipal:(oklahoma-mp-mymp;...)`.
   - Proxy user name is in the operator: `proxy_user="wherehow"` or in the error message.

2. **Check current ACL state** using `picli`:
   ```bash
   picli airflow proxy --list --proxy-users 'wherehow' --impersonating-identities 'oklahoma-mp-mymp'
   ```
   (Check `picli airflow proxy --help` for exact flags in current version.)

3. **Grant the ACL** if missing:
   ```bash
   picli airflow proxy \
     --proxy-users 'wherehow' \
     --impersonating-identities 'oklahoma-mp-mymp' \
     --fabric-groups 'corp,prod'
   ```
   - Use `oklahoma-mp-<MP_NAME>` for MP identity (all DAGs in the MP share the ACL).
   - Use `oklahoma-dag-<DAG_ID>` for DAG-level identity (more restrictive, per-DAG).
   - **You must be an admin of the proxy user** in Grid for this command to succeed.
   - `--fabric-groups 'corp,prod'` covers Faro (corp) + Holdem/War (prod).

4. **Check DataVault** — if the proxy user is a service account backed by DataVault secrets, verify the secret exists and is accessible:
   ```bash
   kms secret get -f prod-ltx1 <secret-name>
   ```

5. **EI environment workaround** — ACLs may not propagate to EI (Engineering Integration) clusters immediately. If the task fails only on EI:
   - Add `-f ei-ltx1` to the fabric group in the picli command.
   - Or test on a prod cluster (holdem) where ACLs are more reliably synced.

6. **Re-run the task** after ACL change. ACL propagation typically takes 1-5 minutes.

**Notes**:
- Upload Plugin validates proxy ACLs at DAG upload time. Fix the ACL first, then re-upload.
- For new DAGs: always run `picli airflow proxy` before uploading to avoid failed validations.
- MP identity is preferred for most use cases (one ACL rule covers all DAGs in the MP). Use DAG identity only when ACL isolation between DAGs is needed.

---

## PB4 — GGW Job Failed: ENVIRONMENT_* Error {#ggw-environment-error}

**When to use**: Task failed with a Grid Gateway `ENVIRONMENT_*` error code (e.g., `ENVIRONMENT_CLUSTER_UNAVAILABLE`, `ENVIRONMENT_PREEMPTION`, `ENVIRONMENT_NODE_FAILURE`). These indicate infrastructure disruption, not application failure.

**Steps**:

1. **Distinguish disruption from application failure**:
   - `ENVIRONMENT_*` errors = infrastructure (pod eviction, node failure, preemption). Safe to retry.
   - Other GGW errors = application-level failure (OOM, bad input, permission denied). Fix before retrying.
   - Check Grid Gateway logs for the execution URN to confirm.

2. **Check if GGW job checkpointing was active**:
   - If `enable_job_checkpoint=True` (default): Airflow saved the execution URN before the pod died. Clearing the task will resume polling the existing GGW job.
   - If `enable_job_checkpoint=False`: the GGW job may still be running orphaned. Check Grid Gateway UI for orphaned executions before re-triggering.

3. **Resume vs. re-trigger**:
   - **Resume (preferred)**: Clear the task in Airflow UI -> the task re-polls the original GGW job. No duplicate work.
   - **Re-trigger**: If the GGW job completed or was cancelled, clearing the task starts a new execution. This is safe for idempotent jobs.

4. **Enable disruption readiness for future protection** (for supported job types):
   ```python
   HadoopJavaOperator(
       ...,
       disruption_ready=True,  # Auto-retry on ENVIRONMENT_.* errors, up to 3 times
   )
   ```
   **Note**: As of April 2026, `disruption_ready=True` is NOT yet functional for `SparkBatchOperator`. Only works for: `hadoopJava`, `java`, `javaprocess`, `command`, `hadoopShell`.

5. **If disruptions are frequent**: file a ticket with the Grid Gateway team (oncall link in GGW error banner: `https://oncall.prod.linkedin.com/team/team/Grid%20Jobs%20Platform`) to investigate YARN cluster stability.

6. **Check for pattern**: if the same DAG/task fails repeatedly with ENVIRONMENT_* errors at the same time each day, it may correlate with a cluster maintenance window or batch job surge. Adjust task scheduling or increase `retries`.

**Notes**:
- GGW error banner in task logs includes direct links to GGW support ticket, oncall, and docs.
- ENVIRONMENT_* errors are not counted against the application's error rate in Grid Gateway's SLO metrics.
- External job checkpointing is an `ExternalJobCheckpointMixin` in the base GGW operator — check if the operator inherits it.

---

## PB5 — Sensor Timeout {#sensor-timeout}

**When to use**: A sensor task (DatasetSensorArray, PythonSensor, AzkabanSensor, etc.) failed with `AirflowSensorTimeout`, or many sensors are consuming worker slots causing other tasks to queue.

**Steps**:

1. **Distinguish timeout vs. slot starvation**:
   - **Timeout**: `AirflowSensorTimeout` in task logs; sensor ran out of time waiting for condition.
   - **Slot starvation**: tasks stuck in `queued` state, scheduler log shows worker slots exhausted.

2. **For sensor timeout — investigate upstream**:
   - Determine what the sensor is waiting for (partition, snapshot, external service).
   - Check if the upstream pipeline that writes the partition/snapshot actually ran and completed.
   - For `PartitionSensorDefinition`: verify the partition name format (`{{ ds }}-00` vs. `{{ ds }}`).
   - For `SnapshotSensorDefinition`: verify `baseline_datetime` template is correct and the watermark field name matches what the upstream writes.

3. **Increase timeout if data genuinely arrives late**:
   ```python
   DatasetSensorArray(
       task_id='check_data',
       sensors=[...],
       poke_interval=300,   # 5 min between checks (reduce polling load)
       timeout=60 * 60 * 6, # 6 hours max wait
   )
   ```

4. **Switch from `poke` to `reschedule` mode** to fix slot starvation:
   ```python
   PythonSensor(
       task_id='my_sensor',
       python_callable=check_condition,
       poke_interval=60,
       mode='reschedule',  # releases worker slot between pokes
   )
   ```
   - `mode='poke'`: holds worker slot entire time. Use only for very short expected waits (< 5 min).
   - `mode='reschedule'`: releases slot between checks. Use for all long-running sensors.
   - `DatasetSensorArray` uses its own polling loop internally; mode doesn't apply the same way.

5. **For `AzkabanSensor` stuck**: check Azkaban cluster connectivity and whether the Azkaban flow completed. AzkabanSensor uses poke-based polling with connection failure tracking — check if Azkaban host is reachable.

6. **Workaround for immediate unblock**: if data is confirmed available and sensor is just stuck, mark the sensor task as "Success" manually via Airflow UI -> Task Instance -> Mark Success.

**Notes**:
- `DatasetSensorArray` is preferred over individual sensors for multiple datasets — it saves worker slots by checking all conditions in one task.
- Default `timeout` for sensors in Airflow 2.x is 7 days if not set explicitly. Always set a sensible timeout.
- `poke_interval` should not be lower than 30 seconds to avoid overwhelming ARMS/Dali with polling traffic.

---

## PB6 — DAG Import Error {#dag-import-error}

**When to use**: DAG appears as "Import Error" in Airflow UI, or scheduler logs show the DAG file cannot be parsed.

**Steps**:

1. **Get the exact error** from Airflow UI -> Browse -> DAG Import Errors, or from scheduler logs:
   ```bash
   kubectl logs <scheduler-pod> -n airflow | grep "Broken DAG"
   ```

2. **Reproduce locally**:
   ```bash
   python -c "from airflow.models import DagBag; db = DagBag('/path/to/dag.py'); print(db.import_errors)"
   ```
   Or validate using the lint command:
   ```bash
   python -m linkedin.oklahoma.workflows.validate_dags
   ```

3. **For `ModuleNotFoundError` (missing dependency)**:
   - Check if the library MP is deployed to the cluster. Library MP must be deployed BEFORE the consumer DAG MP.
   - Verify the dependency is declared in both `product-spec.json` and `.okl_setup.json` of the consumer DAG app.
   - Check that the import path matches the library's actual Python module path.

4. **For circular import** (`ImportError: circular import` or `cannot import name X`):
   - Look for top-level `from airflow.*` imports in `airflow_local_settings.py` or policy framework modules.
   - Fix: move the import inside the function that uses it (lazy import):
     ```python
     # BAD: top-level import causes circular dependency
     from airflow.providers.lnkd.gridgateway.operators.spark_batch import SparkBatchOperator
     
     # GOOD: lazy import inside function
     def create_task():
         from airflow.providers.lnkd.gridgateway.operators.spark_batch import SparkBatchOperator
         return SparkBatchOperator(...)
     ```

5. **For DAG naming convention violations**:
   - DAG ID must match: `<DAG_NAME>__<MP_NAME>` (double underscore separator).
   - `<DAG_NAME>` and `<MP_NAME>` cannot contain double underscores.
   - MP name in DAG ID must match the actual MP name exactly (no hyphen/underscore substitution).
   - Validate: `mint validate-dags` (requires `lipy-oklahoma-airflow >= 0.0.44`).

6. **For version incompatibility** (e.g., Pydantic version conflict):
   - Check `product-spec.json` for dependency version pins.
   - Known fix: `"pydantic": "pypi:pydantic:1.10.7"` if Pydantic v2 breaks providers.
   - Check recent git history for reverts — version rollbacks are common after Airflow core upgrades.

7. **Re-upload or re-deploy the DAG** after fixing and verify the import error is cleared.

**Notes**:
- Import errors surface in Airflow UI within ~30 seconds of the scheduler's DagBag refresh cycle.
- `access_control` is mandatory — its absence won't cause an import error but will prevent users from seeing the DAG.
- Files in the `libs` folder of a DAG app are ignored by the scheduler and will NOT cause import errors even if they have syntax issues.

---

## PB7 — Spark OOM {#spark-oom}

**When to use**: Spark task fails with `java.lang.OutOfMemoryError` or Grid Gateway reports executor/driver memory issue.

**Steps**:

1. **Get Spark application logs** via the log URL in XCom:
   - Airflow UI -> Task Instance -> XCom -> find `spark.log.url` or `mufn.log.url`.
   - Look for the specific OOM stack trace (driver vs. executor).

2. **Identify OOM location**:
   - **Driver OOM**: `java.lang.OutOfMemoryError` in driver logs. Increase `driver_memory`.
   - **Executor OOM**: OOM in executor logs or `FetchFailedException`. Increase `executor_memory`.
   - **GC overhead**: `GC overhead limit exceeded` = heap too small for GC cycles. Increase memory.
   - **OOM Killer**: `Container killed by YARN for exceeding memory limits` = executor exceeded container limit. Increase `executor_memory`.

3. **Increase memory settings**:
   ```python
   SparkBatchOperator(
       ...,
       driver_memory="4G",     # was 2G — increase for driver OOM
       executor_memory="8G",   # was 4G — increase for executor OOM
       executor_cores=4,       # more cores = more memory needed per executor
   )
   ```
   Start with 2x current setting and profile.

4. **Enable rightsizing** (LinkedIn-specific) to let the platform auto-tune:
   ```python
   spark_confs={"spark.rightsizing.enabled": "true"}
   ```
   This may override your memory settings based on workload history. Check XCom output for what values were actually used.

5. **Reduce data skew** — if one executor is OOM but others are fine, the job has data skew. Increase `spark.sql.shuffle.partitions` or add a salting strategy.

6. **For queue capacity issues** (not OOM):
   - Reduce `executor_num` to require fewer YARN containers.
   - Switch to a less-congested queue (contact cluster admin for options).
   - Schedule the job during off-peak hours.

7. **Verify guava/classpath conflicts** — if OOM happens at startup (deserialization):
   - Check for `spark.driver.extraClassPath` / `spark.executor.extraClassPath` conflicts.
   - Guava version must be pinned to 25.0-jre (Feature Cloud Push gotcha: extraClassPath must append, not overwrite).

8. **For straggler mitigation / high runtime variance** (e.g., job varies between 11-92 min):
   - Enable Spark speculation: `spark_confs={"spark.speculation": "true", "spark.speculation.multiplier": "1.5"}`
   - Enable AQE skew join: `spark_confs={"spark.sql.adaptive.skewJoin.enabled": "true"}`
   - Disable rightsizer overrides if they deflate executor/driver memory too aggressively — set explicit memory/cores to prevent auto-tuning below safe thresholds
   - Use cluster affinity to avoid consistently slow fabrics (e.g., Titan): check with Grid/Spark team for `spark.yarn.executor.nodeLabelExpression` options
   - Source: APA-144108 (Spark straggler mitigation guidance)

**Notes**:
- `executor_memory` in `SparkBatchOperator` maps to `--executor-memory` in spark-submit. YARN adds ~10% overhead on top of this.
- `spark.rightsizing.enabled=true` is a LinkedIn internal flag; it may adjust `executor_memory` and `executor_cores` at runtime without operator param changes.
- For very large jobs, consider `executor_cores=2` with more executors instead of fewer large executors — better parallelism and less per-executor memory pressure.

---

## PB8 — Fernet Key 32-Byte Error {#fernet-key}

**When to use**: RDev Airflow fails to decrypt connections, DAG params, or task state. Error mentions `InvalidToken` or `Fernet key must be 32 url-safe base64-encoded bytes`.

**Steps**:

1. **Validate the current key**:
   ```bash
   python -c "
   import os, base64
   key = os.environ.get('AIRFLOW__CORE__FERNET_KEY', '')
   decoded = base64.urlsafe_b64decode(key)
   print(f'Key bytes: {len(decoded)} (need: 32)')
   "
   ```
   Or validate with the Fernet library:
   ```bash
   python -c "from cryptography.fernet import Fernet; Fernet(b'YOUR_KEY_HERE'); print('Key is valid')"
   ```

2. **Generate a valid 32-byte Fernet key**:
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

3. **Use the known-good RDev key** (from commit `a889dd1`):
   ```
   AIRFLOW__CORE__FERNET_KEY=RdevFernetKeyRdevFernetKeyRdevFernetKeyRdev=
   ```
   This key is properly padded to 32 bytes when base64-decoded.

4. **Update `setup_oklahoma_rdev_env.sh`** (or equivalent env setup) to use the valid key. Do NOT use short keys like `RDevFernetKey` — they appear to work until a decryption is attempted.

5. **Re-initialize RDev** after fixing the key. Previously-encrypted values (connections, variables) need to be re-encrypted with the new key.

**Notes**:
- Fernet key issues only affect RDev. Production clusters use securely-generated keys managed by the Oklahoma platform team.
- The error is deferred — Airflow starts without error but fails when decryption is first attempted (e.g., when a task reads a connection).
- URL-safe base64 characters only: `A-Z a-z 0-9 - _` plus `=` padding.

---

## PB9 — Critical Section Duration Too High {#critical-section}

**When to use**: Scheduler `critical_section_duration` metric exceeds threshold (>10-15s sustained), tasks are not being scheduled despite being ready, or load testing shows scheduling delays.

**Steps**:

1. **Confirm the symptom**:
   - Metrics dashboard: look for `critical_section_duration` time series.
   - Airflow UI: look for tasks in `scheduled` state that aren't transitioning to `queued`.
   - Scheduler logs: look for `Acquired lock on critical section...` taking many seconds.

2. **Identify the root cause** — common causes in order of likelihood:
   - **Too many DAGs**: Each scheduler loop processes all parseable DAGs. 10,000+ DAGs significantly slows the loop.
   - **DagBag parse time**: Complex imports or large DAG files increase per-cycle work.
   - **Database lock contention**: Many concurrent writes (task state updates, DAG runs) competing with scheduler.
   - **Too-low `ti_per_loop`**: Scheduler processes fewer tasks per loop iteration than are queued.

3. **Tune `ti_per_loop`** (tasks per loop — how many task instances scheduler processes per cycle):
   ```ini
   [scheduler]
   max_tis_per_query = 512  # default is often 512; increase for high-throughput clusters
   ```
   Based on LinkedIn load testing, tuning `ti_per_loop` in combination with scheduler replica count gives the best results.

4. **Increase scheduler replicas** (Airflow 2.x supports multiple schedulers):
   ```bash
   kubectl scale deployment/airflow-scheduler --replicas=3 -n airflow
   ```
   Multiple schedulers reduce critical section contention by distributing work.

5. **Reduce DAG count on the cluster**:
   - Identify and delete stale/unused DAGs.
   - Move low-priority DAGs to a separate cluster.
   - Ensure DAG files parse quickly: avoid heavy top-level imports; move business logic into operators.

6. **Identify DAGs with excessive runs**: the root cause analyzer has a known scaling issue with DAGs that have very many runs (commit `b90997e8`). These create expensive metadata queries. Purge old DAG runs.

7. **For immediate relief**: temporarily increase `parallelism` and `max_active_tasks_per_dag` to let more tasks flow through the scheduler in fewer iterations.

**Notes**:
- LinkedIn's load testing (airflow-load-testing repo) tests up to 25,000+ DAGs. Results show `critical_section_duration` is the primary bottleneck above 10,000 DAGs.
- The 3D parameter matrix tested: `scheduler_count x ti_per_loop x total_dags`. Optimal configuration depends on your specific workload mix.
- DagBag parsing runs in a separate process from scheduling — slow parsing doesn't directly cause critical section issues but increases overall scheduler loop time.

---

## PB10 — DAG Not Showing in Tradewind UI {#tradewind-missing}

**When to use**: A deployed DAG is visible on the Airflow cluster directly but not in the Tradewind federated UI.

**Steps**:

1. **Confirm the DAG is on the cluster**:
   - Access the cluster directly (e.g., `https://holdem.oklahoma-airflow.grid.linkedin.com/dags`).
   - If the DAG is not on the cluster either, the problem is deployment, not Tradewind. Go to [DAG Not Scheduled](#dag-not-scheduled).

2. **Check DAG naming convention**:
   - DAG ID must be `<DAG_NAME>__<MP_NAME>` with exactly one `__` separator.
   - Tradewind's DAG syncer extracts the MP name from the DAG ID using the `__` convention.
   - If naming doesn't match, the DAG may fail to register in Tradewind's router DB.

3. **Check Tradewind router database registration**:
   - Tradewind exposes routing APIs at `/api/v1/routing/*`.
   - Check if the DAG is registered: `GET /api/v1/routing/dag/{dag_id}` — if 404, it's not registered.
   - Registration typically happens automatically via the DAG syncer on deployment.

4. **Force re-registration** (if available):
   - Re-trigger the CRT deployment for the DAG's MP. This re-fires the DAG sync event.
   - Or contact the Airflow oncall to manually trigger the Tradewind DAG sync for the affected DAG.

5. **Check shard placement**:
   - Tradewind routes `DAG_ID -> logical cluster (holdem) -> physical shard (holdem-1, holdem-2)`.
   - If the shard is wrong, the DAG may show but API calls route to the wrong cluster.
   - Shard placement is determined at registration time from the physical cluster the DAG is on.

6. **Check access_control** — Tradewind's UI respects Airflow's `access_control` DAG param. If the user is not in the specified SGP group, the DAG will not appear in the UI even if it's registered.
   ```python
   access_control={"SGP-CREW-mygroup": {"can_read", "can_edit"}}
   ```
   Also: log out and log back in (or use incognito mode) to refresh permissions cache.

7. **For corp/Faro DAGs**: Faro is a separate cluster from Holdem/War. Tradewind may not federate Faro DAGs in the same view. Check which logical cluster the DAG is registered under.

**Notes**:
- Tradewind's router DB is MySQL with a 3307 port in dev / production DB URL in prod. Schema managed via Alembic migrations.
- The DAG sync may take a few minutes after CRT deployment completes.
- OTel metrics on Tradewind include a `shard_placement_fallback_counter` — if this is elevated, shard placement is failing and DAGs may be misrouted.

---

## PB11 — LDAP Case Sensitivity {#ldap-case}

**When to use**: A user can authenticate to Airflow but cannot see their DAGs, or gets permission errors despite being in the right SGP group.

**Steps**:

1. **Check user record in Airflow**: Admin -> Users -> search for the user. Note the exact username stored.

2. **Compare with LDAP username**: LDAP usernames at LinkedIn should be uppercase (normalized in commit `b777e3ec`). If the user record is stored as `johndoe` but LDAP returns `JOHNDOE`, lookup will fail.

3. **Fix the user record**: Admin -> Users -> edit user -> update username to uppercase. Or delete the stale record so it is re-created on next login.

4. **Check `access_control`** in DAG definition: the group names in `access_control` must match LDAP/SGP group names exactly (case-sensitive).

5. **Verify group membership**: the user must be a **member** of the SGP group, not just an admin. Admins of a crew are in `SGP-CREW-1090-ADMINS`, not `SGP-CREW-1090-MEMBERS`.

**Notes**:
- This issue mainly affects users whose accounts were created before the uppercase normalization fix, or edge cases with SSO login aliases.
- If in doubt: log out, clear cookies, log back in. The security manager will re-create the user record from LDAP on next login.

---

## PB12 — SSL Verification Disabled Warning {#ssl-verify}

**When to use**: Task logs show `InsecureRequestWarning` from DAG validation or upload plugin.

**Steps**:

This is a platform-level issue in `dag_validations.py`. End users cannot fix it themselves.

1. **Suppress in your logs** (not recommended — masks real SSL issues).

2. **File a ticket** with the Oklahoma Airflow team citing the TODO in `dag_validations.py` line ~142. The fix is to replace `verify=False` with `verify=TRUSTSTORE_FILEPATH`.

3. **Do not disable SSL verification in your own DAG code**: if you need to call an HTTPS service, use `verify=TRUSTSTORE_FILEPATH` from `linkedin.oklahoma.helpers`.

**Notes**:
- The `TRUSTSTORE_FILEPATH` constant exists in the codebase but is not yet used in `dag_validations.py`.
- This does not affect task execution — only the upload-time validation request to Grid User Manager.

---

## PB13 — Config Merge Not Applying User Overrides (RDev) {#config-merge}

**When to use**: In RDev, you set nested config overrides but they are silently ignored or top-level keys overwrite your nested values.

**Steps**:

1. **Reproduce**: Set a nested config key with `enable_merge=True` in your RDev config. Check if nested keys are preserved after merge.

2. **Verify your oklahoma-helpers version**: this was fixed in commit `5577898d`. If you're on an older version, update:
   ```bash
   pip install --upgrade linkedin-oklahoma-helpers
   ```

3. **Workaround (if upgrade not possible)**: Flatten your config overrides to avoid relying on recursive merge. Set each leaf key explicitly instead of setting the parent dict.

4. **Report the version** — if you're on a version that claims to have the fix but still shows the bug, file a bug with the Oklahoma helpers team.

**Notes**:
- This only affects RDev environment config. Production clusters use a different config loading path.
- The fix was: pass `is_recursive=self.enable_merge` to `_update_dicts()` in the user override code path.

---

## PB14 — RDev Login / Cert Failure {#rdev-cert}

**When to use**: `picli test login` times out, "Cannot fetch user cert", Airflow rdev UI shows "Invalid login", or rdev setup hangs.

**Steps**:

1. **Fix ssh-ca-cli from your local mac** (not inside the rdev container):
   ```bash
   pkill -f ssh-ca-cli
   ssh-ca-cli refresh
   ```
   Then retry `picli test login`. This fixes ~80% of cert/timeout failures.

2. **If the rdev was created before March 4, 2026** — delete and recreate it:
   ```bash
   picli rdev delete
   picli rdev create
   ```
   Old rdev images had a base image incompatibility (libstdc++ TLS, GGW connectivity). See APA-140407.

3. **If devcontainer.json pins a specific image version** — change the image tag:
   ```json
   // BEFORE (pinned)
   "image": "linkedin/airflow-main-airflow-rdev:0.0.867"
   // AFTER (floating)
   "image": "linkedin/airflow-main-airflow-rdev:stable"
   ```
   Rebuild the devcontainer after changing this.

4. **If Airflow UI shows "Invalid login"** — create the admin user inside the rdev:
   ```bash
   # Inside rdev
   airflow users create \
     --role Admin \
     --username <your_ldap_username> \
     --firstname First \
     --lastname Last \
     --email <your_email>@linkedin.com \
     --password <your_ldap_username>
   ~/.okl_rdev/restart_airflow.sh
   ```

5. **If `grid_gateway_service_default` connection is missing** (GGW tasks fail on first run):
   ```bash
   # Inside rdev
   ~/.okl_rdev/grid_setup.sh -g holdem
   # For faro/yugioh testing:
   ~/.okl_rdev/grid_setup.sh -g faro
   ~/.okl_rdev/grid_setup.sh -g yugioh
   ```

6. **If tasks run in rdev but land on prod clusters** — this is a configuration issue, not a safeguard. Use the oklahoma-config-system to separate rdev vs. prod config. Track: APA-143327.

7. **If INVALID_ARGUMENT: Tag(pool,...) is not allowed** — you must also specify `target_grid_cluster` in the SparkBatchOperator, not just pass `-g faro` to `picli test login`:
   ```python
   SparkBatchOperator(
       ...,
       target_grid_cluster="faro",  # must match picli login -g cluster
   )
   ```

8. **If `CursorUnavailableError` with `picli test login --method cursor`** (APA-140872):
   - Ensure the Cursor IDE is installed and the `cursor` command is in PATH.
   - In Cursor: Shift+Cmd+P -> "Shell Command: Install 'cursor' command in PATH".
   - Alternative: use `rdev code --cursor <mp-name>/<rdev-name>` instead of `picli test login`.

**Notes**:
- `picli test login -g <cluster>` sets the grid cluster context for picli, but operator-level `target_grid_cluster` overrides it at the task level.
- For Yugioh testing: `picli rdev create --branch <branch> <mp>` then `~/.okl_rdev/grid_setup.sh -g yugioh`.
- Always run grid_setup.sh after recreating an rdev.

---

## PB15 — RDev DAGs Not Loading {#rdev-dags}

**When to use**: rdev starts without errors but shows 0 DAGs in the UI, or all DAGs show as ImportErrors.

**Steps**:

1. **Run rdev-init with the reset flag**:
   ```bash
   okl-rdev-init.sh -d <path_to_your_dag_source_directory> -r
   ```
   The `-r` flag resets symlinks and re-initializes the DAG directory structure. This is required after any rdev recreation or if you changed the source directory.

2. **Verify symlinks exist**:
   ```bash
   # Inside rdev
   ls -la /opt/airflow/dags/
   ```
   You should see a symlink to your MP's DAG directory. If missing, rdev-init.sh didn't run or failed.

3. **Check for import errors**:
   ```bash
   # Inside rdev
   airflow dags list-import-errors
   ```
   If errors exist, fix the Python import issues first (see [DAG Import Error](#dag-import-error)).

4. **Restart Airflow inside rdev**:
   ```bash
   ~/.okl_rdev/restart_airflow.sh
   ```
   DAG list refreshes within ~30 seconds after restart.

5. **If still not loading** — open a support ticket via go/ask-airflow with:
   - Your MP name
   - Branch name
   - rdev creation date
   - Output of `airflow dags list-import-errors`

**Notes**:
- The `-d` path must be the root of the Airflow application directory (where your `dags/` folder lives), not the repo root.
- After rdev-init, it may take 1-2 minutes for DAGs to appear as the scheduler processes the DagBag.
- If you're seeing DAGs from your repo but not from a library MP dependency: ensure the library MP is deployed to the holdem cluster before testing.

---

## PB16 — RoundUp Redis Sentinel Down / No Master Found {#roundup-redis}

**When to use**: `kombu.exceptions.OperationalError: No master found for 'leader'` in RoundUp Celery worker logs; tasks stop being picked up.

**Steps**:

1. **Confirm the error**: check worker logs via Rain or SSH:
   ```bash
   ssh -p 20022 <roundup-host>
   # Look for "No master found" in Celery logs
   ```

2. **Check Redis Sentinel status**: look at the Roundup Redis Grafana dashboard at the [Observe dashboard](https://observe.prod.linkedin.com/g/d/de58ymar1dg5cf/ksap-all-lideployment-product-view?orgId=1&var-product=in-redis-roundup-workflows).

3. **Redeploy Redis Sentinel**: trigger an empty commit in the `roundup-workflows` repo to cause CRT to redeploy the sentinel:
   ```bash
   git commit --allow-empty -m "chore: redeploy sentinel for master election fix"
   git push
   ```

4. **Monitor recovery**: tasks should start being picked up within ~2 minutes of sentinel redeployment.

5. **If Azure SSO secret is the root cause** (check expiry dates: `spi-roundup-workflows-ei` expires 2027-05-14, `spi-roundup-workflows-prod` expires 2028-01-06):
   - Rotate the secret in Azure portal
   - Rotate KMS: `kms secret rotate urn:li:kmsSecret:<uuid>`
   - Redeploy roundup (empty commit)

**Notes**: See the Roundup section in the Oklahoma team KB for full RoundUp commands reference.

---

## PB17 — RoundUp Celery Worker Unhealthy / Scale-Up {#roundup-worker}

**When to use**: Tasks are queuing up in RoundUp but no workers are processing them; or worker throughput is too low.

**Steps**:

1. **Identify unhealthy workers**:
   ```bash
   rain instance list --fabric <fabric> <slice-id>
   ```
   Look for instances in error/unhealthy state.

2. **Delete bad instances**:
   ```bash
   rain instance delete <slice-id> -f <fabric> --hosts <bad-host>
   ```

3. **Create replacement instances**:
   ```bash
   rain instance create <slice-id> -f <fabric> --count <N>
   ```

4. **If scale-up needed** (throughput issue, not failure):
   ```bash
   rain instance create <slice-id> -f <fabric> --count <additional-N>
   ```

5. **Monitor**: `go-status -f <fabric> roundup-workflows`

6. **To restart a Celery worker slice**:
   ```bash
   lid-client control restart -f <FABRIC> --with-slice-id <SLICE_ID>
   ```

**Notes**: See the Roundup section in the Oklahoma team KB for full Rain CLI reference.

---

## PB18 — Azkaban Project Recovery (Accidentally Deleted) {#azkaban-project-recovery}

**When to use**: An Azkaban project was deleted and needs to be restored.

**Steps**:

1. **Get credentials from the webserver pod** (no MySQL binary in pod — pod is only used for creds):
   ```bash
   kubectl exec -it <lid-azkaban-holdem<N>-*> -n azkaban-web-x<N>-grid1-ltx1-holdem-cluster01 -- bash
   cat azkaban_conf/azkaban.properties | grep mysql
   # Note: config is at azkaban_conf/azkaban.properties (relative), NOT /opt/azkaban/conf/
   ```

2. **Connect to MySQL via break-glass**:
   ```bash
   ssh eng-portal.corp.linkedin.com
   mysql -h <host_from_properties> --port 3306 -u azkaban_app -p
   # Known DB names: azkaban_holdem3, azkaban_holdem — pattern: azkaban_holdem<N>
   ```

3. **Find the project ID**:
   ```sql
   SELECT id, name, active FROM projects WHERE name = '<project-name>';
   ```

4. **Restore the project**:
   ```sql
   UPDATE projects SET active = 1 WHERE id = <project-id>;
   ```

5. **Restart the Azkaban webserver** from inside the pod (not `kubectl rollout restart`):
   ```bash
   kubectl exec -it <pod> -n azkaban-web-x<N>-grid1-ltx1-holdem-cluster01 -- bash
   bin/control stop && bin/control start
   ```

6. **Verify**: confirm the project appears in the Azkaban UI with all flows intact.

**Notes**:
- Deleted projects have `active = 0`; all flow and schedule data remain in the DB.
- Need `SGP-ENG-azkaban-dev` (not SGP-ENG-oklahoma-dev) for kubectl exec into webserver pods.
- See [Azkaban](../systems/azkaban.md) for full Azkaban runbook.

---

## PB19 — UMP Schema Incompatibility After Dimension Changes {#ump-schema-drop}

**When to use**: A UMP flow fails with schema incompatibility errors on `u_metrics.*_union_staged` or `u_metrics.*_union` tables after a `metric-defs` PR adds new dimensions.

**Steps**:

1. **Confirm the pattern**: Check the error message for schema incompatibility mentioning `u_metrics.<flow>_union` or `u_metrics.<flow>_union_staged`.

2. **Identify the triggering PR**: Look at recent PRs to `linkedin-multiproduct/metric-defs` that added new dimensions to the affected flow.

3. **Request schema drop** (NOT data drop):
   - Request via Slack or ticket to drop only the schema of the affected tables:
     ```
     DROP TABLE IF EXISTS u_metrics.<flow_name>_union;
     DROP TABLE IF EXISTS u_metrics.<flow_name>_union_staged;
     ```
   - **IMPORTANT**: Drop schema only — the next run will recreate the tables with the updated schema. Data in underlying storage is preserved.

4. **Re-run the flow** after schema drop is confirmed.

5. **Verify**: Check that the flow succeeds with the new dimensions in the recreated tables.

**Notes**:
- This pattern recurs each time new dimensions are added to UMP metric definitions. Seven confirmed instances in April 2026: APA-144496 (annotation_quiz_metrics), APA-144369 (conversion_tracking_v2_cpa), APA-144528 (conversion_tracking_v2_plus), APA-144539 (payments_approval_v3), APA-144610 (capi_adoption_metrics_agg), APA-144631 (lms_advertiser_quality_actions_daily), APA-144645 (rsc_candidates_dq / rsc_applications_dq_v2). Pattern is accelerating — 7 instances in 3 weeks.
- The schema drop must be performed by someone with access to `u_metrics` — typically the UMP team or a DB admin.
- If the same flow fails again after the schema drop, check for additional schema changes in subsequent `metric-defs` PRs.

---

## See Also
- [Patterns](patterns.md) — pattern catalog with frequency and signature reference
- [Troubleshooting](../troubleshooting.md) — failure taxonomy and log access paths
- [GGW](../systems/ggw.md) — Grid Gateway architecture, hook internals, failure modes
- [Spark](../systems/spark.md) — SparkBatchOperator reference, submission flow
- [Azkaban](../systems/azkaban.md) — Azkaban runbook and decommission process
- [DAG Authoring](../dag-authoring.md) — operators, sensors, naming conventions
- [Oncall](../oncall/README.md) — escalation paths, SLAs, oncall contacts
