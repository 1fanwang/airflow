# Codebase — Gotchas

> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

## mysqlclient TLS glibc Bug with LD_PRELOAD

**Where**: Docker build, all Airflow images (oklahoma-airflow, rdev)

**What happens**: 
mysqlclient library hits glibc TLS allocation error at runtime. Process hangs or crashes with cryptic memory allocation errors when connecting to MySQL. This affects both regular scheduler runs and background tasks.

**How to avoid / fix**:
- **Docker**: Set `ENV LD_PRELOAD=/lib/libstdc++.so.6` in Dockerfile (not just env, this alone is insufficient)
- **Login shells** (`su - user`): Also add shell profile script: `RUN echo 'export LD_PRELOAD=/lib/libstdc++.so.6' > /etc/profile.d/ld_preload.sh`
- The ENV directive handles direct Docker processes, but login shells need the profile.d entry because they read shell initialization files
- This is a glibc + MySQL bug: https://bugs.mysql.com/bug.php?id=113029
- Discovered: commit a0e79c0, then refined in 7257176 (LD_PRELOAD not applied in login shells)

---

## RDev Fernet Key Length Requirement

**Where**: RDev environment setup (`setup_oklahoma_rdev_env.sh`)

**What happens**: 
If `AIRFLOW__CORE__FERNET_KEY` is not exactly 32 bytes (base64-encoded), Airflow scheduler fails to decrypt DAG params, task state, and connections. The key looks like it works initially but causes mysterious decryption failures downstream.

**How to avoid / fix**:
- Fernet keys must be exactly 32 bytes when base64-decoded
- The key `RdevFernetKeyRdevFernetKeyRdevFernetKeyRdev=` is properly padded
- Don't use short keys like `RDevFernetKey` — Airflow will not error until you try to decrypt something
- Always validate key length: `python -c "from cryptography.fernet import Fernet; Fernet(your_key)"`
- Discovered: commit a889dd1

---

## Config Recursive Merge Not Applied to User Overrides

**Where**: Config loading in RDev (`oklahoma-helpers/config.py`)

**What happens**:
User config overrides in RDev don't respect the `enable_merge` flag. User configurations set with `enable_merge=True` are applied non-recursively, causing top-level keys to overwrite entire nested dictionaries instead of merging them. This silently loses user-set config values that should have been merged.

**How to avoid / fix**:
- When applying user config overrides, explicitly pass `is_recursive=self.enable_merge` to `_update_dicts()`
- The flag exists and is checked elsewhere in the config system but was missing in the RDev user override code path
- Test: set nested config values as a user, verify all nested keys are preserved after merge
- Discovered: commit 5577898d

---

## DAG ID Naming Convention Edge Cases Break Crew Asset Sync

**Where**: DAG syncing and crew asset creation (`dag_syncer.py`)

**What happens**:
DAG IDs that don't follow the strict naming convention (e.g., `standalone_quarantine__lakeshift_holdem`, `kpi_daily_agg__tm_opex`) cause the DAG crew asset creation to fail silently or skip the DAG entirely. The parser extracts the wrong multiproduct name, the validation rejects the DAG, and no crew asset is created. This leaves DAGs without proper asset ownership in crew.

**How to avoid / fix**:
- Parse DAG ID to extract expected MP name, but **fall back to listener.mp_name if it doesn't match**
- Don't reject DAGs based on naming convention — use listener context as ground truth
- Handle these edge cases:
  - Multiple `__` separators (e.g., `abm-hub-job__holdem__account-targeting-offline`)
  - Underscore/hyphen mismatches (e.g., `kpi_daily_agg__tm_opex` vs MP `tm-opex`)
  - Legacy DAGs (e.g., `standalone_quarantine__lakeshift_holdem` vs MP `lakeshift`)
- Log warnings for mismatched DAG IDs so operators see them
- Discovered: commit 893c730a

---

## Circular Import in airflow_local_settings and Policy Framework

**Where**: Policy framework DAG mutation, airflow initialization

**What happens**:
Top-level imports from `airflow.*` in `airflow_policy_framework.policies.lnkd.dag.mutation` cause Airflow to initialize, which imports `airflow_local_settings.py`, which imports the policy module — creating a circular dependency. This causes import failures at startup.

**How to avoid / fix**:
- **Lazy imports inside functions**: Move all `from airflow.*` imports into the function that uses them, AFTER Airflow initialization completes
- Don't import from modules that themselves import from `airflow` at module level (e.g., `utils.py`)
- This is necessary even if the imports look unrelated — Python imports the `airflow` package root, triggering `__init__` → `settings.initialize()` → `airflow_local_settings`
- Use a comment explaining the circular dependency for maintainers
- Discovered: commit 85f5106f

---

## Service Account Authentication Caching Was Premature Optimization

**Where**: API auth manager (`api_auth.py`, `manager.py`)

**What happens**:
Performance optimization that cached service account lookups and skipped LDAP calls caused authentication inconsistencies. When a service account's roles changed, the cached version didn't refresh. The optimization was more fragile than the simple approach and introduced latency-dependent bugs.

**How to avoid / fix**:
- Don't optimize service account authentication with caching and refresh flags unless latency is measured and confirmed as a bottleneck
- The original approach (always hit LDAP) was correct: service accounts have simple roles (name-based), so LDAP is fast
- If optimization is needed in the future, make it explicit and test with role changes
- Discovered: commit 2bbdbf64 (revert of 35e4150c)

---

## CSS Protection Disabled in Airflow Webserver

**Where**: Webserver config (`startup/webserver_app.py`, `webserver_app_2_9.py`)

**What happens**:
`CSRF_PROTECTION_ENABLED = False` is hardcoded. This is noted with TODO but remains unimplemented. Running with CSRF protection disabled is a security risk if the server is exposed to untrusted networks.

**How to avoid / fix**:
- This is a known limitation (TODO comment in code)
- Don't expose the Airflow webserver to untrusted networks without additional authentication
- If you can set this to True, do so — but it's currently disabled for operational reasons not yet documented
- Discovered: TODO comment in `startup/webserver_app.py` line 121

---

## Airflow Dependency Version Rollbacks Due to Incompatibility

**Where**: Provider version management

**What happens**:
Airflow core version upgrades (e.g., 2.9.2.114) sometimes break providers. After release, developers discover incompatibilities (missing dependencies, API changes) and must revert both the provider and core bumps. This can cascade across multiple PRs and waste several days of work.

**How to avoid / fix**:
- Don't bump Airflow core version without running full integration tests first
- Watch for reverts in git history — they're a sign of a fragile version combination
- Test with a staging airflow instance before promoting to production
- Discovered: commits fb2d593a (revert of e452d7a1) and da13e907 (revert of revert)

---

## Missing servicemesh cfg2 Keys Cause Runtime Failures

**Where**: Deployment validation and servicemesh config

**What happens**:
New servicemesh configuration keys (e.g., `announcementConfigs`) are missing from the deployment config template. The deployment succeeds, but the running service crashes when it tries to access the key. This is caught late, during canary rollout.

**How to avoid / fix**:
- Check for missing cfg2 keys **within the built Docker image** before tagging it stable
- Add a build-time validation step that parses the servicemesh config and verifies required keys are present
- Don't rely on production health checks to catch missing config keys
- Discovered: commit 367e6ab and 5e40dae

---

## Monkey-Patching Airflow Security Manager

**Where**: Scheduler initialization (`startup/start_scheduler.py`, `start_scheduler_2_9.py`)

**What happens**:
Custom security manager is monkey-patched into `airflow.www.security_appless` at runtime. This is fragile: if Airflow refactors the security layer, the patch breaks silently or causes subtle bugs. It's also hard to test and reason about.

**How to avoid / fix**:
- This is noted as a known limitation with TODO comment and GitHub issue reference (https://github.com/apache/airflow/issues/23232)
- Contribute this functionality back to Apache Airflow to remove the need for monkey-patching
- In the meantime, add integration tests that verify the patch is applied correctly
- Discovered: TODO comment in `start_scheduler.py` line 83

---

## SSL Verification Disabled in DAG Validation

**Where**: DAG validation endpoint (`dag_validations.py`)

**What happens**:
HTTPS requests to Grid User Manager use `verify=False` for SSL certificate validation. This is a security risk and leaves the code vulnerable to MITM attacks on internal networks. It's noted with TODO but not implemented.

**How to avoid / fix**:
- Set `verify=TRUSTSTORE_FILEPATH` to validate against the company's CA bundle
- The constant `TRUSTSTORE_FILEPATH` already exists; it just needs to be used
- If certificate validation fails, it's likely a networking issue (proxy, DNS) — don't disable verification, fix the root cause
- Discovered: TODO comment in `dag_validations.py` line 142

---

## Large DAG Runs Create Root Cause Analysis Scaling Issues

**Where**: Root cause analyzer, Iris incident detection

**What happens**:
When a workflow has many DAG runs (especially in high-frequency environments), the root cause analyzer query becomes very expensive. It fetches metadata for all runs, gets too many results, times out, or times out the database. This cascades into delayed incident detection and slower Iris notifications.

**How to avoid / fix**:
- The analyzer has a known bug when there are "too many DAG runs" (mitigated but not fully fixed)
- Implement pagination or window-based lookups instead of fetching all runs at once
- Use a configurable retention window: only analyze DAG runs from the last N hours
- Discovered: commit b90997e8

---

## LDAP Extraction Case Sensitivity Issues

**Where**: User authentication (`security/manager.py`)

**What happens**:
LDAP usernames are case-sensitive in the LDAP server but sometimes extracted in mixed case. This causes lookups to fail or create duplicate users (e.g., `JohnDoe` vs `johndoe`). The code now normalizes to uppercase.

**How to avoid / fix**:
- Always extract LDAP user names in uppercase to match the grid standard
- If migrating old user records, normalize existing usernames to uppercase
- Discovered: commit b777e3ec

---

## Service Account Authentication Skip Has Performance / Correctness Trade-Off

**Where**: API authentication, service account detection

**What happens**:
There's a fundamental tension: skipping LDAP for service accounts is fast but breaks if roles change; always checking LDAP is correct but slow. Previous attempts at caching/optimization failed. The current approach (always check) is correct but slower.

**How to avoid / fix**:
- Know that service account auth is slower than human auth by design (for correctness)
- If performance becomes a bottleneck, measure first and propose a solution that includes test cases for role changes
- Don't assume service account roles are static — they can be updated in LDAP
- Discovered: commits 2bbdbf64 and surrounding

---

## [2026-04-13] SKIP LOCKED in bulk_write_to_db Silently Drops Metadata Updates

**Where**: `airflow/models/dag.py`, `DAG.bulk_write_to_db()` (LinkedIn fork)

**What happens**:
PR #87 added `FOR UPDATE SKIP LOCKED` to prevent blocking when concurrent dag-processors sync the same DAG. However, when a processor skips a locked row, it **never retries the write** — metadata updates (`is_paused`, `next_dagrun`, tag associations, dataset references) are permanently lost. DAGs can revert to stale state after a concurrent parse cycle.

**How to avoid / fix**:
- PR #87 was **reverted** by PR #114 (2026-04-09). The blocking `SELECT FOR UPDATE` is restored.
- Do NOT re-apply SKIP LOCKED to `bulk_write_to_db` without a retry/queue mechanism for skipped rows.
- The lock contention issue remains open — alternative approaches are under investigation.
- Source: airflow PR #114

---

## [2026-04-13] Duplicate-Key Race in DAG Serialization Cascades PendingRollbackError

**Where**: `airflow/models/serialized_dag.py` (LinkedIn fork, HA deployments)

**What happens**:
When two dag-processors parse the same new DAG simultaneously, both attempt to INSERT into `serialized_dag`. One succeeds, the other gets `IntegrityError` (duplicate key). The failed session enters `PendingRollbackError` state — **all subsequent DB operations** on that session fail, cascading across the entire processor cycle.

**How to avoid / fix**:
- Fixed in PR #111 (2026-04-10): catch `IntegrityError` on INSERT, rollback the session, fall through to UPDATE.
- If you see `PendingRollbackError` in dag-processor logs, this was likely the cause prior to v2.9.2.166.
- Source: airflow PR #111

---

## [2026-04-13] MySQL Deadlock from QUEUED Event Split in Executor

**Where**: `airflow/jobs/scheduler_job_runner.py`, `_process_executor_events()` (LinkedIn fork)

**What happens**:
PR #83 split QUEUED events (plain UPDATE) from terminal events (FOR UPDATE SKIP LOCKED) for performance. But the two phases acquire row-level locks in different orders. When two schedulers process overlapping TI sets, InnoDB detects a deadlock (`Error 1213`). The scheduler crashed with an unhandled `OperationalError`.

**How to avoid / fix**:
- Fixed in PR #109 (2026-04-09): added `@retry_db_transaction` to the QUEUED-event phase.
- If you modify `_process_executor_events`, test with multiple concurrent schedulers.
- Source: airflow PR #109

---

## [2026-04-13] Override Form Fields with `name` Attribute Overflow MySQL TEXT Column

**Where**: Airflow trigger page, config override UI (LinkedIn fork)

**What happens**:
The `@action_logging` decorator serializes all `request.values` into the MySQL `extra` column (`TEXT`, 65,535 byte limit). Override fields on the trigger page had `name` attributes, causing the browser to include every field value in the POST body. With many tasks/fields, the serialized payload exceeded 65,535 bytes and the trigger request failed silently.

**How to avoid / fix**:
- Fixed in PR #104 (2026-04-06): removed `name` attributes from override fields so the browser excludes them from form submission.
- If adding new form fields to the trigger page, do NOT give them `name` attributes unless you want them in `request.values`.
- Source: airflow PR #104

---

## [2026-04-13] Guava extraClassPath Overwrites User-Supplied Spark Configs

**Where**: `lipy-airflow-providers`, Feature Cloud push task group (result_generator config)

**What happens**:
`result_generator_config = hosted_search_spark_config` was an alias (not a copy), causing the guava jar mutations to also corrupt `hosted_search_spark_config` used by `compliance_annotator`. Additionally, `spark.driver.extraClassPath` and `spark.executor.extraClassPath` were overwritten with `=` instead of appended, destroying user-supplied JAR paths.

**How to avoid / fix**:
- Fixed in PRs #1157/#1159 (2026-04-01): use `dict.copy()` for config; append guava JAR to existing extraClassPath values with `:` separator.
- Always copy config dicts before mutating. Always append to `extraClassPath`, never overwrite.
- Source: lipy-airflow-providers PR #1157, #1159

---

## [Slack-sourced 2026-04-13] Dynamic end_date Stops Scheduler Creating New Runs

**Where**: DAG definition, `end_date` parameter

**What happens**:
Setting `end_date = datetime.utcnow() - timedelta(days=N)` (or equivalent `timezone.utcnow()`) makes `end_date` a moving target evaluated at DAG-bag parse time. Each parse cycle computes a new `end_date` that is still in the past, but Airflow sees `end_date < now` and stops creating future runs. The DAG appears active but silently stops scheduling.

**How to avoid / fix**:
- Never set `end_date` dynamically. Use a fixed date far in the future, or omit `end_date` entirely.
- If you need to process "last N days" of data, put the lookback logic inside the task (e.g., `{{ ds }}` minus N days as an operator param).
- To backfill only a range: use `start_date` + `catchup=True` with a fixed `end_date` in the past, then re-pause the DAG.

---

## [Slack-sourced 2026-04-13] File Passing Between Tasks: Use HDFS Not Local Path

**Where**: DAG tasks using GGW operators with file outputs

**What happens**:
Tasks run in separate Kubernetes pods. A file written by task A to a local path (e.g., `./.data/output.csv` or `/tmp/result`) does not exist when task B runs — each pod has its own ephemeral filesystem.

**How to avoid / fix**:
- Use `job.file.output.hdfs.dir` (HDFS output path) to write from task A.
- Use `job.file.input.location` to read it in task B — but note: `job.file.input.location` does NOT work for cross-task file passing (APA-143133). Use HDFS paths directly in the downstream task config.
- Alternatively: pass a reference (HDFS path string) via XCom and have the downstream task read from HDFS.
- APA-143133 tracks the `job.file.input.location` cross-task gap.

---

## [Slack-sourced 2026-04-13] Manually Triggering a Paused DAG Creates Two Concurrent Runs

**Where**: Airflow UI — manual trigger on a scheduled+paused DAG

**What happens**:
If a DAG is paused mid-schedule and you manually trigger it, Airflow creates the manual run AND queues the missed scheduled run simultaneously. Both run in parallel. If the DAG is not idempotent this causes duplicate processing.

**How to avoid / fix**:
- After a manual trigger: wait for the triggered run to complete before unpausing.
- If you only want a manual run: trigger first, then check the DAG run list and manually mark the unwanted scheduled run as "Success" or "Failed" before it starts.
- Alternatively: use `max_active_runs=1` on DAGs that must not run in parallel.

---

## [Slack-sourced 2026-04-13] create_sql_task_range Returns a List — Don't Double-Wrap

**Where**: DAG authoring with `create_sql_task_range` or similar helper functions

**What happens**:
`create_sql_task_range(...)` already returns a Python list of task objects. Wrapping it in another list (`[create_sql_task_range(...)]`) before using bitshift operators (`>>`) creates a nested list, which Airflow does not know how to set as a dependency. The result is a silent dependency gap — tasks appear connected in the UI but don't actually enforce ordering.

**How to avoid / fix**:
```python
# BAD — double-wrapped
start >> [create_sql_task_range(...)] >> end

# GOOD — unwrapped
tasks = create_sql_task_range(...)   # already a list
start >> tasks >> end
```
- Same applies to any helper that returns a list: pass directly to the bitshift chain.

---

## [Slack-sourced 2026-04-13] LI_BASE_USER Removal and Programmatic API 403s

**Where**: DAG `access_control`, Jeeves automation, service account access

**What happens**:
Jeeves automatically generates PRs to remove `LI_BASE_USER` from `access_control` entries across DAG definitions. After removal, human reviewers and service accounts that relied on `LI_BASE_USER` for read access lose access. Programmatic API calls from service accounts start returning 403.

**How to avoid / fix**:
- Add your service account (or `SGP-CREW-<crew_id>-MEMBERS`) to `access_control` explicitly before approving the Jeeves PR.
- For dashboard/read-only visibility without crew ownership: use `team=<tag>` tag on the DAG instead of `LI_BASE_USER` access.
- Example:
  ```python
  tags=["team:my-team"],
  access_control={
      "SGP-CREW-1090-MEMBERS": {"can_read", "can_edit"},
      "my-service-account": {"can_read"},
  }
  ```
- If a headless service principal A is added as a member of headless B's group, DAGs executing as A can access B's datasets via KSudo impersonation — use this for cross-team dataset access.

---

## [Slack-sourced 2026-04-13] AirflowSkipException Behavior Change Between Provider Versions

**Where**: `lipy-airflow-providers`, `pre_execute` skip logic, versions 0.0.867 → 0.0.881

**What happens**:
In v0.0.867 and prior: a skip exception raised in `pre_execute` caused a graceful SKIP state (task marked SKIPPED, downstream proceeds). After v0.0.881: the same skip raised `AirflowExecuteHookException` and caused the task to FAIL. This was a regression. DAGs that depended on pre_execute skip behavior started failing after the provider upgrade.

**How to avoid / fix**:
- This was a regression — fixed by rolling back to v0.0.867 at the time. The fix should be in post-rollback versions.
- If you see unexpected FAILED states where SKIPPED is expected, check the provider version and compare with the changelog.
- `allow_rdev_runs=False` skip behavior (intentional SKIPPED in rdev) is separate from this bug and continues to work correctly.

---

## [Slack-sourced 2026-04-13] RDev: libstdc++ TLS Error Requires rdev Recreation

**Where**: RDev Airflow pod, `ImportError` on startup

**What happens**:
```
ImportError: /lib/libstdc++.so.6: cannot allocate memory in static TLS block
```
This error appears on rdev images that predate a base image update (approximately March 4, 2026). The same root cause as the `libmariadb.so.3` ImportError. Both are caused by TLS allocation changes in the rdev base image. The pod starts but cannot import the affected library.

**How to avoid / fix**:
- **Recreate the rdev** — rdevs created before March 4, 2026 must be deleted and recreated to pick up the new base image.
- After recreation: run `~/.okl_rdev/grid_setup.sh -g <cluster>` to re-establish Grid Gateway connectivity.
- Related Jira tickets: APA-140407, APA-140417.
- If the error persists after recreation: pin the rdev image to `airflow-main-airflow-rdev:stable` in devcontainer.json instead of a specific version tag.

---

## [Slack-sourced 2026-04-13] SIGTERM During Non-RUNNING GGW State Stops the External Job

**Where**: GGW operators with `_enable_job_checkpoint=True`, SIGTERM handling

**What happens**:
When a Kubernetes pod receives SIGTERM mid-execution, the GGW operator's signal handler fires. If the external job is in a transient state (not yet confirmed RUNNING, e.g., QUEUED or ACCEPTED), the handler calls the GGW cancel API on the job, terminating it. The task is then marked FAILED. If you retry, GGW submits a new job — the original is gone.

**How to avoid / fix**:
- This is a known edge case for `_enable_job_checkpoint=True`. The checkpoint saves the URN but cancellation races with the RUNNING state confirmation.
- Workaround: increase `polling_interval` to reduce the window where the job is in non-RUNNING state before the first confirmed poll.
- If a Spark job is marked FAILED but you suspect it actually ran: check the GGW execution URN in task logs directly against the Grid Gateway UI — the job may have succeeded before cancellation.
- Track: if this impacts you frequently, set `retries=2` on the operator; the retry will re-submit and complete normally.

---

## [2026-04-14] GHD Image Path Broken After Repo Split (InvalidImageName)

**Where**: `oklahoma-airflow-deployment` Helm values, GHD cluster

**What happens**:
After the `oklahoma-airflow` → `oklahoma-airflow-deployment` repo split (March 2026), `defaultAirflowRepository` was changed to `oklahoma-airflow-deployment` — the name of the **deployment** repo, not the **image** repo. Pods provisioned on GHD after commit `10efffecc3` (March 18) hit `InvalidImageName` / `ImagePullBackOff` because the container registry path was wrong.

**How to avoid / fix**:
- Fixed in deployment PR #1037 (2026-04-13): restored correct image repository path and `defaultAirflowTag`.
- When renaming or splitting repos, verify that `defaultAirflowRepository` in Helm values still points to the **container image** registry path, not the GitHub repository name.
- Source: oklahoma-airflow-deployment PR #1037

---

## [Slack-sourced 2026-04-14] commons-lang3 NoSuchMethodError in Spark Jobs (Classpath Conflict)

**Where**: Spark jobs on Holdem via GGW (SparkBatchOperator)

**What happens**:
```
java.lang.NoSuchMethodError: 'org.apache.commons.lang3.Range org.apache.commons.lang3.Range.of(java.lang.Comparable, java.lang.Comparable)'
```
in `NumericEntityEscaper.<init>`. Affects multiple independent pipelines on Holdem (e.g., `trust-nano-offline-pipelines`, `messaging-nano-offline-pipelines`). At least 2 separate teams reported the same stack trace in April 2026.

**How to avoid / fix**:
- This is a classpath conflict: the runtime commons-lang3 version is older than what the application JAR was compiled against. The `Range.of()` method was added in commons-lang3 3.8.
- Check `spark.driver.extraClassPath` and `spark.executor.extraClassPath` — if your application JAR bundles commons-lang3, the YARN classpath's older version may take precedence.
- Use `spark.driver.userClassPathFirst=true` and `spark.executor.userClassPathFirst=true` to prioritize your application JARs.
- Similar to the guava extraClassPath gotcha — always APPEND to extraClassPath, never overwrite. Check for framework-injected JARs that may downgrade your dependency.
- Source: #ask_airflow April 2026, recurring across 2+ teams

---

## [2026-04-08 / Closed 2026-04-20] spark-core-metrics JAXB-RI Shading Breaks com.sun.xml.bind.* APIs (APA-144067)

**Where**: Spark jobs on Holdem and Faro that depend on `com.sun.xml.bind.*` JAXB APIs

**What happens**:
`spark-core-metrics` (LinkedIn's internal Spark metrics library) shades JAXB-RI (the JAXB reference implementation) and **also** includes the shaded `META/services` registrations for `com.sun.xml.bind.*`. When a downstream Spark job uses `com.sun.xml.bind.*` APIs directly (e.g., for XML marshaling/unmarshaling), the shaded version from `spark-core-metrics` takes priority on the classpath and conflicts with the application's own JAXB dependency — causing runtime failures such as `ClassCastException`, `ServiceConfigurationError`, or incorrect JAXB implementation being loaded.

**How to avoid / fix**:
- Closed in APA-144067 (Maxime Xu, Apr 8–20, Minor) — a fix was applied to `spark-core-metrics` to exclude or relocate the conflicting `META-INF/services` registration.
- If you are on an older version of `spark-core-metrics` and see JAXB-related `ClassCastException` or `ServiceConfigurationError` in Spark jobs on Holdem/Faro, upgrade `spark-core-metrics` to the version that includes the APA-144067 fix.
- As a temporary workaround prior to the fix: use `spark.driver.userClassPathFirst=true` and `spark.executor.userClassPathFirst=true` to prefer your application's JAXB JAR over the framework-shaded one.
- **Pattern**: This is the third documented LinkedIn framework-JAR classpath shading conflict (after guava/extraClassPath and commons-lang3). When a framework library shades a dependency AND registers it via `META-INF/services`, all consumer jobs are forced to use the shaded version — even if they bundle their own.
- Source: APA-144067 (Holdem/Faro, Closed Apr 2026)

---

## [2026-04-16 / Closed 2026-04-20] Hive UDF PortalLookup Class Loading Error on Holdem Spark Jobs (APA-144564)

**Where**: Spark jobs on Holdem that register custom Hive UDFs (e.g., `PortalLookup`)

**What happens**:
Spark job on Holdem fails with a class loading error when registering a Hive UDF (`PortalLookup`). The UDF class exists in the application JAR but cannot be loaded by Spark's Hive UDF registry mechanism — typically because the UDF class is in a shaded JAR whose relocation changes the class name, or because the classloader resolving the UDF registration is not the application classloader that holds the JAR.

**How to avoid / fix**:
- Closed in APA-144564 (Ruolin Fan, Apr 16–20, Major) — fix was applied.
- If Hive UDF registration fails with class loading errors on Holdem: first check whether the UDF class is in a shaded JAR (shading relocates the class to a new package — the UDF `CREATE FUNCTION` statement still references the original name, which no longer matches the shaded class path). Ensure `CREATE TEMPORARY FUNCTION` uses the fully qualified post-shading class name if the JAR is shaded.
- Set `spark.executor.userClassPathFirst=true` to prefer the application JAR's classloader over the Holdem YARN framework classloader, which can mask the application's UDF class.
- **Cluster specificity**: This was Holdem-specific. The same UDF may register correctly on War or Faro if those clusters have different YARN/Hive metastore classpath configurations.
- **Assignee pattern**: Ruolin Fan resolved this alongside the dali-data-sdk classpath conflicts (APA-144609, APA-144545) — suggesting broad classpath cleanup activity on Holdem in the Apr 15–20 window.
- **Pattern**: Fourth documented LinkedIn Holdem classpath conflict (after guava/extraClassPath, commons-lang3 NoSuchMethodError, and spark-core-metrics JAXB-RI shading). Holdem's framework JAR set is more aggressive than War/Faro in shadowing application classes.
- Source: APA-144564 (Holdem, Closed Apr 2026)

---

## [Slack-sourced 2026-04-14] Cross-Cluster HDFS Reads Not Supported by Feature Cloud Push

**Where**: Feature Cloud push DAGs, cross-datacenter HDFS access

**What happens**:
When a DAG on WAR writes GPU inference outputs to WAR HDFS, Feature Cloud push tasks running on Holdem cannot read those files — cross-cluster HDFS reads are not supported by the FedEx (Feature Cloud) push framework.

**How to avoid / fix**:
- Run the entire pipeline (inference + feature cloud push) on the same cluster.
- If inference must run on WAR (e.g., GPU availability), set up the Airflow DAG on WAR instead of Holdem.
- There is no cross-cluster HDFS bridge available — this is a platform limitation, not a config issue.
- Track: APA-143406
- Source: #ask_airflow April 2026

---

## [Slack-sourced 2026-04-14] REST API Returns 401 Despite Valid Client Certs

**Where**: Airflow REST API (`/api/v1/*`) when called with `curl --cert/--key`

**What happens**:
`curl -k --cacert ... --cert ... --key ... -X GET https://<cluster>/api/v1/dags` returns `{"status": 401, "title": "Unauthorized"}`, even though the same user can access DAG details in the browser. The two auth mechanisms are different.

**How to avoid / fix**:
- Browser auth uses Azure AD SSO (cookie-based). REST API auth requires a **DataVault identity token** passed as `datavaultIdentityToken` header, NOT client certificate TLS.
- Correct API call pattern:
  ```bash
  curli --dv-auth SELF https://<cluster>/api/v1/dags
  ```
- Source: #ask_airflow Slack (recurring)

---

## [Slack-sourced 2026-04-14] External Python Modules (pandas, numpy) Not Available in PythonOperator

**Where**: DAG authoring with `PythonOperator`

**What happens**:
```
ModuleNotFoundError: No module named 'pandas'
```
Users expect `pandas`, `numpy`, or other PyPI libraries to be available in `PythonOperator` callables. They are not — Airflow worker pods only have the base Airflow image dependencies.

**How to avoid / fix**:
- **PythonOperator** runs in the Airflow process; only libraries in the Airflow image are available.
- For custom Python dependencies, use **GGW with a shiv** (packaged Python application). Example: `picli` PR #589.
- Alternatively: write the logic as a Spark job or a GGW `CommandOperator` with a custom Docker image.
- This is a known platform limitation, not a bug.
- Source: #ask_airflow Slack, Oklahoma team guidance

---

## [Slack-sourced 2026-04-14] IPv6 gRPC UNAVAILABLE in Darwin/inDBT Sandbox

**Where**: inDBT sandbox testing on Darwin, Grid Gateway gRPC calls

**What happens**:
```
grpc._channel._InactiveRpcError: StatusCode.UNAVAILABLE -- failed to connect to all addresses;
last error: UNKNOWN: ipv6:[...]:443: connect: Network is unreachable (101)
```
The gRPC client attempts to connect to Grid Gateway via IPv6 but the network path is not available. Seen on 2026-04-13. Recreating the Darwin Airflow instance did NOT fix the issue.

**How to avoid / fix**:
- May be related to IPv6 enablement rollout (deployment PR #894 for webserver, PR #1028 for GHD).
- Workaround: Force gRPC to use IPv4 by setting `GRPC_DNS_RESOLVER=native` environment variable (unconfirmed).
- Status: No resolution posted in Slack as of 2026-04-14.
- Source: #ask_airflow Slack (2026-04-13)

---

## [2026-04-14] Trino/SQLOperator Writes Do Not Emit DCE — Partition Sensors Return False Forever

**Where**: DAGs using partition sensors (e.g., `PartitionSensorDefinition`) after Trino/SQLOperator writes

**What happens**:
A partition sensor waits for a dataset partition that was written by a Trino `SQLOperator` or similar Trino-based write. The sensor runs indefinitely, never returning True — even though the data is present in the table. The sensor eventually times out with `AirflowSensorTimeout`.

**Root cause**: Trino/SQLOperator writes do **not** emit Data Change Events (DCE) to ARMS/Jasper. Partition sensors rely on ARMS to detect new partitions via DCE. Since no DCE is emitted, ARMS never knows the partition exists, and the sensor returns False forever. This is a known platform gap tracked as APA-137978.

**How to avoid / fix**:
- Switch from Trino writes to **Spark** writes, which do emit DCE to ARMS.
- Alternatively, use **time-based sensors** (e.g., `TimeDeltaSensor` or `TimeSensor`) instead of partition sensors if the write schedule is predictable.
- Do NOT rely on `PartitionSensorDefinition` for datasets written by Trino.
- Source: APA-141942, APA-137978

---

## [2026-04-14] Custom Airflow Worker Images Not Supported — Use GGW image_url

**Where**: Airflow task execution, custom Docker images

**What happens**:
Users want to run custom tools (e.g., LinkedIn CLIs, JDK dependencies) inside Airflow tasks but the default oklahoma-airflow worker image does not include them. Attempting to use the rdev image or other custom images as the Airflow worker image is not supported by the platform.

**How to avoid / fix**:
- Airflow does **not** support custom worker images. All tasks run in the standard oklahoma-airflow image.
- For custom dependencies, use **Grid Gateway operators** with the `image_url` parameter to specify a custom Docker image for the GGW job execution environment.
- Note: some users report `image_url` does not work as expected — escalate to the GGW team if issues arise.
- For Python dependencies: use GGW with a **shiv** (packaged Python application). See `picli` PR #589.
- Source: APA-144147

---

## [Slack-sourced 2026-04-15] sla_miss_callback Type Signature Change Causes mypy Failures

**Where**: DAG definitions using `sla_miss_callback`, LinkedIn Airflow fork

**What happens**:
The `sla_miss_callback` parameter type signature changed in a LinkedIn Airflow fork update. DAGs that define SLA miss callbacks using the old signature started producing mypy type-check failures during build validation. Tasks and DAGs continued to function at runtime, but CI fails broke deployments.

**How to avoid / fix**:
- If you have a `sla_miss_callback` defined in your DAG and see mypy type errors, check the current expected signature in the LinkedIn Airflow fork.
- APA-139517 tracks this change. Check the ticket comments for the exact before/after signature diff.
- Update your callback signature to match the new type annotations.
- Source: #ask_airflow Slack, APA-139517

---

## [Slack-sourced 2026-04-15] KMS Secret Rotation Requires MP Redeployment

**Where**: Airflow DAGs that reference KMS-backed secrets (credentials, tokens, service keys)

**What happens**:
After rotating a KMS secret, running Airflow worker pods continue using the old cached value. Secrets are read at pod startup — there is no hot-reload mechanism. Users who rotated KMS credentials found their DAGs started failing with auth/permission errors even though the new secret was already live in KMS.

**How to avoid / fix**:
- After rotating any KMS secret used by your Airflow DAG or MP, **redeploy the MP** to restart pods so they pick up the new value.
- Redeploying means re-uploading via CRT (triggers a rolling pod restart), or manually triggering a rolling restart via `kubectl rollout restart deployment/<mp> -n airflow`.
- Plan KMS rotations to happen just before a scheduled CRT deployment to minimize manual intervention.
- Source: #ask_airflow Slack

---

## [2026-04-17] picli project_name Normalization for .okl_setup.json Path

**Where**: `picli` — `_inject_missing_mp_packages` in `enforce_policy.py`

**What happens**:
The `.okl_setup.json` path is built as `mp_root / project_name / "src" / mp_name / project_name / ".okl_setup.json"`. `project_name` comes from `dag_dir.parent.name` and can be a hyphenated Gradle module name (e.g., `my-dag-app`), but the actual filesystem directory uses the normalized (underscored) Python package name (e.g., `my_dag_app`). This mismatch causes the `.okl_setup.json` lookup to fail silently — the file exists on disk but picli doesn't find it because it uses the hyphenated name in the path.

**How to avoid / fix**:
- Fixed in picli PR #684 (2026-04-17): `project_name` is now normalized (hyphens → underscores) before constructing the path.
- If you have a Gradle module with hyphens in the name, ensure you're on the latest picli version.
- Prior to the fix, this could manifest as DAG import failures or missing package metadata during `picli upload` when the MP uses a hyphenated project name.
- Source: picli PR #684

---

## [2026-04-18] EmailOperator Silently Redirected in RDev

**Where**: RDev environment, any DAG using `EmailOperator`

**What happens**:
In rdev environments, `EmailOperatorPatchPlugin` (lipy-airflow-providers PR #1187, merged 2026-04-18) intercepts all `EmailOperator` sends and redirects both the `to` and `from` fields to `$USER@linkedin.com`. This prevents accidentally spamming team distribution lists (e.g., `oklahoma-dev@linkedin.com`) during end-to-end DAG testing in rdev. The redirect is **silent** — no warning or log message indicates the recipients were changed.

**How to avoid / fix**:
- This is intentional behavior, not a bug. Be aware that emails sent from rdev will NOT reach the original recipients.
- If you need to test actual email delivery to a DL, you must test on a non-rdev environment.
- The plugin only activates in rdev; production clusters are unaffected.
- Source: lipy-airflow-providers PR #1187

---

## [2026-04-18] picli validate-dags / enforce-policy Hyphenated Aliases

**Where**: `picli` CLI, DAG validation and policy enforcement commands

**What happens**:
Prior to picli PR #685, only the underscore forms (`validate_dags`, `enforce_policy`) worked. Users who typed the hyphenated forms (`validate-dags`, `enforce-policy`) — consistent with other airflow subcommands like `crt-sync` and `rdev-init` — got "command not found" errors.

**How to avoid / fix**:
- Both `-` and `_` forms now work (picli PR #685, merged 2026-04-17).
- Additionally, noisy `'NoneType' object has no attribute 'hook'` log errors in `validate-dags` are now suppressed by calling `settings.configure_vars()` before plugin loading.
- Source: picli PR #685

---

## [2026-04-20] Corrupt HDFS Parquet File Causes Repeated GGW FAIL_AT_RUNNING (APA-144555)

**Where**: Any Spark job on War (prod-lva1) or Holdem that reads from an HDFS path containing stale/corrupt files

**What happens**:
Spark job fails with `FAIL_AT_RUNNING` on every retry (4/4 task attempts). Error signature:
```
java.lang.RuntimeException: hdfs://lva1-warnn01.../part-00000-....c000
    is not a Parquet file. expected magic number at tail [80, 65, 82, 49] but found [-72, -71, -44, -128]
```
Root cause: a previously interrupted write left a truncated file at the HDFS path. The valid Parquet magic bytes are `[80, 65, 82, 49]` = ASCII `PAR1`. Any other trailing bytes = corrupt/not-Parquet. Airflow retries all fail because the corrupt file persists on HDFS.

**Second footgun** (same ticket, after cleanup): If the `CREATE TABLE` DDL uses only `avro.schema.literal` with **no explicit column definitions**, Spark cannot infer schema when the HDFS location is empty or contains non-Parquet files. Fails at `DESCRIBE` with schema inference error. Fix: add explicit column definitions to the DDL alongside `avro.schema.literal`.

**How to fix**:
1. Identify the corrupt file from the error stack trace (the HDFS path is in the exception)
2. Verify: `hadoop fs -ls <hdfs_path>` — look for suspiciously small files or files with `modification_time=0`
3. Check for additional corrupt blocks: `hdfs fsck <hdfs_path> -files -blocks`
4. Delete: `hadoop fs -rm <hdfs_path>/part-XXXXX-corrupt-file` (requires proxy user credentials for that path, or Darwin shell)
5. Clear the failed Airflow task to re-queue it — GGW/Spark does not orphan the old job when `FAIL_AT_RUNNING` occurs
6. Add HDFS path cleanup (`hadoop fs -rm -r <path>/*`) before writes in the DAG/Spark job to prevent future stale-file buildup

**Note**: The Airflow team does not have HDFS write access to user data paths — the fix must be performed by the team that owns the proxy user (`cmoffline`, etc.) or via Darwin (`darwin_shell`) with appropriate credentials. Escalate to Grid Storage (`go/grid-data-policy`) if the team lacks CLI access.

**Prior art**: APA-129977 (same "expected magic number" error), APA-83967 (cmoffline table schema issues), APA-144555 (WAR cluster, `u_cmoffline.db/uploadable_customer_master_company_segment`, Apr 16–20)

---

## [2026-04-20] Holdem devDeploy Fails at createSetuSession — ptyproxy `[[` Syntax Error (APA-143228)

**Where**: Dali `devDeploy` to Holdem (`./gradlew devDeploy -Pcluster=holdem`), any task that SSHes through `ltx1-holdemgw01` via `ptyproxysh`

**What happens**:
`createSetuSession` task fails with:
```
/export/content/granular/etc/pty-proxy/scripts/ptyproxysh: eval: line 30: syntax error near unexpected token 'then'
```
The command that triggers it:
```
ssh -K -o StrictHostKeyChecking=no divygupt@ltx1-holdemgw01.grid.linkedin.com if [[ ! -f ~/identity.p12 ]]; then ...; fi;
```
`ptyproxysh` runs with `/bin/sh`, which does not support `[[ ]]` (bash-only syntax). The `li-dali-plugin` Gradle task constructs the SSH command with `[[ ]]`, causing a parse failure in the gateway shell. This has been a persistent bug since at least **2026-03-24** (27+ days open as of Apr 20, APA-143228 assigned Amit Panda).

**How to avoid / fix**:
- Upgrading `li-dali-plugin` to `6.0.134` or `7.0.1` (as suggested in comments) does **not** fix the issue — the ptyproxy shell incompatibility persists.
- The root fix is a server-side change to `ptyproxysh` to use `[ ]` instead of `[[ ]]` (POSIX sh compatible), but this requires the Holdem gateway team to deploy it.
- **Workaround**: Use SSH tunneling to manually establish a session and bypass `createSetuSession`:
  ```bash
  ssh -L 7552:ltx1-holdemhcat01.grid.linkedin.com:7552 <ldap>@ltx1-holdemgw01.grid.linkedin.com
  # In the SSH-connected terminal:
  id-tool grestin sign
  ./gradlew devDeploy -Pcluster=holdem -PproxyUser=<proxy>
  ```
  Note: even with this workaround, `createSetuSession` may still fail — as of Apr 14 the reporter reproduced the error even after SSH tunneling.
- **Status**: Still open, no confirmed workaround. Escalate to Dali/gateway oncall and reference APA-143228 for context.

**Who's affected**: Any team running Dali `devDeploy` for snapshot validation on Holdem. Does not affect Faro or War. Does not affect Airflow DAG execution (this is about developer workflow, not runtime).

**Source**: APA-143228 (Blocker, In Progress, created 2026-03-24, last updated 2026-04-20)

---

## [2026-04-23] Dali-data-spark joda-time Missing After Shaded-Spark Migration (APA-143126)

**Where**: Spark write path using `dali-data-spark` (dali-mp) after migrating to shaded-spark

**What happens**:
```
java.lang.NoClassDefFoundError: org/joda/time/R...
```
After the shaded-spark migration (starting v1.0.79 of the affected MP), running flows on Holdem fails with a missing joda-time class inside Dali's Coral library during a write operation via `VersionedWrites`. The flow last ran successfully on v1.0.48, before the shaded-spark migration.

**Root cause**: The shaded-spark migration repackages Spark dependencies, and joda-time was excluded from or not correctly shaded into the `dali-data-spark` classpath. This is the write-path analog of APA-133936 (which was the same issue for `dali-data-sdk`).

**How to avoid / fix**:
- Explicitly add `joda-time` to the classpath via `spark.driver.extraClassPath` / `spark.executor.extraClassPath` or `classpath_jars` in `SparkBatchOperator`.
- Or pin a `dali-data-spark` (dali-mp) version that includes the joda-time dependency fix.
- If hitting this after a shaded-spark migration: compare the fat JAR contents between the pre-migration and post-migration versions to identify missing transitive dependencies.
- **Pattern**: Fifth documented LinkedIn classpath conflict (after guava, commons-lang3, JAXB-RI shading, Hive UDF class loading). Shaded-spark migrations are a high-risk surface for dependency exclusion errors.
- Source: APA-143126 (Resolved, Apr 2026)

---

## [2026-04-23] dali-cli `last_known_good` Resolves to Deprecated Release (APA-143389)

**Where**: Any workflow using `dali-cli` / `Multiproduct("dali-mp").last_known_good` without explicit version pinning

**What happens**:
`Multiproduct("dali-mp").last_known_good` resolves to **11.0.1** — a soft-deprecated release (status=ACTIVE but `deprecatedAt` set) whose higher major version number shadows the current trunk (10.x). Without `LI_DALI_VERSION` explicitly pinned, every user silently gets the deprecated version instead of the active trunk.

**Root cause**: The `last_known_good` resolution algorithm picks the highest version with ACTIVE status, but does not exclude soft-deprecated releases (those with `deprecatedAt` set). Major version 11.x was published as a candidate, then deprecated, but its version number is higher than 10.x trunk.

**How to avoid / fix**:
- **Always pin** `LI_DALI_VERSION` in your environment or product-spec.json when using dali-cli to avoid silently pulling deprecated artifacts.
- Fixed in dali-cli PR #259 — the resolution now skips soft-deprecated releases. Ensure you are on a dali-cli version that includes this fix.
- Source: APA-143389 (Resolved, Apr 2026)

---

## [2026-04-23] Darwin Notebook Spark Session Fails with HTTPS Protocol HDFS Path (APA-144695)

**Where**: Darwin Notebooks creating Spark sessions with `spark.jars.ivy` pointing to HDFS snapshots

**What happens**:
```
java.lang.IllegalArgumentException: protocol = https host = null
```
When creating a Spark session in Darwin Notebook using a proxy user (e.g., `gtmmarketingdev`) with Holdem Spark 3.1 profile, and passing an HDFS snapshot path via `spark.jars.ivy`, the session creation fails because the HDFS path is formatted as an HTTPS URL that Spark cannot resolve.

**Root cause**: The HDFS path used in `spark.jars.ivy` was an HTTPS URL (e.g., from an HDFS web UI link) rather than a proper `hdfs://` URI. Spark's Ivy resolver expects a local path or `hdfs://` URI, not `https://`.

**How to avoid / fix**:
- Use `hdfs://` URIs (e.g., `hdfs://ltx1-holdemnn01.grid.linkedin.com/...`) not `https://` URLs for HDFS paths in Spark configuration.
- When copying paths from HDFS web UI (WebHDFS), convert the URL to the native `hdfs://` scheme.
- Source: APA-144695 (Resolved, Apr 2026)

---

## [2026-04-24] RDev References Old oklahoma-airflow Image After Repo Split

**Where**: `devcontainer.json` in user MPs, oklahoma-airflow-deployment

**What happens**:
After the March 2026 repo split (`oklahoma-airflow` → `oklahoma-airflow-deployment`), users' `devcontainer.json` may still reference the old `oklahoma-airflow` image name instead of `oklahoma-airflow-deployment`. The rdev creates successfully but uses a stale or wrong image, potentially missing recent fixes and features.

**How to avoid / fix**:
- Deployment PR #1052 (2026-04-21) adds a warning when `devcontainer.json` references the old image.
- Update your `devcontainer.json` image reference from `oklahoma-airflow/airflow-main-airflow-rdev` to `oklahoma-airflow-deployment/airflow-main-airflow-rdev`.
- Use `airflow-main-airflow-rdev:stable` tag (not a pinned version) to always get the latest validated image.
- Source: oklahoma-airflow-deployment PR #1052

---

## [2026-04-24] picli crt-sync FileNotFoundError for Hyphenated App Names

**Where**: `picli airflow crt-sync`, apps with hyphenated Gradle module names

**What happens**:
`picli airflow crt-sync` constructs a file path using the raw Gradle module name (e.g., `my-dag-app`). But the Python package directory on disk uses underscores (`my_dag_app`). The mismatch causes a `FileNotFoundError` when picli tries to read configuration files during CRT sync.

**How to avoid / fix**:
- Fixed in picli PR #686 (2026-04-24): module names are now normalized from `-` to `_` before constructing paths.
- If on an older picli version: manually rename the Gradle module to use underscores, or update picli.
- This is distinct from the earlier `.okl_setup.json` path normalization fix (picli PR #684, 2026-04-17) — that fixed `project_name` normalization, while PR #686 fixes the CRT sync path specifically.
- Source: picli PR #686

---

## [2026-04-27] hdfs-instrumentation Inconsistent Class Shading (APA-144546)

**Where**: Spark jobs on Holdem that depend on `hdfs-instrumentation-2.0.234`

**What happens**:
```
java.lang.NoClassDefFoundError: org/apache/hadoop/fs/InstrumentedFSDataInputStream
```
`com.linkedin.hadoop.metrics.fs.PerformanceTrackingFSDataInputStream` refers to `org.apache.hadoop.fs.InstrumentedFSDataInputStream`, but the same JAR relocates it to `hdfs_metrics_shade.org.apache.hadoop.fs.InstrumentedFSDataInputStream`. The inconsistent shading means internal class references break at runtime — one class was relocated, but its caller was not updated to use the relocated path.

**How to avoid / fix**:
- Pin to a version of `hdfs-instrumentation` where shading is consistent (before 2.0.234, or after the fix).
- If stuck on 2.0.234: use `spark.driver.userClassPathFirst=true` and `spark.executor.userClassPathFirst=true` to prioritize application-provided HDFS classes over the shaded ones.
- **Pattern**: Sixth documented LinkedIn classpath shading conflict (after guava/extraClassPath, commons-lang3, JAXB-RI/spark-core-metrics, Hive UDF class loading, dali-data-spark joda-time). The common thread: framework JARs shade transitive dependencies but leave internal references pointing to the original package, breaking at runtime.
- Source: APA-144546 (Closed — Fixed, Apr 2026, Holdem Pro cluster)

---

## [2026-04-27] WEAVER_URLS Override in RDev Causes Fabric/Weaver Failures (DEPEND-102101)

**Where**: Oklahoma rdev environment setup (`setup_oklahoma_rdev_env.sh`)

**What happens**:
The rdev environment setup script exported a hardcoded `WEAVER_URLS` value. This override was never directly referenced in Airflow code — it was only used internally by the `lipy-fabric` library. Over time, the hardcoded URL drifted from the actual Weaver discovery URL, causing Fabric/Weaver failures in rdev. Users saw intermittent failures when `lipy-fabric` tried to resolve service endpoints via the wrong Weaver URL.

**How to avoid / fix**:
- Fixed in deployment PR #1063 (2026-04-24): the `WEAVER_URLS` export was removed from rdev setup.
- If you're on an older rdev image that still has this override, either recreate the rdev or manually unset `WEAVER_URLS` in your shell: `unset WEAVER_URLS`.
- The `lipy-fabric` library discovers the correct Weaver URL automatically — no override is needed.
- Source: DEPEND-102101, oklahoma-airflow-deployment PR #1063

---

## [2026-04-27] LCD Deployments Fail at EKG/Canary Step (ArgoCD Incompatibility)

**Where**: Oklahoma-Airflow LCD (LinkedIn Continuous Delivery) deployment pipeline

**What happens**:
When Oklahoma-Airflow was migrated from CRT to LCD (BDP-98089), the LCD pipeline included EKG/Canary deployment steps by default. However, EKG/Canary is **not supported** for MPs deployed with ArgoCD. The LCD pipeline would reach the EKG step and fail, blocking all deployments. This was first discovered in PR #1060 (Apr 23), reverted in PR #1064 (Apr 24), and required three additional PRs to resolve:
- PR #1066 (re-onboard with simpler config)
- PR #1067 (fix pipeline creation trigger)
- PR #1070 (disable EKG/canary steps)

**How to avoid / fix**:
- Fixed in deployment PR #1070 (2026-04-27): EKG/canary steps are now disabled in the LCD config.
- If adding a new deployment step to the LCD pipeline, verify it is compatible with ArgoCD-deployed MPs. EKG/Canary assumes traditional deployment mechanisms that ArgoCD does not support.
- Source: BDP-98089, oklahoma-airflow-deployment PRs #1060, #1064, #1066, #1067, #1070

---

## See Also
- [Patterns](../patterns.md)
- [Troubleshooting](../references/troubleshooting.md)

## Jira Auto-Triage Script Comment Extraction Bug

**Files**: `server/tools/jira_prompt.py` (lines 143-145) and `server/tools/jira_tool.py` (line 309)

**Issue**: Triage prompt defines delimiters (`===TRIAGE_COMMENT_START===` ... `===TRIAGE_COMMENT_END===`) but extraction logic at line 309 fails to parse content between delimiters correctly, posting only partial text (e.g., "and" instead of full triage comment) to Jira tickets.

**Impact**: Auto-triage comments are incomplete and unhelpful for ticket assignees.

**Workaround**: Manual Jira comments via web UI work correctly.
