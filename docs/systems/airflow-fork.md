> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# System — LinkedIn's Airflow Fork (li-2.9.2)

> LinkedIn's production Airflow is a fork of Apache Airflow 2.9.2, versioned as `2.9.2.X` (currently at v2.9.2.170+), with a set of LinkedIn-specific features layered on top: DAG-level SLA callbacks, PipelineMD root-cause links, DagRun conf size enforcement, and several InnoDB lock contention fixes.

## Base Version

Apache Airflow **2.9.2** (`airflow/__init__.py`: `__version__ = "2.9.2"`).

The branch tracking LinkedIn's changes is `BR_REL_li-2.9.2`. Git tags follow the pattern `v2.9.2.X` (e.g. `v2.9.2.170`). The patch number increments with every PR merged.

The fork lives at `linkedin-multiproduct/airflow` and is maintained by the Grid Execution (oklahoma) team.

---

## Key LinkedIn Customizations

### 1. DAG-Level SLA (`dag_sla_timeout` + `DagSlaCBRequest`)

Standard Airflow SLAs apply at the task level and use a polling-based miss detector that only fires after the DAG run completes. LinkedIn added a **DAG-level SLA** that fires a callback mid-run, at a wall-clock deadline relative to the DAG run's start time.

How it works:

- DAG authors set `dag_sla_timeout: timedelta` and `dag_sla_miss_callback` on the `DAG` object.
- The scheduler loop (`_schedule_dag_run`) computes `sla_deadline = dag_run.start_date + dag.dag_sla_timeout` each cycle.
- If the current scheduler loop's scheduling window straddles the deadline (i.e., `previous_last_scheduling_decision < sla_deadline <= latest_last_scheduling_decision`), a `DagSlaCBRequest` is enqueued.
- The DAG processor picks it up and fires `dag_sla_miss_callback` with the usual DAG-run context.

The callback class is named `DagSlaCBRequest` (not `DagSlaCallbackRequest`) specifically because `DbCallbackRequest.callback_type` is a `VARCHAR(20)` column in MySQL — the longer original name overflowed. A backward-compat alias `DagSlaCallbackRequest = DagSlaCBRequest` is kept so old DB rows can still be deserialized.

Files: `airflow/models/dag.py`, `airflow/callbacks/callback_requests.py`, `airflow/jobs/scheduler_job_runner.py`, `airflow/dag_processing/processor.py`, `airflow/serialization/serialized_objects.py`.

### 2. PipelineMD Global Extra Link

**PipelineMD** is LinkedIn's root-cause analysis tool. For tasks that fail, a red "PipelineMD (Experimental)" button is surfaced in the Airflow UI's task-instance detail panel.

Originally this was a frontend-only injection in `ExtraLinks.tsx`, which meant it was invisible for tasks that had no operator-level extra links (e.g. `sensorArray`). As of PR #107 (commit `5e6ad7c7`), PipelineMD is registered as a **global extra link** plugin in `lipy-airflow-providers` (`apache-airflow-providers-lnkd`), so it flows through the standard `global_operator_extra_links` mechanism and appears for every task.

Frontend behavior (preserved): button is always shown, but is only clickable when `dagRunState === "failed"`.

Files changed in fork: `airflow/www/static/js/dag/details/taskInstance/ExtraLinks.tsx` (removed the frontend PMD-fetch hook, now treats PMD as a regular named link `"PipelineMD"`).

### 3. DagRun `conf` Size Validation (65,535-byte limit)

MySQL stores the DagRun `conf` dict in a `BLOB` column, which has a hard 65,535-byte limit. Passing an oversized `conf` causes the scheduler to fail trying to write the DagRun to the database, blocking task execution silently.

LinkedIn added `validate_dagrun_conf_size(conf)` in `airflow/models/dagrun.py`, called from `DAG.create_dagrun()`. It serializes the conf via `pickle.dumps()` and raises `AirflowException` at trigger time rather than letting the DB error propagate.

- Default limit: **65,535 bytes** (configurable via `airflow.cfg` key `core.max_dagrun_conf_size_bytes`).
- Error message directs users to store large payloads externally (XCom, Variables, file storage) and pass only references in conf.

Files: `airflow/models/dagrun.py`, `airflow/models/dag.py`.

### 4. Scheduler Lock Contention Fixes

A cluster of PRs addressed InnoDB lock contention that was causing scheduler latency spikes and LOCK WAIT timeouts at LinkedIn's scale (thousands of concurrent task instances). Three distinct areas were fixed:

**a. Sensor reschedule lock (`_do_handle_reschedule`)**: See codebase/airflow-fork-internals for full details.

**b. DAG metadata write-lock contention** (PR #87, commit `ebb36cd9` — **REVERTED by PR #114, 2026-04-09**): The original fix used a two-phase `SKIP LOCKED` approach, but this introduced a **correctness bug** — when a processor cannot acquire the lock, it silently skips the metadata write, permanently losing updates to `is_paused`, `next_dagrun`, tags, and dataset references. The revert restores the blocking `SELECT FOR UPDATE` (without `SKIP LOCKED`). The lock contention issue remains **unresolved**.

**e. DAG serialization duplicate-key race** (PR #111, 2026-04-10): Two concurrent dag-processors parsing the same new DAG race to INSERT into `serialized_dag`. The loser gets `IntegrityError`, which puts the session into `PendingRollbackError` state, cascading failures across all subsequent DB operations in that processor cycle. Fix: catch `IntegrityError` on INSERT, rollback, fall through to UPDATE.

**f. Executor event deadlock** (PR #109, 2026-04-09): PR #83's QUEUED event split introduced a MySQL deadlock between the plain `UPDATE` (QUEUED) and `FOR UPDATE SKIP LOCKED` (terminal) phases when two schedulers process overlapping TI sets. Fix: `@retry_db_transaction` decoration on the QUEUED phase.

**c. RTIF gap lock contention** (PR #80, commit `32f91ec8`): `RenderedTaskInstanceFields.write()` used `session.merge()` which issues a SELECT before INSERT. For mapped tasks with many indices, InnoDB's gap locks on the clustered index serialized concurrent writes. Fix: MySQL-specific `INSERT ... ON DUPLICATE KEY UPDATE` which skips the prior SELECT and acquires only insert-intention locks (compatible with concurrent mapped-task writes).

**d. Worker `IDLE_IN_TRANSACTION` from redundant dep checks** (PR #82, commit `6b9c4344`): Workers were running pool/concurrency dependency checks that are only meaningful at the scheduler. These checks held open DB transactions while waiting for GGW/Kubernetes to confirm task state. Removing them reduced `IDLE_IN_TRANSACTION` connection time significantly.

### 5. Executor Event Batching: QUEUED Split

(PR #83, commit `b04bf4f4`) The `_process_executor_events()` loop originally ran all executor events (QUEUED, FAILED, SUCCESS) through a single `FOR UPDATE SKIP LOCKED` path. QUEUED events only need to write `external_executor_id` (the pod name) — they don't need to lock the TI row. Splitting them out into a direct `UPDATE` (no lock) reduces lock contention on the terminal-event path.

### 6. NKS Migration: Execution Balancer

The `ExecutionBalancer` (`airflow/executors/execution_balancer.py`) enables gradual traffic migration from LKS (old cluster) to NKS (new Kubernetes cluster). Key features:

- **Pin/block by DAG ID regex**: Airflow Variables `PINNED_DAGS_FOR_EXEC_VAR_NAME` and `BLOCKED_DAGS_FOR_EXEC_VAR_NAME` control which DAGs execute on which cluster.
- **Shard-based rampup**: DAG IDs are hashed (SHA1 -> last 4 hex chars -> mod 128) into 128 shards. The `DAG_SHARDS_TO_RAMPUP` variable controls which shards execute on the new cluster, allowing incremental traffic migration.
- **SHARD_COUNT = 128**. Shard lists can be expressed as ranges (e.g. `0-63`) or individual values.

### 7. `DAGS_ON_OLD_VERSION` Backdoor

`airflow.cfg` key `SCHEDULER.DAGS_ON_OLD_VERSION` accepts a list of DAG IDs. For those DAGs, `TaskInstance.generate_command()` emits `/opt/airflow/airflow2.9/bin/airflow tasks run` (the pinned older binary) instead of the default `airflow` entrypoint. This allows a subset of DAGs to keep running on an older Airflow image during a staged upgrade rollout.

### 8. Other Notable Changes

| Change | PR | Notes |
|--------|----|-------|
| `cleanup_stale_dags` disabled in scheduler | #86 (commit `219033c8`) | Disabled by gating on `and False`; DagProcessor already handles stale cleanup; the scheduler version caused DAG drops in production |
| `/admin` endpoint for DNS Disco healthchecks | Commit `732f73da` | Returns plain `"GOOD"`, used by LinkedIn's DNS Disco health-check infrastructure |
| Flask-Compress gzip for API responses | PR #56 (commit `bde2e7f7`) | `flask_compress.Compress` added to `airflow/www/app.py`; min size 1 KB, algorithm gzip |
| Kubernetes executor pod log capture | Commit `ac0aad43` | On generic "exit code 1" failures, the executor reads pod logs to provide pre-execution error context |
| Config override UI on trigger page | PRs #86, #92 | `grid_gateway_params` and `_overridable_attrs_map` fields exposed in the DAG trigger form so users can override task parameters (num-executors, etc.) at run time |
| Override field `name` attributes removed | PR #104 | Browser was including override fields in POST body -> overflowing MySQL TEXT `extra` column (65,535 bytes). Fix: removed `name` attrs. |
| Jinja-template false change detection fix | PR #101 | `type=number` inputs cannot display Jinja expressions -> browser coerces to empty -> false "changed" detection. Fix: compare against raw template string. |
| `dagrun.first_task_start_delay` metric fix | PR #105 | Metric was inflated by rescheduled sensors (start_date reset each reschedule). Fix: filter out `UP_FOR_RESCHEDULE` tasks. |
| Custom histogram buckets via OTEL | PR #58, #63 | `airflow/metrics/histogram_buckets.py` defines LinkedIn-tuned bucket boundaries for delay, duration, pool-slot, and UI metrics |
| `LI_BASE_USER` DAGs hidden from home page | PR #77 (commit `dd2274b6`) | Public/base-user DAGs filtered from the `/home` listing |
| Serialized-DAG metric sent early in processor | PR #34 | Metric emitted at start of `DagFileProcessorProcess` rather than end, to reduce metric drops during long parses |
| Log DAG IDs during DAG deletion | PR #112 (2026-04-20) | `DagFileProcessorManager._refresh_dag_dir` now logs specific DAG IDs being deleted (not just count). Aids debugging silent DAG disappearances and the dag-processor file stats race condition. |
| `dagrun.deadlocked` metric | PR #120 (2026-04-22) | New metric `Stats.incr("dagrun.deadlocked")` emitted in `_schedule_dag_run` when a DagRun's `task_instance_deadlocked()` returns True (all tasks finished or none schedulable, but run not complete). Aids alerting on deadlocked DAG runs. |
| Test fixes | PR #115 (2026-04-21) | Test suite maintenance — fixes for flaky or broken unit tests in the LinkedIn fork. |

---

## Versioning

Tags follow `v2.9.2.X` where X is a monotonically increasing patch counter. Each PR merge to `BR_REL_li-2.9.2` increments X. The base `__version__` string in `airflow/__init__.py` stays `"2.9.2"` (Apache upstream value); the LinkedIn patch number is only in the git tag.

As of 2026-04-14, the latest deployed tag is **v2.9.2.170** (deployed via oklahoma-airflow-deployment PR #1044, 2026-04-13).

---

## Upgrade Process Considerations

When rebasing to a new Apache upstream (e.g. 2.9.3 or 2.10):

1. The `VARCHAR(20)` constraint on `DbCallbackRequest.callback_type` requires `DagSlaCBRequest` to keep its short name. Do not rename.
2. The RTIF `INSERT ... ON DUPLICATE KEY UPDATE` is MySQL-specific; the upstream `session.merge()` path is preserved for other DB backends.
3. `innodb_lock_wait_timeout = 4` is MySQL-only; the code guards on `session.bind.dialect.name == "mysql"`.
4. The `ExecutionBalancer` reads from Airflow Variables at runtime; verify those Variables exist in the new cluster before enabling `EXECUTION_BALANCER_ENABLED`.
5. The PipelineMD plugin must be deployed in `lipy-airflow-providers` (`apache-airflow-providers-lnkd`) before upgrading the fork; the frontend now relies on the backend to emit the PMD extra link.

---

## See Also

- [lipy-airflow-providers](lipy-airflow-providers.md) — the PipelineMD global extra link plugin lives here
- [GGW](ggw.md) — Grid Gateway, the primary execution target
- [Codebase Overview](../codebase/README.md) — Repo structure & local setup

## DAG Serialization Race Conditions

When multiple Airflow processors (scheduler, webserver, etc.) serialize DAGs concurrently, duplicate-key IntegrityErrors (MySQL error 1062) can occur if the same DAG is written to the database simultaneously. Instead of retrying on 1062, recognize that another processor already successfully wrote the serialized DAG. Treat 1062 as success by returning early rather than retrying. This pattern is implemented in `airflow/models/dagbag.py` in the DAG serialization exception handling logic.

### Concurrent DAG Serialization (1062 Race Condition Handling)

LinkedIn's Airflow fork optimizes concurrent DAG serialization by gracefully handling duplicate-key IntegrityErrors (1062). When multiple processors serialize the same DAG simultaneously, the first write succeeds; subsequent processors detect the duplicate key and return success without retry. This avoids unnecessary reprocessing and reduces lock contention in high-concurrency environments.
