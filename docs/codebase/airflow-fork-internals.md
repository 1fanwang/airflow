# Codebase — Airflow Fork Internals (li-2.9.2)

> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

## Sensor Reschedule Lock (`_do_handle_reschedule`)

### The Problem

When a sensor re-schedules itself (raises `AirflowRescheduleException`), the worker calls `TaskInstance._handle_reschedule()`, which writes a `TaskReschedule` row and locks the parent `DagRun` row to prevent deadlocks. Under high concurrency (many sensors rescheduling simultaneously), multiple workers compete for the same `DagRun` row lock.

MySQL's default `innodb_lock_wait_timeout` is **50 seconds**. A blocked worker holds a DB connection in `IDLE_IN_TRANSACTION` state for up to 50 s before giving up, stacking up connection exhaustion across the pool.

### The Fix (PR series: `fix/p02-sensor-reschedule-5s-timeout-15-retries`)

`_do_handle_reschedule` is decorated with `@retry_db_transaction(retries=10)`.

Before acquiring the `DagRun` row lock, it issues a per-session override:

```python
if session.bind.dialect.name == "mysql":
    session.execute(text("SET innodb_lock_wait_timeout = 4"))
```

This sets a **4-second per-attempt lock timeout** (not 50 s). If InnoDB cannot acquire the lock within 4 s, it raises `OperationalError(1205, 'Lock wait timeout exceeded')`. The `@retry_db_transaction` decorator catches this, rolls back, waits (exponential backoff via `tenacity.wait_random_exponential(multiplier=0.5, max=5)`), and retries.

**Worst-case total window** with `retries=10`:
- 10 x 4 s lock-wait = 40 s
- 9 backoff gaps: up to 0.5 + 1 + 2 + 4 + 5 + 5 + 5 + 5 + 5 = 32.5 s
- Total cap: **~72.5 seconds** before giving up

This caps blocking time and prevents indefinite starvation of other lock waiters.

**History of tuning** (visible in git log):
- Original: 5 s timeout, 15 retries (~145 s window) — `5e6ad7c` era
- Tuned to 4 retries, 4 s — intermediate
- Final: `retries=10`, `innodb_lock_wait_timeout=4`

**Key code path** (`airflow/models/taskinstance.py`):

```
_handle_reschedule()            # public, test_mode guard only
  └─ _do_handle_reschedule()   # @retry_db_transaction(retries=10)
       ├─ SET innodb_lock_wait_timeout = 4  (MySQL only)
       ├─ SELECT DagRun FOR UPDATE  (acquires row lock)
       ├─ INSERT TaskReschedule
       ├─ SET ti.state = UP_FOR_RESCHEDULE
       └─ session.merge(ti)
```

---

## Executor Event Batching: QUEUED Split

File: `airflow/jobs/scheduler_job_runner.py`, method `_process_executor_events()`.

### Upstream Behavior

All executor events (QUEUED, FAILED, SUCCESS) were processed in a single `FOR UPDATE SKIP LOCKED` batch. QUEUED events just needed to write `external_executor_id` (the Kubernetes pod name), but they went through the heavy locking path anyway.

### LinkedIn Change (PR #83, commit `b04bf4f4`)

Two-phase processing:

**Phase 1 — QUEUED events** (no lock):
```python
session.execute(
    update(TI)
    .where(TI.dag_id == ..., TI.task_id == ..., TI.run_id == ..., TI.map_index == ...)
    .values(external_executor_id=external_id)
    .execution_options(synchronize_session=False)
)
```
Rationale: Only the scheduler that dispatched the TI emits a QUEUED event for it (it holds the pod name from its own executor instance). No multi-scheduler race exists, so no row lock is needed.

**Phase 2 — Terminal events** (FAILED/SUCCESS):
The original `FOR UPDATE SKIP LOCKED` path is preserved for terminal events where multi-scheduler race conditions are real.

---

## DAG Metadata Write-Lock Contention

File: `airflow/models/dag.py`, method `DAG.bulk_write_to_db()`.

### Upstream Behavior

`SELECT DagModel FOR UPDATE` — no `SKIP LOCKED`. Two dag-processors trying to sync the same DAG at the same time would block on each other.

### LinkedIn Fix Attempt (PR #87, commit `ebb36cd9`) — REVERTED

Three-step pattern:

1. **Non-locking SELECT** — determines which DAG IDs already exist in `dag` table.
2. **`FOR UPDATE SKIP LOCKED`** — acquires locks on rows not held by another processor.
3. **Classification**:
   - `existing_dags`: rows we locked → update normally
   - `skipped_dag_ids` = `all_existing_ids - locked_ids` → skip (other processor is writing these)
   - `new_dag_ids` = `dag_ids - all_existing_ids` → insert new `DagModel` rows

Dataset references for `skipped_dag_ids` are also skipped (the code guards `if dag.dag_id in skipped_dag_ids: continue` in the dataset reference sync loop).

### Revert (PR #114, 2026-04-09) — Correctness Bug

PR #87 was **reverted** because the `SKIP LOCKED` approach introduced a **correctness bug**: when a dag-processor cannot acquire the lock on a `dag` table row, it silently skips the write and moves on. This means that **metadata updates are permanently lost** for any DAG whose row was locked at the time — `is_paused`, `next_dagrun`, tag associations, and dataset references are never written. The existence pre-check only prevented the "re-create as new" bug but could not prevent the silent-skip data loss.

**Status as of 2026-04-13**: Reverted to upstream behavior (blocking `SELECT FOR UPDATE` without `SKIP LOCKED`). The lock contention issue is **unresolved** — the team is investigating alternative approaches that preserve correctness.

---

## RTIF Gap Lock Contention

File: `airflow/models/renderedtifields.py`, method `RenderedTaskInstanceFields.write()`.

### Upstream Behavior

`session.merge(self)` — SQLAlchemy issues a `SELECT` before `INSERT` to decide whether to insert or update. Under `REPEATABLE READ` (MySQL default), InnoDB acquires a **gap lock** on the clustered index range to protect the SELECT result. For mapped tasks, all map indices share the same `(dag_id, task_id, run_id)` prefix → same index region → each concurrent worker's gap lock blocks the others' inserts.

### LinkedIn Fix (PR #80, commit `32f91ec8`)

MySQL-only `INSERT ... ON DUPLICATE KEY UPDATE`:

```python
from sqlalchemy.dialects.mysql import insert
stmt = insert(RenderedTaskInstanceFields).values(...)
stmt = stmt.on_duplicate_key_update(
    rendered_fields=stmt.inserted.rendered_fields,
    k8s_pod_yaml=stmt.inserted.k8s_pod_yaml,
)
session.execute(stmt)
```

No prior SELECT → no gap lock → concurrent mapped-task workers acquire only **insert-intention locks**, which are fully compatible with each other.

Non-MySQL backends continue to use `session.merge()`.

---

## DAG Serialization Duplicate-Key Race (PR #111, 2026-04-10)

File: `airflow/models/serialized_dag.py`

### The Problem

In HA deployments (multiple schedulers, each with multiple dag-processors), two processors can parse the same DAG file simultaneously. When a DAG is being serialized for the first time, both processors race to INSERT the same row into `serialized_dag`:

1. Both check → row doesn't exist → both attempt INSERT.
2. One succeeds; the other gets a `IntegrityError` (duplicate key).
3. The losing processor's session enters `PendingRollbackError` state. All subsequent DB operations on that session (including unrelated DAG processing) fail with `PendingRollbackError`, cascading across the entire processor cycle.

### The Fix

Wrapped the INSERT in a try/except for `IntegrityError`. On duplicate-key collision, the code catches the error, rolls back the session, and falls through to an UPDATE of the existing row instead. This prevents the `PendingRollbackError` cascade without requiring an explicit lock.

---

## Executor Event Deadlock Fix (PR #109, 2026-04-09)

File: `airflow/jobs/scheduler_job_runner.py`, method `_process_executor_events()`.

### The Problem

PR #83's two-phase split (QUEUED events via plain `UPDATE`, terminal events via `SELECT FOR UPDATE SKIP LOCKED`) introduced a **MySQL deadlock**. The plain `UPDATE` for QUEUED events acquires row-level locks in a different order than the `FOR UPDATE SKIP LOCKED` in the terminal-event phase. When two schedulers process overlapping TI sets, their lock acquisition orders can conflict, triggering InnoDB deadlock detection (`Error 1213: Deadlock found`). The scheduler crashed with an unhandled `OperationalError`.

### The Fix

Added `@retry_db_transaction` decoration to the QUEUED-event processing phase, so that deadlocks are caught, rolled back, and retried automatically (same pattern used elsewhere in the codebase). The two-phase split is preserved for its performance benefits; the retry handles the rare deadlock case.

---

## Scheduler Critical Section Modifications

The scheduler's critical section is `_critical_section_enqueue_task_instances()`, which holds a `pool FOR UPDATE` lock.

### Pool/Concurrency Dep Checks Removed from Workers (PR #82, commit `6b9c4344`)

Workers were running pool-slot and concurrency dependency checks (`PoolSlotsAvailableDep`, `ConcurrencyDep`) that are only meaningful at scheduling time. These checks were opening DB transactions and holding connections while waiting for GGW job state, causing `IDLE_IN_TRANSACTION` connection accumulation. They were removed from the worker-side dep evaluation.

### `cleanup_stale_dags` Disabled (PR #86, commit `219033c8`)

```python
if self._standalone_dag_processor and False:   # LinkedIn: intentionally disabled
    timers.call_regular_interval(..., self._cleanup_stale_dags)
```

The scheduler's stale DAG cleanup was causing accidental DAG drops in production. `DagProcessor` already handles this; the scheduler's version is permanently disabled by the `and False` gate.

### Execution Balancer Integration

`_critical_section_enqueue_task_instances()` now accepts an `ExecutionBalancer` instance and passes it to `_executable_task_instances_to_queued()`. The balancer extends the TI query with:

```python
not_(TI.dag_id.op('REGEXP')(execution_balancer.blocked_dags_for_exec_regex)),
(func.conv(func.substring(func.sha1(TI.dag_id), -4), 16, 10) % SHARD_COUNT).in_(execution_balancer.dag_shards_to_execute),
TI.dag_id.op('REGEXP')(execution_balancer.pinned_dags_for_exec_regex)
```

---

## Model Changes

### `DAG` model additions

| Field | Type | Purpose |
|-------|------|---------|
| `dag_sla_timeout` | `timedelta \| None` | Wall-clock deadline for a DAG run; triggers `DagSlaCBRequest` when crossed |
| `dag_sla_miss_callback` | callable / list | Callback(s) fired when `dag_sla_timeout` is exceeded |
| `has_dag_sla_miss_callback` | `bool` (derived) | `True` if `dag_sla_miss_callback is not None`; serialized to avoid deserializing callables |

### `DagRun` additions

| Function | Purpose |
|----------|---------|
| `validate_dagrun_conf_size(conf)` | Validates serialized conf <= `MAX_DAGRUN_CONF_SIZE` (default 65,535 B) before DB write |
| `MAX_DAGRUN_CONF_SIZE` | Module-level constant, configurable via `core.max_dagrun_conf_size_bytes` |

### `TaskInstance` changes

| Change | Notes |
|--------|-------|
| `_do_handle_reschedule` split from `_handle_reschedule` | `@retry_db_transaction(retries=10)` added; `innodb_lock_wait_timeout = 4` set per-session |
| `generate_command()` | If `dag_id` is in `SCHEDULER.DAGS_ON_OLD_VERSION`, emits `/opt/airflow/airflow2.9/bin/airflow tasks run` |

### `RenderedTaskInstanceFields` changes

`write()` uses `INSERT ... ON DUPLICATE KEY UPDATE` on MySQL instead of `session.merge()`.

---

## Webserver / API Changes

### `airflow/www/views.py`

| Change | Notes |
|--------|-------|
| `/admin` endpoint | Returns `"GOOD"` — used by LinkedIn's DNS Disco healthcheck infrastructure |
| `build_task_override_fields()` | New function; reads `_overridable_attrs_map` and `grid_gateway_params` from each task, builds per-task override field definitions for the trigger page UI |
| XCom view uses `run_id` | Fixed to use `run_id` instead of `execution_date` for XCom lookups (PR #96, commit `cc8e4d66`) |
| Override field `name` attributes removed | Override fields on the trigger page had `name` attributes causing the browser to include every field value in the POST body, overflowing the MySQL `extra` column (TEXT, 65,535 bytes). Fix: removed `name` attrs so browser excludes override fields from form submission (PR #104, 2026-04-06) |
| Jinja-template false change detection fix | `type=number`/`type=checkbox` inputs cannot display Jinja expressions (e.g., `{{ 90 * params.workload_size }}`); the browser coerces them to empty/0, causing false "changed" detection. Fix: compare against raw template string when `field.value` is a Jinja expression (PR #101, 2026-04-04) |

### `airflow/www/app.py`

Flask-Compress added:
```python
compress = Compress()
flask_app.config["COMPRESS_MIN_SIZE"] = 1024
flask_app.config["COMPRESS_ALGORITHM"] = ["gzip"]
compress.init_app(flask_app)
```

### API schema (`airflow/api_connexion/schemas/dag_schema.py`)

`dag_sla_timeout` field added to `DAGSchema` as a `TimeDeltaSchema` dump-only field.

---

## Metrics Infrastructure

File: `airflow/metrics/histogram_buckets.py`

LinkedIn added `HISTOGRAM_BUCKET_VIEW_SPECS` — a tuple of `(pattern, aggregation)` pairs that configure OTEL histogram bucket views:

| Pattern | Aggregation |
|---------|-------------|
| `pool.*_histogram` | Explicit buckets: 0–10000 (count-style) |
| `*_duration` | Exponential (OTel SDK default: max_size=160, max_scale=20) |
| `*_delay` | Exponential |
| `*_bytes` | Exponential |
| `ui.*_ms` | Exponential |
| `*_count` | Explicit count-style buckets |

Patterns use glob semantics (fnmatch). Views are applied with the configured prefix at runtime via `build_histogram_bucket_views(prefix)`.

IPv6 support added for OTel metrics emission (PR #50).

---

## Metrics Fix: `dagrun.first_task_start_delay` (PR #105, 2026-04-07)

The `dagrun.first_task_start_delay` metric was inflated by rescheduled sensors. The task instance's `start_date` is reset on each reschedule cycle, so the metric computed the delay from DagRun start to the *last* sensor reschedule start — not to the first genuine task execution. Fix: the metric now filters out tasks in `UP_FOR_RESCHEDULE` state, capturing only the first non-sensor task start.

---

## New Metric: `dagrun.deadlocked` (PR #120, 2026-04-22)

File: `airflow/jobs/scheduler_job_runner.py`, method `_schedule_dag_run()`.

### What it does

When the scheduler evaluates a DagRun and finds it deadlocked (via `dagrun.task_instance_deadlocked()` — all task instances are in a terminal or unschedulable state, but the run is not complete), it now emits:

```python
Stats.incr("dagrun.deadlocked")
```

With the `airflow.` OTEL prefix, this appears in Geneva/MDM as `airflow.dagrun.deadlocked`.

### Why it matters

Deadlocked DagRuns were previously silent — the run stayed in `running` state indefinitely with no metric signal. The scheduler would eventually set the run to `failed`, but there was no proactive alerting mechanism. This metric enables Grafana alerting on deadlock occurrence.

### Deadlock conditions

A DagRun is considered deadlocked when:
- All task instances are finished (success, failed, skipped, upstream_failed) **or** no task instance is schedulable
- But the DagRun itself is not in a terminal state (not success, not failed)
- Common cause: all remaining tasks depend on a failed task with `trigger_rule=all_success` and no available retry

---

## Callbacks Infrastructure

File: `airflow/callbacks/callback_requests.py`

`DagSlaCBRequest` class added. The class name is intentionally <=20 characters because `DbCallbackRequest.callback_type` is `VARCHAR(20)`:

```python
class DagSlaCBRequest(CallbackRequest):
    """VARCHAR(20) constraint on callback_type column drives the short name."""
    def __init__(self, full_filepath, dag_id, run_id, processor_subdir, msg=None):
        ...

DagSlaCallbackRequest = DagSlaCBRequest   # backward-compat alias for old DB rows
```

---

## Serialization Changes

File: `airflow/serialization/serialized_objects.py`

`dag_sla_timeout` is serialized/deserialized via the existing `timedelta` handling path. `has_dag_sla_miss_callback` is added to the set of boolean DAG attributes that are serialized (so the scheduler can check it without deserializing the callback itself).

---

## Key Changed Files vs. Upstream

| File | Nature of change |
|------|-----------------|
| `airflow/models/taskinstance.py` | Reschedule lock retry, `generate_command` DAGS_ON_OLD_VERSION, dep check removal |
| `airflow/models/dag.py` | DAG-level SLA fields, `bulk_write_to_db` SKIP LOCKED fix, conf size validation call |
| `airflow/models/dagrun.py` | `validate_dagrun_conf_size`, `MAX_DAGRUN_CONF_SIZE` |
| `airflow/models/renderedtifields.py` | MySQL `INSERT ... ON DUPLICATE KEY UPDATE` |
| `airflow/jobs/scheduler_job_runner.py` | QUEUED event batching split, cleanup_stale_dags noop, ExecutionBalancer integration |
| `airflow/callbacks/callback_requests.py` | `DagSlaCBRequest` class |
| `airflow/dag_processing/processor.py` | `DagSlaCBRequest` dispatch, SLA miss callback invocation |
| `airflow/metrics/histogram_buckets.py` | Custom OTEL histogram bucket views (new file) |
| `airflow/metrics/otel_logger.py` | Histogram instrument, IPv6 support |
| `airflow/executors/execution_balancer.py` | NKS/LKS shard-based execution balancer (new file) |
| `airflow/www/app.py` | Flask-Compress gzip |
| `airflow/www/views.py` | `/admin` healthcheck, config override UI, XCom run_id fix |
| `airflow/www/static/js/dag/details/taskInstance/ExtraLinks.tsx` | PipelineMD as backend-driven extra link |
| `airflow/providers/cncf/kubernetes/executors/kubernetes_executor_utils.py` | Pod log capture on generic failures, DAGS_ON_OLD_VERSION image pin |
| `airflow/api_connexion/schemas/dag_schema.py` | `dag_sla_timeout` field |
| `airflow/serialization/serialized_objects.py` | DAG-level SLA serialization |

---

## See Also

- [Airflow Fork](../systems/airflow-fork.md) — high-level feature summary and versioning
- [Performance](performance.md) — load testing and lock contention benchmarks
- [Gotchas](gotchas.md) — MySQL-specific footguns, InnoDB lock quirks
- [lipy-airflow-providers](../systems/lipy-airflow-providers.md) — PipelineMD global extra link plugin
