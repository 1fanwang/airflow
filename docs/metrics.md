> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

# Airflow at LinkedIn — Metrics

## 1. Pipeline Overview

Since Airflow 2.7.0 (enabled cluster-wide Aug 2025, PR #700), **OpenTelemetry is the primary metrics transport**. StatsD/AMF is legacy and disabled on all prod clusters except DBT.

```
Airflow pod
  -- OpenTelemetry SDK (60 s export interval)
       -- FluentBit sidecar (host port 22784, HTTP/JSON)
            -- Geneva (MDM) — aggregates into minute buckets
                 -- Grafana (Geneva Datasource, KQL-M queries)
```

**Key design notes:**
- OTEL uses **Delta temporality** (`otel_preferred_temporality = DELTA`), required by Geneva
- Each Airflow pod emits independently; Geneva aggregates across pods
- Metrics are namespaced per cluster: `airflow-<cluster>` (e.g. `airflow-holdem`)
- `otel_host` is set to the host node's IP (not localhost) because Airflow runs on pod network

### Cluster Status

| Cluster | MDM Namespace | OTEL Enabled | Notes |
|---------|---------------|:---:|-------|
| holdem | `airflow-holdem` | Yes | Prod primary — batch workloads |
| war | `airflow-war` | Yes | Prod secondary — online workloads |
| faro | `airflow-faro` | Yes | EI / staging |
| lasso | `airflow-lasso` | Yes | |
| corp | `airflow-corp` | Yes | |
| dbt | `airflow-dbt` | No (StatsD) | Re-enabled StatsD Aug 2025, PR #706 |

---

## 2. MDM Configuration

| Parameter | Value |
|-----------|-------|
| **Monitoring account** | `LNKD-MP-OKLAHOMA-AIRFLOW` (uppercase is canonical in Helm; Geneva UI shows mixed case) |
| **Namespace pattern** | `airflow-<cluster>` (e.g. `airflow-holdem`, `airflow-war`) |
| **MDM endpoint** | `https://metric-server-mdm.corp.linkedin.com/api/v3/kqlm` |
| **Export interval** | 60 seconds |
| **Temporality** | Delta |
| **FluentBit port** | 22784 |

### Geneva / Grafana links

- **Geneva UI** (pre-aggs): https://portal.microsoftgeneva.com/manage/metrics/v1?account=LNKD-MP-Oklahoma-Airflow&namespace=airflow-dev — change namespace to e.g. `airflow-holdem` for prod
- **Geneva Limits Dashboard**: https://portal.microsoftgeneva.com/dashboard/LNKD-MP-Oklahoma-Airflow/GenevaQos/%25E2%2586%2590%2520MdmQos
- **Operational Grafana dashboard**: https://observe.prod.linkedin.com/g/d/eeo69wtjw50xsd/otel-airflow-holdem-operational-dashboard
- **Alerts Grafana dashboard**: https://observe.prod.linkedin.com/g/d/eepdcpdadmpkwb/otel-airflow-alerts-dashboard

---

## 3. Querying Metrics

### Via nochill Metrics page

1. Open nochill -> Metrics (chart icon in nav)
2. Set **Namespace** to e.g. `airflow-holdem`
3. Pick metrics from the catalog in Section 5
4. Set time range -> **Fetch**

Query body format (KQL-M):
```
metric("airflow.scheduler.critical_section_duration").samplingTypes("Average")
```

URL parameters: `monitoringAccount`, `metricNamespace`, `startTime` (epoch s), `endTime` (epoch s).

### Via Grafana

1. Select **Geneva Datasource**
2. Enter `LNKD-MP-OKLAHOMA-AIRFLOW` as account, `airflow-holdem` (or other) as namespace
3. Select metric name (prefixed with `airflow.`)
4. Choose sampling type — `Sum` for counters, `Average` for gauges/durations
5. For dimension filtering (e.g. by `dag_id` or `pool_name`) you must have a pre-aggregation defined in Geneva

KQL-M resources: [go/kqlm](https://go/kqlm) / [Microsoft KQL-M Docs](https://eng.ms/docs/products/geneva/metrics/advanced/kql/overview/overview)

---

## 4. Debug Scenarios

### scheduler_health — Is the scheduler alive?
- `airflow.scheduler.critical_section_duration`
- `airflow.scheduler.scheduler_loop_duration`
- `airflow.scheduler_heartbeat`
- `airflow.dag_processor_heartbeat`
- `airflow.dagrun.schedule_delay`
- `airflow.dagbag_size`
- `airflow.dag_processing.import_errors`
- `airflow.zombies_killed`

### latency — Time-to-schedule / time-to-start delays
- `airflow.dagrun.schedule_delay`
- `airflow.dagrun.first_task_start_delay`
- `airflow.dagrun.first_task_scheduling_delay`
- `airflow.scheduler.critical_section_duration`
- `airflow.pool.starving_tasks`
- `airflow.ti.start`
- `airflow.ti.finish`
- `airflow.task.queued_duration`

### throughput — Task/DAG processing rate
- `airflow.scheduler.queued_tasks`
- `airflow.scheduler.tasks.executable`
- `airflow.ti.start`
- `airflow.ti.finish`
- `airflow.task.duration`
- `airflow.dagrun.duration.success`
- `airflow.executor.running_tasks`

### resource — Pool / slot / executor capacity
- `airflow.scheduler.tasks.starving`
- `airflow.pool.open_slots`
- `airflow.pool.running_slots`
- `airflow.pool.starving_tasks`
- `airflow.executor.open_slots`
- `airflow.executor.queued_tasks`
- `airflow.db.lock.stmt_time_ms`
- `airflow.db.lock.hold_time_ms`

### reliability — Failures, zombies, orphans
- `airflow.task.failures`
- `airflow.zombies_killed`
- `airflow.scheduler.orphaned_tasks.cleared`
- `airflow.dag_processing.import_errors`
- `airflow.dagrun.duration.failed`
- `airflow.executor.pod_creation.403`
- `airflow.executor.pod_creation.500`
- `airflow.executor.pod_deletion.500`
- `airflow.kafka.emission.errors`

---

## 5. Full Metrics Catalog

Metrics are emitted with the `airflow.` prefix (set by `otel_prefix`). Metric names in MDM/Geneva include the prefix.

### Naming: dots vs. underscores

Airflow metrics use two styles. Some have dots separating logical segments (`scheduler.tasks.starving`, `pool.open_slots`); others are flat underscore names (`scheduler_heartbeat`, `dagbag_size`, `zombies_killed`, `operator_successes`). With `otel_prefix = airflow`, both styles get the same prefix: `airflow.scheduler.tasks.starving` and `airflow.scheduler_heartbeat`. The flat-underscore metrics tend to be older or emitted from non-scheduler components. Metric names in the catalog below match exactly what Geneva stores — check the source notes when in doubt.

### Dual-emit pattern

Many metrics are emitted in two forms at the same call site — a legacy flat StatsD name with the dimension embedded in the name, and a modern OTEL-dimensioned form. Example from `scheduler_job_runner.py`:

```python
Stats.timing("dagrun.schedule_delay." + dag.dag_id, delay)        # StatsD flat
Stats.timing("dagrun.schedule_delay", delay, tags={"dag_id": dag.dag_id})  # OTEL dimensioned
```

On OTEL clusters (holdem, war, faro, corp) **only the dimensioned form** is stored in Geneva. The flat form is emitted but ignored because StatsD is disabled. On DBT (StatsD only) only the flat form is stored. The catalog below lists the canonical OTEL-dimensioned name.

### Scheduler

| Metric | Unit | Description | Healthy threshold |
|--------|------|-------------|-------------------|
| `airflow.scheduler.critical_section_duration` | seconds | Wall-clock time inside scheduler critical section per loop. High = DB contention or overload. | < 0.1 s; > 1 s = problem |
| `airflow.scheduler.scheduler_loop_duration` | seconds | Full scheduler loop time. Should stay well below heartbeat interval. | < 5 s |
| `airflow.scheduler.tasks.starving` | count | Tasks ready to run but blocked on pool or executor slots. | 0 in steady state |
| `airflow.scheduler.tasks.executable` | count | Tasks the scheduler selected for dispatch in the current loop. | -- |
| `airflow.scheduler.tasks.killed_externally` | count | Tasks killed by an external process (not scheduler zombie detection). Dimensions: `dag_id`, `task_id`. | -- |

> **Phantom metrics (do not query):** `airflow.scheduler.tasks.pending` and `airflow.scheduler.tasks.running` do not exist in the source — `tasks.pending` is commented out with a TODO in `scheduler_job_runner.py`, and `tasks.running` has no emit site. Use `airflow.scheduler.queued_tasks` and `airflow.executor.running_tasks` instead.
| `airflow.scheduler_heartbeat` | count | Incremented each scheduler heartbeat (`Stats.incr("scheduler_heartbeat")`). Flat/zero = scheduler stopped. Note the underscore form — not `scheduler.heartbeat`. | Monotonically rising |
| `airflow.dag_processor_heartbeat` | count | Liveness heartbeat from the dag-processor component. Flat/zero = dag-processor stopped. | Monotonically rising |
| `airflow.scheduler.orphaned_tasks.cleared` | count | Orphaned task instances auto-cleared and re-queued. | -- |
| `airflow.scheduler.orphaned_tasks.adopted` | count | Orphaned task instances successfully re-adopted by the scheduler (complement to `.cleared`). | -- |
| `airflow.scheduler.executor_events_batch_size` | count | Number of executor events processed per batch. | -- |
| `airflow.scheduler.executor_events_loop_duration` | seconds | Duration of one full executor-event processing loop inside the scheduler. | -- |
| `airflow.scheduler.do_scheduling_duration` | seconds | Wall-clock time for the `_do_scheduling()` call per scheduler loop. Complements `scheduler_loop_duration`. | -- |
| `airflow.scheduler.queued_tasks` | count | Tasks currently in QUEUED state waiting for executor dispatch. Distinct from `tasks.pending`. | -- |
| `airflow.scheduler.scheduling_task_instance_duration` | seconds | Duration spent scheduling task instances in a single loop. | -- |
| `airflow.scheduler.dag_file_process_agent_duration` | seconds | Duration of dag-file-process-agent work per loop. | -- |
| `airflow.scheduler.sql.task_instance_scheduling.succeed` | count | SQL operations for task instance scheduling that succeeded. | -- |
| `airflow.scheduler.sql.task_instance_scheduling.failure` | count | SQL operations for task instance scheduling that failed. | -- |
| `airflow.scheduler.sql.adopt_or_reset.failure` | count | SQL failures during orphan adopt-or-reset operations. | -- |
| `airflow.scheduler.pending_running_dags.count` | count | Number of DagRuns in pending+running states. | -- |
| `airflow.scheduler.scheduled_running_dags.count` | count | Number of DagRuns that are scheduled and running. | -- |
| `airflow.scheduler.queued_dagrun_reaching_max_active_limit.count` | count | Queued DagRuns blocked because the DAG has reached its `max_active_runs` limit. | -- |
| `airflow.scheduler.dag_runs_queued.dataset.count` | count | DagRuns queued due to dataset trigger conditions. | -- |
| `airflow.scheduler.dag_runs_queued.non_dataset.count` | count | DagRuns queued due to schedule (non-dataset) trigger conditions. | -- |
| `airflow.scheduler.critical_section_busy` | count | Incremented when the scheduler tries and fails to acquire the critical section lock (already held). | 0 in steady state |
| `airflow.scheduler.critical_section_query_duration` | seconds | Duration of the DB query executed inside the critical section. | -- |
| `airflow.scheduler.execution_balancer.num_shards_to_exec` | count | Number of shards the execution balancer selected to execute in this loop. | -- |
| `airflow.scheduler.executor_heartbeat_duration` | seconds | Duration of the executor heartbeat call per scheduler loop. | -- |
| `airflow.scheduler.executor_slot.full` | count | Incremented when the executor has no available slots to accept new tasks. Sustained values = parallelism ceiling hit. | 0 in steady state |
| `airflow.scheduler.processor_agent_heartbeat_duration` | seconds | Duration of the DAG-processor-agent heartbeat step inside the scheduler loop. | -- |
| `airflow.scheduler.tasks.killed_externally` | count | Tasks killed by an external process (not by scheduler zombie detection). Dimensions: `dag_id`, `task_id`. | -- |

### DAG Processing

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.dag_processing.total_parse_time` | seconds | Total time spent parsing all DAG files in one loop. |
| `airflow.dag_processing.file_path_queue_size` | count | Number of DAG files queued for parsing. |
| `airflow.dag_processing.file_path_queue_update_count` | count | Files added/removed from the processing queue during a directory refresh. |
| `airflow.dag_processing.loop_file_parsing_latency_sum` | seconds | Cumulative latency across all file parses in one loop. |
| `airflow.dag_processing.dag_parsing_loop_count` | count | Number of completed DAG processor loops. |
| `airflow.dag_processing.import_errors` | count | DAG files that failed to import during dag-processor parsing. This is the only import-errors metric that actually exists in source — `airflow.dagbag.import_errors` does not exist. |
| `airflow.dag_processing.last_duration` | seconds | Parse duration for the most recently completed file. Dimension: `file_name`. Useful for per-file parse time tracking. |
| `airflow.dag_processing.last_run.seconds_ago` | seconds | Seconds since the last successful parse of each DAG file. **OTEL caveat**: the OTEL form has no `file_name` tag (emitted as a plain timing without tags); `file_name` is only available in the StatsD flat form. On OTEL clusters this is aggregated across all files. |
| `airflow.dag_processing.manager_stalls` | count | Times the DagFileProcessorManager stalled waiting for a processor slot to free up. |
| `airflow.dag_processing.other_callback_count` | count | Non-SLA callbacks (e.g. DAG-level on_success/on_failure) processed by the dag processor per loop. |
| `airflow.dag_processing.processes` | count | DAG file processor lifecycle events. Dimensions: `file_path`, `action` (start/stop/finish/timeout/terminate). |
| `airflow.dag_processing.processor_timeouts` | count | DAG files that exceeded `DAG_FILE_PROCESSOR_TIMEOUT` during parsing. Dimension: `file_path`. |
| `airflow.dag_processing.refresh_dag_dir` | seconds | Duration of the full DAG directory scan (listing new/removed files). |
| `airflow.dag_processing.sla_callback_count` | count | SLA miss callbacks processed by the dag processor per loop. |
| ~~`airflow.dag.loading-duration`~~ | seconds | **Deprecated** — not present in current source. Replaced by `airflow.dag_processing.last_duration`. |

### DAG Run

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.dagrun.schedule_delay` | seconds | Delay from scheduled start time to DagRun creation. Spikes = scheduler overload or parse failures. Acceptable < 60 s. |
| `airflow.dagrun.first_task_start_delay` | seconds | Delay from DagRun creation to first task instance reaching RUNNING. |
| `airflow.dagrun.first_task_scheduling_delay` | seconds | End-to-end: scheduled time -> first task starting. Sum of schedule_delay + executor dispatch. |
| `airflow.dagrun.duration.success` | seconds | Duration of successfully completed DAG runs. |
| `airflow.dagrun.duration.failed` | seconds | Duration of failed DAG runs. |
| `airflow.dagrun.dependency-check` | seconds | Time spent checking task dependencies for a DagRun. Dimension: `dag_id`. |
| `airflow.dagrun.deadlocked` | count | DagRuns detected as deadlocked — all tasks finished or no tasks schedulable, but the run is not complete. Emitted via `Stats.incr("dagrun.deadlocked")` in `_schedule_dag_run` when `dagrun.task_instance_deadlocked()` is True. Added in LinkedIn fork PR #120 (2026-04-22). Dimensions: none (counter only). |
| `airflow.dag.callback_exceptions` | count | Exceptions raised during DAG-level callbacks (`on_success_callback`, `on_failure_callback`, etc.). Dimension: `dag_id`. |

> Note: `first_task_start_delay` had a bug fixed in PR #105 (fork-specific). Verify on holdem if values look unexpectedly small before that deployment.

### Task

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.task.duration` | seconds | Execution duration of individual task instances. Dimensions: `dag_id`, `task_id`. |
| `airflow.task.failures` | count | Task instance failure count. |
| `airflow.task.successes` | count | Task instance success count. |
| `airflow.task.queued_duration` | seconds | Time a task instance spends in QUEUED state before reaching RUNNING. High values indicate executor saturation or pod scheduling latency. Dimensions: `dag_id`, `task_id`. |
| `airflow.ti.start` | count | Task instance start event. Dimensions: `dag_id`, `task_id`. |
| `airflow.ti.finish` | count | Task instance finish event. Dimensions: `dag_id`, `task_id`, `state` (success/failed/up_for_retry/etc.). |
| `airflow.ti_failures` | count | Task instance failures (flat form emitted alongside `task.failures`). |
| `airflow.ti_successes` | count | Task instance successes (flat form emitted alongside `task.successes`). |
| `airflow.task_instance_created` | count | Task instances created when a DagRun is scheduled. Dimension: `task_type`. |
| `airflow.previously_succeeded` | count | Task instances that had previously succeeded being re-queued (e.g. after a clear). |
| `airflow.task_removed_from_dag` | count | Tasks removed from DAG definition while older runs still referenced them. |
| `airflow.task_restored_to_dag` | count | Previously removed tasks restored to the DAG definition. |
| `airflow.zombies_killed` | count | Tasks killed by scheduler because they were zombies (worker heartbeat lost). Dimensions: `dag_id`, `task_id`. |
| `airflow.local_task_job.task_exit` | count | Local task job exit event (LocalExecutor/CeleryExecutor). Dimensions: `job_id`, `dag_id`, `task_id`, `return_code`. |
| `airflow.local_task_job_prolonged_heartbeat_failure` | count | Local task job missed heartbeat for longer than the zombie threshold. Indicates a stuck worker. |

### Pool

Pool metrics carry a `pool_name` dimension when queried via OTEL. In StatsD (legacy) they used flat names like `airflow.pool.open_slots.<pool_name>`.

> **Naming note:** The source emits `pool.running_slots` and `pool.deferred_slots`. Earlier versions of the catalog incorrectly listed these as `pool.used_slots` and `pool.deferred_tasks` — those names do not exist in the codebase.

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.pool.open_slots` | count | Unused slots in a task pool. Zero = pool full -> tasks will starve. Dimension: `pool_name`. |
| `airflow.pool.running_slots` | count | Slots occupied by currently running tasks. Dimension: `pool_name`. |
| `airflow.pool.queued_slots` | count | Slots reserved by queued (not yet running) tasks. Dimension: `pool_name`. |
| `airflow.pool.deferred_slots` | count | Slots occupied by tasks in DEFERRED state waiting on an external trigger. Dimension: `pool_name`. |
| `airflow.pool.starving_tasks` | count | Tasks ready to run but blocked because the pool has no open slots. Dimension: `pool_name`. |
| `airflow.pool.open_slots_histogram` | count | Histogram of open slot counts (explicit buckets: 0-10000). Dimension: `pool_name`. |
| `airflow.pool.queued_slots_histogram` | count | Histogram of queued slot counts. Dimension: `pool_name`. |
| `airflow.pool.running_slots_histogram` | count | Histogram of running slot counts. Dimension: `pool_name`. |
| `airflow.pool.deferred_slots_histogram` | count | Histogram of deferred slot counts. Dimension: `pool_name`. |

### Executor

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.executor.running_tasks` | count | Tasks currently running on the executor. |
| `airflow.executor.queued_tasks` | count | Tasks queued in executor waiting for a worker slot. |
| `airflow.executor.open_slots` | count | Available worker slots (parallelism headroom). |
| `airflow.executor.pod_creation.latency` | seconds | Latency for Kubernetes pod creation (KubernetesExecutor). |
| `airflow.executor.pod_patching.latency` | seconds | Latency for pod patch operations (KubernetesExecutor). |
| `airflow.executor.pod_creation.200` | count | Kubernetes pod creation requests that returned HTTP 200 (success). |
| `airflow.executor.pod_creation.403` | count | Pod creation requests rejected with HTTP 403 (permission denied). |
| `airflow.executor.pod_creation.500` | count | Pod creation requests that failed with HTTP 500 (server error). |
| `airflow.executor.pod_deletion.200` | count | Pod deletion requests that returned HTTP 200. |
| `airflow.executor.pod_deletion.404` | count | Pod deletion requests that returned HTTP 404 (pod already gone). |
| `airflow.executor.pod_deletion.500` | count | Pod deletion requests that failed with HTTP 500. |
| `airflow.executor.adopted_task_instance.success` | count | Task instances successfully re-adopted by a restarted executor. |
| `airflow.executor.adopted_task_instance.failure` | count | Task instances that could not be re-adopted. |
| `airflow.executor.adopted_completed_pod.success` | count | Completed pods successfully adopted during executor recovery. |
| `airflow.executor.adopted_completed_pod.failure` | count | Completed pods that failed adoption during executor recovery. |

### DAGBag

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.dagbag_size` | count | Number of DAGs loaded in the DagBag. Sudden drop = parse failures or file sync issue. Source: `Stats.gauge("dagbag_size", ...)` — note underscore form, not `dagbag.size`. |
| `airflow.serialized_dag.count` | count | Number of serialized DAGs stored in DB. |
| `airflow.serialized_dag.count_error` | count | Errors encountered while serializing or deserializing a DAG to/from the DB. |
| `airflow.collect_db_dags` | seconds | Duration to load DAGs from the database into the DagBag. |
| `airflow.dag_file_refresh_error` | count | Errors during DAG file refresh (distinct from import errors). Dimension: `file_path`. |
| `airflow.dag_file_processor_timeouts` | count | DAG file processor timeout counter (flat form of `dag_processing.processor_timeouts`). |

### Triggerer

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.triggerer_heartbeat` | count | Triggerer job heartbeat (flat name, like `scheduler_heartbeat`). Flat/zero = triggerer stopped. |
| `airflow.triggers.running` | count | Number of actively running triggers on this triggerer pod. Dimension: `hostname`. |
| `airflow.triggers.succeeded` | count | Triggers that fired successfully and unblocked a deferred task. |
| `airflow.triggers.failed` | count | Triggers that raised an exception and could not fire. |
| `airflow.triggers.blocked_main_thread` | count | Triggers that blocked the triggerer's main event loop thread (should be zero; non-zero indicates a CPU-bound trigger). |

### Dataset

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.dataset.updates` | count | Dataset update events received (a task produced a dataset). |
| `airflow.dataset.triggered_dagruns` | count | DagRuns triggered by dataset conditions being met. |
| `airflow.dataset.orphaned` | count | Datasets with no consuming DAG registered. |

### Webserver API (StatsD only — holdem/war disabled)

These metrics are emitted via StatsD by the webserver. On clusters where StatsD is disabled (holdem, war, faro), they are not available in MDM.

| Metric | Unit | Description | Tags |
|--------|------|-------------|------|
| `airflow.webserver.api.count` | count | API request count per route. | route, method, status |
| `airflow.webserver.api.latency` | seconds | API request latency. | route, method, status |

### Operators

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.operator_successes` | count | Successful task executions. Dimensions: `task_type`, `dag_id`, `task_id`. |
| `airflow.operator_failures` | count | Failed task executions. Dimensions: `operator`, `dag_id`, `task_id`. |
| `airflow.operator_exceptions_categorized` | count | Operator exceptions broken down by category and exception class. Dimensions: `operator`, `category`, `exception_class_name`. |

### SLA

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.sla_missed` | count | SLA misses detected for a task instance. Dimensions: `dag_id`, `task_id`. |
| `airflow.sla_email_notification_failure` | count | SLA miss email notifications that failed to send. Dimension: `dag_id`. |
| `airflow.sla_callback_notification_failure` | count | SLA miss callback invocations that raised an exception. Dimensions: `dag_id`, `func_name`. |

### Kubernetes Executor (Internal)

These are emitted by the KubernetesExecutor internals and are separate from the generic `executor.*` metrics above.

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.kubernetes_executor.adopt_task_instances.duration` | seconds | Duration for the KubernetesExecutor to re-adopt running task instances after a scheduler restart. |
| `airflow.kubernetes_executor.clear_not_launched_queued_tasks.duration` | seconds | Duration to clear tasks that were queued but whose pods were never created (stale queue entries). |

### Grid Gateway

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.grid_gateway_rpc_calls` | count | Grid Gateway RPC call count. Use `status=error` dimension to track GGW communication failures. |

### External Job / Pod Disruption

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.external_job.kubernetes_pod_disruption_detected` | count | Kubernetes pod disruptions detected (evictions, OOM kills, node failures). |
| `airflow.external_job.kubernetes_pod_disruption_preserved` | count | Disrupted pods preserved (not immediately retried) for investigation. |
| `airflow.external_job.kubernetes_pod_disruption_recovered` | count | Disrupted tasks successfully recovered after pod disruption. |

### Oklahoma Custom

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.oklahoma.config_override.applied` | count | LinkedIn-specific config override applications. Tracks how often cluster-level config overrides are applied at runtime. |

### Kafka Emission

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.kafka.emission.attempts` | count | Kafka event emission attempts from oklahoma-listener plugin (DAG run and task lifecycle events). |
| `airflow.kafka.emission.success` | count | Kafka events successfully delivered. |
| `airflow.kafka.emission.errors` | count | Kafka event delivery failures. Non-zero = lifecycle event data loss. |

### UI / Frontend Performance

Emitted by the Airflow React frontend (webserver). Uses exponential histogram aggregation (see Section 6, `ui.*_ms` pattern).

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.ui.home.page_ready_ms` | ms | Time from navigation start until the home page is interactive. |
| `airflow.ui.home.load_ms` | ms | Full page load time for the home view. |
| `airflow.ui.home.fcp_ms` | ms | First Contentful Paint — time until first content is rendered. |
| `airflow.ui.home.dcl_ms` | ms | DOM Content Loaded event time. |
| `airflow.ui.home.ttfb_ms` | ms | Time to First Byte — network + server response latency. |
| `airflow.ui.home.resource_count_count` | count | Number of resources (JS/CSS/images) loaded for the home page. |
| `airflow.ui.home.transfer_total_bytes_bytes` | bytes | Total bytes transferred to load the home page. |

### DB Lock Timing

LinkedIn-custom metrics for tracking MySQL lock behavior on the metadata DB. Dimension: `table`.

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.db.lock.stmt_time_ms` | ms | MySQL statement execution time per table. High values indicate slow queries or lock contention. |
| `airflow.db.lock.hold_time_ms` | ms | MySQL lock hold time per table. Sustained high values risk cascading lock waits. |

### OpenLineage Plugin

Emitted by the OpenLineage Airflow provider when installed.

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.ol.emit.attempts` | timer | Time to emit an OpenLineage event (all types). |
| `airflow.ol.emit.attempts.<event_type>.<transport_type>` | timer | Broken down by event type and transport. |
| `airflow.ol.emit.failed` | count | OpenLineage events that failed to emit after all retries. |
| `airflow.ol.extract.<event_type>.<operator_name>` | timer | Time to extract lineage metadata from an operator. |
| `airflow.ol.event.size.<event_type>.<operator_name>` | gauge | Serialized size (bytes) of OpenLineage events by type and operator. |

### Internal / Startup

Low-frequency metrics emitted at startup or during internal subsystem operations. Useful for diagnosing slow startup or serialization overhead.

| Metric | Unit | Description |
|--------|------|-------------|
| `airflow.airflow.io.load_filesystems` | seconds | Duration to load and register configured filesystems (ObjectStorage plugin) at startup. Note: the Stats call in `airflow/io/__init__.py` already embeds the `airflow.` prefix in the name (`Stats.timer("airflow.io.load_filesystems")`), so the exported name has a double prefix — this is a source-level inconsistency. |
| `airflow.serde.load_serializers` | seconds | Duration to discover and load serializer plugins at startup. |

---

## 6. Histogram Bucket Configuration

The LinkedIn Airflow fork customizes OTEL histogram bucket boundaries in `airflow/metrics/histogram_buckets.py`. The SDK applies these as Views (using glob/fnmatch pattern matching):

| Pattern | Aggregation | Rationale |
|---------|-------------|-----------|
| `pool.*_histogram` | Explicit: 0-10000 (count) | Pool slot counts are bounded integers |
| `*_duration` | Exponential (max_size=160) | Duration spans seconds to hours; exponential gives uniform relative resolution |
| `*_delay` | Exponential (max_size=160) | Same as duration; covers wide delay ranges |
| `*_bytes` | Exponential (max_size=160) | Byte counts span orders of magnitude |
| `ui.*_ms` | Exponential (max_size=160) | Frontend timing in ms |
| `*_count` | Explicit: 0-10000 (count) | Count-style histograms |

Metrics not matching any pattern use the OTEL SDK default (explicit boundaries).

---

## 7. Adding a New Metric

1. **Emit in code** — use `Stats.gauge(...)`, `Stats.counter(...)`, or `Stats.timer(...)`. Add dimensions via the `tags` parameter:
   ```python
   Stats.gauge("my.new.metric", value=42, tags={"dag_id": dag_id, "cluster": cluster})
   ```

2. **Verify locally** — set `otel_debugging_on = True` in the Helm chart (or patch the config map) to print exported metrics to pod logs:
   ```
   kubectl logs -n airflow <scheduler-pod> | grep "my.new.metric"
   ```

3. **Confirm in Geneva** — visit the [Geneva UI](https://portal.microsoftgeneva.com/manage/metrics/v1?account=LNKD-MP-Oklahoma-Airflow&namespace=airflow-dev), select account `LNKD-MP-OKLAHOMA-AIRFLOW` and namespace `airflow-dev` (dev cluster) or `airflow-holdem` for prod. Your metric should appear in the metric dropdown.

4. **Set up pre-aggregations** — required if you want to filter/group by dimensions (e.g. `dag_id`, `pool_name`):
   - In Geneva UI -> Queries row -> `+ Add`
   - Enter pre-aggregate name and select dimensions
   - Without this, the metric is queryable only as a total (no dimension breakdown)

5. **Add to Grafana** — open the target dashboard -> Add -> Visualization -> Geneva Datasource -> select metric and pre-agg.

**MDM limits to be aware of:**
- Client event volume and time series count are both metered
- Time series = unique (metric + dimension combination) — avoid high-cardinality dimensions like task instance IDs
- Max ~20-30M active time series per account; check [Limits Dashboard](https://portal.microsoftgeneva.com/dashboard/LNKD-MP-Oklahoma-Airflow/GenevaQos/%25E2%2586%2590%2520MdmQos)

---

## 8. Legacy: StatsD / AMF (Reference Only)

StatsD was disabled on holdem/war/faro/corp in Aug 2025 (PR #705) and remains enabled only for DBT (re-enabled PR #706).

**AMF sidecar image:** `container-image-registry.corp.linkedin.com/lps-image/linkedin/oklahoma-airflow/airflow-main-airflow-amf-stats-client:0.0.299`

When StatsD is enabled, metric names use flat dot notation instead of OTEL dimensions:
```
airflow.pool.open_slots.<pool_name>   <- StatsD flat format
airflow.pool.open_slots               <- OTEL with pool_name dimension
```

StatsD mappings are defined in `.linkedin/kube/airflow/files/statsd-mappings.yml` in the deployment repo. The AMF sidecar receives on port 9125 and batches to AMF every 60 seconds.

---

## See Also

- [Clusters](clusters.md) — cluster topology and environments
- [Troubleshooting](troubleshooting.md) — error patterns and fixes
- [Oncall](oncall/README.md) — SLOs, alert thresholds, runbooks
- [oklahoma-airflow-deployment metrics doc](../../../oklahoma/oklahoma-airflow-deployment/docs/docs/oklahoma-on-k8s/metrics.md) — authoritative OTEL config reference
