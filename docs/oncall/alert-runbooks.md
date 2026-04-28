> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# Oncall — Alert Runbooks

> Per-alert RC triage guides for all 94 infrastructure alerts owned by Crew 1090 (Oklahoma).
> Each section covers: what fires it, root causes, step-by-step triage, false positives, and useful links.

---

## Quick-Reference: Alert → First Check

| Alert | First Check | Escalate To |
|---|---|---|
| Platform Success Rate | Alerts dashboard → which regression DAG failing | `#mysql` if DB errors; Grid oncall if only Spark |
| Webserver Availability | `kubectl get pods -n airflow \| grep webserver` | `#mysql` if DB errors |
| Webserver API Errors (4xx/5xx) | Is it 4xx or 5xx? Logs for exceptions | `#mysql` if 5xx; client team if 4xx spike |
| AlwaysOk Regression | Multi-cluster? → bad deployment. Single cluster? → scheduler/DB | CRT rollback; `#mysql` |
| Spark Regression | Is AlwaysOk also failing? No → Grid Gateway issue | `#go-gridOH` |
| Number of Schedulers Running | `kubectl get pods -n airflow \| grep scheduler` | `#mysql` if DB errors; `#grid-k8s` if node issues |
| Scheduler Pending DAGs | `critical_section_duration` metric | Same as schedulers |
| Number of DagProcessors Running | `kubectl get pods -n airflow \| grep dag-processor` | `#storage-oncall` if NFS issue |
| Open Slots Per Pool | Airflow UI → Admin → Pools; zombie tasks? | Cluster owner to scale pool |
| Executor Open Slots | Pool-based running tasks vs executor slots | Redis health (`#kafka` or Redis oncall) |
| First Task Start Latency | `critical_section_duration` → scheduler contention? | `#mysql` if DB errors |
| Serialized DAG Count Drop | CRT for recent MP undeployments | Secondary oncall for bad deployment rollback |
| DAG Import Errors | Airflow UI → Browse → Import Errors | User (if 1 MP); secondary oncall (if platform-wide) |
| Kafka Emission Errors | Pod logs: `grep -i "kafka\|emit"` | `#kafka` if broker; cert rotation team if cert |
| ARMs Pod Count | `kubectl get pods -n airflow -l app=arms-<cluster>` | `#ask_indbt`, `#ask_flyte` if extended outage |
| VPA Metrics | Are schedulers actually healthy? → likely platform issue | `#grid-k8s-oncall` |

---

## Cluster Reference

| Cluster | Fabric | MySQL DB | Schedulers | DagProcs | Webservers |
|---|---|---|---|---|---|
| **holdem** | prod-ltx1 / grid1 | `airflow` | 24 | 12 | 10 |
| **war** | prod-lva1 / grid2 | `airflow_war` | 12 | 12 | ~4 |
| **faro** | ei-ltx1 | `airflow_faro` | 3 | 2 | ~2 |
| **lasso** | prod-ltx1 | `airflow_lasso` | 2 | 1 | — |
| **corp** | corp-lva1 | `airflow_corp` | — | — | — |

**Grafana dashboards** (swap `var-cluster=` / `var-metrics_namespace=airflow-<cluster>`):
- Operational: `https://observe.prod.linkedin.com/g/d/eeo69wtjw50xsd/airflow-operational-dashboard-otel`
- Alerts: `https://observe.prod.linkedin.com/g/d/eepdcpdadmpkwb/airflow-alerts-dashboard-otel`

---

## 1. Platform Success Rate

**Alerts:** `Airflow Holdem/War/Faro Platform Success Rate`

### What fires it
Regression DAG success rate drops on the cluster's sentinel (canary) DAGs. Thresholds from `extra-alarms-dashboard.template`:
- AlwaysOk: `successes < 0.9` for 15 consecutive events (15-min window)
- SparkBatch/CommandOperator: `successes < 0.9` for 30 consecutive events (30-min window)

### Root causes (in order of likelihood)
1. **Bad deployment** — new `oklahoma-airflow-deployment` version broke something
2. **Scheduler instability** — pods crashing or heartbeat dropping
3. **MySQL issues** — slow queries, deadlocks, `Lost connection to MySQL server`
4. **Nodepool exhaustion** — CPU/memory >80%, worker pods can't be scheduled
5. **NFS issues** — DAG or log NFS quota/inode exhaustion
6. **Executor slot exhaustion** — pool open slots below threshold
7. **Grid Gateway issue** — if *only* SparkBatch/CommandOperator fails, not AlwaysOk

### Triage steps
```bash
# 1. Open Alerts Dashboard for affected cluster, note which DAG is failing
# 2. Check scheduler pods
kubectl get pods -n airflow | grep <cluster>-scheduler
kubectl logs <scheduler-pod> -n airflow -c scheduler | grep -i "error\|Lost connection"

# 3. Check MySQL
curl 'https://mysqlaccess.prod.linkedin.com/triage/<db_name>'  # DB: airflow / airflow_war / airflow_faro

# 4. Check node pool health in Grafana Operational dashboard

# 5. If recent deployment → rollback
mint catalog deprecate oklahoma-airflow-deployment <bad_version>
# Redeploy prior version via CRT
```

**Kusto:**
```kusto
AirflowDagLifecycleEvent
| where airflowClusterName has '<cluster>'
| where dagRunState has 'FAILED'
| order by dagRunEndDate desc
| take 20
```

### False positives
- MySQL scheduled failover — resolves within 5-10 min; check `mysqlaccess` for "Failover in last 30 min"
- Transient GGW blip — if only SparkBatch/CommandOperator fails for 1-2 runs
- DAG_DIR_LIST_INTERVAL gap — brief cadence gap from new DAG file discovery

---

## 2. Webserver Availability

**Alerts:** `Airflow Holdem/War/Faro Webserver Availability`

### What fires it
InMon synthetic monitoring drops below **99% availability** for 15 consecutive checks on the cluster webserver URL.

URLs:
- Holdem: `https://holdem.oklahoma-airflow.grid.linkedin.com`
- War: `https://war.oklahoma-airflow.grid.linkedin.com`
- Faro: `https://faro.oklahoma-airflow.grid.linkedin.com`

### Root causes
1. **Webserver pod crash / OOMKill** — holdem: 3Gi request / 8Gi limit; war: similar
2. **Gunicorn worker exhaustion** — all workers stuck; recycle at `GUNICORN_MAX_REQUESTS=1150`
3. **MySQL connectivity** — webserver fails all page renders when DB is down
4. **SSL certificate expiry** — `identity.cert` / `identity.key` from `k8s-lare` init container
5. **SSO proxy unreachable** — Kraken SSO proxy at `OKLAHOMA_AIRFLOW_WEBSERVER_SSO_PROXY_URL`
6. **Bad deployment** — new version fails to start

### Triage steps
```bash
# Check pod health
kubectl get pods -n airflow | grep <cluster>-webserver
kubectl describe pod <webserver-pod> -n airflow
kubectl logs <webserver-pod> -n airflow -c webserver --tail=200

# Check SSL cert expiry
kubectl exec -it <webserver-pod> -n airflow -- \
  openssl x509 -in /var/cluster/identity.cert -noout -dates

# Rolling restart if Gunicorn workers exhausted
kubectl rollout restart deployment/airflow-<cluster>-webserver -n airflow
```

### False positives
- Rolling restart during CRT deployment — brief transient; confirm deployment is ongoing
- Single inmon check failure — threshold is 15 consecutive events
- Faro (EI) — lower traffic, more prone to transient blips

---

## 3. Webserver API Errors (4xx/5xx)

**Alerts:** `Webserver API Error(4xx/5xx) - holdem/war/faro [OTEL]`
Also: legacy `Airflow Holdem/War/Faro Webserver Availability` (older metric).

### What fires it
Error rate exceeds 30% for 5 consecutive events in a 5-minute window. Source: `airflow.gunicorn.requests` vs `airflow.gunicorn.request.status.200`.

### Root causes
**4xx:**
1. Authentication/authorization failures (expired SSO sessions, invalid API keys)
2. Bad API calls from user integrations / CI-CD pipelines making malformed requests
3. 404s from requests for deleted/renamed DAGs

**5xx:**
1. MySQL slow or down — all page/API handlers fail
2. Gunicorn worker timeout (`web_server_worker_timeout: 21600s`)
3. Application exceptions from a bad deployment
4. Memory pressure / OOM on webserver pod
5. InLogs API down — log endpoints return 5xx

### Triage steps
```bash
# 1. Determine 4xx vs 5xx from Alerts Dashboard
# 2. Check webserver logs for exceptions
kubectl logs <webserver-pod> -n airflow -c webserver --tail=500 | grep -E "ERROR|Exception|500"

# 3. Rolling restart if widespread 5xx
kubectl rollout restart deployment/airflow-<cluster>-webserver -n airflow

# 4. Check MySQL (see Alert 1 for MySQL triage link)
# 5. Check InLogs API for log-endpoint 5xx
```

**Kusto:**
```kusto
kube_pod_logs
| where kubernetes__namespace_name == "airflow"
  and kubernetes__pod_name startswith "airflow-<cluster>-webserver-"
| where timestamp > datetime("<issue_time>")
| where log has "ERROR" or log has "500" or log has "Exception"
| project kubernetes__pod_name, timestamp, log
```

### False positives
- Health-check crawlers hitting bad endpoints
- DAG cleanup operations → 404 spike
- SSO token refresh storms → brief 401 spike
- CI/CD pipeline with bad API calls → 4xx tied to deployment window

---

## 4. AlwaysOk Regression DAGs

**Alerts:** `AlwaysOk Regression DAG Success/Failures - holdem/war/faro/lasso`

### What it is
The simplest possible platform canary: `echo "I am alive!"` via BashOperator, runs every minute (`*/1 * * * *`), `max_active_runs=5`, execution timeout 4 min.
- Source: `oklahoma_system_dags/generic_tests/oklahoma_test_regression_alwaysOk__oklahoma_system_dags.py`
- **If this fails → Airflow platform itself is broken**, not user DAGs.

Alert thresholds:
- **Success alert**: `successes < 0.9` for `consecutive-events: 15` / 15-min window
- **Failures alert**: `failures > 0.1` for `consecutive-events: 15` / 15-min window
- Clears after `consecutive-good: 2`

### Triage steps

**Step 1 — Establish blast radius**
- Multiple clusters firing simultaneously → platform-wide / bad deployment
- Single cluster → scope to that cluster

**Step 2 — Check CRT for recent deployments**
```
https://crt.prod.linkedin.com/#/deployment/actions?pathName=DEFAULT&productName=oklahoma-airflow-deployment
```
If deployment in last 15-30 min → initiate rollback immediately.

**Step 3 — Check scheduler pod health**
```bash
kubectl get pods -n airflow | grep <cluster>-scheduler
kubectl logs <scheduler-pod> -n airflow -c scheduler --tail=200
kubectl logs <scheduler-pod> -n airflow -c scheduler | grep -i "mysql\|OperationalError\|Lost connection"
```

**Step 4 — Kusto**
```kusto
AirflowDagLifecycleEvent
| where airflowClusterName has '<cluster>'
| where dagId has 'oklahoma_test_regression_alwaysOk'
| where dagRunState has 'FAILED'
| order by dagRunEndDate desc
| take 20
```

**Step 5 — Check MySQL, NFS, DAG paused state**
```bash
kubectl exec -it <scheduler-pod> -n airflow -- bash
airflow dags list | grep alwaysOk
airflow dags unpause oklahoma_test_regression_alwaysOk__oklahoma_system_dags
```

### False positives
- **Metric pipeline gap (statsd → AMF stall)** — most common FP. If *other* AMF metrics are also missing, the pipeline is the problem. Check `scheduler_loop_duration` metric.
- Post-deployment metric gap — brief gap during pod restart; 15-event window absorbs short blips
- Holdem: `dag_dir_list_interval=86400s` — new file not yet discovered ≠ scheduling failure
- faro alone firing — lower severity; faro is the EI cluster with historically more instability

---

## 5. Spark Regression DAG

**Alerts:** `Spark Regression DAG Success - holdem/war [OTEL]`

### What it is
Submits a real Spark wordcount job via `SparkBatchOperator` through Grid Gateway. Runs at `8,38 * * * *` (every 30 min), `max_active_runs=1`, `dagrun_timeout=55m`, 1 retry. Has `on_failure_callback=create_iris_incident_callback(plan="Oklahoma Regression Dag")` — auto-creates an IRIS incident.
- Source: `oklahoma_system_dags/grid_tests/oklahoma_test_regression_operator_sparkbatch__oklahoma_system_dags.py`

Alert threshold: `consecutive-events: 30`, `window: 30 min`, `min: 0.9`

### Triage steps

**Step 1 — Check if AlwaysOk is also firing**
- Yes → platform-level issue; follow AlwaysOk triage first
- No → Grid Gateway / Spark issue specifically

**Step 2 — Check IRIS incident auto-created by DAG**
The failure callback auto-creates an IRIS incident with the stack trace.

**Step 3 — Check CommandOperator regression DAG too**
If `oklahoma_test_regression_operator_command__oklahoma_system_dags` is also failing → GGW connectivity broadly broken. Contact `#go-gridOH`.

**Step 4 — Check Kusto for task errors**
```kusto
OklahomaLoggingEvent
| where timestamp > datetime("<ALERT_TIME>")
| where loggingContext.dagId has 'sparkbatch'
| where level == 'ERROR' or message has 'Error'
| project timestamp, message
| order by timestamp desc
| take 50
```

**Step 5 — DAG has 1 retry**; if both attempts fail, issue is persistent.

### False positives
- Grid cluster down independently of Airflow — check `#go-gridOH`
- `is_paused_upon_creation` set to true on new cluster; DAG hasn't run yet → 30-event window fires
- Artifactory JAR dependency unavailable (transient)

---

## 6. Number of Schedulers Running

**Alerts:** `Number of Schedulers Running - holdem/war/faro [OTEL]`

### Alert thresholds (from `cluster_dashboard_params.json`)
| Cluster | Threshold | Replicas |
|---|---|---|
| holdem | min **8** (absolute) | 24 |
| war | min **70%** (ratio) | 12 |
| faro | min **70%** (ratio) | 3 |

### Root causes
1. Pod crash / OOMKilled — scheduler ran out of memory
2. MySQL connectivity loss — `OperationalError: Lost connection to MySQL`
3. Rolling deployment in progress — transient dip (check CRT)
4. NFS mount failure — DAG NFS unavailable on startup
5. Node pool pressure — pod eviction or scheduling failures
6. Liveness probe failure — `SchedulerJob.is_alive()` fails for >5 min → pod restart

### Triage steps
```bash
# Step 1 — Pod status
kubectl get pods -n airflow | grep <cluster>-scheduler
# Look for: CrashLoopBackOff, OOMKilled, Error, Pending

# Step 2 — Describe crashed pod
kubectl describe pod <scheduler-pod> -n airflow
# Check Last State + Events

# Step 3 — Logs
kubectl logs <scheduler-pod> -n airflow -c scheduler --previous
# Look for: OOM, MySQL errors, NFS errors

# Step 4 — Force restart if stuck
kubectl rollout restart deployment/airflow-<cluster>-scheduler -n airflow
```

**Kusto:**
```kusto
kube_pod_logs
| where kubernetes__namespace_name == "airflow"
  and kubernetes__pod_name startswith "airflow-<cluster>-scheduler-"
| where timestamp > datetime("<incident_time>")
| where log has "error" or log has "Exception" or log has "Lost connection"
| project kubernetes__pod_name, timestamp, log
| order by timestamp desc
```

### False positives
- CRT deployment in progress — rolling restart causes transient dip
- Holdem fires at absolute "8" — can fire during partial deployment even if most schedulers healthy
- Metric pipeline lag — OTel exports every 60s; short pod restart may look like extended outage

---

## 7. Scheduler Pending DAGs

**Alerts:** `Scheduler Pending DAGs - holdem/war/faro/lasso [OTEL]`

### Root causes
1. **Scheduler overload / slow main loop** — `scheduler_loop_duration` rising
2. **MySQL slow queries or lock contention** — `critical_section_query_duration` elevated
3. **Too many DAGs / large dagbag** — holdem hosts 20,000+ DAGs; high `total_dag_parse_time`
4. **Insufficient scheduler replicas** — fewer schedulers = less scheduling throughput
5. **DAG processor backlog** — processors slow to parse/serialize
6. **NFS latency** — slow GPFS file reads inflate parse times

### Triage steps
```bash
# Step 1 — Check Grafana Operational dashboard
# Panels: "Scheduler Main Loop Performance", "Average DagRun Schedule Delay",
#         "Average First Task Scheduling Delay"

# Step 2 — Verify scheduler count (see Alert 6)

# Step 3 — Check DAG parsing time in Grafana "DAG Processing Time" panel
kubectl logs <dag-processor-pod> -n airflow -c dag-processor | grep -i "error\|import\|parse" | tail -50

# Step 4 — Kusto for MySQL lock wait
kube_pod_logs
| where kubernetes__pod_name startswith "airflow-<cluster>-scheduler-"
| where log has "MySQLdb.OperationalError" or log has "lock wait timeout"
```

### False positives
- Cluster restart / deployment — brief pending spike normal
- Batch of large DAG pushes — parse time spike; transient
- Holdem business hours — 20K+ DAGs naturally creates peak pressure at hourly burst

---

## 8. Number of DagProcessors Running

**Alerts:** `Number of DagProcessors Running - holdem/war/faro [OTEL]`

### Configured replicas
| Cluster | Replicas |
|---|---|
| holdem | 12 |
| war | 12 |
| faro | 2 |
| lasso | 1 |

Liveness probe: kills dag-processor after >5 min without heartbeat → auto-restart.

### Root causes
1. **OOMKilled** — large/malformed DAG causes memory spike
2. **DAG import error / infinite parse loop** — bad DAG file locks a thread
3. **NFS stale or unavailable** — cannot read from `/opt/airflow/dags/`
4. **MySQL connectivity** — can't write parsed DAG metadata
5. **Rolling deployment** — same as scheduler pods

### Triage steps
```bash
# Step 1 — Pod status
kubectl get pods -n airflow | grep dag-processor

# Step 2 — Describe + logs
kubectl describe pod <dag-processor-pod> -n airflow
kubectl logs <dag-processor-pod> -n airflow -c dag-processor --previous | tail -100
kubectl logs <dag-processor-pod> -n airflow -c dag-processor | grep -i "error\|fatal\|import" | tail -50

# Step 3 — Identify culprit DAG file (check for recently modified files)
kubectl exec -it <scheduler-pod> -n airflow -- bash
ls -lt /opt/airflow/dags/<mp_name>/

# Step 4 — NFS health (from Grafana "NFS Utilization" panel)
# Alert threshold: 80% capacity

# Step 5 — Restart if stuck
kubectl rollout restart deployment/airflow-<cluster>-dag-processor -n airflow
```

**NFS mount points:**
- Holdem DAG NFS: `airflow-cluster02-dags` on `ltx1-gpfs01-nfs.corp.linkedin.com`
- War DAG NFS: `war-airflow-cluster01-dags` on `lva1-gpfs2-nfs.prod.linkedin.com`
- Faro DAG NFS: `ei-oklahoma-cluster01-dags` on `ei-gpfs02-nfs.stg.linkedin.com`
- Lasso DAG NFS: `lasso-airflow-cluster01-dags` on `ltx1-gpfs02-nfs.prod.linkedin.com`

**Duplicate app-name fix (if DAG ID duplication):**
```bash
# Inside scheduler pod
rmdir -r /opt/airflow/dags/<mp_name>/<stale_airflow_app>
rmdir -r /opt/airflow/dags/<mp_name>/<stale_airflow_app>.dir
```

### False positives
- CRT deployment — pods restarted during rolling deployment
- Lasso (1 replica) — single restart drops count to 0; normal; verify pod recovers within 2-3 min

---

## 9. Open Slots Per Pool

**Alerts:** `Open Slots Per Pool - holdem/war/faro [OTEL]`

### Alert thresholds (from `cluster_dashboard_params.json`)
| Cluster | Min open slots |
|---|---|
| holdem | **500** |
| war | **100** |
| faro | **100** |

### Root causes
1. **Pool fully consumed** — large batch of tasks filled the pool
2. **Hung/stuck tasks (zombie tasks)** — tasks in `running` state but dead; hold slots without working
3. **Worker pod crashes** — tasks remain in `running` in DB but not executing
4. **default_pool misconfiguration** — slot count was changed via API/UI
5. **Sensor slot starvation (P5)** — sensors in `poke` mode holding slots

### Triage steps
```bash
# Step 1 — Airflow UI → Admin → Pools (for affected cluster)
# Look at Running Slots, Queued Slots, Open Slots per pool

# Step 2 — Find zombie tasks (oldest running tasks)
# Airflow UI: Browse → Task Instances, filter State=running, sort Start Date ascending

# Step 3 — Mark zombie tasks failed to free slots
# Airflow UI: Browse → Task Instances → select stuck tasks → Actions → Mark Failed

# Step 4 — Check Celery worker pods
kubectl get pods -n airflow | grep <cluster>-worker
kubectl logs <worker-pod> -n airflow -c worker | tail -50
```

**Kusto (find tasks running >2 hours):**
```kusto
AirflowTaskLifecycleEvent
| where airflowClusterName has '<cluster>'
| where taskState == 'running'
| where timestamp < datetime("<incident_time>") - 2h
| project dagId, taskId, runId, timestamp
| order by timestamp asc
```

### False positives
- Expected peak load windows — hourly burst DAGs; slots recover within 15-30 min
- Holdem threshold of 500 — high threshold; can transiently dip below during large batch runs; cross-reference with user complaints
- User-defined pools — team may have intentionally set a low limit

---

## 10. Executor Open Slots

**Alerts:** `Executor Open Slots - holdem/war/faro [OTEL]`

> Known metric accuracy issue: `airflow.executor.open_slots` may be inaccurate due to pod adoption. Always cross-check with pool-based running tasks count.

### Root causes
1. **Worker saturation** — all Celery slots occupied; `worker_concurrency: 16` per pod
2. **Runaway DAG / backfill explosion** — DAG creates many task instances simultaneously
3. **Slow long-running tasks** — hold slots; combined with normal load → saturation
4. **Worker pods unavailable** — crash reduces total available slots
5. **Redis (Celery broker) issue** — tasks queue but not dispatched

### Triage steps
```bash
# Step 1 — Grafana Operational → "Executor Metrics" panel
# Check: open_executor_slots near 0, queued_tasks high, running_tasks at max
# Cross-check: "Total Running Tasks" panel (pool-based) to verify accuracy

# Step 2 — Celery worker pods
kubectl get pods -n airflow | grep <cluster>-worker
kubectl describe pod <worker-pod> -n airflow
kubectl logs <worker-pod> -n airflow -c worker | tail -100

# Step 3 — Redis health
kubectl in status lideployment --product in-redis-roundup-workflows \
  --application in-redis-roundup-workflows --fabric prod-lva1
```

**Kusto (find top DAGs consuming slots):**
```kusto
AirflowTaskLifecycleEvent
| where airflowClusterName has '<cluster>'
| where taskState == 'running'
| summarize count() by dagId
| order by count_ desc
| take 20
```

```bash
# Cancel runaway DAG
kubectl exec -it <scheduler-pod> -n airflow -- bash
airflow dags pause <dag_id>
```

### False positives
- **Pod adoption metric bug** — `executor.open_slots` can show 0/negative even with capacity; always cross-check pool-based count
- Transient burst at top of hour — recovers within 5-10 min; only escalate if sustained
- Faro / lasso — small clusters; saturation from moderate load is expected

---

## 11. First Task Start Latency

**Alerts:** `Airflow - First Task Start Latency - holdem/war/faro/DBT`

**SLO: P95 < 10 minutes** (time from DAG run created → first task enters `running`)

### Root causes (in order of likelihood)
1. **Scheduler critical section contention (P10)** — DB lock serialization; spikes above 10,000 DAGs; worse on holdem (24 schedulers, highest DAG volume). Look for `critical_section_duration` >10-15s sustained.
2. **DagBag parse slowness** — heavy top-level imports; holdem `min_file_process_interval=1800s`, war `900s`
3. **MySQL transaction lock / DB contention** — `OperationalError: Lost connection` in scheduler logs
4. **Worker pod startup latency (KubernetesExecutor)** — pods created on-demand; NFS mount delays, image pulls; worker creation batched at 50 pods
5. **Sensor slot starvation (P5)** — poke-mode sensors hold all `parallelism` slots (holdem: 1024, faro: 512)
6. **Scheduler replica / health issues** — holdem: 24 replicas; losing replicas reduces throughput; `scheduler.heartbeat_timeout=180s`

### Triage steps
```bash
# Step 1 — Open Operational Dashboard for affected cluster
# Check "Scheduler Main Loop Performance" → critical_section_duration
# If >10-15s sustained → scheduler is bottleneck

# Step 2 — Check task state distribution in Airflow UI
# Many in 'scheduled' (not queued) → scheduler contention
# Many in 'queued' (not running) → worker pod startup or slot starvation

# Step 3 — Scheduler pod health
kubectl get pods -n airflow | grep scheduler
kubectl logs <scheduler-pod> -n airflow -c scheduler | grep -i "mysql\|OperationalError"

# Step 4 — Worker pods not starting → NFS mount issues
kubectl get events -n airflow | grep -E "Evicted|OOMKilled|Failed|ContainerCreating"

# Step 5 — Scale schedulers if critical_section_duration spiking
kubectl scale deployment/airflow-holdem-scheduler --replicas=<N+2> -n airflow
```

### False positives
- Holdem `min_file_process_interval=1800s` — new DAG may not be parsed yet ≠ scheduling delay
- Burst of scheduled runs at midnight / top-of-hour — coordinated `@daily`, `@hourly` starts; brief spike expected
- Scheduler rolling restart in progress — transient spike during pod replacement
- DBT cluster (Airflow 2.5.3) — older version; P95 baselines naturally higher

---

## 12. Serialized DAG Count Drop

**Alerts:** `Serialized DAG Count - holdem/war/faro [OTEL]`

**Threshold: >50% week-over-week drop**

### Root causes
1. **Intentional mass DAG deletion / MP undeployment** (most common — check CRT first)
2. **Mass import errors after deployment** — broken shared module; all DAGs importing it fail to parse
3. **NFS mount problems** — scheduler/dag-processor cannot read DAG files; known issue: `fsGroup` mismatch on new NKS nodes (fixed in PRs #1035/#1036)
4. **DagProcessor replicas down** — faro has only 2; losing one is high-impact
5. **`dag_dir_list_interval`** — holdem: `86400s` (24h); drops may not surface quickly. War/faro: `360s` (6 min)
6. **`dag_retention=3600s` on holdem** — inactive DAGs cleaned from `dag` table after 1 hour

### Triage steps
```bash
# Step 1 — Check CRT for recent MP undeployments
https://crt.prod.linkedin.com/#/deployment/actions?pathName=DEFAULT&productName=oklahoma-airflow-deployment
# If an MP was removed intentionally → expected behavior, close as known

# Step 2 — Check DAG Import Errors in Airflow UI → Browse → Import Errors

# Step 3 — dag-processor logs
kubectl logs <dag-processor-pod> -n airflow | grep -E "Broken DAG|ImportError|ModuleNotFoundError"

# Step 4 — Verify DAG files exist on NFS
kubectl exec -it <scheduler-pod> -n airflow -- bash
ls /opt/airflow/dags/<mp_name>/
```

**Kusto:**
```kusto
kube_pod_logs
| where kubernetes__pod_name startswith "airflow-holdem-dag-processor-"
| where timestamp > ago(2h)
| where log has "Broken DAG" or log has "ImportError"
| project timestamp, kubernetes__pod_name, log
```

### False positives
- Intentional MP cleanup / offboarding — most common FP; always cross-check CRT
- Holdem 24h dag_dir_list_interval — drop may appear delayed relative to file removal
- Scheduler restart / rolling upgrade — transient dip during pod replacement

---

## 13. DAG Import Errors

**Alerts:** `DAG Import Errors - holdem/war/faro/corp/EI/lasso`

### What fires it
`dag_processing.import_errors` gauge goes above zero. Fires when any DAG file raises an exception during `DagBag` load.

### User-caused vs. platform-caused

| Signal | User-caused | Platform-caused |
|---|---|---|
| Scope | One MP's DAGs | Many MPs or all DAGs |
| Timing | After MP CRT deployment | After `oklahoma-airflow-deployment` release |
| Error type | `ImportError`, `SyntaxError`, `ModuleNotFoundError` | `AttributeError` on `airflow.*`, missing provider symbol |
| Blast radius | Single `dag_id` spike | Broad spike across all parsers |

### Triage steps
```bash
# Step 1 — Airflow UI → Browse → Import Errors (fastest diagnosis)
# Lists each file path + full traceback

# Step 2 — Scope: one MP → user issue; many MPs → platform issue

# Step 3 — dag-processor logs
kubectl logs <dag-processor-pod> -n airflow --tail=200 | \
  grep -i "import error\|ImportError\|SyntaxError\|ModuleNotFoundError\|Failed to import"

# Step 4 — Check for duplicate app-name (application rename caused 2 dirs)
kubectl exec -it <scheduler-pod> -n airflow -- bash
ls /opt/airflow/dags/<mp_name>/
# Fix:
rmdir -r /opt/airflow/dags/<mp_name>/<stale_app>
rmdir -r /opt/airflow/dags/<mp_name>/<stale_app>.dir

# Step 5 — If platform-caused → rollback oklahoma-airflow-deployment
mint catalog deprecate oklahoma-airflow-deployment <bad_version>
```

**DAGBAG_IMPORT_TIMEOUT:** `120s` on holdem/war/lasso. A slow-parsing DAG can trigger this even if syntactically correct.

**CRT deployment gate:** Import errors can block downstream MP deployments. Override procedure at: `/Users/viagarwa/oklahoma/oklahoma-airflow-deployment/docs/docs/crt-dag-deployment.md`

**Guide for users:** `airflow-docs/docs/users/dag-authoring/cross-mp-imports.md` — explains cross-MP import failure patterns.

### False positives
- `DAGBAG_IMPORT_TIMEOUT` — slow but valid DAG; alert fires until timeout is relaxed or DAG fixed
- Transient NFS blip — single parse cycle shows errors then resolves on next cycle; monitor for persistence
- `__MACOSX` metadata in ZIPs — `"ValueError: source code string cannot contain null bytes"`

---

## 14. Kafka Emission Errors

**Alerts:** `Kafka Emission Errors - holdem/war/faro/lasso`

### What fires it
`kafka.emission.errors` counter spikes. Emitted by `KafkaHelper` in `oklahoma-helpers` whenever:
- Kafka client init fails (broker/schema registry unreachable at startup)
- `client.produce()` raises an exception
- `client.produce()` returns non-True
- Schema is not properly initialized

**Topics involved:**
- `AirflowDagLifecycleEvent` — DAG run state transitions
- `AirflowTaskLifecycleEvent` — task instance state transitions
- `AirflowDagUploadEvent` — DAG deployment events

**Broker URL pattern:** `kafka.{kafka_cluster}.kafka.{fabric}.atd.disco.linkedin.com:16637`

### Broker-side vs. client-side

| Signal | Broker-side | Client-side |
|---|---|---|
| Scope | All pods, all topics uniformly | Specific topics or specific pods |
| Other Kafka services | Also affected | Fine |
| Log message | `"Unable to initialize kafka client"` | `"Serialization error"` or `"Failed to emit"` |
| Check | `#kafka` Slack / `go/menagerie` | Cert expiry; schema mismatch |

### Triage steps
```bash
# Step 1 — Check pod logs for error type
kubectl logs -n airflow <scheduler-pod> --tail=300 | grep -i "kafka\|emit\|Failed to emit\|kafka client"

# Key log messages:
# "Unable to initialize kafka client and/or fetch its schema" → init failure (broker/registry)
# "Failed to emit kafka message:"                            → runtime produce failure
# "Kafka client returned non-True result:"                   → REST proxy rejecting messages
# "Serialization error:"                                     → schema mismatch
# "Messages are blocked from being emitted."                 → block_emittance flag (dev cluster)

# Step 2 — Check Kafka broker health
# go/menagerie: https://sre.corp.linkedin.com/apps/menagerie/<fabric>/v2/kafka/metrics
# Ping #kafka Slack

# Step 3 — Check cert validity
kubectl exec -n airflow <scheduler-pod> -- \
  openssl x509 -in /var/cluster/identity.cert -noout -dates
kubectl exec -n airflow <scheduler-pod> -- \
  openssl x509 -in /var/run/secrets/li-spiffe/certs/spiffe-identity.cert -noout -dates
```

**Kusto:**
```kusto
OklahomaLoggingEvent
| where timestamp > datetime("<issue_time>")
| where message has "kafka" and message has "error"
| project dagId = tostring(loggingContext.dagId), timestamp, message
| order by timestamp desc
```

### False positives
- **Dev/test clusters** — `block_emittance=True` → errors counted but intentional; no action
- Transient REST proxy blip — brief HTTP 503; look for sustained rates, not one-off spikes
- Simulation cluster (`airflow-load-test`) uses test topics: `AirflowTestDagLifecycleEvent` / `AirflowTestTaskLifecycleEvent`

**Source files:**
- `lipy-airflow-providers/oklahoma-helpers/src/linkedin/oklahoma/helpers/kafka.py` — `KafkaHelper`, all `kafka.emission.*` metrics
- `oklahoma-listener/src/linkedin/airflow/plugins/oklahoma/listener/utils/constants.py` — topic names, broker URL

---

## 15. ARMs Pod Count

**Alerts:** `Number of ARMs Pods Running - holdem/war`

### What ARMs is
**ARMs = Artifact Resource Metadata Service** (`bdp-artifact-metadata-service`). gRPC service (port 50085) that fetches table, partition, and snapshot metadata from Dali, Hive, and HDFS. Used by `PartitionSensor` and `DatasetSensorArray`.

**Deployment:**
- `arms-holdem`: 2 replicas, `airflow` namespace, `prod-ltx1`, image `0.0.191`
- `arms-war`: 2 replicas, `airflow` namespace, `prod-lva1`, image `0.0.191`
- Resources: `requests: cpu 1, memory 2Gi` / `limits: cpu 6, memory 6Gi`

**Alert threshold:** fires when running pod count drops below 2.

**Downstream impact:** If both pods go down → ALL `DatasetSensorArray` / `PartitionSensor` tasks in the cluster will stall or error. Notify `#ask_indbt` and `#ask_flyte` if outage >5 min.

**Validation DAG:** `oklahoma_test_regression_sensor_dataset_sensor_array__oklahoma_system_dags`

### Root causes
1. **OOMKilled** — memory limit 6Gi exceeded during large Hive/HDFS metadata queries
2. **init container `k8s-lare` failure** — PKI agent socket `/var/run/lipki/pki-agent.sock` unavailable on node → pod stuck in `Init`
3. **Image pull failure** — bad new image pushed
4. **Node eviction** — pod evicted from node

### Triage steps
```bash
# Step 1 — Check pod status
kubectl get pods -n airflow -l app=arms-holdem
kubectl get pods -n airflow -l app=arms-war

# Step 2 — Describe pod (check OOMKill, init container)
kubectl describe pod -n airflow <arms-pod-name>
# Check "Init Containers" → k8s-lare should be "Completed"
# Check "Last State" → OOMKilled?

# Step 3 — Logs
kubectl logs -n airflow <arms-pod-name>
kubectl logs -n airflow <arms-pod-name> --previous

# Step 4 — Recent events
kubectl get events -n airflow --sort-by='.lastTimestamp' | grep arms

# Step 5 — Restart
kubectl rollout restart deployment arms-holdem -n airflow
kubectl rollout restart deployment arms-war -n airflow

# Step 6 — Rollback if bad version
kubectl rollout undo deployment arms-holdem -n airflow
# Last known good: 0.0.191
```

**If OOMKilled repeatedly:** increase memory limit in `deployment/prod-ltx1/arms-holdem.yaml` or `deployment/prod-lva1/arms-war.yaml` and apply patch:
```bash
kubectl patch deployment arms-holdem -n airflow --patch-file deployment/prod-ltx1/arms-holdem-patch.yaml
```

### False positives
- ARMs deployment in progress — pod count drops to 1 during rolling restart; resolves in ~1-2 min

**Source files:**
- `bdp-artifact-metadata-service/deployment/prod-ltx1/arms-holdem.yaml`
- `bdp-artifact-metadata-service/deployment/prod-lva1/arms-war.yaml`

---

## 16. VPA Metrics

**Alerts (25 total):** `VPA Object Count`, `VPA Container Usage Samples`, `VPA Aggregate Container States Count`, `Avg VPA Total Execution Latency`, `Avg VPA MaintainCheckpoints Execution Latency Percentage` — for holdem3/5/7, war/war2

### What VPA is in this context
> These are **LinkedIn platform-level VPA controller metrics**, NOT Oklahoma-owned VPA objects. Oklahoma does not deploy any `VerticalPodAutoscaler` CRDs. The Grid K8s platform team runs a VPA controller that monitors all pods in the `airflow` namespace cluster-wide for right-sizing recommendations.

The holdem3/5/7, war/war2 suffixes correspond to specific scheduler pod instances or VPA controller shards.

### What causes each to fire
- **VPA Object Count low** — VPA controller not tracking expected workload objects; controller may be down
- **VPA Container Usage Samples low** — VPA not collecting CPU/memory samples; happens during frequent pod restarts or metrics pipeline disruption
- **VPA Aggregate Container States Count low** — VPA lost in-memory state (controller restart)
- **Avg VPA Total Execution Latency high** — API server pressure, many objects to scan, or resource pressure on VPA controller pod
- **VPA MaintainCheckpoints Latency high** — etcd write latency elevated; VPA checkpoint maintenance consuming excess loop time

### Triage steps
```bash
# Step 1 — Check if Airflow components are actually healthy (most important check)
kubectl get pods -n airflow | grep scheduler
kubectl get pods -n airflow | grep dag-processor
# If schedulers/processors are healthy → VPA alerts are platform-level, low urgency

# Step 2 — Check #grid-k8s-oncall Slack
# If multiple teams seeing VPA alerts → platform issue, not Oklahoma

# Step 3 — Check VPA controller pods (platform namespace)
kubectl get pods -A | grep -i vpa-admission\|vpa-recommender\|vpa-updater

# Step 4 — Check if VPA objects exist for Airflow
kubectl get vpa -n airflow
# If none → VPA is purely observational, alerts are informational only
```

### False positives
- **Deployments** — rolling restart causes VPA to lose sample history; resolves 10-15 min after deployment
- **etcd maintenance** — Grid K8s platform runs periodic etcd compaction; VPA checkpoint writes slow; check `#grid-k8s-oncall` for maintenance windows
- **holdem3/5/7 suffixes** — partial VPA coverage; few instances slow during deployment ≠ systemic issue

> **Action item:** Confirm with Grid K8s platform team whether Oklahoma owns any VPA objects or whether these 25 alerts should be transferred to platform ownership.

---

## Common kubectl Commands Reference

```bash
# --- Read-only (prod airflow namespace) ---
kubectl get pods -n airflow | grep <component>
kubectl describe pod <pod-name> -n airflow
kubectl logs <pod-name> -n airflow -c <container>      # container: scheduler, dag-processor, worker, webserver
kubectl logs <pod-name> -n airflow -c <container> --previous
kubectl get events -n airflow --sort-by='.lastTimestamp' | tail -30
kubectl get deployment -n airflow

# --- Write (prod airflow namespace - authorized operations only) ---
kubectl rollout restart deployment/airflow-<cluster>-scheduler -n airflow
kubectl rollout restart deployment/airflow-<cluster>-dag-processor -n airflow
kubectl rollout restart deployment/airflow-<cluster>-webserver -n airflow
kubectl rollout restart deployment arms-holdem -n airflow
kubectl rollout undo deployment arms-holdem -n airflow

# --- Full access (airflow-test namespace only) ---
kubectl exec -it <pod> -n airflow-test -- bash
```

## Key Links

| Resource | URL |
|---|---|
| Holdem Operational Dashboard | https://observe.prod.linkedin.com/g/d/eeo69wtjw50xsd/airflow-operational-dashboard-otel |
| Holdem Alerts Dashboard | https://observe.prod.linkedin.com/g/d/eepdcpdadmpkwb/airflow-alerts-dashboard-otel?orgId=1 |
| War Alerts Dashboard | same + `&var-cluster=war` |
| Faro Alerts Dashboard | same + `&var-cluster=faro` |
| Crew 1090 Alerts (All 399) | https://observe.prod.linkedin.com/alerts?Fabrics=prod&crewID=1090 |
| CRT Deployments | https://crt.prod.linkedin.com/#/deployment/actions?pathName=DEFAULT&productName=oklahoma-airflow-deployment |
| MySQL Triage | https://mysqlaccess.prod.linkedin.com/triage/<db_name> |
| Oncall Jira Board | https://jira01.corp.linkedin.com:8443/secure/RapidBoard.jspa?rapidView=15108 |
| Oncall Handover Doc | https://docs.google.com/document/d/1w0-LqacjfyfyEtoa5nk2gmQ2YrtQLNXgFvXYbhF3VFU |
| Airflow SLO | go/airflow-slo |
| Kusto | go/kusto (DBs: Oklahoma, Kubernetes) |
| go/supportal | For filing user tickets |
| Menagerie Kafka | https://sre.corp.linkedin.com/apps/menagerie/<fabric>/v2/kafka/metrics |

## See Also
- [Runbook](runbook.md) — First-response steps, escalation, operational rules
- [Monitoring](monitoring.md) — Grafana dashboards, Kusto queries, kubectl playbooks
- [Playbooks](playbooks.md) — Step-by-step solutions for known issue types
- [Troubleshooting](../troubleshooting.md) — Failure taxonomy (11 categories), debug checklist
- [Codebase Overview](../codebase/README.md) — Repo branches and PR guidelines

### Serialized DAG Count - War [OTEL]

**Alert**: `/Serialized DAG Count - War [OTEL]` — serialized DAG count for War zone drops significantly (>50% WoW)

**Root Cause**: dag-processor pods failing to initialize, preventing active DAG parsing and serialization

**Quick Diagnosis**:
```bash
kubectl get pods -n airflow | grep war-dag-processor
```
Look for pods stuck at `Init:X/Y` status (e.g., `Init:0/2`) persisting >5 minutes

**Detailed Investigation**:
1. Check init container failure details:
   ```bash
   kubectl describe pod <pod-name> -n airflow | tail -60
   ```
   Scan for init container error events and missing dependencies

2. Verify deployment status (new revision failing):
   ```bash
   kubectl get pods -n airflow | grep war-dag-processor | awk '{print $2, $5}'
   ```
   Pattern: new pods stuck at `Init:0/2`, old pods at `Terminating` → **zero active dag-processors**

3. Check k8s events for context:
   ```bash
   kubectl get events -n airflow --sort-by='.lastTimestamp' | grep -i dag-processor
   ```

**Resolution**: Identify init container failure (dependency, config, resource constraint, or image pull issue) in new deployment revision. Rollback or merge fix to unblock initialization.

**Precedent** (Iris 261155087, 2026-04-19): Pods `airflow-war-dag-processor-78cff44db8-*` stuck in Init:0/2 for 5+ min → serialized DAG count dropped 80% (7,251 → 1,454) WoW. RC action: identified init failure via `kubectl describe`, traced root cause, coordinated rollback.
