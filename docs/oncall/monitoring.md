> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# Oncall — Monitoring

## SLOs (Oklahoma and RoundUp)

| Metric | Oklahoma SLO | RoundUp SLO |
|--------|-------------|-------------|
| Platform success rate | 99% | 95% |
| Webserver availability | 99% | 95% |
| Webserver latency (P99) | < 1.5s | < 5s |
| DAG first task start latency (P95) | < 10m | — |
| DAG load latency (P95) | < 15m | < 15m |

**Customer contracts**: < 500 DAGs per MP, < 12h max task runtime.

**SLO dashboards:**
- go/airflow-slo — Oklahoma SLO dashboard
- go/airflow-slo-doc — SLO definitions document
- go/okl-oncall — Oklahoma oncall dashboard

---

## Grafana Dashboards

Switch cluster in any dashboard by changing the namespace/cluster variable at the top of the UI.

| Cluster | Operational Dashboard | Alerts Dashboard |
|---------|----------------------|-----------------|
| Holdem | [Operational](https://observe.prod.linkedin.com/g/d/eeo69wtjw50xsd/airflow-operational-dashboard-otel?orgId=1) | [Alerts](https://observe.prod.linkedin.com/g/d/eepdcpdadmpkwb/airflow-alerts-dashboard-otel?orgId=1) |
| War | Same dashboard — change `var-metrics_namespace=airflow-war` | Same — `var-cluster=war` |
| Faro | Same dashboard — change `var-metrics_namespace=airflow-faro` | Same — `var-cluster=faro` |
| Lasso | Same dashboard — change `var-metrics_namespace=airflow-lasso` | Same — `var-cluster=lasso` |

Each cluster also has dedicated MySQL and NFS dashboards — links are in [Clusters](../clusters.md).

**Redis (Roundup) monitoring:** [Observe dashboard](https://observe.prod.linkedin.com/g/d/de58ymar1dg5cf/ksap-all-lideployment-product-view?orgId=1&var-product=in-redis-roundup-workflows)

**MUFN Canary dashboard:** [Observe dashboard](https://observe.prod.linkedin.com/g/d/nEQMPY84k/mufn-canary-dashboard?orgId=1&var-scenario=All&var-stack=All&refresh=60s&var-fabric=prod-ltx1-prod-1&var-fabric=prod-ltx1-beta-1) — canary monitoring across prod-ltx1-prod-1 and prod-ltx1-beta-1 fabrics; auto-refreshes every 60s.

---

## Kusto Log Queries

Navigate to `go/kusto` → select the DB → run the query.

- Use the **`Oklahoma`** DB for DAG/task data.
- Use the **`Kubernetes`** DB for pod-level logs.

### Control Plane Logs (scheduler, webserver)

```kusto
// Table: kube_pod_logs, DB: Kubernetes
kube_pod_logs
| where cluster_name == "ei-ltx1-k8s-0"
    and kubernetes__namespace_name == "oklahoma"
    and kubernetes__pod_name startswith "airflow-faro-scheduler-"
| where timestamp > datetime("2024-11-25T23:40:00Z")
| project kubernetes__pod_name, timestamp, log
| where log has "error"
```

Adjust `kubernetes__pod_name startswith` to the target cluster and pod prefix.

### DAG / Task Lifecycle Events

```kusto
// Table: AirflowDagLifecycleEvent or AirflowTaskLifecycleEvent, DB: Oklahoma
AirflowDagLifecycleEvent
| where airflowClusterName has 'holdem' or airflowClusterName has 'dbt'
| where dagRunState has 'FAILED'
| where runId has "2025-01-15"
```

### Task Instance Logs (Historical)

```kusto
// Table: OklahomaLoggingEvent, DB: Oklahoma
OklahomaLoggingEvent
| where timestamp > datetime("2025-02-01")
| where message has "MySQLdb.OperationalError"
| project dagId = tostring(loggingContext.dagId)
| distinct dagId
```

---

## kubectl Commands Reference

**Namespace policy:** `airflow` namespace is read-only (get, describe, logs, top). Full read/write in `airflow-test`. See k8s-namespace-policy.

```bash
# List all pods in a cluster
kubectl get pods -n airflow | grep airflow-holdem

# Describe a pod (events, image, config, errors)
kubectl describe pod <pod-name> -n airflow

# Logs for a pod
kubectl logs <pod-name> -n airflow

# Logs for a specific container (e.g., scheduler container)
kubectl logs <pod-name> -c scheduler -n airflow

# Exec into a pod
kubectl exec -it <pod-name> -n airflow -- bash

# Watch events for deployment failures
kubectl get events -n airflow | grep <pod-prefix>

# Restart a deployment
kubectl rollout restart deployment airflow-holdem-scheduler -n airflow
kubectl rollout restart deployment airflow-holdem-webserver -n airflow

# Check resource usage
kubectl top pods -n airflow
```

---

## Azkaban Kusto Queries

**Executor pod logs** — cluster: `inlogsliprod eastus`, DB: `Kubernetes`
```kusto
kube_pod_logs
| where cluster_name == "ei-ltx1-k8s-0"
    and kubernetes__namespace_name == "cop-dev"
    and kubernetes__pod_name startswith "fc-dep-<clusterName>-"
| where timestamp > datetime("2024-11-25T23:40:00Z")
| project kubernetes__pod_name, timestamp, log
```

**Azkaban webserver logs** — cluster: `inlogscorpplatform southcentralus`, DB: `Kubernetes`
```kusto
kube_pod_logs
| where kubernetes__namespace_name startswith "azkaban-web-"
| where timestamp > ago(1h)
| project kubernetes__pod_name, timestamp, log
| where log has "ERROR"
```

---

## See Also
- [Oncall Quick Reference](README.md)
- [Runbook](runbook.md)
- [Clusters](../clusters.md)
- [Azkaban](../systems/azkaban.md) — Azkaban monitoring and runbook
