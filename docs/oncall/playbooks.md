> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# Oncall — Playbooks

Note: these playbooks cover **operator/on-call engineer** issues. For DAG author issues (sensor timeouts, proxy ACLs, import errors, etc.), see [Jira Playbooks](../jira/playbooks.md).

---

## Scheduler Down / Not Running

**Symptoms:** DAGs not executing; scheduler pod in `CrashLoopBackOff` or `Pending`.

**Steps:**
1. `kubectl get pods -n airflow | grep <cluster>-scheduler` — check pod state.
2. `kubectl describe pod <scheduler-pod> -n airflow` — look at Events section for image pull failures, OOM, config errors.
3. `kubectl logs <scheduler-pod> -c scheduler -n airflow` — look for startup errors.
4. Check Grafana Operational dashboard for scheduler heartbeat gaps.
5. Check Kusto: `kube_pod_logs` filtered to the scheduler pod for the relevant timeframe (see [Monitoring](monitoring.md)).
6. If MySQL error like `MySQLdb.OperationalError: (2013, 'Lost connection...')`, page **#mysql** oncall.
7. If the issue is a bad deployment: roll back via CRT (see [Deployment](../deployment.md#rollback)).

---

## DAG Not Running (User Report)

**Steps:**
1. Ask user for DAG ID and cluster name.
2. Check the Airflow UI for the DAG — is it paused? Is `is_active=True`?
3. Check if the DAG has import errors (visible in UI under DAGs list, or in scheduler logs).
4. Check if the upload succeeded: look for the DAG directory under `/opt/airflow/dags/<mp_name>/` by exec'ing into the scheduler pod.
5. If duplicate DAG (two directories with same DAG IDs), see [Changed Application Name](#changed-application-name) below.
6. Verify the DAG's schedule has not changed in a way that skips the expected run.

---

## Task Failure

**Steps:**
1. Direct the user to check task logs in the Airflow UI — logs are served via the InLogs API (Kusto-backed); if empty, fallback is NFS.
2. If logs are empty, check `INLOGS_SECRET` secret and `LOGGING_FALLBACK_TO_FILE_ENABLED` env var are configured correctly on the webserver.
3. For historical logs, use the `OklahomaLoggingEvent` Kusto table (see [Monitoring](monitoring.md)).
4. For Grid Gateway (Spark, Flink, etc.) failures: the XCom `spark.log.url` field in the task instance contains the job log URL — share this with the user.

---

## Deployment Failure (User DAG Upload)

**Symptoms:** User's DAG is not appearing after a new version was pushed through CRT, or deployment is stuck.

**Steps:**
1. Confirm with the user their MP name, application name, and version.
2. Check if import errors are blocking: look at the upload endpoint response, or check scheduler logs for `ImportError` in the DAG file path.
3. If blocked by import errors and it is urgent, add the MP to `IGNORE_IMPORT_ERRORS_MP_ALLOW_LIST` in helm values and redeploy, or use the manual upload script (see [Deployment](../deployment.md#override-blocked-crt-deployments)).
4. Check the `airflow-crt-action` pod logs in the `orchestration-actions` namespace on `corp-lva1-k8s-0`.

---

## Changed Application Name (Duplicate DAG Issue)

**Symptoms:** DAG behaves inconsistently, alternating between two versions of the same DAG ID.

**Root cause:** Old Airflow application directory not cleaned up during deployment — scheduler alternates between two file paths.

**Confirm:**
1. UI shows DAG code changing between page loads.
2. In the Airflow DB, `dag.fileloc` oscillates between two paths.
3. `ls /opt/airflow/dags/<mp_name>/` shows two application directories.

**Fix:**
```bash
kubectl exec -it <scheduler-pod> -n airflow -- bash
cd /opt/airflow/dags/<mp_name>/
rmdir -r <stale_app_name>
rmdir -r <stale_app_name>.dir
# Wait ~15 minutes for scheduler reconciliation
```

---

## Roundup Scheduler Count Decrease

**Symptoms:** Roundup alert fires for fabric `ei4`, `prod-lva1`, or `corp-lca1`.

**Step 1 — Get scheduler slice ID:**
```bash
go-status -f <FABRIC> roundup-workflows
# Note the slice ID for scheduler instances (last column)
```

**Step 2 — Redeploy schedulers one at a time:**
```bash
lid-client control restart -f <FABRIC> --with-slice-id <SLICE_ID>
# Takes 5-10 minutes, redeploys schedulers one by one
```

**Step 3 — If redeployment fails, SSH into scheduler host:**
```bash
# From go-status output, find the scheduler host (e.g. lva1-app132793.prod.linkedin.com)
ssh -p 20022 lva1-app132793.prod.linkedin.com
cat logs/li-airflow-scheduler.out
# If MySQL error → page #mysql oncall
```

---

## InLogs / Task Log Troubleshooting

Task logs in Airflow are read via the **InLogs API** (Kusto-backed).

**Symptoms:**
- Task logs showing blank in the UI.
- "Falling back to file" messages in webserver logs.

**Quick validation (from inside a cluster pod):**
```python
import requests
payload = {'environment': 'inlogsprod', 'kqlQuery': '<your query>', 'application': ''}
url = 'https://inlogs-api.prod.linkedin.com/api/v2/inlogs/kqlRunner'
headers = {'x-api-key': '<API-Key>', 'X-Li-R2-W-Ic-1-Datavaultidentitytoken': '<DV-Token>'}
response = requests.post(url, json=payload, headers=headers)
print(response.status_code, response.text)
```

**Required helm chart env vars for InLogs:**
- `INLOGS_SECRET` — loaded from `airflow-inlogs-api-secret` k8s secret.
- `LOGGING_FALLBACK_TO_FILE_ENABLED: 'True'`
- `AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS: 'airflow.providers.lnkd.log.oklahoma_logging_config.OKLAHOMA_LOGGING_CONFIG'`

---

## Redis (Roundup) Troubleshooting

Redis for Roundup is managed via the `in-redis-roundup-workflows` MP.

```bash
# Check deployment status
kubectl in status lideployment \
    --product in-redis-roundup-workflows \
    --application in-redis-roundup-workflows \
    --fabric prod-lva1
# Fabrics: ei4, prod-lva1, corp-lca1
```

Monitoring: [Observe dashboard](https://observe.prod.linkedin.com/g/d/de58ymar1dg5cf/ksap-all-lideployment-product-view?orgId=1&var-product=in-redis-roundup-workflows)

---

## See Also
- [Oncall Quick Reference](README.md)
- [Runbook](runbook.md)
- [Monitoring](monitoring.md)
- [Jira Playbooks](../jira/playbooks.md)
- [Clusters](../clusters.md)
