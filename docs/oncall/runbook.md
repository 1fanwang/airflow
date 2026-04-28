> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# Oncall — Runbook

## When Paged: Ordered Checklist

1. **Check the Grafana dashboard** — open the Operational and Alerts dashboards for the affected cluster. Look for scheduler heartbeat gaps, task scheduling rate drops, and error rate spikes. See [Monitoring](monitoring.md) for URLs.

2. **Check pod health:**
   ```bash
   kubectl get pods -n airflow | grep <cluster-name>
   ```
   Look for `CrashLoopBackOff`, `Error`, `Pending`.

3. **Read pod logs** for any unhealthy component:
   ```bash
   kubectl logs <pod> -n airflow
   kubectl describe pod <pod> -n airflow
   ```

4. **Check Kusto** for control plane errors using the `kube_pod_logs` query. See [Monitoring](monitoring.md) for the query.

5. **Check CRT** for recent deployments that may have introduced the issue:
   `https://crt.prod.linkedin.com/#/deployment/actions?pathName=DEFAULT&productName=oklahoma-airflow-deployment`

6. **If MySQL related:** page `#mysql` oncall. Find the DB name from [Clusters](../clusters.md) (e.g., `airflow` for holdem, `airflow_war` for war).

7. **If a deployment needs rollback:** deprecate the bad version and re-nominate the last good version through CRT in the standard cluster order (grid1-test → faro → corp → holdem/dbt → war).

8. **Escalate to the secondary oncall** for any bugfix or code change needed.

For specific issue types, go to [Playbooks](playbooks.md).

---

## Severity Triage

Triage in this order — stop at the first category that applies:

| Severity | Definition | Action |
|----------|-----------|--------|
| **GCN** | Widespread outage affecting production DAG execution | Drop everything — full primary attention immediately. Engage secondary for bugfix work. |
| **Siteup** | Major customer cluster disruption (FeedAI, DQ, etc.) | Treat like GCN — prioritize above all general user asks. |
| **Urgent user block** | User deployment or execution completely blocked | Prioritize within current oncall session. |
| **Standard ask** | General questions, non-blocking issues | Address FIFO during the oncall shift. |

---

## Escalation Rules

- **Primary oncall:** handles triage and all user communication.
- **Secondary oncall:** handles bugfixes and code changes. Engage secondary for any fix that requires a code change or deployment.
- **Ticket threshold:** if an issue is not resolved in ~10–15 minutes, ask the user to file a Supportal ticket. Always file a ticket for confirmed bugs.

---

## Operational Rules

- **Friday deployment freeze:** no production deployments after 2pm on Fridays.
- **Deployment order:** grid1-test → faro → corp → holdem/dbt → war. Verify each cluster before proceeding.
- **After War deployment:** tag RDev stable image via manual GitHub Actions workflow in `oklahoma-airflow-deployment`.

---

## On-Call Responsibility Priority

(From oncall handover doc — priority order)

1. **Check Deployment Freshness** — ensure latest deployments are live and healthy
2. **Be available 24×7** for Airflow reliability
3. **React to all system IRIS alerts**
4. **Monitor Oklahoma Dashboards** (go/okl-oncall, go/airflow-slo)
5. **Engage in incident channels** caused by or impacting Airflow
6. **Attend Airflow office hours** (go/airflow-oh-signup) — engage secondary if a site-up issue prevents you from attending
7. **Respond to Slack threads** when tagged by manager or leads (prevent escalations)
8. **Handle support tickets** — triage by age, move waiting-for-user to "On Hold", open bugfix tickets
9. **Reply to Slack** in #ask_airflow, #oklahoma-hi-support, #roundup
10. **Pick up bug fixes** as time permits

---

## Handover Standing Agenda

1. **[Secondary]** Review OSUA (OS Upgrade Automation) status — check dashboard on expiry, start/check upgrade process; for major OS version bump, roll out in EI first; check stuck nodes (review Thursday)
2. Review weekly support metrics
3. Discuss incidents and site-up issues
4. Review and prioritize Jira bugs opened during on-call
5. Discuss notable on-call issues
6. Handoff open ticket context
7. **Bulk reassign tickets** to new oncall using this JQL:
   ```
   (labels = "supportal-v1-problem-type:ask_airflow/airflow-migration-support"
    OR labels = "supportal-v1-problem-type:ask_airflow/airflow-infrastructure")
   AND assignee IS NOT EMPTY AND status NOT IN (closed, resolved)
   AND assignee = currentUser()
   ```
   Use Tools → "Bulk Change: all X issue(s)" — **uncheck** "Send mail for this update" to avoid spam.

---

## Known Operational Issues (from Oncall Handover)

### Airflow → GGW Connection Failures (Recurring)

Two distinct sub-patterns have been observed causing Airflow tasks to fail when connecting to Grid Gateway:

1. **GGW DB connection cleaned up too early**: The GGW database connection is garbage-collected before the Airflow task finishes polling, causing the task to get retried despite the underlying job running properly.
2. **LKS to NKS redirection timeout**: The background LKS→NKS (Legacy Key Service → New Key Service) migration causes GGW connections to time out in Airflow when the redirect takes too long.

**Impact**: Tasks get retried despite the underlying GGW job succeeding. This leads to duplicate job executions unless `enable_job_checkpoint=True` (default).

**Mitigation**: Ensure `enable_job_checkpoint=True` is set. If task keeps retrying: check GGW execution URN in task logs — the original job may have succeeded.

### RDev → Prod Accidental Writes (Venice Safety Warning)

Multiple users have accidentally wiped production Venice stores when connecting their rdev to Holdem without proper safeguards. The Venice team requested more prominent warning messages in the `picli rdev setup` flow.

**Key risk**: RDev DAGs that write to Venice or other production data stores have no built-in guardrail preventing writes to prod. The `allow_rdev_runs=False` flag only applies to operator-level execution skipping, not to the underlying data store being written to.

**Mitigation**: Always use oklahoma-config-system to separate rdev vs. prod config (APA-143327). Use test/staging Venice stores in rdev, not production stores.

### DQ DAG Mass Failure After Deployment (Observed Mar 2026)

When a deployment doesn't properly clean up old DAG files, the scheduler can pick up both old and new versions of the same DAG, causing mass import errors or execution failures. In one incident, a DQ DAG cleanup failure led to a temporarily bumped file limit from 500 to 5,000 files.

**Mitigation**: When mass DAG failures appear after a deployment, check for duplicate DAG directories under `/opt/airflow/dags/<mp_name>/`. See [Playbooks](playbooks.md#changed-application-name) for cleanup steps. The DQ team agreed to coordinate large DAG deletions with Oklahoma.

### Roundup Task Instances Not Progressing (Recurring)

Roundup task instances can get stuck (not progressing) after Redis restarts or OSUA upgrades. Root cause is unknown but mitigation is consistent:

1. **Fastest**: Start new Celery instances (`rain instance create <slice-id> -f <fabric> --count <N>`)
2. **Alternative**: Redeploy (`lid-client control restart -f <FABRIC> --with-slice-id <SLICE_ID>`)
3. **Alternative**: Full restart via CRT (empty commit to trigger redeploy)

---

## See Also
- [Oncall Quick Reference](README.md)
- [Playbooks](playbooks.md)
- [Monitoring](monitoring.md)
- [Contacts](contacts.md)
- [Clusters](../clusters.md)
