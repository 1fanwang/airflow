> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# Oncall — Quick Reference

**When paged: go to [Runbook](runbook.md)**

---

## Top Dashboards

| Cluster | Operational | Alerts |
|---------|-------------|--------|
| Holdem | [Operational](https://observe.prod.linkedin.com/g/d/eeo69wtjw50xsd/airflow-operational-dashboard-otel?orgId=1) | [Alerts](https://observe.prod.linkedin.com/g/d/eepdcpdadmpkwb/airflow-alerts-dashboard-otel?orgId=1) |
| War | Same — change `var-metrics_namespace=airflow-war` | Same — `var-cluster=war` |
| Faro | Same — change `var-metrics_namespace=airflow-faro` | Same — `var-cluster=faro` |

## Support Channels

| Channel | Purpose |
|---------|---------|
| **#ask_airflow** (Slack) | Primary user-facing support |
| **go/supportal** | Ticket queue — primary tracking |
| **ask_airflow@linkedin.com** | Creates Supportal ticket automatically |

## Oncall Roster

- **Who is oncall right now:** `oncall.prod.linkedin.com/team/airflow`
- **Jira oncall dashboard:** `https://jira01.corp.linkedin.com:8443/secure/RapidBoard.jspa?rapidView=15108`
- **Handover doc:** `https://docs.google.com/document/d/1w0-LqacjfyfyEtoa5nk2gmQ2YrtQLNXgFvXYbhF3VFU`

---

## Sub-Pages

| Page | Contents |
|------|----------|
| [Runbook](runbook.md) | First steps when paged, severity triage, escalation rules |
| [Playbooks](playbooks.md) | Per-issue step-by-step playbooks (scheduler down, DAG issues, etc.) |
| [Monitoring](monitoring.md) | Grafana dashboards, Kusto queries, kubectl commands |
| [Contacts](contacts.md) | Support channels, SLAs, recurring tasks, external contacts |

---

## See Also
- [Runbook](runbook.md)
- [Clusters](../clusters.md)
