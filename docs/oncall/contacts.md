> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# Oncall — Contacts

## Support Channels

| Channel | Purpose | Notes |
|---------|---------|-------|
| **#ask_airflow** (Slack) | Primary user-facing support channel | LamaBot answers common questions using FAQ |
| **#oklahoma-hi-support** (Slack) | HI-related migration asks | LamaBot also active here |
| **ask_airflow@linkedin.com** (email) | Creates a Supportal ticket automatically | Preferred for tracking |
| **go/supportal** | Ticket queue for all user asks | Primary tracking system |
| **go/ask-airflow** | Supportal shortlink | Redirects to ticket queue |
| **go/airflow-oh-signup** | Office hours signup sheet | 2–3pm Mon–Thu |

**Office hours:** 2–3pm, Monday through Thursday. Max 5 users per session, 10-minute slots. Require a ticket before the session.

**FAQ maintenance:** Add repeated questions to LinkedIn StackOverflow (`go/stackoverflow`) with tags: `airflow-hi`, `azkaban-migration`, `azkaban-to-airflow`, `oklahoma-hi`. LamaBot uses these as a source.

---

## Oncall Roster and Docs

- **Current primary/secondary oncall:** `oncall.prod.linkedin.com/team/airflow`
- **Oncall dashboard (Jira):** `https://jira01.corp.linkedin.com:8443/secure/RapidBoard.jspa?rapidView=15108`
- **Handover doc:** `https://docs.google.com/document/d/1w0-LqacjfyfyEtoa5nk2gmQ2YrtQLNXgFvXYbhF3VFU` — review before and after each rotation.
- **HI ticket requests:** `go/airflow-new-hi-ticket` — new HI tickets are created every Tuesday and Thursday.

---

## SLA / Severity Table

| Severity | Definition | First Response |
|----------|-----------|----------------|
| GCN | Widespread outage affecting production DAG execution | Immediate — drop everything |
| Siteup | Major customer cluster disruption | Within minutes — treat as GCN |
| Urgent user block | User deployment or execution completely blocked | Prioritize within current oncall session |
| Standard ask | General questions, non-blocking issues | FIFO during the oncall shift |

---

## Recurring Tasks

| Task | Frequency | Details |
|------|-----------|---------|
| Deployment (infrastructure) | Each CRT version | Follow grid1-test → faro → corp → holdem/dbt → war order; verify each cluster; tag RDev stable after War |
| Rdev stable image tagging | After every War deployment | Manual GitHub Actions workflow in `oklahoma-airflow-deployment` |
| Office hours | Mon–Thu 2–3pm | Sign up via `go/airflow-oh-signup` |
| Oncall handover meeting | End of rotation | Review handover doc; hand off open tickets |
| Office hours signup cleanup | End of month | Create new `OH Signup <MONTH> <YEAR>` sheet tab at `go/airflow-oh-signup` |
| HI ticket creation | Every Tuesday and Thursday | Use `go/airflow-new-hi-ticket` sheet as input |

---

## External Escalation Contacts

| System / Issue | Contact |
|---------------|---------|
| MySQL connection issues (`MySQLdb.OperationalError`) | **#mysql** oncall (Slack) |
| Grid Gateway (Spark, Flink, other GGW errors) | [Grid Jobs Platform oncall](https://oncall.prod.linkedin.com/team/team/Grid%20Jobs%20Platform) |
| Grid Gateway crew page | https://engx.corp.linkedin.com/crews/1095 |
| Grid Gateway support | https://engx.corp.linkedin.com/products/100/support |
| DataVault / identity token issues | DataVault/identity team — escalate via go/supportal |

---

## See Also
- [Oncall Quick Reference](README.md)
- [Runbook](runbook.md)
- [Clusters](../clusters.md)
- [Teams](../teams/README.md)
