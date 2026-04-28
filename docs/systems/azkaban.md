> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# Azkaban (Airflow Integration)

> Legacy job orchestrator maintained by Oklahoma team. In deprecation — go/bye-bye-az.

## Airflow Integration Points

- **`AzkabanFlowExecutionOperator`** — triggers flows; embeds sensor when `wait_for_completion=True`
- **`AzkabanFlowExecutionSensor`** — polls until completion; tracks state in XCom; max 3 consecutive connection failures
- Use for legacy workflow migration only — prefer Grid Gateway for new DAGs

## Error Codes (When Azkaban Jobs Surface in Airflow Tickets)

| Code | Meaning |
|------|---------|
| `AIRFLOW::TASK_EXEC::ABORTED` | SIGTERM from pod disruption during GGW/ARMS processing |
| `AIRFLOW::TASK_EXEC::HEARTBEAT_LOST` | Zombie task — pod evicted or stale logging recursion crash |
| `AIRFLOW::TASK_EXEC::PRE_EXEC_FAILURE` | DAG file not found / FileNotFoundError during setup |
| `AIRFLOW::MISSING::NON_GGW::EMPTY_FAILURE_MSG` | DBT cluster (Airflow 2.5.3) or ExternalPythonOperator in legacy env |

See [Jira Patterns](../jira/patterns.md) P24–P27 for Azkaban-sourced error patterns.

## See Also

- [GGW](ggw.md) — GGW handles job execution
- [DAG Authoring](../dag-authoring.md) — operator reference
