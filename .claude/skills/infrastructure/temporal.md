---
name: Nephos Temporal
description: Temporal workflow orchestration patterns in oklahoma-managed-airflow workspace
---

# Nephos Temporal

## Usage in This Workspace

Temporal (via LinkedIn's Nephos platform) is used for workflow orchestration in li-productivity-agents.

### Key Files
- `li-productivity-agents/qa-agent/qa-agent-worker/` — Temporal worker for QA agent
- `li-productivity-agents/platform/mae-worker/` — Temporal worker for MAE agent

### Dependencies
- `temporalio` — Temporal Python SDK

### Patterns
- Temporal workflows define long-running, durable execution flows
- Workers poll Temporal task queues and execute workflow/activity code
- Activities are the unit of work — workflows compose activities
- Temporal handles retries, timeouts, and failure recovery automatically

### When Working With Temporal Code
- Define workflows and activities in worker modules
- Use `@workflow.defn` and `@activity.defn` decorators
- Activities should be idempotent — Temporal may retry them
- Use the `infra-specs-expert` skill for Temporal namespace and task queue configuration
