---
name: Grid
description: LinkedIn Grid compute platform patterns in oklahoma-managed-airflow workspace
---

# Grid

## Usage in This Workspace

Grid is LinkedIn's compute platform, used through Airflow providers for job submission and management.

### Key Files
- `lipy-airflow-providers/apache-airflow-providers-lnkd/src/airflow/providers/lnkd/gridgateway/` — Grid Gateway Airflow operators and hooks
- `mufn-service/` — Uses Grid for compute workloads

### Patterns
- Airflow DAGs submit Grid jobs via custom operators in `gridgateway/`
- Grid Gateway provides REST API for job submission and monitoring
- Jobs are submitted with resource requirements (CPU, memory, GPU)

### When Working With Grid Code
- Grid operators are in `lipy-airflow-providers` under `gridgateway/`
- Use Grid Gateway operators for new compute workloads in DAGs
- Use the `infra-specs-expert` skill for Grid resource limits and queue configuration
