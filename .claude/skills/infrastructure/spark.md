---
name: Spark
description: Apache Spark compute patterns via Grid Gateway in oklahoma-managed-airflow workspace
---

# Spark

## Usage in This Workspace

Spark is used for distributed compute jobs, submitted through Grid Gateway via Airflow operators.

### Key Files
- `lipy-airflow-providers/apache-airflow-providers-lnkd/src/airflow/providers/lnkd/gridgateway/operators/spark_batch.py` — Spark batch job operator for Airflow

### Patterns
- Spark jobs are submitted as batch jobs through Grid Gateway
- The `spark_batch.py` operator wraps Spark submission for Airflow DAGs
- Jobs are configured with Spark-specific parameters (executor count, memory, etc.)

### When Working With Spark Code
- Use the Spark batch operator from `gridgateway/operators/` for new Spark jobs in DAGs
- Spark configurations are passed through the operator, not set globally
- Use the `infra-specs-expert` skill for Spark cluster sizing and tuning
