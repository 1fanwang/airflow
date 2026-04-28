# Codebase — Performance

> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

## What Is Tested

This repository contains automated load testing infrastructure for Apache Airflow at LinkedIn.
Designed to measure scheduler performance, test scheduling latency, identify bottlenecks, and validate performance at scale.

## Test Scenarios

### Named Configurations

Pre-built test scenarios available:

- single_workflow.json: 1000 DAGs, 5 tasks/dag, linear, 10s sleep
- scheduling_performance.json: 10 DAGs, 100 tasks/dag, no structure, 0s sleep
- concurrent_tasks.json: 100 DAGs, 50 tasks/dag, no structure, 1h sleep
- tiny_workflow.json: 10 DAGs, 2 tasks/dag, linear, 0s sleep
- blocking_workflow.json: 100 DAGs, 100 tasks/dag, no structure, 10h sleep
- skipping_workflow.json: 100 DAGs, 100 tasks/dag, linear, 0s sleep

### Automated Load Testing Parameters

The load-test command scales three dimensions simultaneously:

- Scheduler Scaling: 12, 18, 24, 30, 36 replicas
- Query Limits (ti_per_loop): 48, 72, 96, 120, 192
- DAG Load: 7500, 15000, 25000

---

## Known Bottlenecks

### Scheduler Critical Section
Metric: airflow.scheduler.critical_section_duration (microseconds)
- Global lock around DAG parsing and task scheduling
- Scales with DAG count and MAX_TIS_PER_QUERY
- Horizontal scaling reduces per-replica contention

### Scheduler Loop Duration
Metric: airflow.scheduler.scheduler_loop_duration (seconds)
- Time for one complete scheduler loop
- Typical range: 0.2–2.0 seconds

### Task Scheduling Latency
Metrics: schedule_delay, first_task_start_delay, first_task_scheduling_delay
- Grows with DAG count
- Can reach 30–60+ seconds under extreme load

### Task Instance Throughput
Metric: airflow.ti.start (count, SUM aggregation)
- Bounded by MAX_TIS_PER_QUERY
- 36 schedulers x 192 ti_per_loop: ~6,912 TIs/loop possible

### DAG Serialization
- Can take 10–15+ minutes for 25,000 DAGs
- Scales with DAG complexity

### Database Lock Contention
- MySQL bottleneck under high throughput
- Mitigated by using stable webserver pods for database operations
