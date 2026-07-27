"""Emit the metrics POC's exact ti_failures / operator_failures tags via the real
Airflow OTel Stats client, across a realistic failure matrix, to the live collector."""
import random
import time

from airflow.stats import Stats

KINDS = ["infra", "application", "timeout", "manual"]
DAGS = [f"etl_{i}" for i in range(5)]
TASKS = ["extract", "transform", "load"]
random.seed(7)

series = set()
emitted = 0
for dag in DAGS:
    for task in TASKS:
        # Realistic: every task hits infra + application; timeout/manual only sometimes.
        hits = {"infra": random.randint(1, 9), "application": random.randint(1, 6)}
        if random.random() < 0.5:
            hits["timeout"] = random.randint(1, 3)
        if random.random() < 0.35:
            hits["manual"] = 1
        for kind, count in hits.items():
            series.add((dag, task, kind))
            for _ in range(count):
                Stats.incr("ti_failures", tags={"dag_id": dag, "task_id": task, "failure_kind": kind})
                Stats.incr(
                    "operator_failures",
                    tags={"dag_id": dag, "task_id": task, "operator_name": "PythonOperator", "failure_kind": kind},
                )
                emitted += 1

base = len({(d, t) for d, t, _ in series})
print(f"[emit] {emitted} increments; base (dag x task) = {base}; ti_failures series = {len(series)}")
print(f"[emit] worst-case if every task hit all 4 kinds = {base * 4}; actual = {len(series)} (tasks fail a subset)")
print("[otel] flushing...")
time.sleep(6)
print("[done]")
