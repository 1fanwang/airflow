#!/bin/bash
cd ~/workspace/airflow-aip97-failure-details
IMG=ghcr.io/apache/airflow/main/ci/python3.10:latest
docker run --rm --add-host=host.docker.internal:host-gateway -v "$PWD":/opt/airflow -v /tmp/aip97-metrics:/e2e --entrypoint bash "$IMG" -lc '
export PATH=/usr/python/bin:$PATH; cd /opt/airflow
export AIRFLOW__METRICS__OTEL_ON=True AIRFLOW__METRICS__OTEL_HOST=host.docker.internal
export AIRFLOW__METRICS__OTEL_PORT=4318 AIRFLOW__METRICS__OTEL_PREFIX=airflow
export AIRFLOW__METRICS__OTEL_INTERVAL_MILLISECONDS=1000
python /e2e/emit_direct.py 2>&1 | grep -E "\[emit\]|\[otel\]|\[done\]"
'
