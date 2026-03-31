---
name: Kafka
description: Apache Kafka streaming usage patterns in oklahoma-managed-airflow workspace — producers, consumers, and Airflow integration
---

# Kafka

## Usage in This Workspace

Kafka is used for event streaming, primarily through Airflow providers and the mufn-service event databroker.

### Key Files
- `lipy-airflow-providers/oklahoma-helpers/src/linkedin/oklahoma/helpers/kafka.py` — Kafka helper utilities for Airflow DAGs
- `mufn-service/mufn-event-databroker/` — Event databroker service using Kafka
- `lipy-airflow-providers/apache-airflow-providers-lnkd/` — LinkedIn Airflow providers with Kafka operators

### Dependencies
- `lipy-kafka` — LinkedIn's Python Kafka client library

### Patterns
- Use `lipy-kafka` for Python Kafka producers/consumers, not raw `confluent_kafka`
- Kafka helpers in `oklahoma-helpers` wrap common patterns for DAG authors
- The mufn-service event databroker handles event routing and schema management
- Avro schemas are used for Kafka message serialization

### When Working With Kafka Code
- Check `lipy-airflow-providers` for Airflow-specific Kafka operators and hooks
- Check `mufn-service/mufn-event-databroker/` for the event broker service
- Use the `infra-specs-expert` skill for Kafka topic configuration and schema registry details
