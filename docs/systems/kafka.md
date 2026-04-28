> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# System — Kafka

> LinkedIn's distributed event-streaming backbone, used in Airflow primarily to bulk-push batch data from HDFS into Kafka topics via Grid Gateway's KafkaPushJob function.

## What It Is

Kafka is LinkedIn's internal distributed event-streaming platform. It underpins near-real-time data pipelines across the company. In the Airflow context Kafka is not queried directly — instead Airflow submits *batch push jobs* that move HDFS data into a Kafka topic using LinkedIn's internal KafkaPushJob (KPJ) service, which runs inside Grid Gateway.

A second role of Kafka inside Airflow is as a **log transport**: `OklahomaKafkaHandler` (extends `linkedin.kafka.log_handler.KafkaHandler`) batches task log events and produces them to a Kafka topic for downstream ingestion by Inception/EKG.

## How Airflow Uses It

### KafkaPushOperator

**Module**: `airflow.providers.lnkd.gridgateway.operators.kafka_push.KafkaPushOperator`

**Inheritance chain**: `KafkaPushOperator` -> `HadoopJavaOperator` -> `HadoopJavaProcessOperator` -> `GridGatewayBaseOperator`

KafkaPushOperator delegates to Grid Gateway's `KafkaPushJob` function (enum value `FunctionNames.KAFKA_PUSH_JOB`). It does not produce to Kafka directly — it submits a job to Grid Gateway, which runs KPJ on the grid cluster.

#### Constructor Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `proxy_user` | `str` | yes | LDAP user or headless account to impersonate |
| `topic` | `str` | yes | Destination Kafka topic name |
| `input_path` | `str` | no | HDFS path containing Avro data to push; pass `input.dali.resource` in `grid_gateway_params` for Dali input |
| `name_node` | `str` | no | HDFS namenode; KPJ auto-detects if omitted |
| `batch_size` | `int` | no | Producer batch size in bytes (`batch.num.bytes` KPJ param) |
| `kafka_url` | `str` | no | Override destination Kafka cluster URL; KPJ auto-selects data deployment cluster if omitted |
| `disable_schema_registration` | `bool` | no | Skip schema registration with the topic (KPJ still verifies the schema is already registered) |
| `enable_job_checkpoint` | `bool` | default `True` | Resume polling on Airflow pod restart without re-submitting the KPJ job |

All `grid_gateway_params`, `dependency_ivy`, `grid_gateway_dependencies`, `grid_gateway_function_overrides`, and `allow_rdev_runs` from `GridGatewayBaseOperator` are also available.

#### Grid Gateway Parameter Mapping

KafkaPushOperator translates its keyword arguments to GGW flat-dict params:

| Operator param | GGW param key |
|---|---|
| `name_node` | `name.node` |
| `batch_size` | `batch.num.bytes` |
| `kafka_url` | `kafka.url` |
| `input_path` | `input.path` |
| `disable_schema_registration` | `disable.schema.registration` |
| `topic` | `topic` |

#### Execution Flow

1. `execute()` calls `super()._execute(context, proxy_user, "KafkaPushJob", xcom_log_url="kafka_push.log.url")`
2. `GridGatewayBaseOperator._execute()`:
   - Skips execution in rdev environments unless `allow_rdev_runs=True`
   - Obtains auth context via `GridGatewayHook.get_auth_context()`
   - Calls `hook.start_execution(function_namespace="grid", function_name="KafkaPushJob", ...)` — returns an execution URN
   - Stores URN for external job checkpointing (`_external_job_id`)
   - Polls `hook.get_execution(urn)` every `max(polling_interval, 30)` seconds until terminal state
   - Pushes `kafka_push.log.url` (KPJ log) and `mufn.log.url` (GGW log) to XCom on first observation
3. On success: returns execution response dict (if `do_xcom_push=True`)
4. On failure: raises `GridGatewayExecutionError` with error code + message from GGW

#### XCom Keys

| Key | Content |
|-----|---------|
| `kafka_push.log.url` | Direct link to the KafkaPushJob YARN log |
| `mufn.log.url` | Grid Gateway execution log URL |

Both keys surface as clickable links in the Airflow task detail page via `KafkaPushOperatorLink` and `GridGatewayBaseOperatorLink`.

### Feature Cloud Metadata Push (internal use)

Inside `feature_cloud_push_task_group.py`, a `KafkaPushOperator` is used to push lineage metadata to the `MCE_AirflowDag_Union` Kafka topic after a successful Venice push. This is an internal infrastructure use, not a user-facing API:

```python
KafkaPushOperator(
    task_id="push_metadata_to_kafka",
    topic="MCE_AirflowDag_Union",
    kafka_url="kafka.local-metrics.kafka.corp-ltx1.atd.corp.linkedin.com:16637",
    input_path=metadata_dir_xcom_get,  # mce_artifacts/lineage subdirectory
    grid_gateway_params={"obtain.hcat.token": "true"},
    target_grid_cluster=<holdem|war>,
    retries=1,
)
```

Failure of this metadata push is treated as a soft failure — it does not mark the DAG run as failed (`trigger_rule=TriggerRule.ALL_DONE` on the downstream sentinel task).

### OklahomaKafkaHandler (log transport)

`airflow.providers.lnkd.log.oklahoma_kafka_handler.OklahomaKafkaHandler` extends LinkedIn's internal `KafkaHandler` to batch-produce task log events. It calls `client.batch_produce(topic=..., data_list=..., schema_id=...)` in chunks of `kafka_batch_size`. Failures emit to stderr (not raised) to avoid disrupting task execution. In dev fabric the handler is a no-op if the Kafka client is not initialized.

## Common Failure Modes

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| `GridGatewayExecutionError: FAILED` with `errorCode` from KPJ | Schema mismatch — input Avro schema not registered for the topic | Register schema with Kafka topic, or set `disable_schema_registration=False` and ensure schema matches |
| `GridGatewayExecutionError` containing "topic not found" | Topic does not exist on the target Kafka cluster | Create topic or verify `kafka_url` is pointing to the right cluster |
| `GridGatewayProxyUserPermissionException` | `proxy_user` does not have ACL rights to produce to the topic | Grant proxy user produce permission on the Kafka topic |
| Task hangs indefinitely / never reaches terminal state | KPJ stuck in RUNNING on YARN; polling never resolves | Check KPJ YARN logs via `kafka_push.log.url` XCom; kill YARN app if stuck |
| `GridGatewayConnectionException` | GGW gRPC endpoint unreachable (network or service down) | Check Grid Gateway service status; retry after cluster recovery |
| Task succeeds but no messages appear in topic | `input_path` is empty or HDFS path does not exist at execution time | Verify upstream task wrote data; check HDFS path with `hadoop fs -ls` |
| Schema registration disabled but job fails | KPJ still validates the schema is registered even when `disable_schema_registration=True` | Ensure schema is already registered before pushing |
| rdev task unexpectedly skipped | `allow_rdev_runs` defaults to `True` on `GridGatewayBaseOperator` but some subclasses differ | Explicitly set `allow_rdev_runs=True` if rdev push is intentional |

## How It Differs from Direct Kafka Clients

- **No direct Kafka SDK calls** — all pushes go through Grid Gateway's KafkaPushJob function, which handles Kerberos authentication, cluster routing, and schema validation on the grid side.
- **Input is always HDFS** — data must be staged to HDFS (or Dali) before pushing; there is no streaming/record-by-record API.
- **Schema enforcement** — KPJ always validates Avro schemas against the topic's schema registry; this cannot be fully bypassed.
- **Checkpointing** — `enable_job_checkpoint=True` (default) means Airflow pod restarts resume polling the already-running KPJ job rather than re-submitting it, preventing duplicate pushes.

## Contacts / Owners

Kafka as a platform is owned by the **BDP (Big Data Platform)** team. The `KafkaPushOperator` and GGW integration are owned by the **Oklahoma Airflow** team (same owners as `lipy-airflow-providers`).

## See Also
- [Systems Index](README.md)
- [GGW](ggw.md)
- [Venice](venice.md)
- [Spark](spark.md)
- [DAG Authoring](../dag-authoring.md)
