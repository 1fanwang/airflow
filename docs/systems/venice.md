> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# System — Venice

> LinkedIn's distributed key-value store (online feature store), used in Airflow to bulk-push feature data and document-store records via VenicePushOperator over Grid Gateway.

## What It Is

Venice is LinkedIn's internal distributed key-value / feature store. It stores the online (real-time serving) side of feature data, indexed by entity keys (e.g. member URN). Venice is the primary online store for the Feature Cloud platform — when a feature group has a `veniceStores` entry, Airflow is responsible for pushing new data into it on each pipeline run. Venice is also used as the online backing store for the Document Store platform (Espresso-adjacent, see [Espresso](espresso.md)).

Venice stores are fabric-scoped — a single logical store typically has separate physical instances per fabric (`ltx1`, `lor1`, `lva1`).

## How Airflow Uses It

### VenicePushOperator

**Module**: `airflow.providers.lnkd.gridgateway.operators.venice_push.VenicePushOperator`

**Inheritance chain**: `VenicePushOperator` -> `GridGatewayBaseOperator`

VenicePushOperator submits a `VenicePushJob` to Grid Gateway. It does not call Venice APIs directly — Grid Gateway's VPJ function reads Avro data from HDFS and writes it to the Venice store.

#### Constructor Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `proxy_user` | `str` | yes | LDAP user or headless account to impersonate |
| `input_path` | `str` | yes | HDFS path to the Avro input data |
| `venice_store_name` | `str` | yes | Name of the Venice store; used for schema validation |
| `key_field` | `str` | no | Avro field to use as the Venice key (e.g. `"keys"`, `"key"`) |
| `value_field` | `str` | no | Avro field to use as the Venice value (e.g. `"features"`, `"value"`) |
| `enable_job_checkpoint` | `bool` | default `True` | Resume polling on pod restart without re-submitting |

All `grid_gateway_params`, `dependency_ivy`, `grid_gateway_dependencies`, `grid_gateway_function_overrides`, `polling_interval`, and `allow_rdev_runs` from `GridGatewayBaseOperator` are also available.

#### Grid Gateway Parameter Mapping

| Operator param | GGW param key |
|---|---|
| `input_path` | `input.path` |
| `venice_store_name` | `venice.store.name` |
| `key_field` | `key.field` |
| `value_field` | `value.field` |
| DSL type (hardcoded) | `dsl.type` = `"VenicePushJob"` |

Additional Venice behavior can be controlled via `grid_gateway_params`, for example:
- `defer.version.swap=true` — delay version swap for roll-forward scenarios (used when `veniceRollForwardConfig` is set on the feature group)
- `rewind.epoch.time.in.seconds.override` — set rewind time (used by Document Store to replay Kafka changelog after watermark)

#### Execution Flow

1. `execute()` calls `super()._execute(context, proxy_user, "VenicePushJob", xcom_log_url="venice_push.log.url")`
2. `GridGatewayBaseOperator._execute()`:
   - Skips in rdev unless `allow_rdev_runs=True` (Feature Cloud explicitly disables Venice push in non-faro rdev environments to prevent data corruption)
   - Obtains auth context, submits `VenicePushJob` to GGW, stores execution URN
   - Polls every `max(polling_interval, 30)` seconds
   - Pushes `venice_push.log.url` and `mufn.log.url` to XCom on first observation
3. On success: returns execution response dict
4. On failure: raises `GridGatewayExecutionError`

#### XCom Keys

| Key | Content |
|-----|---------|
| `venice_push.log.url` | Direct link to VenicePushJob log |
| `mufn.log.url` | Grid Gateway execution log URL |

Both surface as clickable links via `VenicePushOperatorLink` and `GridGatewayBaseOperatorLink`.

### Feature Cloud Integration (primary use case)

The main production use of VenicePushOperator is inside `feature_cloud_push_task_group.py` via the `feature_cloud_push()` function. This is a **TaskGroup** (not a single operator) that orchestrates the full Feature Cloud ingestion pipeline.

#### When Venice Push is Included

Venice push is included in the task group when the feature group's spec contains a non-empty `veniceStores` list. The store name is read from `feature_group_json["veniceStores"][0]["storeName"]`.

#### Full Task Sequence (online store path)

```
init_task
  -> prepare_temp_directory
  -> feature_group_write (Spark: writes feature group metadata to HDFS)
  [-> fetch_and_process_data_quality_assertions (PythonOperator, if DQ enabled)]
  [-> data_quality_check (DataQualityJobOperator, if DQ enabled)]
  -> openhouse_write (SparkBatchOperator: writes offline store to OpenHouse)
  -> venice_preprocessing (SparkBatchOperator: com.linkedin.featurecloud.ingestion.VenicePushPreparation)
  -> venice_push (VenicePushOperator)
  -> venice_push_cleanup (HadoopShellOperator: hadoop fs -rm -r -f <temp_dir>/venice_push)
  [-> feature_cloud_metadata_job/push_metadata_to_kafka (KafkaPushOperator)]
```

Key details:
- `venice_preprocessing` uses `SparkBatchOperator` to run `com.linkedin.featurecloud.ingestion.VenicePushPreparation` from `feature-cloud-ingestion-impl` JAR
- Output is written to a timestamped temp directory: `<project_directory>/featurecloud/temp/<datetime>/venice_push`
- `key_field="keys"`, `value_field="features"` (Feature Cloud convention)
- Roll-forward config: if `veniceRollForwardConfig` is set on the store, `defer.version.swap=true` is added to `grid_gateway_params`
- rdev safety: Venice push is disabled if the ARMS instance is not `arms-faro` (to prevent dev DAGs from corrupting prod Venice stores)

#### Disabling Venice Push

Pass `disable_venice_push=True` to `feature_cloud_push()` to skip the Venice push and cleanup tasks entirely. This is done automatically in rdev non-faro environments.

### Document Store Integration

The `document_store_push_task_group.py` provides a second production use case. Document Store pushes to Venice for three fabrics (`ltx1`, `lor1`, `lva1`) in parallel.

Per-fabric flow (implemented in `PushJobHelper`):
```
start
  -> merge_with_venice_etl_<fabric> (SparkBatchOperator: com.linkedin.documentstore.spark.MergeDocJobImpl)
  -> read_watermark_file_<fabric>   (HadoopShellOperator: hadoop fs -cat <watermark_file>)
  -> venice_push_<fabric>           (VenicePushOperator, store name = <document_store_name>_<fabric>)
  -> merge (fan-in)
-> venice_push_cleanup
```

The `rewind.epoch.time.in.seconds.override` GGW param is populated from the watermark XCom value of the preceding `read_watermark_file` task, ensuring Venice replays the Kafka changelog from the correct offset.

## Common Failure Modes

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| `GridGatewayExecutionError` "schema validation failed" | Input Avro schema does not match the Venice store schema | Ensure the preprocessing Spark job outputs data matching the registered store schema |
| Venice push succeeds but data is not visible in serving | `defer.version.swap=true` is set; the swap is deferred to the roll-forward job | Check if the roll-forward Airflow job ran; trigger it manually if needed |
| venice_push task skipped in rdev | rdev guard: ARMS_INSTANCE is not `arms-faro` | Expected behavior; Venice push is intentionally disabled in non-faro rdev to protect prod |
| `GridGatewayProxyUserPermissionException` | `proxy_user` lacks YARN/Venice push permissions | Grant proxy user permission on the Venice store |
| `venice_push_cleanup` fails but venice_push succeeded | HDFS temp directory already cleaned up or path resolution from XCom failed | Idempotent — safe to manually delete the temp path; does not affect Venice data |
| Document Store push fails on `merge_with_venice_etl` | Input `field_data_paths` point to empty or missing HDFS data | Verify upstream ETL jobs completed and wrote output to the expected paths |
| Watermark XCom missing for Document Store push | `read_watermark_file` task failed or returned empty output | Check HDFS watermark file exists; the watermark is required for correct Venice rewind |
| rdev venice_push unexpectedly runs in non-prod | `disable_venice_push=False` explicitly passed to `feature_cloud_push()` | Remove explicit override; let the rdev guard handle it automatically |

## OpenHouse Integration

OpenHouse is the **offline store** counterpart in Feature Cloud. When a feature group has `offlineStores`, `SparkBatchOperator` runs `com.linkedin.featurecloud.ingestion.OpenHousePush` before the Venice preprocessing step. OpenHouse and Venice pushes are sequenced: offline (OpenHouse) -> online (Venice preprocessing -> Venice push). This sequencing ensures the feature group metadata on HDFS (written by `feature_group_write`) is available to both jobs.

## Contacts / Owners

Venice as a platform is owned by the **Venice team** (part of BDP). The `VenicePushOperator` and Feature Cloud integration are owned by the **Feature Cloud / AIM team**. The Document Store task group is owned by the **Document Store team**.

## See Also
- [Systems Index](README.md)
- [GGW](ggw.md)
- [Kafka](kafka.md)
- [Spark](spark.md)
- [Espresso](espresso.md)
- [DAG Authoring](../dag-authoring.md)
