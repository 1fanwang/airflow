> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# System — Espresso

> LinkedIn's distributed document/NoSQL store; has no direct Airflow operator but appears in Airflow as batch-pipeline DAGs that bulk-load data into Espresso stores and perform materialized-view recovery, exempt from standard alerting policies due to their legacy structure.

## What It Is

Espresso is LinkedIn's internal distributed document store — a horizontally scalable NoSQL database built on top of Helix, MySQL, and a proprietary replication layer. It stores semi-structured JSON documents indexed by database/table/key, and supports materialized views (MVs) built from the document changelog. Espresso powers a large number of LinkedIn's production services for both profile and entity data storage.

Unlike Venice (a key-value online feature store) or Kafka (event streaming), Espresso is a general-purpose document database with read/write APIs used by online services. Airflow interacts with Espresso in batch mode only — there is no streaming path.

## How Airflow Uses It

### No Direct Operator

There is no `EspressoOperator`, `EspressoHook`, or any Espresso-specific class in `lipy-airflow-providers`. Espresso is **not a first-class citizen** in the Oklahoma Airflow provider library.

### Bulk Loading (espresso-spark3-bulk-loader)

The primary Airflow interaction with Espresso is a set of DAGs in the `espresso-spark3-bulk-loader` multiproduct. These DAGs run three stages per database (cluster variants: `lasso`, `loop`, `snap`):

| DAG ID pattern | Purpose |
|---|---|
| `MoveValidatorFlow_<cluster>__espresso-spark3-bulk-loader` | Validates the incoming data before bulk load |
| `RepartitionerDiffFixer_<cluster>__espresso-spark3-bulk-loader` | Repartitions and computes diffs between snapshots |
| `EspressoBulkLoader_<cluster>__espresso-spark3-bulk-loader` | Performs the actual bulk load into Espresso |

These DAGs use standard Grid Gateway operators (most likely `HadoopJavaOperator` or `SparkBatchOperator`) to run Java/Spark jobs that write data directly into Espresso's storage layer. The implementation lives in the `espresso-spark3-bulk-loader` multiproduct, not in `lipy-airflow-providers`.

### Materialized View Recovery (espresso-mv-recovery)

A second set of DAGs handles Espresso MV recovery:

| DAG ID pattern | Purpose |
|---|---|
| `EspressoMVRecoveryFlow_<cluster>__espresso-mv-recovery` | Rebuilds materialized views from the Espresso changelog after failures |

MV recovery is triggered after Espresso changelog failures, node replacements, or bulk loads that require MV rebuild. Like the bulk loader DAGs, these run on standard Grid Gateway operators.

### Entity Node Venice Push (Espresso → Venice bridge)

A special DAG ID exemption exists for:
- `*__entity_node_espresso_offline_venice_push`
- `*__entity_node_espresso_offline_venice_push_dark`

These DAGs read entity data *from* Espresso (or a derived offline copy) and push it into Venice stores. This represents the pattern of Espresso being used as the source of truth for entity data, with Venice serving as the online feature-serving layer. The `_dark` variant likely runs shadow/canary traffic for validation.

### Policy Exemptions

Because these DAGs are legacy pipelines managed by the Espresso team (pre-dating Oklahoma policy enforcement), they carry blanket exemptions in the policy framework:

1. **Deprecated operator exemption** (`validation.py`): DAGs in `espresso_oklahoma_airflow_workflows` are exempt from the deprecated-operator check. This indicates they may still use `_BasePythonVirtualenvOperator` or other deprecated types.

2. **Alerting policy exemption** (`alerting.py`): All `espresso-spark3-bulk-loader` and `espresso-mv-recovery` DAGs are exempt from the `airflow_prod_on_failure_alert` and `airflow_prod_sla_miss_alert` policies (DEPEND-59889). They do not need to configure IRIS incident callbacks or dagrun timeouts.

These exemptions were added in bulk (DEPEND-59889) as a migration accommodation — they are not the intended long-term state.

### Common Patterns for Querying Espresso from DAGs

If you need to query Espresso data from a DAG, the standard approaches at LinkedIn are:

1. **Read from the offline copy via Dali/HDFS** — Espresso tables are usually snapshotted to HDFS via Databus/Gobblin pipelines. Prefer reading the HDFS snapshot rather than querying Espresso directly.
2. **Use `HadoopJavaOperator` with a job class that wraps Espresso's Java client** — There is no Airflow hook for Espresso's REST/Java API, but you can submit a HadoopJava job that uses `com.linkedin.espresso.*` client libraries.
3. **Use the Venice push pattern** — For read-heavy online serving, push derived data from HDFS to Venice via `VenicePushOperator`; don't query Espresso from serving paths.

## Common Failure Modes

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| `EspressoBulkLoader_*` DAG fails mid-load | HDFS input data is corrupt or missing required schema | Check Spark logs via `mufn.log.url` XCom; validate input data before re-running |
| `MoveValidatorFlow_*` fails validation | Source snapshot has schema changes not reflected in Espresso store definition | Update Espresso store schema or regenerate input snapshot |
| `RepartitionerDiffFixer_*` OOM | Input partition is too large for the configured executor memory | Increase executor memory in the Grid Gateway job params or reduce partition size |
| `EspressoMVRecoveryFlow_*` stuck in RUNNING | MV rebuild is a long-running operation; normal for large stores | Monitor Espresso cluster directly; do not kill the Airflow task prematurely |
| Entity node Venice push fails | Espresso offline snapshot is stale or Venice store schema mismatch | Verify HDFS snapshot freshness; check Venice store schema against Espresso entity model |
| Policy check failures on bulk loader DAGs | Alerting policies are not exempt on a new cluster variant (new DAG ID prefix) | Add a `DagPolicyExemption` regex in `airflow-policy-framework/.../alerting.py` for the new DAG ID pattern |
| Hive view `AnalysisException` / wrong field paths | Hive view definition out of sync with Espresso table schema after Kyoto migration; union type wrapping (`tag`/`field0` fields) changes | Regenerate Hive view; prefer querying via Trino which handles schema evolution. **Recurring issue — 4+ months of flip-flopping** (APA-144168, 2026-04-14). See [Jira Patterns](../jira/patterns.md#P18) |

## Contacts / Owners

Espresso as a platform is owned by the **Espresso team** (part of LinkedIn's data infrastructure). The bulk loader and MV recovery DAGs are owned by the same team. For policy exemption changes, coordinate with the **Oklahoma Airflow team**.

## See Also
- [Systems Index](README.md)
- [Venice](venice.md)
- [GGW](ggw.md)
- [Spark](spark.md)
- [DAG Authoring](../dag-authoring.md)
