# Synthesis — Open Questions

> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

## DAG Metadata Write-Lock Contention — No Fix Available

**Domain**: codebase / airflow-fork
**Why it matters**: PR #87's SKIP LOCKED approach was reverted (PR #114, 2026-04-09) due to a correctness bug (silent metadata loss). The original lock contention (blocking `SELECT FOR UPDATE` in `bulk_write_to_db`) remains unresolved. At LinkedIn's scale (thousands of DAGs, multiple dag-processors), this can cause 23-30s lock waits.
**What's known so far**: SKIP LOCKED without a retry/queue mechanism drops updates. Upstream Apache Airflow has the same issue but at lower scale.
**Where to look next**: Consider advisory locks, row-level versioning (optimistic concurrency), or partitioned processing (assign DAG subsets to specific processors).
**Opened**: 2026-04-13

---

## Config Override Rollback → Re-bump Pattern (Apr 6–7)

**Domain**: deployment
**Why it matters**: Airflow was rolled back from 2.9.2.163 to 2.9.2.158 (PR #1025, Apr 6) then re-bumped to 2.9.2.164 (PR #1026, Apr 7). This suggests the config override trigger page changes had issues on first deploy.
**What's known so far**: PRs #101 and #104 fixed false change detection and POST body overflow, respectively. The rollback happened before these fixes were applied.
**Where to look next**: Review whether a staging gate (e.g., Faro) caught the issue or if it was found in production. Consider adding trigger-page smoke tests to the regression suite.
**Opened**: 2026-04-13

---

## Potential Regressions

*(Cross-reference of recent PRs vs. newly filed tickets — updated each kb_sync run)*

No regression signals detected for 2026-04-13 run. Jira search was attempted but MCP tools were unavailable in this session.

### [2026-04-14] Hive-Espresso Schema Flip-Flop (APA-144168)

**Signal**: APA-144168 (Closed — Fixed) notes that the `prod_learning.careerintent` Hive view has been flip-flopping between working and broken states for **4+ months**. The Kyoto migration fix was applied again on Holdem, but reporter calls it a recurring issue. No upstream PR identified.

**Risk**: Any DAG querying Hive views over Espresso tables with union-type columns is at risk of breakage after schema migrations. This is not a one-time fix — it may recur.

**Action**: Monitor for new tickets matching this pattern (Hive `AnalysisException` / `tag`/`field0` field path issues). Consider adding a sentinel DAG that queries known Hive-Espresso views and alerts on schema drift.

### [2026-04-14] OPAL Decommission Wave — Ongoing Oracle-to-MySQL Cutovers

**Signal**: Previous wave: six tickets (APA-144410, APA-143604, APA-143339–APA-143342) for `prod_conns.connections` / `connections_cnt`. New wave (2026-04-14 scan): three more `prod_monarch.*` tables cut over (APA-143600, APA-143602, APA-143603) — `company_associations`, `invoice_delivery_associations`, `invoice_delivery_selections`. All completed on both Holdem and War.

**Recurring validation pattern**: Oracle-to-MySQL cutovers consistently expose whitespace differences — Oracle strips trailing whitespace and treats `''` as NULL; MySQL preserves both. During data validation, expect: (1) trailing whitespace/newline in MySQL string columns not present in Oracle; (2) NULL (Oracle) vs empty string (MySQL) mismatches. Fix with `RTRIM()` in MySQL where downstream SQL uses exact string matching.

**Risk**: Downstream Airflow DAGs that read from these OPAL/Oracle datasets may silently receive empty results or fail if they haven't been migrated to the MySQL endpoints. DAGs doing `IS NULL` checks may need `IS NULL OR = ''` post-cutover.

**Action**: Check if any Oklahoma-managed DAGs reference `prod_conns.connections`, `prod_monarch.*` tables. If so, verify they've been updated to point to the MySQL lineage.

### [2026-04-14] Trex Tier 0 Metrics Outage (APA-144146 — Canonical)

**Signal**: Five-day data lag (4/7–4/11) on all LIX experiment tier 0 reports. Five tickets filed (APA-144146, APA-144364, APA-144360, APA-144285, APA-144402). Root cause identified and backfill completed by Dibyadarshan Hota. Not directly an Airflow issue, but DAGs that depend on Trex experiment metrics (e.g., for auto-ramping, experiment health checks) could have been affected.

**Risk**: Low for Airflow — unless DAGs have sensors waiting on Trex data.

### [2026-04-19] OPAL Decommission Wave 4 — 7 More prod_monarch Tables + phone_numbers Pending

**Signal**: Seven `prod_monarch` tables cut over Oracle→MySQL (all Closed — Fixed, 2026-04-19): `legal_name_history` (APA-143635), `customer_do_not_sell_products` (APA-143632), `line_of_business` (APA-143636), `contact_phone_numbers` (APA-143629), `site_contacts` (APA-143643), `sites` (APA-143644), `customers` (APA-143633). Continues the OPAL decommission wave from waves 1-3.

**Pending cutover (In Progress)**: **APA-143642** — `prod_monarch.phone_numbers` transparent cutover, created Mar 31, still **In Progress** as of Apr 20. This table has not yet completed the Oracle→MySQL transition despite being filed ~3 weeks ago. Any DAG or flow reading `prod_monarch.phone_numbers` may be reading from Oracle until this cutover completes.

**Risk**: Low-medium. Same whitespace/NULL validation risk as prior waves (Oracle strips trailing whitespace, treats `''` as NULL; MySQL preserves both). Any DAGs reading `prod_monarch.*` tables need to verify they point to MySQL lineage and handle whitespace/NULL differences. `prod_monarch.phone_numbers` specifically is still mid-cutover.

### [2026-04-20] OPAL Decommission Extends to OMS Schema — 4 Tables Still Open

**Signal**: Four `OMS.*` table decommission tickets created **2026-03-30**, all still **Open** as of Apr 20 10:23 (assigned Ajay Kulkarni, all updated together suggesting batch triage):
- **APA-143443** — `OMS.REFUNDABLE_BALANCES`
- **APA-143444** — `OMS.REFUNDABLE_BALANCES_TX`
- **APA-143449** — `OMS.AUTHORIZED_USERS`
- **APA-143514** — `OMS.COLO`

These are Oracle→MySQL cutovers for the **OMS** (Order Management System) schema, distinct from the `prod_monarch.*` waves documented above. All four have been open for ~3 weeks with no Closed status — suggesting they are either blocked, in flight, or require manual validation steps not yet complete.

**Risk**: Low-medium. Any Airflow DAG or flow that reads from `OMS.REFUNDABLE_BALANCES`, `OMS.REFUNDABLE_BALANCES_TX`, `OMS.AUTHORIZED_USERS`, or `OMS.COLO` via OPAL Oracle endpoints may be affected when these tables are cut over. The same whitespace/NULL mismatch risk applies (Oracle strips trailing whitespace; MySQL preserves it). Check DAG configs that query these tables for Oracle-vs-MySQL endpoint handling.

**Action**: Monitor these 4 APA-143xxx tickets for status transitions. When Closed, verify any DAGs querying `OMS.*` tables are pointing to MySQL lineage.

### [2026-04-14] Auto-Unlock System DAG Failing (APA-144481)

**Signal**: APA-144481 (In Progress, Major, created 2026-04-14 23:04, Harsh Shah, updated Apr 20 10:22) — "Auto-unlock DAG failing." A system-level Airflow maintenance DAG responsible for automatically unlocking some Airflow resource (DAG import locks, task instance locks, scheduler locks, or Hive metastore locks) has been failing since approximately Apr 14.

**Why it matters**: Oklahoma maintains a suite of system DAGs (`*__oklahoma_system_dags`) for cluster maintenance. An auto-unlock DAG failure means the cleanup mechanism it provides is degraded — over time, stuck locks or blocked resources it was meant to clear will accumulate. The timing (Apr 14) is concurrent with the NameNode HA instability wave and the Hive metastore split-brain (APA-144652), suggesting possible correlation — NN failovers may have left locks in an inconsistent state that the auto-unlock DAG now cannot clear.

**What's unclear**:
- What specific resource does this DAG unlock? (DAG-level `import_error` locks, Airflow DB row locks, Hive metastore table locks, scheduler advisory locks?)
- Is the DAG itself failing to start (import error / task failure) or is the unlocking logic throwing an exception?
- Which clusters are affected (holdem only, or all)?
- Has the accumulation of stuck locks caused any user-visible downstream failures yet?

**Action**: Check the `auto_unlock__oklahoma_system_dags` DAG logs on holdem via Airflow UI (or `kubectl logs` on the worker pod). Identify whether it's a code bug, a DB connectivity issue, or a lock state that the DAG can't handle. Assignee is Harsh Shah — check PR history for when this DAG was added and what it unlocks. Cross-reference with APA-144652 (Hive metastore split-brain) and the NameNode Wave 5 instability.

**Opened**: 2026-04-14

### [2026-04-20] OPAL Decommission — True Scale: 3 Schemas, 30+ Tables, Multiple Still In-Flight

**Signal**: The KB's OPAL decom entries (OPAL Wave 1–4 prod_monarch tables + OMS schema 4 tables) significantly understate the true scope. A scan of all APA tickets matching "OPAL Decom" updated on Apr 20 reveals:

**Riya Verma batch (Mar 30, all Closed by Apr 20)** — MONARCH schema:
`TRADE_STATUSES` (APA-143571), `TRADE_STATUS_HISTORY` (APA-143569), `TRADE_SCREENING_VERSIONS` (APA-143568), `TAX_REGISTRATIONS` (APA-143567), `TAX_REGISTRATION_EXEMPTIONS` (APA-143566), `TAX_EXEMPTIONS` (APA-143565), `TAX_DOCUMENT_HISTORY` (APA-143564), `SUBSYSTEM_MERGE_STATUSES` (APA-143562), `SUBSYSTEM_CHANGES` (APA-143561), `SOVOS_REQUESTS` (APA-143560), `MERGE_EXECUTIONS` (APA-143550), `CASE_CREATION_REQUESTS` (APA-143532), `LABEL_SETS` (APA-143545), `LABEL_ASSIGNMENTS` (APA-143543), `LABEL_NAMESPACES` (APA-143544), `LABELS` (APA-143546) — all **Closed**.

**Riya Verma batch — OMS schema (Closed)**: `OMS.AMENDMENTS` (APA-143437), `OMS.FIN_CONTRACT_MAPPING` (APA-143520), `OMS.PAYMENT_ACCOUNTS` (APA-144595) — all **Closed**.

**PTAGENT schema (NEW — not previously documented in KB)**: `PTAGENT.ALIPAY_TX` (APA-143461, Closed), `PTAGENT.AMEX_REGISTRY_TX` (APA-143463, Closed) — a **third schema** (payment agent) being decommissioned alongside MONARCH.* and OMS.*. Both Closed by Apr 20.

**Still In Progress (Riya Verma)**:
- `MONARCH.TRADE_STATUS_METADATA` (APA-143570) — In Progress
- `MONARCH.WORD_REPLACEMENTS` (APA-143572) — In Progress
- `OMS.PRODUCT_SCHEDULE_LINE_HEADERS` (APA-143494, Mar 30) — In Progress
- `OMS.PRODUCT_SCHEDULES` (APA-144581, Apr 16) — In Progress *(newly discovered — created Apr 16, part of PRODUCT_SCHEDULE* family)*
- `OMS.PRODUCT_SCHEDULE_HEADERS` (APA-144582, Apr 16) — In Progress *(newly discovered)*
- `OMS.PRODUCT_SCHEDULE_LINES` (APA-144583, Apr 16) — In Progress *(newly discovered)*

> The three APA-144581/582/583 tickets form a complete group for the OMS PRODUCT_SCHEDULE* family (SCHEDULES + HEADERS + LINES + LINE_HEADERS), all assigned Riya Verma, created Apr 16 as a second OPAL decom wave for this family (the LINE_HEADERS ticket APA-143494 was filed Mar 30). True In-Progress OMS count is now **4 PRODUCT_SCHEDULE\* tables** (not 1).

**Ajay Kulkarni — still Open** (documented in prior entries): `OMS.REFUNDABLE_BALANCES` (APA-143443), `OMS.REFUNDABLE_BALANCES_TX` (APA-143444), `OMS.AUTHORIZED_USERS` (APA-143449), `OMS.COLO` (APA-143514) — Open, Mar 30.

**APA-143193 — Blocker** (separate track, Riya Verma, Mar 24): `[commerce-pricing] Decom prod_oracle_oms.recurrences` — **In Progress, Blocker** priority. Created Mar 24 (predates the main wave), still unresolved. This uses `prod_oracle_oms` schema (distinct from `OMS.*` above) and is a Blocker-priority decom involving the commerce-pricing team. Has been open for ~4 weeks.

**Risk**: The OPAL decommission is a rolling multi-team operation affecting MONARCH, OMS, PTAGENT, and prod_oracle_oms schemas. Most tables are Closed on Riya Verma's track. The 4 Open OMS tables (Ajay Kulkarni), 3 In-Progress MONARCH tables, and the APA-143193 prod_oracle_oms Blocker are the live risk surfaces. Any DAG reading from `MONARCH.TRADE_STATUS_METADATA`, `MONARCH.WORD_REPLACEMENTS`, `OMS.PRODUCT_SCHEDULE_LINE_HEADERS`, `PTAGENT.*`, or `prod_oracle_oms.recurrences` via Oracle endpoints may be mid-migration.

**Action**: Monitor APA-143193 (Blocker — commerce-pricing). When the 3 In-Progress MONARCH tables close, verify no Airflow DAGs reference their Oracle lineage.

### [2026-04-20] ~~Dataset Retention Expiry — prod_dbchanges_lsssavedleadaccount (APA-144644)~~ — CLOSED

**Signal**: APA-144644 (**Closed — Fixed**, Apr 20–22, Pushpam Anand) — "Retention issue for dataset `prod_dbchanges_lsssavedleadaccount.savedlead` on Holdem cluster." Retention policy was extended/fixed. A companion ticket APA-144643 was a mis-filed duplicate closed immediately. Note: APA-50657 (2021) shows this same dataset had a retention extension request before — recurring lifecycle issue for a long-lived dataset.

**Outcome**: Resolved. Dataset retention corrected on Holdem. The pattern (retention expiry → data disappears → sensors time out) remains valid for future incidents but this specific instance is closed.

### [2026-04-14] HDFS ltx1-holdem-cluster07 Disk Utilization at 2x Threshold

**Signal**: ~30 Iris alert tickets (APA-144235 through APA-144300) generated Apr 12–13 for `DfsUsedHighUtilizationFraction` on `ltx1-holdem-cluster07`. Disk utilization at 41.7% against a 20% threshold. All tickets closed without comments or documented resolution.

**Risk**: Holdem Airflow DAGs that write large Spark outputs to HDFS on cluster07 may fail or slow down if utilization continues to climb. No root cause documented — could recur.

**Action**: Monitor for new alerts on the same cluster. If HDFS write failures start appearing in Holdem DAG logs, this is the likely cause. Escalate to HDFS oncall if utilization exceeds 50%.

### [2026-04-14] NameNode StandBy Instances Dropped to Zero — Recurring Across Multiple Clusters

**Signal**: First wave (Apr 13): APA-144181, APA-144167, APA-144230–APA-144232 — clusters `lva1-war-cluster07`, `ltx1-holdem-cluster03`, `lva1-war-cluster08`, `ltx1-holdem-cluster05`, `ltx1-holdem-cluster11`. All closed without comments.

**Second wave (Apr 14)**: APA-144463 (`ltx1-holdem-cluster04`), APA-144464 (`ltx1-holdem-cluster08`), APA-144465 (`ltx1-holdem-cluster10`), APA-144466 (`ltx1-holdem-cluster09`), APA-144457 (`ltx1-holdem-cluster08`), APA-144458 (`ltx1-holdem-cluster10`), APA-144459 (`ltx1-holdem-cluster09`). All Open. Multiple alerts per cluster within hours — indicates instability rather than one-off blip.

**Risk**: Medium. Two consecutive days of NameNode standby alerts across 4+ Holdem HDFS clusters. If NameNode HA is degraded, any HDFS operation (Spark writes, sensor reads) from Airflow DAGs could fail during NN failover windows. Combined with the existing `ltx1-holdem-cluster07` disk utilization issue, this suggests broader HDFS instability on Holdem.

### [2026-04-14] Iceberg StackOverflowError — Migration Risk for tracking.pageViewEvent

**Signal**: APA-143248 (Closed) — `AxiaEngagementTracking` flow failed with `java.lang.StackOverflowError` after migrating from `prod_foundation_tables.fact_talent_report` to `tracking.pageViewEvent`. Trino's Iceberg partition pruning overflows the stack on deeply nested boolean predicates pushed down to large Iceberg tables.

**Risk**: Any DAG migrating to `tracking.pageViewEvent` (or other large Iceberg tables) with complex join predicates could hit this. The fix (move filters into CTEs, restructure JOIN conditions) is specific to each query — no platform-level mitigation.

**Action**: If other teams report similar StackOverflowErrors in Trino Iceberg queries, point them to APA-143248 and lts-reporting-insights-offline PR #909 for the fix pattern.

### [2026-04-14] GGW gRPC Connectivity Failures — Multiple Signals Converging

**Signal (Slack)**: #ask_airflow (April 13) reports `grpc._channel._InactiveRpcError: StatusCode.UNAVAILABLE ... ipv6:[...]:443: connect: Network is unreachable (101)` during inDBT sandbox testing on Darwin.

**Signal (Jira APA-144425, Critical → Closed as Duplicate)**: `GridGatewayConnectionException` RPC UNAVAILABLE on inDBT Airflow Sandbox, failing to connect to `ipv4:144.2.184.1:443`. 10+ workflows blocked across multiple teams. Dev testing completely blocked. Created 2026-04-14. **Update 2026-04-14**: Closed as Duplicate — likely duplicate of APA-144447 or another GGW connectivity ticket.

**Signal (Jira APA-144447, Critical)**: SSL handshake `CERTIFICATE_VERIFY_FAILED` on holdem Airflow when connecting to GGW proxy at `ipv4:10.167.66.80:30100`. GGW team ruled out their side. Assigned to Vinayak. Created 2026-04-14.

**Correlation with merged PRs**:
- **PR #1044** (Airflow 2.9.2.170, merged Apr 13): The scheduler crash fix includes changes to `_process_executor_events`. If any networking/SSL library versions changed in the image, this could affect gRPC/SSL behavior.
- **PR #1028** (IPv6 on GHD, merged Apr 14): IPv6 enablement could cause DNS resolution changes if GGW service endpoints start returning AAAA records. Both IPv4 and IPv6 connection failures are appearing.

**Risk**: HIGH. Two critical open tickets + one Slack report, all within 24h of PR #1044/#1028 merges. All involve GGW connectivity failures. Pattern is across both sandbox (inDBT) and production (holdem).

**Additional signal (Apr 14)**: APA-144462 (Open) — `GridIdentitySingletonFactory` throws when no cluster is passed. This is a code-level bug in Grid identity initialization that could contribute to GGW connection failures when cluster context is missing or misconfigured.

**Action**:
1. Check if APA-144447 started after the 2.9.2.170 deployment landed on holdem.
2. APA-144425 closed as Duplicate — likely same root cause as APA-144447.
3. Monitor GHD cluster health after PR #1028 rollout. Check if GHA `grid-integration-testing-cli` succeeds.
4. Check if APA-144462 (GridIdentitySingletonFactory) is related to the connectivity failures.
5. If APA-144447 is confirmed as a regression: prepare rollback of PR #1044 from holdem.

### [2026-04-14] Spark 3.1.1 Avro→Parquet Registration on War (APA-144439)

**Signal**: APA-144439 (Open, Blocker) — Spark 3.1.1 `CREATE EXTERNAL TABLE` with Avro format suddenly registers as Parquet on War cluster starting 2026-04-13. No user-side code changes. Suspected grid-side Spark configuration change.

**Risk**: Any DAG on War that creates Hive external tables from Avro sources via Spark 3.1.1 may silently write incorrect table metadata. Downstream queries would fail with deserialization errors or return corrupt data.

**Action**: Monitor for additional reports. If confirmed as a grid-side change, escalate to Spark/Grid team. Check if Holdem is also affected.

### [2026-04-14] commons-lang3 Classpath Conflict — Recurring Across Multiple Teams

**Signal**: At least 2 teams (trust-nano-offline-pipelines, messaging-nano-offline-pipelines) reported `java.lang.NoSuchMethodError: 'org.apache.commons.lang3.Range org.apache.commons.lang3.Range.of'` in Spark jobs on Holdem in April 2026.

**Risk**: commons-lang3 version conflict is a systemic issue — the YARN classpath provides an older version that overrides application-bundled JARs. Unlike the guava conflict (which has a known fix via extraClassPath append), this conflict may require `userClassPathFirst=true` which has its own side effects.

**Action**: Investigate whether the Holdem YARN cluster's commons-lang3 version was recently changed (e.g., Hadoop upgrade). Document recommended Spark classpath configuration for applications that depend on commons-lang3 3.8+.

### [2026-04-19] UMP Schema Incompatibility — Continuing Pattern, Now Includes Dimension Removal

**Signal**: APA-144528 (Closed/Fixed, 2026-04-16) — `conversion_tracking_v2_plus` flow type casting errors resolved via schema drop. APA-144369 (Closed/Fixed) — `conversion_tracking_v2_cpa` also resolved.

**Continuing**: APA-144539 (Open) — `payments_approval_v3`. APA-144610 (Open, 2026-04-17) — `capi_adoption_metrics_agg`. **APA-144631 (Closed Apr 20, 2026-04-19)** — `lms_advertiser_quality_actions_daily` — **first confirmed instance triggered by dimension removal** (removing `datepartition` from dimensions), not addition; schema drop resolved it. **APA-144645 (Open, Critical, 2026-04-20)** — `rsc_candidates_dq` / `rsc_applications_dq_v2` — schema inconsistency errors, assigned Sourav Patel. This is now the **7th instance** of this pattern in April 2026.

**Risk**: **CRITICAL**. The pattern is accelerating — 7 instances in 3 weeks. Now confirmed to trigger on both dimension additions AND removals. Each `metric-defs` dimension PR triggers manual schema drop requests, creating operational overhead and blocking downstream flows. The operational cost is compounding. APA-144627 (also open, 2026-04-19) requests deletion of a datepartition from another u_metrics table (`lss_predictive_user_persona_hist_union`) — may be a related incident.

**Action**: Strongly recommend proposing automated schema migration tooling for UMP dimension changes to the UMP team. Track APA-144539, APA-144610, and APA-144631 resolution.

### [2026-04-19] NameNode HA/Capacity Alerts Continue — Wave 4, Now Also War Zone

**Signal**: APA-144529 (Open) — NameNode RPC alert on `ltx1-holdem-cluster02`. This continues the pattern from waves 1 (Apr 13: clusters 03, 05, 07, 08, 11), 2 (Apr 14: clusters 04, 08, 09, 10), and 3 (Apr 16: cluster02). **Wave 4 (Apr 19)**: APA-144626, APA-144625 — `CriticalTotalFilesystemObject` alerts on **lva1-war-cluster08** (War zone). APA-144623 — NameNode standby instances alert on `ltx1-holdem-cluster11`. APA-144630 — NameNode down on `ltx1-yugioh-cluster01`. Now affecting **three zones** (Holdem, War, Yugioh), not just Holdem.

**Wave 5 (Apr 20)**: APA-144649, APA-144650 — NameNode standby instances dropped to zero on **lva1-war-cluster09** (08:25, 08:48 AM). APA-144653 — NameNode alerts on **lva1-war-cluster10** (09:01 AM). **APA-144656** — `NameNodeTotalObserverInstance` alert on **lva1-war-cluster11** (10:21 AM). **APA-144658** — second `NameNodeTotalObserverInstance` alert on **lva1-war-cluster10** (10:48 AM, Open Major, assigned Chris Trezzo). All Open. **Five alerts across three consecutive War zone clusters (09, 10, 11) in a 2.5-hour window.** Cluster10 was hit twice — first at 09:01, then again at 10:48 — indicating the issue was not resolved between alerts or a second observer instance was lost.

**Risk**: **HIGH**. Five+ consecutive days of NameNode alerts across Holdem, War, and Yugioh zones. War zone is now the primary hotspot with five alerts across clusters 09, 10, and 11 within a single 2.5-hour morning window on Apr 20 — all assigned to Chris Trezzo with no resolution yet.

### [2026-04-16] ~~Kafka Event Drops on Holdem (APA-144537)~~ — CLOSED

**Signal**: APA-144537 (**Closed — Fixed**) — ~20% of query events dropped from `PrestoQueryCompletedEventV2` Kafka topic on Holdem. Resolved as of Apr 22.

**Update [2026-04-20]**: **APA-144655** (Open, Major, Apr 20 10:16, assigned Tanmay Shukla) — SQL query failure on stored view `hive.service_column.prestoquerycompletedeventv2` due to **unresolved column `inputs`**, causing a production flow failure. The `inputs` column exists in the underlying Kafka event schema but is not resolvable in the Hive stored view. This is the **schema drift** problem: Kafka topic schema evolves but the ETL materialization is manual/lagging.

### [2026-04-18] GGW Venice Push Failure with Revenue Impact (APA-144622)

**Signal**: APA-144622 (Open) — Grid Gateway execution failure causing Venice push job failure with revenue impact.

**Risk**: Medium-high due to revenue impact. Check if this is a transient cluster issue or a systemic problem.

### [2026-04-18] ARMS Returns False for Existing Partitions (APA-144621)

**Signal**: APA-144621 (Open) — ARMS returns false for existing `u_flytedev1.workflowevents3m` partitions (2025-11-01 to 2025-11-10) and reports missing partitions (2025-09-01 to 2025-09-10) in lva1 cluster. Data exists but ARMS denies it.

**Risk**: Medium. This is distinct from the Trino DCE gap (where DCE was never emitted). Here, ARMS appears to have stale or incorrect metadata for partitions that should exist.

### [2026-04-20] Hive Metastore Inconsistency — Table Simultaneously Exists and Doesn't Exist (APA-144652)

**Signal**: APA-144652 (Open, Major, Apr 20) — On Holdem, attempting to `DROP TABLE u_sopsdf.saas_users_history` returns `TABLE DOES NOT EXISTS`, but attempting to `CREATE TABLE u_sopsdf.saas_users_history` returns `table already exists`. Both errors are consistent (not transient). Assigned to Chris Trezzo (same person handling the NameNode Wave 5 alerts).

**Risk**: Medium-high. A Hive metastore in split-brain state can block DDL operations entirely, wedging dependent DAGs that manage their own table lifecycle (create-on-first-run, drop-and-recreate patterns).

**Hypothesis**: NameNode HA failover during the multi-day NN instability wave may have left the Hive metastore out of sync with the actual HDFS state.

### [2026-04-22] HDFS DataNode War-Cluster07 Alert Storm — Wave 6

**Signal**: 8 new `DfsUsedHighUtilizationFraction` alerts on **lva1-war-cluster07** (APA-144751 through APA-144759) filed Apr 22. All Open. This extends the pattern from Holdem-cluster07 (Apr 12–13, ~30 alerts) to the War zone.

### [2026-04-24] Regression Check — 18 Merged PRs vs 10 Open Tickets

**Regression signal**: **None confirmed**. The LCD migration (PR #1060) and dagrun.deadlocked metric (PR #120) are additive changes with no user-facing behavior modification.

### [2026-04-25] No Regressions — GaaS/distcp Failures Signal Continued HDFS Instability

Notable signals:
- **APA-144931** (Open): Daily metricsV2 distcp flows failing with GaaS polling timeout after 25 retries.
- **APA-144928** (Open): GaaS distcp holdem→war failing with **ZooKeeper connection errors** — a new error vector.
- **APA-144922** (Open): NameNode alert on lva1-war — **Wave 7** of the ongoing HDFS NameNode HA alert series.

### [2026-04-25] OPAL Decommission — ENTERPRISE Schema Added (Wave 6)

Two new OPAL decom tickets for the **ENTERPRISE** schema closed on Apr 24–25. This is a **fifth schema** in the OPAL decommission campaign (after MONARCH, OMS, PTAGENT, prod_oracle_oms).

### [2026-04-27] No Regressions — New Deployment Error Pattern + War DAG Anomaly

- **APA-144949** (Open): `uv pip install` from `pinned.txt` returns exit status 2 — a new deployment failure vector.
- **APA-144947** (Open): **Airflow DAG marked failed despite all tasks succeeding** on War cluster — second report of a "phantom failure" on War.

### [2026-04-28] LCD Deployment Saga — No Regressions, But Operational Learning

The LCD saga (PRs #1060→#1064→#1066→#1067→#1070) was self-contained — the EKG/canary incompatibility with ArgoCD was the root cause, resolved by disabling EKG/canary steps.

**OPAL workflow cutovers (APA-144983, 144985, 144986)**: Three `prod_workflow` tables (`workflows`, `activities`, `activity_queue`) completed transparent Oracle→MySQL cutover.

### [2026-04-29] No Regressions — Spark 3.1 Iceberg Gap + OPAL Cutovers Continue

**Regression check**: 18 merged PRs (oklahoma-airflow-deployment #1048–#1070, airflow #101–#120, lipy-airflow-providers #1167–#1203, picli #657–#686, tradewind #63–#85) vs 10 open tickets. **No regressions detected.**

Notable signals (not regressions):
- **APA-145082** (Closed): Spark 3.1 does not inject sort before AppendData for Iceberg tables with `write.distribution.mode=hash`. This is a Spark 3.1-specific gap, not caused by any merged PR. Added as pattern P32 in [Jira Patterns](../jira/patterns.md).
- **APA-145083** (Open): Same Spark 3.1/Iceberg issue still tracked as open (companion to APA-145082).
- **APA-145076** (Open): HDFS NameNode capacity alert on `ltx1-holdem-cluster11` — continuation of the ongoing multi-wave NameNode alert series (Waves 1–7 documented above).
- **APA-144143** (Closed — Fixed): Dali SDK/Jasper falsely failing partition check for `tracking.profileeditevent` on Holdem. Same class as P29 (ARMS false negative). Added as pattern P33 in [Jira Patterns](../jira/patterns.md).
- **OPAL cutovers (APA-145057, APA-145063)**: PTAGENT schema — `STANDING_INSTRUCTIONS` and `PPRO_TX` transparent cutovers resolved. This is a **sixth schema** (PTAGENT agent-level tables) continuing the OPAL decommission campaign.

---

## [2026-04-20] #ask_airflow Not Indexed in Captain's Slack Search

**Domain**: KB sync infrastructure
**Why it matters**: The daily `#ask_airflow` KB sync workflow has failed on every attempt (2026-04-19, 2026-04-20). Three distinct root causes were identified and resolved:
1. **SSO token expired** — `authn-cli` requires interactive re-auth
2. **NAMESPACES missing `slack,messaging`** — captain's `search_slack` tool returned 0 results
3. **#ask_airflow channel not in the index** — even with auth and namespaces fixed, captain's Slack search index does NOT include this channel

**Workaround**: KB sync can still pull from Jira (`search_jira_issues`) and Confluence — those work fine. Only the Slack component is blocked.

**Opened**: 2026-04-20

---

## ~~[2026-04-20] APA-144562 — Airflow War Cluster Stuck / FlyteOperator XCom Regression~~ — RESOLVED

**Domain**: systems/ggw, lipy-airflow-providers
**Status**: **Resolved** as of 2026-04-21.

**Opened**: 2026-04-16 | **Resolved**: ~2026-04-21

---

## See Also
- [Hypotheses](hypotheses.md)
- [Contradictions](contradictions.md)
