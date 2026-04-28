# lipy-airflow-providers Changelog

> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

## 2026-04-24 — DarwinOperator Disruption Readiness + TLS Fix + Backport

- **DarwinOperator disruption readiness** (#1193, 2026-04-21): DarwinOperator now inherits `_external_job_id` assignment from `GridGatewayBaseOperator._execute()`, enabling disruption readiness (auto-retry on `ENVIRONMENT_.*` errors). DarwinOperator joins hadoopJava, java, javaprocess, command, and hadoopShell as disruption-ready GGW operators.
- **airflow-oc-image bump** (#1194, 2026-04-21): Dependency update for airflow-oc-image.
- **Typo fix** (#1195, 2026-04-21): Minor typo correction in provider codebase.
- **Backport `use_hourly_for_daily` to BR_REL_airflow-2.9.2** (#1200, 2026-04-23): Backported the `use_hourly_for_daily` hourly partition sensor feature (originally PR #1158) to the release branch. Daily datasets reading from hourly-partitioned sources now use UMP calendar-day boundaries on the stable release.
- **airflow-oc-image 0.1.113→0.1.114 (Flyte TLS fix)** (#1202, 2026-04-24): Bumped airflow-oc-image for non-Grestin Ambassador cluster TLS compatibility. Fixes TLS handshake failures on clusters not using Grestin-managed certificates.

---

## 2026-04-20 — Dependency Bump + Code Yellow Coverage

- **ADU: lipy-azkaban-client 0.1.160 → 0.1.162** (#1192, 2026-04-19): Automated dependency update for `lipy-azkaban-client` (and 1 more). 50% direct consumer adoption at time of merge.
- **Code Yellow: featurecloud branch coverage** (#1191, 2026-04-20): Test coverage improvement for featurecloud module toward the 80% branch coverage Code Yellow requirement (IRP-11687). No behavior changes.

---

## 2026-04-18 — RDev Email Redirect

- **EmailOperatorPatchPlugin** (#1187, 2026-04-18): Airflow plugin that intercepts `EmailOperator` in rdev environments and redirects both `to` and `from` to `$USER@linkedin.com`. Prevents spamming team DLs during rdev testing.

---

## Version Progression

| Version | Date | Key Event |
|---------|------|-----------|
| 8.4.x   | May–Sep 2025 | Pre-major-bump series; periodic syncs from master |
| 9.1.x   | Nov 2025 | Bump to 9.1.* minor series (#1012) |
| 10.0.x  | Dec 2025 | Major version bump to 10.0.* (#1038); post-Nimbus era |
| 10.0.59 | Apr 2026 | Current version as of 2026-04-09 |

---

## 2025-05 to 2025-06 — Foundation Merges for 2.9.2 Branch

- Added version compatibility plugin (#694, 2025-05-19); updated compatibility plugin (#705, 2025-05-20)
- Merged master branch changes into 2.9.2 release branch (#708, #723, #734)
- Merged darwin and Spark changes into 2.9.2 (#752)
- Added monkey patch for pydantic compatibility (#740, 2025-06-05)
- Added cache for user/DAG info access (#757); later reverted (#774, 2025-06-17) due to issues
- Disabled DAG failure due to kafka metadata job (#766, 2025-06-16)
- Merged Feature Cloud updates (#760, 2025-06-12)
- Added MP version to task listener events (#791, 2025-06-27)

## 2025-07 — IRIS Root Cause Improvements

- Mitigated bug in root cause analyzer from too many DAG runs (#812, 2025-07-24)
- Added root cause URL to context parser as a field (#804, 2025-07-15)
- **De-duplicated IRIS alerts** with tagging (dag id, run id) in create/filter/match (#803, 2025-07-15)
- Synced updates from master through version 8.4.87 (#814, 2025-07-24)
- Added feature to check grid_gateway state (#818, 2025-07-28)
- Switched non-prod clusters to emit to test Kafka topics (#827, 2025-07-28)

## 2025-08 — Grid Gateway Reliability: Timeouts & Retries

- **Added timeout to all Grid Gateway calls** (#859, 2025-08-21) — prevents indefinite hangs
- **Added retry for all GG calls** (#864, 2025-08-26) — resilience on transient failures
- Used index for `find_user` to speed up authentication latency for APIs (#867, 2025-08-27)
- Faster API responses for service accounts (#842); later reverted (#856, 2025-08-15) — too aggressive
- Fixed artifact macro for patch upload (#850)
- Synced updates from master through 8.4.97 (#841)
- Enabled customized GGW authority in UI (#871, 2025-09-03)

## 2025-09 — Job Checkpointing Foundation; GGW Integration

This period marks the beginning of the **external job checkpointing** feature — the ability to resume GGW jobs after Kubernetes pod disruptions rather than failing.

- Added `External Job Checkpointing Utilities` for Kubernetes pod disruption handling (#901, 2025-09-22)
- **Integrated GridGatewayBaseOperator with External Job Checkpointing** (#902, 2025-09-22)
- Implemented Job Checkpoint Cleanup Logic (#905, 2025-09-23)
- Enhanced DB session handling in job checkpointing (#936, 2025-10-14)
- **Gated job checkpoint behind feature flag** (#937, 2025-10-02)
- Added NKS prod fabric support: Airflow reading grid configs in NKS prod fabrics (#891, 2025-09-23)
- BDP-38007: Enhanced IRIS incident callback to support default plan creation when plan arg absent (#899, 2025-09-23)
- **Support batch uploading in patch endpoint** (#903, 2025-09-25)
- Blocked patch uploads to non-symlink directories (#934, 2025-10-01)
- Fixed task log date comparison failure (#933, 2025-10-01)
- Added LinkedIn auth failure metrics (#916, 2025-09-25)
- Propagated airflow msg from DAG listener hook (#919, 2025-09-29)

## 2025-10 — Disruption Readiness Ramp; Lifecycle Events Enriched

- **Enriched metrics dimensions for Nimbus disruption readiness** (#928, 2025-09-30)
- Fixed disruption readiness metrics dimensions (#929, 2025-09-30)
- **Ramped job checkpoint disruption readiness to Trino SQL GGW jobtype** (#930, 2025-09-30)
- **Ramped job checkpoint disruption readiness to SparkBatch GGW jobtype** (#931, 2025-10-14)
- **Enabled checkpoint disruption readiness for all GGW** (#955, 2025-10-14) — global ramp
- Ramped disruption readiness for GGW operator with explicit flag (#959, 2025-10-14)
- Added Ti map index in grid context (#935, 2025-10-02)
- **Added Map Index to Task Lifecycle Events** (#944, 2025-10-07)
- **Populated Failure Classification Fields in Airflow Task Lifecycle Events** (#947, 2025-10-09)
- Updated FCPO to configure HostedSearch exporters for AIM Feature Groups (#949, 2025-10-09)
- Added new feature group schema (#943, 2025-10-07)
- Standardized Grid Gateway and ARMS error messages (#951, 2025-10-14)
- Cherrypicked IndBT WAP enablement into 2.9.2 (#961, 2025-10-16)
- Modified dispatchState calculation in Task Lifecycle Event (#960, 2025-10-15)
- Persisted `timed_out` in DAG run conf (#975, 2025-10-27)
- Added qwen3 embeddings for hosted search push (#969, #978)

## 2025-11 — Provider 9.1.x; DAG Mutation Policy; Crew Sync

- **Bumped Provider minor version to 9.1.x** (#1012, 2025-11-19)
- Updated airflow-core dependency to 2.9.2.114 and removed OSS cncf-kubernetes (#1011, 2025-11-19); reverted/re-applied (#1019, #1022) due to instability
- Fixed rdev cfg not merging user config (#1008, 2025-11-17)
- Fixed DAG crew asset creation bug (#990, 2025-11-07)
- Added crew sync denylist (#1000, 2025-11-12)
- Unwrapped special default Airflow exceptions (#992, 2025-11-07)
- Added Date Utility Macros for Data Triggers (#987, 2025-11-05)
- Batched logging; read from Kusto only after job terminates (#983, 2025-11-03)
- Updated proxy user permission error message (#1020, 2025-11-24)
- Bumped airflow-oc-image to 0.1.88 (#1015, 2025-11-20)

## 2025-12 — Version 10.0.x; Post-Nimbus Cleanup; IRIS SLA Alerting

- **DAG mutation policy to auto-mutate/inject MP/application tags** (#1018, 2025-12-01)
- **Bumped major version to 10.0.x** (#1038, 2025-12-02) — new major series post-Nimbus migration
- **Added New Policy for Deprecated Operators** (#1044, 2025-12-09) — enforcement of operator deprecation
- Fixed policy-airflow-settings circular dependency (#1039, 2025-12-04)
- Filled out IRIS context properly on DAG timeout cases (#1024, 2025-12-04)
- Added NFS unavailable check when serving logs (#989, 2025-12-09)
- Added lakeshift_war DAGs to policy exemption (#1045, 2025-12-09)
- **BDP-60120: Added SLA alerting support through IRIS** (#1030, 2025-12-22)

## 2026-01 — Disruption Readiness for Sensors; Error Banners; Cachetools

- BDP-39123: Added test cases for IRIS incident enhancement (#1036, 2026-01-07)
- Added `cachetools` as direct dependency (#1064, 2026-01-13)
- **Prevent task failures from stale NFS file handles** (#1065, 2026-01-14)
- **Added Airflow Logs error banner for Grid Gateway failures** (#1066, 2026-01-15) — redirects users to correct support team
- **Support Disruption Readiness for Data Sensors** (#1070, 2026-01-22) — extends disruption readiness beyond GGW operators to sensor tasks
- Added crew mapping script (#1054, 2026-01-12)
- Fixed error message for policy enforcement (#1071, 2026-02-04)
- Used skipped task and added timed-out message for Iris context on timed-out DAGs (#1081, 2026-02-04)

## 2026-02 — DataVault Token Feature; Rules Repo; DQ Retry; Checkpointing for TriggerDag

- **Deleted removed DAGs on upload** (#1085, 2026-02-12) — GC for stale DAG bundles
- Added PipelineMD diagnostic URL to Iris notifications (#1087, 2026-02-17)
- Used Nimbus GGW logs dashboard URL in Airflow error banner (#1092, 2026-02-20)
- Added notebook name and git resource path to Darwin email notifications (#1097, 2026-02-23)
- **Added param to enable retry on DQ assertion failure** (`enable_dq_retry`) (#1098, 2026-02-25) — configurable retry for DataQualityJobOperator assertion failures
- **Granted `can_read` permissions on all DAGs to LI_BASE_USER role** (#1101, 2026-02-25)
- **Added `enable_rules_repository` param** (#1103, 2026-02-27) — flag to turn on rules-repo-driven policy evaluation
- **Added external job checkpointing for TriggerDagRunOperator** (#1102, 2026-02-27) — extends checkpointing beyond GGW operators
- Added regression test for user config recursive merge (APA-137578) (#1099, 2026-03-02)
- Updated the GGW authority param for Nimbus connections (#1105, 2026-02-27)

## 2026-03 — Access Control Policies; DataVault Exception; Trustbridge

- Upgraded lipy-kafka, lipy-datavault, lipy-fabric, lipy-oklahoma-airflow dependencies (#1109, 2026-03-03)
- Added PipelineMD diagnostic URL to Iris context parser (#1110, 2026-03-03)
- Added AI agent rules to 2.9.2 release branch (#1111, 2026-03-03)
- **Added new policy for disallowed roles in DAG access control** (#1112, 2026-03-03)
- **Added proxy_user ACL validation during bundle deployment** (#1113, 2026-03-06)
- Made DAG access role policy message clearer (#1115, 2026-03-04)
- Toned down GGW error banner wording; improved log visibility (#1116, 2026-03-04)
- **Added `GridGatewayDataVaultTokenException`** for DataVault token failures (#1121, 2026-03-06) — distinct exception class for DV auth failures
- Bumped airflow-oc-image to v0.1.105 (#1122, 2026-03-06)
- Fixed cross-MP artifact resolution always using corp artifactory in RDEV (#1124, 2026-03-09)
- Fixed TB endpoint for GGW in rdev (#1126, 2026-03-10)
- Used `deployment_metadata.json` MP name for dagUrn in listener events (#1127, 2026-03-11)
- **Logged Trustbridge request header keys in Grid Gateway hook** (2026-03-11) — diagnostic visibility for TB-proxied GGW calls

## 2026-04 — Config Override Expansion; PipelineMD Plugin; Sensor & Spark Fixes

- **Add overridable attrs to all GGW operators** (#1156, 2026-03-31) — each operator declares `get_overridable_attrs()` using `super()` inheritance
- **Fix guava extraClassPath overwriting user-supplied spark configs** (#1157, 2026-04-01; backport #1159) — `dict.copy()` and append instead of overwrite
- **Support `use_hourly_for_daily` in hourly partition sensor** (#1158, 2026-04-09) — daily datasets reading from hourly-partitioned sources now use UMP calendar-day boundaries
- **Lowercase `cluster_id` in PipelineMD URL** (#1161, 2026-04-02) — server expects lowercase
- **Fix off-by-one in Iris alert try number** (#1164, 2026-04-06) — `ti.try_number` returns next attempt when not running; APA-143850
- **Darwin notification email HTML formatting** (#1155, 2026-03-30) — structured HTML table layout with color-coded status banner
- **Add `override.destinationConnectionString: metrics`** to metadata kafka push job (#1170, 2026-04-07)
- **PipelineMD global extra link plugin** (#1171, 2026-04-08) — PMD button now appears for all tasks via `global_operator_extra_links`
- **Support v1 schedule DAG IDs in darwin email notification resource link** (#1188/#1189, 2026-04-15) — updated regex in `darwin_email_notification` to match v1 schedule DAG IDs so the "View resource" link is included in email notifications for Darwin notebook schedule runs
- Auto-formatting (isort, black) applied to 2.9.2 branch (#1153, 2026-03-27)
- Dependency updates: lipy-utils (#1178), lipy-azkaban-client (#1177), iris-client (#1165), ligradle-core (#1166, #1179)
- wc-test job added to post-merge workflow (#1162)

---

## See Also
- [Deployment Changelog](deployment-changelog.md) — infrastructure and Helm chart evolution
- [GGW](../systems/ggw.md) — Grid Gateway operator reference
- [Troubleshooting](../references/troubleshooting.md) — failure taxonomy and debug paths
- [DAG Authoring](../references/dag-authoring.md) — operator usage patterns
