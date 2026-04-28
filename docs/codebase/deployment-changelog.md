# Deployment Infrastructure Changelog

> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

## 2023–2024 — Foundation & Early Clusters

Initial clusters (Holdem, War, Corp, EI/Faro, Test, DEX) set up 2023 on Airflow 2.5.3 / Helm v1.9.0. 2023: KMS support, Emissary ingress, Holdem scaled. 2024: War cluster scaled (Jan); Faro cluster onboarded as EI equivalent (May); DAG parsing and StatsD tuned (Jun–Aug); Holdem webserver scaled up (Sep); Mariner OS upgrade and Redis removed cluster-wide (Nov); Oklahoma listener plugin enabled; DAGs → Crews sync enabled.

## 2025-01 to 2025-02 — GHD Cluster Added

- Added Helm charts for Airflow at GHD (Grid Hosted Deployment) (#482, 2025-02-25)
- Added PVCs for GHD Airflow (#470, 2025-02-05)
- Enabled DB migration for GHD (#533, 2025-03-25)
- Added grid2 dev pod (#487)
- Enabled IPv6 traffic for Airflow Webserver (#894, 2026-01-15, and #951, 2026-03-27)
- In-redis deployment freshness process established (#464, 2025-01-30)

## 2025-03 — Airflow 2.5 → 2.9.2 Chart Migration

- **Major milestone**: Replaced v2.5.3 Helm chart with v2.9.2 chart cluster-wide (#517, 2025-03-12)
- Updated scheduler/webserver startup commands for new Airflow version (#585)
- Fixed Airflow EI webserver routing through Emissary (#536)
- Added namespace overwrite support (#523)

## 2025-04 — Load Test Cluster Added

- Added load test cluster spec (#538, 2025-04-09)
- Cert-based MySQL access enabled for load test cluster (#562, 2025-04-16)
- Bumped holdem schedulers to 20 (#560, 2025-04-15)

## 2025-05 — 2.9.2 Rollout Across All Clusters

- Holdem: bumped webserver instance count (#586, 2025-05-13)
- Corp: bumped to 2.9.2 (#603, 2025-05-27); async RTI delete solution enabled (#616)
- EI/Faro: bumped to Airflow 2.9 (#600, 2025-05-23)
- War cluster: added same scheduler/webserver count as Holdem (#596, 2025-05-22)

## 2025-06 — War/Grid1 Full 2.9.2 Rollout

- Airflow 2.9 version for grid1 cluster (#609, 2025-06-04)
- 2.9 version upgrade for War cluster rollout (#631, 2025-06-12)
- Async delete RTI solution deployed to Holdem and War (#625, #633)
- Dev cluster bumped to 2.9 (#649, 2025-06-18)
- Added Lasso dashboards (#682, 2025-07-22)

## 2025-07 — Lasso Cluster Added; Standalone DAG Processor

- **Lasso cluster**: deployment spec added (#677, 2025-07-18); PVCs added (#656, 2025-07-07)
- Added DAG processor deployment to Holdem (#672, 2025-07-16)
- Enabled standalone DAG processor in all Oklahoma clusters (#698, 2025-08-01)
- Added extra DAG processor to Faro for testing (#683, 2025-07-22)
- OpenTelemetry metrics enabled cluster-wide (#700, 2025-08-04)
- StatsD disabled across all clusters (#705), then re-enabled for DBT cluster (#706)
- Added DAG deletion limit configuration (#685)
- Fixed triggerer command and dedup inLogs config (#703)

## 2025-08 — Holdem Scheduler Scale-up to 24

- **Holdem scheduler count increased to 24** (#717, 2025-08-13)
- Reduced memory for schedulers / increased for webservers on Holdem (#732, 2025-08-26)
- Worker pod cleanup on infra failure enabled (#714)
- Added OTEL docs and graphs (#716)
- Set wait-for-airflow-migrations to false for GHD (#687)
- PR template updated with mint helm-dry-run command (#731)

## 2025-09 — Nimbus/NKS Migration Begins

Nimbus is LinkedIn's next-generation Kubernetes platform (NKS = Nimbus K8s Service), replacing legacy LKS (LinkedIn K8s Service). The migration involves moving all Airflow cluster pods to NKS fabric while maintaining DAG/log storage via sdnas-csi-driver for NFS mounts.

- **[NKS migration]** Added NKS Helm charts and NFS partition mounting via sdnas-csi-driver (#750, 2025-09-10)
- Added new NKS clusters for helm dry-run validation (#758, 2025-09-16)
- **[NKS migration]** Integrated with UT for DNS discovery in NKS (#764, 2025-09-23)
- Added metrics namespaces for new NKS dashboards (#769, 2025-09-29)
- **[NKS migration]** Enabled execution allow/disallow feature to pin/block task executions per cluster (#774, 2025-09-30)
- Added Faro and Corp PVCs for NFS via sdnas-csi-driver (#756, 2025-09-15)
- Ensured EI and Corp LKS deployments run on LKS-only nodes (#760, 2025-09-18)
- Removed old Prod cluster Helm charts (decommissioned) (#743, 2025-09-04)

## 2025-10 — NKS Migration Ramps Up; EI Decommissioned

- **[NKS migration]** Enabled execution rampup in NKS (#782, 2025-10-06)
- Added NKS test DAG and flag to enable it (#783, #791)
- Changes needed for GHD on NKS (#801, 2025-10-16)
- Enabled listener plugin in DAG processor (#803)
- Enabled slow rollouts for scheduler temporarily (#805)
- **Remove Airflow EI Helm charts** — EI cluster decommissioned (#807, 2025-10-22); Faro is now the EI-equivalent cluster
- Disabled DAG parsing in LKS for Faro and Corp clusters (moved to NKS) (#793, 2025-10-09)
- NKS webserver deployment enabled (#812, 2025-10-29)
- Enabled DAG id regex for pinned/blocked execution specification (#786)
- Synced Faro webserver settings with Holdem's (#788)
- Jasper Nimbus instances configured (#841, 2025-11-20)

## 2025-11 — Nimbus Migration Completion: All Clusters

- **Add Dev Helm charts for Nimbus** (#818, 2025-11-04)
- **[NKS migration]** Temp change for correct Disco URL during migration (#819, 2025-11-07)
- Added webserver topology spread constraints for NKS (#826, 2025-11-10)
- **[NKS migration] Faro added to Nimbus** (#821, 2025-11-10)
- **Corp NKS Helm charts added for CRT** (#829, 2025-11-13)
- Increased DAG delete limit temporarily in War and Holdem (#833)
- Reduced LKS controlplane DB load (#838, 2025-11-19)
- Disabled execution balancer post-Nimbus migration in Test, Lasso, Faro, Corp clusters (#848, 2025-11-24)

## 2025-12 — Post-Nimbus Cleanup; War/Holdem Balancer Disabled

- Increased War webserver replicas (#857, 2025-12-02)
- **Disabled execution balancer post-Nimbus migration in Holdem and War** (#850, 2025-12-01)
- Added SCA scanning migration to GitHub Actions (#855)
- Restored metrics namespace name post-Nimbus migration (#858, 2025-12-03)
- Reverted temp Corp/Faro chart changes post-migration (#861, 2025-12-03)
- Cleaned up Helm charts post-Nimbus migration (#864, 2025-12-05)
- Fixed Airflow 2.9.2+ health check probes for correct Python environment (#873, 2025-12-10)
- Added DAG mutation policy for Test and Lasso clusters (#871)
- Updated development docs and cluster metadata post-Nimbus migration (#880, 2025-12-16)
- **Bumped major version to 10.x.x** provider series begins

## 2026-01 — CI Migration to GitHub Actions; Minor Fixes

- Migrated to GitHub Actions CI (#895, 2026-01-13)
- Cleaned up unused DAG gitsync and removed squid proxy address (SECACTN-2279) (#882, 2026-01-07)
- Updated dev Helm and docs for `airflow-test` namespace (#886)
- Reverted DAG mutation policy (initially applied everywhere, then scoped) (#887)
- Added cursor rule for agent-driven version bumps (#891)
- Enabled IPv6 traffic for Airflow Webserver (#894, 2026-01-15)

## 2026-02 — DBT Cluster Decommissioned; Load Test Nimbus

- **Removed Airflow DBT Helm chart** — DBT cluster decommissioned (#915, 2026-02-04)
- Disabled execution balancer post-Nimbus in Load Test cluster (#913, 2026-02-03)
- Created fast testing script and documentation (#919, #920, 2026-02-11)
- Airflow image now at 0.0.879+ range
- Added nodeAffinity to prefer nodes without upcoming maintenance (#955, 2026-03-11)

## 2026-03 — Repo Split: oklahoma-airflow → oklahoma-airflow-deployment

- **Major milestone**: Repo split — `oklahoma-airflow` monorepo split into `oklahoma-airflow-deployment` (Helm/infra) and a separate image repo
- Added airflow-main/, e2e-tests/, build config, CI workflows from oklahoma-airflow v0.0.906 (#942–#945)
- Fixed image registry path to point to `oklahoma-airflow-deployment` (#946)
- Updated all doc references from oklahoma-airflow to oklahoma-airflow-deployment (#965)
- Added `$.Values.mpVersion` as fallback for Airflow image tag (#961)
- Disabled proxy user ACL validation on Faro (EI) cluster (#983)
- Added Claude AI rules for bumping versions (#986)
- Fixed rdev startup mode for external deps (#997)
- Moved `.okl_setup.json` lookup path to `src/<mp>/<module>/` (#989)
- Added commit changelog lookup to version bump AI rules (#993)
- RDev webserver title now shows MP name and RDev name (#1009)

## 2026-04 — Active Development

- Removed Airflow 2.5 build from Dockerfile (#991, 2026-04-02) — 2.5 fully retired
- Empty commit to trigger build (#1016, 2026-04-02)
- Fixed 'disallow-pod-affinity' policy violation warning (#1007, 2026-03-30)
- Bumped lipy-airflow-providers to 10.0.54 (#1018, 2026-04-03)
- Bumped lipy-airflow-providers to 10.0.57 (#1024, 2026-04-06) — lowercase cluster_id, PipelineMD URL fixes
- Bumped Airflow version to 2.9.2.163 (#1023, 2026-04-06) — sensor reschedule lock retry
- **Rolled back Airflow to 2.9.2.158** (#1025, 2026-04-06) — reverted config override UI commits (trigger page issues)
- **Re-bumped Airflow to 2.9.2.164** (#1026, 2026-04-07) — config override fixes applied, re-enabled
- Bumped Airflow to 2.9.2.165 (#1030, 2026-04-08) — dagrun.first_task_start_delay fix, PipelineMD global link
- Bumped lipy-airflow-providers to v10.0.58 (#1029, 2026-04-08) — metadata kafka push destinationConnectionString
- Bumped Airflow to **2.9.2.166** + lipy-airflow-providers to **v10.0.59** (#1034, 2026-04-10) — PendingRollbackError fix, executor deadlock fix
- **Fixed pod fsGroup to prevent NFS mount hangs** on new NKS nodes (#1035, 2026-04-10) — wrong GID caused uninterruptible sleep
- Updated fsGroup for worker and webserver (#1036, 2026-04-10)
- **Added SMTP config to rdev** for EmailOperator support (#1038, 2026-04-11)
- ligradle-core upgrade ramps (#1027, #1039, 2026-04-11)
- ADU dependency updates: python-image 0.1.86 (#1041), lipy-indbt-providers 0.0.183 (#1040)
- Added Slack notification and adjusted PR format for bump automation rules (#1012)
- **Fixed GHD Airflow image path** (#1037, 2026-04-13) — Clusters provisioned after commit 10efffecc3 (March 18) on GHD hit `InvalidImageName` because `defaultAirflowRepository` was changed to `oklahoma-airflow-deployment` (the deployment repo, not the image repo). Pods like `vjlmk3bmno-run-airflow-migrations-wskkf` showed `ImagePullBackOff`. Fix: restored correct image path and `defaultAirflowTag`.
- **Enabled IPv6 on GHD Airflow cluster** (#1028, 2026-04-14) — Applied IPv6 enablement pattern from PR #894 to GHD (Groundhog Day) cluster. Added GHA invocation of `grid-integration-testing-cli` for post-merge integration testing.
- **Bumped Airflow to 2.9.2.170** (#1044, 2026-04-13) — Changelog (2.9.2.166 → 2.9.2.170): Fix scheduler crash from MySQL deadlock in `_process_executor_events` (QUEUED TI `external_executor_id` updates, airflow PR #109).
- **Suppressed noisy rdev connection setup logs** (#1046, 2026-04-15) — Added `PYTHONWARNINGS=ignore` and `AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR` env var prefixes to `airflow connections delete` and `airflow connections add` CLI calls in both `grid_setup.sh` files. Eliminates verbose INFO logs and Python deprecation warnings during rdev connection setup.
- **RDev deprecated image warning** (#1052, 2026-04-21) — Warns when `devcontainer.json` references the old `oklahoma-airflow` image instead of `oklahoma-airflow-deployment`. Helps users update their rdev config after the March 2026 repo split.
- **Kusto Airflow logs reference doc** (#1056, 2026-04-21) — New documentation covering 4 cluster/database combos for querying Airflow logs via Kusto. Provides oncall-ready query patterns for different cluster log sources.
- **LCD deployment migration** (#1060, 2026-04-23, BDP-98089) — Oklahoma-Airflow beginning migration from CRT (Continuous Release Train) to LCD (LinkedIn Continuous Delivery) for infrastructure deployments. Adds LCD pipeline configuration alongside existing CRT flow.
- **Flyte TLS fix: airflow-oc-image 0.1.113→0.1.114** (#1061, 2026-04-24) — Bumped airflow-oc-image to 0.1.114 for non-Grestin Ambassador cluster TLS compatibility. Fixes TLS handshake issues on clusters not using Grestin-managed certificates.
- **Removed WEAVER_URLS override from rdev env setup** (#1063, 2026-04-24, DEPEND-102101) — Removed hardcoded `WEAVER_URLS` export from Oklahoma rdev environment setup. The override was never directly referenced in airflow code (only used internally by `lipy-fabric` library) and caused Fabric/Weaver failures when the hardcoded URL drifted from the actual discovery URL.
- **Reverted LCD onboarding** (#1064, 2026-04-24) — Reverted commit d1260091 (PR #1060 LCD migration) to unblock deployments. The LCD deployments were failing in the EKG step because EKG/Canary is not supported for MPs deployed with ArgoCD. Weekend unblock.
- **ADU dependency upgrades** (#1049, #1065, 2026-04-26) — Automated dependency updates: ligradle-core and linkedin-common-image 0.0.125→0.0.127 + 4 more.
- **Re-onboarded to LCD deployments** (#1066, 2026-04-27, BDP-98089) — Second attempt at CRT→LCD migration after the revert in #1064. Simple deployment config to validate migration. Test and Lasso deployments will run in parallel in a follow-up.
- **Fixed LCD pipeline creation** (#1067, 2026-04-27) — LCD pipeline failed to be created in #1066; added comment to re-attempt pipeline creation.
- **Fixed metrics_stop 500 crash when rate-limited** (#1068, 2026-04-27) — When Flask-Limiter blocks a request (`RateLimitExceeded` during `before_request`), `metrics_start` never runs and `g.start_time` is never set. `metrics_stop` then crashes with `AttributeError` → 500. Fix: guard `metrics_stop` to handle missing `g.start_time`.
- **Documented corp Airflow cluster in Kusto logs reference** (#1069, 2026-04-27) — Extended the Kusto Airflow logs reference doc (added in #1056) to include the `corp` cluster. Discovered while debugging APA-144949 (osos_dags_dev deployment failure on corp).
- **Disabled EKG/canary steps with LCD deployments** (#1070, 2026-04-27, BDP-98089) — EKG/Canary is not supported for MPs deployed with ArgoCD. Disabled these steps in the LCD configuration to prevent LCD deployment failures. Completes the LCD migration saga (#1060→#1064→#1066→#1067→#1070).

---

## Cluster Timeline Summary

| Cluster | Added | Key Events |
|---------|-------|------------|
| Holdem  | Pre-2023 | Scheduler scaled: 20 (Apr 2025) → 24 (Aug 2025). Nimbus Dec 2025. |
| War (grid2) | Pre-2023 | Scaled Jan 2024. 2.9.2 rollout Jun 2025. Nimbus Dec 2025. |
| Corp | Pre-2023 | 2.9.2 May 2025. Corp NKS Helm Nov 2025. IPv6 Mar 2026. |
| EI → Faro | Pre-2023 | Renamed/replaced by Faro. EI decommissioned Oct 2025. Faro on Nimbus Nov 2025. |
| Test | Pre-2023 | Continuous testing ground; Nimbus migration Nov 2025. |
| Lasso | Jul 2025 | New cluster. Nimbus migration Nov 2025. |
| DBT | Pre-2023 | Decommissioned Feb 2026. |
| Prod | Pre-2023 | Decommissioned Sep 2025. |
| GHD | Feb 2025 | Grid Hosted Deployment. NKS support Oct 2025. IPv6 Apr 2026. Image path fix Apr 2026. |
| Load Test | Apr 2025 | Cert-based MySQL access. Nimbus balancer disabled Feb 2026. |
| Dev (rdev) | Ongoing | Fast-testing script added Feb 2026. SMTP config Apr 2026. |

---

## Airflow Version History

| Version | Timeline | Notes |
|---------|----------|-------|
| 2.5.3 | Pre-2023 – Mar 2025 | Original version. Helm chart v1.9.0. Removed from Dockerfile Apr 2026. |
| 2.9.2 (chart) | Mar 2025 | Helm chart replaced with 2.9.2 chart (#517). Rollout to all clusters May-Jun 2025. |
| 2.9.2.x (image) | Jun 2025 – present | Internal LinkedIn-patched image. Currently at **2.9.2.170** (Apr 2026). Note: 2.9.2.163→2.9.2.158 rollback (Apr 6) then re-bump to 2.9.2.164+ (Apr 7). |

---

## See Also
- [Clusters](../infrastructure.md) — per-cluster details (URLs, owners, configs)
- [lipy-changelog](lipy-changelog.md) — provider library evolution
- [Deployment](../references/deployment.md) — CRT flow, promotion, testing
- [GGW](../systems/ggw.md) — Grid Gateway dependency
