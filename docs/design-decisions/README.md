# Design Decisions

> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

## Overview

This directory captures significant architectural and design decisions for the Oklahoma Managed Airflow workspace. Entries follow the ADR (Architecture Decision Record) format: Context, Decision, Consequences.

## Decisions

### DD-001: LinkedIn Airflow Fork on Release Branches

**Context**: Apache Airflow evolves rapidly. LinkedIn needs stability for production clusters while maintaining the ability to cherry-pick fixes and apply internal patches.

**Decision**: Maintain a LinkedIn fork of Apache Airflow (`airflow` repo) on release branches (e.g., `BR_REL_li-2.9.2`). The `master` branch is not used for development. `lipy-airflow-providers` tracks the same release version (e.g., `BR_REL_airflow-2.9.2`).

**Consequences**: All work on the core Airflow fork and providers must target the correct release branch. Upgrades to a new Airflow version require creating a new release branch and migrating providers.

### DD-002: Federated Cluster Architecture

**Context**: Different workloads have different SLA and resource requirements. A single Airflow cluster cannot serve all use cases efficiently.

**Decision**: Run multiple Airflow clusters (holdem, war, faro, ei-ltx1) with `tradewind` providing a unified Router API and UI across all clusters.

**Consequences**: DAG authors don't need to know which cluster runs their DAGs. Deployment topology is managed through oklahoma-airflow-deployment Helm charts per cluster.

### DD-003: Policy Enforcement Before Deployment

**Context**: DAGs can cause production issues if they violate resource limits, naming conventions, or security policies.

**Decision**: Enforce policies at build time via `picli` and `airflow-workflow-gradle-plugin` before DAGs reach production. `airflow-autopilot` provides quality scoring as an additional gate.

**Consequences**: DAG authors get early feedback. Policy violations block deployment via CRT. Quality scores guide improvement.

### DD-004: Grid Gateway as Unified Compute Control Plane

**Context**: Batch job submission was fragmented across multiple systems (Azkaban, Spark, custom schedulers).

**Decision**: `mufn-service` (Grid Gateway) provides a unified gRPC control plane for all batch jobs, decoupling orchestration from execution.

**Consequences**: Airflow operators in `lipy-airflow-providers` submit jobs through Grid Gateway. Job lifecycle management is centralized.

### DD-005: Native Policy Convergence

**Context**: picli's custom `@DagPolicy` framework and Airflow's built-in cluster policies (`dag_policy`/`task_policy`) are two separate enforcement paths. Maintaining both increases complexity and divergence between local checks and server-side enforcement.

**Decision**: Converge on Airflow's native cluster policy hooks. New checks are added as native functions in `lipy-airflow-providers/airflow-policy-framework/native/` that log warnings (soft enforcement) rather than raising exceptions. picli loads these via an adapter so the same code runs at build time and on the Airflow scheduler. Plan doc at picli PR #679.

**Consequences**: Policies run identically in local (picli) and server-side (scheduler) contexts. New soft checks provide advisory feedback without blocking deployment. The old `@DagPolicy` checks will be migrated to native functions over time. Reference PRs: lipy-airflow-providers #1182, #1185; picli #680, #681.

## Adding New Decisions

Create a new entry above with format:
- **DD-NNN: Title**
- **Context**: What situation prompted this decision?
- **Decision**: What was decided and why?
- **Consequences**: What are the implications (both positive and negative)?
