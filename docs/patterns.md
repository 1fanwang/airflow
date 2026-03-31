# Patterns and Conventions

> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

## Overview

Cross-repo coding conventions, git workflow, and workspace-wide patterns. For repo-specific implementation patterns, see the linked repo docs.

## Git Workflow

- Feature branches off `master` (or the appropriate release branch for airflow / lipy-airflow-providers)
- PRs require review before merge
- Run `workspace clean` before committing changes in any MP
- Run `workspace sync` after committing to reconnect the workspace

## Workspace Conventions

### Multiproduct Structure
- Every MP has a `product-spec.json` at root defining metadata and dependencies
- Gradle-based MPs use `build.gradle` + `settings.gradle` with `ligradle-core` plugin
- Python MPs use Gradle wrappers (ligradle-python) or native tooling (tox, uv, hatch)
- Dependencies between MPs are declared in `product-spec.json`, not build files

### DAG Development Pattern
DAG authors follow this workflow:
1. Write DAGs in their workflow MP (or use `airflow_starter_kit` as reference)
2. Use operators from `lipy-airflow-providers` for LinkedIn-specific integrations
3. Run `airflow-autopilot score` for quality assessment
4. Policy enforcement via `picli` / `airflow-workflow-gradle-plugin`
5. Deploy via `airflow-crt-action` through CRT

### Python Service Pattern
Python services in this workspace follow a common pattern:
- Apollo for service lifecycle management
- CFG2 for configuration (`config/app/**/*.src`)
- `lipy-web` / `lipy-gunicorn` for HTTP serving
- SQLAlchemy + Alembic for database access and migrations
- Used by: tradewind, trails-tools, rdev-server, orchestrator-tde, roundup-workflows

### gRPC Service Pattern
gRPC services define contracts in `.proto` files:
- API modules are published as separate subprojects (e.g., `mufn-control-api`, `rdev-api`)
- Proto files are the source of truth — never modify generated code
- Python clients auto-generated from proto definitions
- Used by: mufn-service, rdev-api, bdp-artifact-metadata-service, orchestrator-tde

## Self-Documenting Repo Patterns

For implementation patterns specific to these repos, see their canonical docs:

| Repository | Patterns Location | Topics |
|------------|------------------|--------|
| lipy-airflow-providers | `.linkedin/ai-agent/` | Provider development, operator conventions, testing |
| oklahoma-airflow-deployment | `.linkedin/ai-agent/` | Image building, version bumping |
| airflow-autopilot | `.linkedin/ai-agent/` + `.claude/CLAUDE.md` | Scoring dimensions, plugin architecture, CLI patterns |
| picli | `.linkedin/ai-agent/` | Policy enforcement, CLI patterns |
| orchestrator-tde | `.linkedin/ai-agent/` | gRPC patterns, multi-module structure, migrations |
| airflow-docs | `.linkedin/ai-agent/` | Docusaurus content conventions |
| airflow-oc-image | `.linkedin/ai-agent/` | Flyte integration patterns |

## Repos with CLAUDE.md

These repos have their own CLAUDE.md with conventions and context:

| Repository | Location |
|------------|----------|
| airflow-autopilot | `.claude/CLAUDE.md` |
| gdp-sales-analytics | `CLAUDE.md` |
| torch-autopilot | `CLAUDE.md` |
| training-platform-agents | `CLAUDE.md` |
| tradewind | `CLAUDE.md` |
| trails-tools | `CLAUDE.md` |
