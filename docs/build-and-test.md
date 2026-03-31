# Build and Test

> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

## Overview

Quick-reference build, test, lint, and dev server commands for all repositories in the workspace. For repo-specific testing internals (fixtures, mocking, test data), see the repo's own documentation.

## Workspace Commands

| Command | Description |
|---------|-------------|
| `workspace init .` | Clone/update all workspace repos |
| `workspace pull` | Pull latest changes from all repos |
| `workspace list` | List multiproducts and their status |
| `workspace add <name>` | Add a multiproduct to the workspace |
| `workspace clean` | Remove workspace scaffolding before committing |
| `workspace sync` | Reconnect workspace after committing |

**Important**: Always run `workspace clean` before committing and `workspace sync` after.

## Build Command Reference

### Gradle-based repos (Java/Scala/Python with Gradle)

| Repository | Build | Test | Lint/Format |
|------------|-------|------|-------------|
| lipy-airflow-providers | `./gradlew build` | `./gradlew pytest coverageByOwner` | `./gradlew ruff` |
| oklahoma-airflow-deployment | `./gradlew buildImage` | `mint buildImage` | N/A |
| airflow-crt-action | `./gradlew -Prelease=true build` | `./gradlew pytest` | N/A |
| airflow-workflow-gradle-plugin | `./gradlew build -Prelease=true` | `./gradlew build coverageByOwner` | checkstyle, spotbugs |
| airflow_starter_kit | `./gradlew -Prelease=true build` | `./gradlew check coverageByOwner` | N/A |
| airflow-load-testing | `./gradlew -Prelease=true build` | `./gradlew pytest` | `./gradlew ruff` |
| oklahoma_system_dags | `./gradlew build` | N/A (integration via clusters) | N/A |
| airflow-workflow | `./gradlew build` | `mint test-template` | N/A |
| mufn-service | `./gradlew build` | `./gradlew test` (ScalaTest) | scoverage (40% min) |
| bdp-artifact-metadata-service | `./gradlew build` | `./gradlew test` | N/A |
| picli | `./gradlew build` | `./gradlew pytest coverageByOwner` | black, isort, mypy |
| rdev-api | `./gradlew build` | N/A (API definitions) | N/A |
| rdev-cli | `./gradlew build` | `./gradlew pytest coverageByOwner` | black, isort, mypy |
| rdev-server | `./gradlew build` | `./gradlew pytest coverageByOwner` | black, isort, mypy |
| rdev-base-image | `./gradlew build` | N/A (Docker images) | N/A |
| roundup-workflows | `./gradlew build` | `./gradlew pytest coverageByOwner` | black, isort, mypy |
| orchestrator-tde | `./gradlew build` | `.linkedin/bin/tox -e test,coverage-by-owner` | ruff, mypy |
| airflow-oc-image | `./gradlew build` | `./gradlew pytest` | N/A |
| gdp-sales-analytics | `./gradlew build` | `./gradlew pytest` | N/A |

### Python-native repos (tox/uv/hatch)

| Repository | Build | Test | Lint/Format |
|------------|-------|------|-------------|
| airflow | `python setup.py build` | `pytest tests/` | N/A |
| airflow-autopilot | `mint setup && .linkedin/bin/tox -p` | `.linkedin/bin/tox -e test,coverage-by-owner` | ruff format + check |
| li-productivity-agents | `uv sync` (per module) | `pytest` (per module) | ruff, pyright |
| training-platform-agents | `uv sync` (per module) | `pytest` (per module) | ruff |
| torch-autopilot | `uv sync` | `pytest test/` | N/A |

### Frontend repos (Node.js/Yarn)

| Repository | Build | Test | Lint/Format |
|------------|-------|------|-------------|
| airflow-docs | `yarn build` | `yarn test` | N/A |
| tradewind (frontend) | `cd frontend && npm run build` | `npm run test` (Vitest) | N/A |
| trails-tools (frontend) | `cd ux && npm run build` | N/A | N/A |

### Python webapp repos (tox + Flask/FastAPI)

| Repository | Build | Test | Lint/Format |
|------------|-------|------|-------------|
| tradewind | `mint setup` | `.linkedin/bin/tox -e test` | ruff, mypy |
| trails-tools | `mint setup` | `.linkedin/bin/tox -e test` | ruff, mypy |

## Repo-Specific Testing Details

For detailed test fixtures, mocking strategies, and test data setup patterns, see each repo's own docs:

| Repository | Testing Docs Location |
|------------|----------------------|
| lipy-airflow-providers | `.linkedin/ai-agent/` |
| airflow-autopilot | `.linkedin/ai-agent/` and `.claude/CLAUDE.md` |
| picli | `.linkedin/ai-agent/` |
| orchestrator-tde | `.linkedin/ai-agent/` |

## Branch Notes

- **airflow**: Uses branch `BR_REL_li-2.9.2` (not master)
- **lipy-airflow-providers**: Uses branch `BR_REL_airflow-2.9.2` (not master)
- All other repos use `master` as their primary branch
