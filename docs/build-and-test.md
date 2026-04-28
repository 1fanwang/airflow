> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

# Build and Test

## Workspace Commands

| Command | Description |
|---------|-------------|
| `workspace init .` | Clone/update all workspace repos |
| `workspace pull` | Pull latest changes from all repos |
| `workspace list` | List multiproducts and their status |
| `workspace add <name>` | Add a multiproduct to the workspace |
| `workspace clean` | **Always run before committing** — removes scaffolding that causes CI failures |
| `workspace sync` | **Run after committing** — reconnects the workspace |

---

## Branch Notes

- **airflow**: branch `BR_REL_li-2.9.2` — never commit to master
- **lipy-airflow-providers**: branch `BR_REL_airflow-2.9.2` — never commit to master
- All other repos: `master`

---

## Gradle-Based Repos (Java / Scala / Python with Gradle)

| Repository | Build | Test | Lint / Format |
|------------|-------|------|---------------|
| lipy-airflow-providers | `./gradlew build` | `./gradlew pytest coverageByOwner` | `./gradlew ruff` |
| oklahoma-airflow-deployment | `./gradlew buildImage` | `mint buildImage` | — |
| airflow-crt-action | `./gradlew -Prelease=true build` | `./gradlew pytest` | — |
| airflow-workflow-gradle-plugin | `./gradlew build -Prelease=true` | `./gradlew build coverageByOwner` | checkstyle, spotbugs |
| airflow_starter_kit | `./gradlew -Prelease=true build` | `./gradlew check coverageByOwner` | — |
| airflow-load-testing | `./gradlew -Prelease=true build` | `./gradlew pytest` | `./gradlew ruff` |
| oklahoma_system_dags | `./gradlew build` | — (integration via clusters) | — |
| airflow-workflow | `./gradlew build` | `mint test-template` | — |
| mufn-service | `./gradlew build` | `./gradlew test` (ScalaTest) | scoverage (40% min) |
| bdp-artifact-metadata-service | `./gradlew build` | `./gradlew test` | — |
| picli | `./gradlew build` | `./gradlew pytest coverageByOwner` | black, isort, mypy |
| rdev-api | `./gradlew build` | — (API definitions only) | — |
| rdev-cli | `./gradlew build` | `./gradlew pytest coverageByOwner` | black, isort, mypy |
| rdev-server | `./gradlew build` | `./gradlew pytest coverageByOwner` | black, isort, mypy |
| rdev-base-image | `./gradlew build` | — (Docker images) | — |
| roundup-workflows | `./gradlew build` | `./gradlew pytest coverageByOwner` | black, isort, mypy |
| orchestrator-tde | `./gradlew build` | `.linkedin/bin/tox -e test,coverage-by-owner` | ruff, mypy |
| airflow-oc-image | `./gradlew build` | `./gradlew pytest` | — |
| gdp-sales-analytics | `./gradlew build` | `./gradlew pytest` | — |

---

## Python-Native Repos (tox / uv / hatch)

| Repository | Build | Test | Lint / Format |
|------------|-------|------|---------------|
| airflow (fork) | `python setup.py build` | `pytest tests/` | — |
| airflow-autopilot | `mint setup && .linkedin/bin/tox -p` | `.linkedin/bin/tox -e test,coverage-by-owner` | ruff format + check |
| li-productivity-agents | `uv sync` (per module) | `pytest` (per module) | ruff, pyright |
| training-platform-agents | `uv sync` (per module) | `pytest` (per module) | ruff |
| torch-autopilot | `uv sync` | `pytest test/` | — |

---

## Frontend Repos (Node.js / Yarn / npm)

| Repository | Build | Test | Notes |
|------------|-------|------|-------|
| airflow-docs | `yarn build` | `yarn test` | Docusaurus site |
| tradewind (frontend) | `cd frontend && npm run build` | `npm run test` | Vitest |
| trails-tools (frontend) | `cd ux && npm run build` | — | — |

---

## Python Webapp Repos (tox + Flask / FastAPI)

| Repository | Build | Test | Lint / Format |
|------------|-------|------|---------------|
| tradewind | `mint setup` | `.linkedin/bin/tox -e test` | ruff, mypy |
| trails-tools | `mint setup` | `.linkedin/bin/tox -e test` | ruff, mypy |

---

## Repo-Specific Testing Docs

For detailed fixtures, mocking strategies, and test data patterns:

| Repository | Docs Location |
|------------|--------------|
| lipy-airflow-providers | `.linkedin/ai-agent/` |
| airflow-autopilot | `.linkedin/ai-agent/` and `.claude/CLAUDE.md` |
| picli | `.linkedin/ai-agent/` |
| orchestrator-tde | `.linkedin/ai-agent/` |

---

## See Also
- [Codebase Overview](codebase/README.md) — Key repos, entry points, working branches
- [Deployment](deployment.md) — CRT/LCD flow, picli commands, RDev testing
- [Gotchas](codebase/gotchas.md) — Known footguns that affect builds
