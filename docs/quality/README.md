# Quality

> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

## Overview

Quality grades and tech debt tracking for repositories in the workspace.

## Grading Scale

- **Solid**: Well-tested, well-documented, follows all conventions
- **Adequate**: Functional with reasonable test coverage, some gaps
- **Needs Work**: Limited testing, documentation gaps, or convention violations

## Quality Grades

| Repository | Tests | Docs | Code Quality | Overall |
|------------|-------|------|-------------|---------|
| airflow-autopilot | Solid (70%+ coverage) | Solid | Solid (ruff enforced) | Solid |
| li-productivity-agents | Adequate | Adequate | Solid (ruff + pyright) | Adequate |
| mufn-service | Adequate (40% min) | Adequate | Adequate (scoverage) | Adequate |
| orchestrator-tde | Solid (90%+ coverage) | Adequate | Solid (ruff + mypy) | Solid |
| tradewind | Adequate | Adequate | Solid (ruff + mypy) | Adequate |
| trails-tools | Adequate | Adequate | Solid (ruff + mypy) | Adequate |
| training-platform-agents | Adequate | Solid (CLAUDE.md) | Adequate | Adequate |
| torch-autopilot | Solid (135 tests) | Solid (CLAUDE.md) | Adequate | Solid |
| bdp-artifact-metadata-service | Needs Work (minimal) | Needs Work | Adequate | Needs Work |
| oklahoma_system_dags | Needs Work (minimal) | Needs Work | Adequate | Needs Work |

## Tech Debt

- **bdp-artifact-metadata-service**: Very limited test coverage (1 test file). Needs comprehensive testing.
- **oklahoma_system_dags**: Minimal testing; relies on integration validation via Airflow clusters.
- **airflow (fork)**: Large codebase inherited from Apache Airflow. LinkedIn-specific changes should be well-tested.
- **roundup-workflows**: Legacy DAG patterns; DAG validation script (`verify_no_new_dags.py`) suggests a migration in progress.
