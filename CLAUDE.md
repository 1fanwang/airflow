
# Oklahoma Managed Airflow

> LinkedIn's managed Apache Airflow platform — workflow orchestration for data pipelines, ML training, and batch processing.
> Generated on 2026-03-31. Run `/context-repo:generate-claude-md` to regenerate.

## Repositories

| Repository | Purpose | Primary Language |
|------------|---------|------------------|
| airflow | LinkedIn Airflow fork (2.9.2) — scheduler, executor, web UI | Python |
| lipy-airflow-providers | Custom operators, hooks, sensors for LinkedIn infra | Python |
| oklahoma-airflow-deployment | Production/RDev Docker images and Helm charts | Dockerfile, YAML |
| oklahoma_system_dags | System DAGs for regression, maintenance, backfill | Python |
| picli | Pipelines CLI — DAG policy enforcement | Python |
| airflow-workflow-gradle-plugin | Gradle plugin for DAG packaging and policy checks | Java |
| airflow-workflow | MP template for new Airflow workflow repos | Gradle |
| airflow_starter_kit | Example DAGs demonstrating operators and best practices | Python |
| airflow-crt-action | CRT GitHub Action for DAG deployment | Python |
| airflow-docs | Docusaurus documentation site | JavaScript |
| airflow-load-testing | Load testing and benchmarking for clusters | Python |
| airflow-autopilot | DAG quality scoring and agentic authoring | Python |
| torch-autopilot | TF-to-PyTorch conversion with quality scoring | Python |
| li-productivity-agents | Developer productivity AI agent platform (go/lipa) | Python |
| training-platform-agents | AI agents for training failure analysis | Python |
| mufn-service | Grid Gateway — batch job control plane (gRPC, Pekko) | Scala |
| bdp-artifact-metadata-service | ARMS — dataset metadata for sensors | Java |
| tradewind | Federated Router API and UI across Airflow clusters | Python, TypeScript |
| trails-tools | Data infra analytics with AI failure analysis | Python, TypeScript |
| orchestrator-tde | Orchestrator talent insights via gRPC | Python |
| gdp-sales-analytics | GTM data platform dashboards and notebooks | Python |
| roundup-workflows | RoundUp risk detection DAGs | Python |
| rdev-api | gRPC/REST API definitions for rdev | Java |
| rdev-server | Backend service for rdev environments | Python |
| rdev-cli | CLI for managing remote dev environments | Python |
| rdev-base-image | Base Docker images for rdev containers | Dockerfile |
| airflow-oc-image | CLI for Flyte execution triggers from Grid | Python |

## Where to Look

| Topic | Location | Read When |
|-------|----------|-----------|
| System architecture and repo relationships | [docs/architecture.md](docs/architecture.md) | Before cross-repo changes |
| Build commands, testing, and dev servers | [docs/build-and-test.md](docs/build-and-test.md) | When building or running tests |
| Code patterns, conventions, and git workflow | [docs/patterns.md](docs/patterns.md) | Before writing or reviewing code |
| Infrastructure systems and deployment | [docs/infrastructure.md](docs/infrastructure.md) | When touching infra-related code |
| Security architecture and service account auth | [docs/security-architecture.md](docs/security-architecture.md) | When working with auth, DV tokens, or service accounts |
| Domain terminology and jargon | [docs/glossary.md](docs/glossary.md) | When encountering unfamiliar terms |
| Design decisions and rationale | [docs/design-decisions/](docs/design-decisions/) | When questioning why something works this way |
| Product domain and features | [docs/product/](docs/product/) | When adding features or changing behavior |
| Quality grades and tech debt | [docs/quality/](docs/quality/) | During reviews or refactoring |
| External references and resources | [docs/references/](docs/references/) | When needing external documentation |
| Infrastructure skill files | [.claude/skills/infrastructure/](.claude/skills/infrastructure/) | When working with Kafka, gRPC, Grid, etc. |
| lipy-airflow-providers rules | lipy-airflow-providers/.linkedin/ai-agent/ | When working within lipy-airflow-providers |
| airflow-autopilot rules | airflow-autopilot/.linkedin/ai-agent/ | When working within airflow-autopilot |
| picli rules | picli/.linkedin/ai-agent/ | When working within picli |
| orchestrator-tde rules | orchestrator-tde/.linkedin/ai-agent/ | When working within orchestrator-tde |

## Critical Rules

1. Run `workspace clean` before committing any MP changes, `workspace sync` after
2. airflow uses branch `BR_REL_li-2.9.2` and lipy-airflow-providers uses `BR_REL_airflow-2.9.2` — never commit to master on these repos
3. Proto files are the source of truth for gRPC APIs — never modify generated code
4. DAGs must pass policy enforcement (picli / airflow-workflow-gradle-plugin) before deployment via CRT

## Never Do

1. Commit workspace scaffolding files — always `workspace clean` first
2. Hardcode secrets — use KMS via `kms_utils.py`
3. Bypass DAG policy enforcement — picli checks are mandatory before CRT deployment

<!-- WORKSPACE CLI MANAGED - DO NOT EDIT THIS SECTION -->
## Workspace Overview

This directory contains several interconnected repositories managed by the `workspace` CLI.
Changes to one of these repositories will likely need to be reflected in others.
Because of that, you need to explore all of them before proposing a change plan.

### Managing Repositories

To add a new repository to this workspace:

```bash
workspace add <MP-name>
```

Where `<MP-name>` is the name of the multiproduct to add.

To get a list of products in a workspace:

```bash
workspace list
```

### Committing Changes

Before committing any changes, always run:

```bash
workspace clean
```

This will remove the scaffolding connecting the repositories that can
cause a CI failure if pushed to the remote repository.

After committing, run this to reconnect the workspace:

```bash
workspace sync
```
<!-- END WORKSPACE CLI MANAGED -->
