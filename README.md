# Oklahoma Managed Airflow

LinkedIn's managed Apache Airflow platform — workflow orchestration for data pipelines, ML training, and batch processing.

## About This Workspace

Oklahoma is LinkedIn's managed Airflow platform providing scalable workflow orchestration across multiple clusters. This workspace contains the core Airflow fork, custom providers and operators, deployment infrastructure, developer tooling, AI-powered quality scoring, and supporting services. It enables data engineers, ML engineers, and platform teams to build, deploy, and operate production workflows.

- **Created**: 2026-03-31

## Repositories

| Repository | Purpose | Language |
|------------|---------|----------|
| airflow | LinkedIn fork of Apache Airflow 2.9.2 — core scheduler, executor, web UI | Python |
| lipy-airflow-providers | Custom operators, hooks, and sensors for LinkedIn infrastructure | Python |
| oklahoma-airflow-deployment | Production and RDev Docker images, Helm charts | Dockerfile, YAML |
| oklahoma_system_dags | System DAGs for regression, maintenance, and backfill | Python |
| picli | Pipelines CLI for DAG policy enforcement and deployment | Python |
| airflow-workflow-gradle-plugin | Gradle plugin for DAG packaging and policy checks | Java |
| airflow-workflow | MP template for scaffolding new Airflow workflow repos | Gradle |
| airflow_starter_kit | Example DAGs demonstrating operators and best practices | Python |
| airflow-crt-action | CRT GitHub Action for DAG deployment workflows | Python |
| airflow-docs | Docusaurus documentation site for Airflow and Oklahoma | JavaScript |
| airflow-load-testing | Load testing and performance benchmarking for clusters | Python |
| airflow-autopilot | DAG quality scoring and agentic authoring tool | Python |
| torch-autopilot | TensorFlow to PyTorch conversion with quality scoring | Python |
| li-productivity-agents | Developer productivity AI agent platform (go/lipa) | Python |
| training-platform-agents | AI agents for training failure analysis and debugging | Python |
| mufn-service | Grid Gateway — unified batch job control plane (gRPC, Pekko) | Scala |
| bdp-artifact-metadata-service | ARMS — dataset metadata service for sensors | Java |
| tradewind | Federated Router API and React UI across Airflow clusters | Python, TypeScript |
| trails-tools | Data infrastructure analytics with AI-powered failure analysis | Python, TypeScript |
| orchestrator-tde | Orchestrator talent insights via gRPC API | Python |
| gdp-sales-analytics | GTM data platform dashboards and notebooks | Python |
| roundup-workflows | RoundUp risk detection and mitigation DAGs | Python |
| rdev-api | gRPC and REST API definitions for the rdev ecosystem | Java |
| rdev-server | Backend service for remote development environments | Python |
| rdev-cli | CLI for managing remote development environments | Python |
| rdev-base-image | Base Docker images for rdev containers | Dockerfile |
| airflow-oc-image | CLI for triggering Flyte executions from Grid Gateway | Python |

## Getting Started

New to this workspace? Follow these steps:

1. **Clone this repo**
   ```bash
   git clone org-262467312@github.com:linkedin-context/oklahoma-managed-airflow.git
   cd oklahoma-managed-airflow
   ```

2. **Sync the workspace** — This clones all the repositories listed above:
   ```bash
   workspace init .
   ```

3. **Launch Claude Code**
   ```bash
   claude
   ```
   Then switch to Opus with `/model` and select Opus.

4. **Explore the knowledge base** — Claude automatically reads `CLAUDE.md` for context. Browse `docs/` for detailed documentation about architecture, patterns, build commands, and more.

## Returning to This Workspace

Already set up? Here's how to pick up where you left off:

1. **Pull latest changes** and sync repos:
   ```bash
   git pull
   workspace init .
   workspace pull
   ```

2. **Check active work:**
   ```
   /context-repo:execute-plan --status
   ```

3. **Resume execution** on an existing plan:
   ```
   /context-repo:execute-plan <feature-name>
   ```

## Workflow

This workspace uses a three-phase workflow: **Spec -> Plan -> Execute**.

| Phase | Command | What It Does |
|-------|---------|--------------|
| 1. Spec | `/context-repo:generate-spec` | Interactive conversation to define what to build |
| 2. Plan | `/context-repo:generate-plan <feature>` | Generates a step-by-step execution plan |
| 3. Execute | `/context-repo:execute-plan <feature>` | Implements changes repo by repo, creating PRs |

Each phase produces artifacts (spec.md, plan.md, execution-state.json) that are committed to this repo for team visibility and review.

## Quick Reference

| Command | Description |
|---------|-------------|
| `workspace init .` | Clone/update all workspace repos |
| `workspace pull` | Pull latest changes from all repos |
| `workspace list` | List multiproducts in the workspace |
| `workspace add <name>` | Add a multiproduct to the workspace |
| `workspace clean` | Remove scaffolding before committing |
| `workspace sync` | Reconnect workspace after committing |
| `/context-repo:generate-spec` | Create a new feature spec |
| `/context-repo:generate-plan <feature>` | Generate execution plan from a spec |
| `/context-repo:execute-plan <feature>` | Start or resume plan execution |
| `/context-repo:execute-plan --status` | Check execution progress |
| `/context-repo:generate-claude-md` | Regenerate the workspace knowledge base |

## Documentation

| Resource | Description |
|----------|-------------|
| [CLAUDE.md](CLAUDE.md) | AI agent navigation map (auto-generated) |
| [docs/](docs/) | Detailed workspace knowledge base |
