---
name: LCD
description: LinkedIn Continuous Delivery (LCD) configuration patterns in oklahoma-managed-airflow workspace
---

# LCD

## Usage in This Workspace

LCD (LinkedIn Continuous Delivery) is used for deployment configuration across multiple CLI tools and services.

### Key Files
- `picli/.linkedin/lcd/` — LCD config for picli
- `tradewind/.linkedin/lcd/` — LCD config for tradewind
- `trails-tools/.linkedin/lcd/` — LCD config for trails-tools
- `airflow-load-testing/.linkedin/lcd/` — LCD config for load testing
- `orchestrator-tde/.linkedin/lcd/` — LCD config for orchestrator-tde
- `rdev-cli/.linkedin/lcd/` — LCD config for rdev-cli
- `roundup-workflows/.linkedin/lcd/` — LCD config for roundup-workflows

### Patterns
- LCD configs live in `.linkedin/lcd/` within each MP
- LCD handles build, test, and deploy pipeline configuration
- Each MP has its own LCD config tailored to its deployment needs

### When Working With LCD
- Edit `.linkedin/lcd/` configs for deployment pipeline changes
- LCD changes affect CI/CD pipelines — verify pipeline behavior after changes
- Use the `infra-specs-expert` skill for LCD pipeline configuration options
