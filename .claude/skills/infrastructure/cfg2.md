---
name: CFG2
description: CFG2 configuration management patterns in oklahoma-managed-airflow workspace
---

# CFG2

## Usage in This Workspace

CFG2 is LinkedIn's configuration management system, used in li-productivity-agents for application configuration.

### Key Files
- `li-productivity-agents/config/app/**/*.src` — CFG2 source configuration files
- `li-productivity-agents/platform/lipa-core/src/liproductivityagents/core/cfg2.py` — Python CFG2 client

### Patterns
- Configuration files use `.src` extension in `config/app/` directories
- CFG2 supports dimension-based overrides (environment, datacenter, fabric)
- Python code accesses config via the CFG2 client in `cfg2.py`
- Config changes are deployed separately from code changes

### When Working With CFG2
- Edit `.src` files in `config/app/` for configuration changes
- Use dimension overrides for environment-specific values
- Never hardcode values that should be configurable — use CFG2 instead
- Use the `infra-specs-expert` skill for CFG2 schema and dimension details
