---
name: Multiproduct
description: LinkedIn Multiproduct (MP) system patterns — product-spec.json, dependencies, and workspace structure
---

# Multiproduct (MP)

## Usage in This Workspace

All 27 repositories in this workspace are LinkedIn Multiproducts, each with a `product-spec.json` defining metadata, dependencies, and build configuration.

### Key Files
- `*/product-spec.json` — MP metadata, dependencies, and configuration

### Patterns
- Each MP has a unique name in `product-spec.json`
- Inter-MP dependencies are declared in `product-spec.json` under `dependencies`
- The `workspace` CLI manages multi-MP workspaces (add, sync, list, clean)
- Always run `workspace clean` before committing to remove workspace scaffolding
- Always run `workspace sync` after committing to reconnect the workspace

### When Working With MPs
- Check `product-spec.json` for dependency and build configuration
- Use `workspace list` to see all MPs in the workspace
- Use `workspace add <name>` to add a new MP
- Cross-MP changes should be coordinated — explore related MPs before proposing changes
