---
name: LiX
description: LinkedIn experimentation (LiX / T-REX) patterns in oklahoma-managed-airflow workspace
---

# LiX (T-REX)

## Usage in This Workspace

LiX is LinkedIn's experimentation platform (also known as T-REX), used in mufn-service for feature flagging and A/B testing.

### Key Files
- `mufn-service/mufn-admission-controller-plugins/lix-plugin/` — LiX plugin for MUFN admission controller

### Dependencies
- `lix-rest-api` — LiX REST API client

### Patterns
- LiX experiments are checked via the REST API client
- The lix-plugin integrates experimentation into MUFN's admission control flow
- Feature flags gate new functionality behind experiment keys

### When Working With LiX
- Use `lix-rest-api` client for experiment checks
- LiX keys follow naming conventions — check existing keys for patterns
- Use the `infra-specs-expert` skill for LiX experiment setup and ramping
