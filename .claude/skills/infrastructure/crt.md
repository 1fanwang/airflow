---
name: CRT
description: Change Request Tracker (CRT) deployment patterns in oklahoma-managed-airflow workspace
---

# CRT

## Usage in This Workspace

CRT (Change Request Tracker) manages production deployments, with the entire `airflow-crt-action` MP dedicated to CRT integration.

### Key Files
- `airflow-crt-action/` — Dedicated MP for CRT GitHub Actions integration
- CRT backend utilities used across deployment workflows

### Patterns
- CRT tracks and gates production changes
- GitHub Actions in `airflow-crt-action` automate CRT workflows
- Deployments require CRT approval before proceeding to production

### When Working With CRT
- Modify `airflow-crt-action/` for deployment workflow changes
- CRT changes affect production deployment pipelines — test thoroughly
- Use the `infra-specs-expert` skill for CRT API and approval flow details
