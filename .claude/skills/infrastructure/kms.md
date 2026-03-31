---
name: KMS
description: Key Management Service (KMS) patterns for secret management in oklahoma-managed-airflow workspace
---

# KMS

## Usage in This Workspace

KMS (Key Management Service) is used for secret management and encryption, primarily in lipy-airflow-providers.

### Key Files
- `lipy-airflow-providers/oklahoma-helpers/src/linkedin/oklahoma/helpers/utils/kms_utils.py` — KMS utility functions

### Dependencies
- `lipy-key-management-service` — LinkedIn's Python KMS client library

### Patterns
- Use `kms_utils.py` helpers for encrypting/decrypting secrets in Airflow DAGs
- Secrets are stored encrypted and decrypted at runtime via KMS
- Never store plaintext secrets in code, config files, or environment variables

### When Working With Secrets
- Use KMS utilities from `oklahoma-helpers` for secret access in DAGs
- Use the `infra-specs-expert` skill for KMS API details and key provisioning
- Follow LinkedIn security guidelines — no secrets in git repos
