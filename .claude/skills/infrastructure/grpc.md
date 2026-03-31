---
name: gRPC
description: gRPC service definitions and usage patterns across oklahoma-managed-airflow workspace
---

# gRPC

## Usage in This Workspace

gRPC is used for service-to-service communication with Protocol Buffer definitions across multiple MPs.

### Key Files
- `mufn-service/mufn-control-api/src/main/proto/` — MUFN control plane API protos
- `rdev-api/rdev-api/src/main/serviceProto/` — Rdev service API protos
- `bdp-artifact-metadata-service/api/src/main/proto/` — BDP artifact metadata API protos
- `li-productivity-agents/qa-agent/qa-agent-python-api/src/proto/` — QA agent API protos

### Patterns
- Proto files define service contracts in `src/main/proto/` or `src/main/serviceProto/`
- Java/Kotlin services use LiGradle protobuf plugin for code generation
- Python services use `grpcio` with generated stubs
- Service APIs are published as separate `-api` modules (e.g., `mufn-control-api`, `rdev-api`)

### When Working With gRPC Code
- Proto files are the source of truth — modify protos, not generated code
- API modules are typically separate subprojects (check `settings.gradle`)
- Use `grpcurli` CLI tool to test gRPC endpoints locally
- Use the `infra-specs-expert` skill for gRPC service mesh and mTLS configuration
