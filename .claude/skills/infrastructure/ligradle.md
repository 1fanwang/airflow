---
name: LiGradle
description: LinkedIn Gradle build system patterns across oklahoma-managed-airflow workspace
---

# LiGradle

## Usage in This Workspace

LiGradle is LinkedIn's customized Gradle build system, used across 20+ MPs in this workspace.

### Key Files
- `*/build.gradle` — Build configuration in each MP
- `*/settings.gradle` — Multi-project settings
- `*/product-spec.json` — MP metadata and dependency declarations

### Patterns
- All Java/Kotlin MPs apply `ligradle-core` plugin
- Multi-module projects use `settings.gradle` to define subprojects
- Dependencies are declared in `product-spec.json` and resolved by LiGradle
- Use `gradle build` or `gradle test` for local builds

### When Working With Build Files
- `product-spec.json` is the primary place to declare MP dependencies
- `build.gradle` handles module-level build configuration
- Do not manually manage dependency versions — LiGradle resolves them from `product-spec.json`
- Use `mint build` for CI-style builds locally
