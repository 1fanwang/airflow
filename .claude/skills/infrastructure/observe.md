---
name: Observe / OpenTelemetry
description: Observability and monitoring patterns (OpenTelemetry, metrics) in oklahoma-managed-airflow workspace
---

# Observe / OpenTelemetry

## Usage in This Workspace

Observability is implemented through OpenTelemetry SDK for metrics and tracing across li-productivity-agents and mufn-service.

### Key Files
- `li-productivity-agents/platform/lipa-core/src/liproductivityagents/core/monitoring/otel_metrics.py` — OTel metrics setup and custom exporters
- `mufn-service/` — OpenTelemetry SDK instrumentation

### Patterns
- OpenTelemetry SDK is the standard for metrics and distributed tracing
- Custom metric exporters in `otel_metrics.py` for LinkedIn's monitoring infrastructure
- Metrics are emitted using OTel meter API, not custom metric libraries
- Traces propagate context across service boundaries via gRPC metadata

### When Working With Observability
- Add metrics using OpenTelemetry meter API patterns from `otel_metrics.py`
- Follow existing naming conventions for metric names and labels
- Use the `infra-specs-expert` skill for metric pipeline and alerting configuration
