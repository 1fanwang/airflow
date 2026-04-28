> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# Iris

## Alert Suppression APIs

### Suppressing Alerts on a Plan

**Limitation**: You cannot suppress all alerts on an Iris plan in a single operation. Suppressions operate at the individual **alert ID** (autoalerts ID) level, not at the plan level.

**Why**: The Iris plan is a notification-routing construct. Alert suppression happens upstream at the alert-evaluation layer, before alerts are routed through the Iris plan. To suppress alerts from an Iris plan, you must:
1. Identify the individual autoalert IDs that feed into the plan
2. Suppress each alert individually using the alert suppression API

**Key Detail**: Plan name and alert ID are distinct concepts in the Iris architecture — plans route notifications, alerts are evaluated and suppressed independently.

### Suppression Mechanics

- **Scope of Suppressions**: Individual alert IDs (autoalerts)
- **Plan Concept**: Iris plans define notification routing (where alerts go, who gets notified, escalation chains)
- **Suppression Layer**: Happens upstream at the alert-evaluation layer, before alerts are generated

### Implication for Oncall

To suppress alerts for a given service or monitoring scope, you must suppress each individual alert ID separately. There is no batch or bulk suppression mechanism at the plan level.
