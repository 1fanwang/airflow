> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# Systems — Index

> One-line summary of every LinkedIn system the wiki knows about

| System | Role in Airflow ecosystem | Page |
|--------|--------------------------|------|
| GGW | LinkedIn's unified job execution layer; Airflow tasks delegate work to GGW via GGW operator/hook | [GGW](ggw.md) |
| Espresso | Distributed document/NoSQL store; no direct operator — Airflow runs bulk-load and MV-recovery DAGs via standard Grid Gateway operators | [Espresso](espresso.md) |
| Venice | Distributed key-value online feature store; VenicePushOperator pushes HDFS Avro data via GGW's VenicePushJob | [Venice](venice.md) |
| Kafka | Distributed event-streaming backbone; KafkaPushOperator pushes HDFS batch data to Kafka topics via GGW's KafkaPushJob | [Kafka](kafka.md) |
| Spark | Airflow submits Spark jobs via GGW; KJP debugging covers triple-download HDFS pattern, ExecutorResourceLocalizer, SIGPWR, Volcano events | [Spark](spark.md) |
| Tradewind | Federated Airflow router and API proxy | [Tradewind](tradewind.md) |
| Airflow Fork (li-2.9.2) | LinkedIn's fork of Apache Airflow 2.9.2 with DAG-level SLA, PipelineMD, lock contention fixes, and NKS execution balancer | [Airflow Fork](airflow-fork.md) |
| Trust Bridge | Authentication and connectivity verification at LinkedIn; `captain setup trustbridge`, `curli --tb-auth`, DataVault token auth | [Trustbridge](trustbridge.md) |
| D2 | LinkedIn service discovery; use `d2://` scheme (camelCase names) with `curli -f <fabric> --dv-auth SELF`; `d2state` to list services | [D2](d2.md) |
| Iris | Alert routing and notification; suppression operates per alert ID (no plan-level bulk suppress) | [Iris](iris.md) |
| Email | Internal SMTP gateway (`mail-gw.corp.linkedin.com:25`) for sending email from DAGs; no auth needed for @linkedin.com | [Email](email.md) |
| JKS / Truststore | JKS parsing via `pyjks`; existing impls in `id-tools` and `lipy-truststore` — reuse before implementing new | [JKS](jks.md) |
| Azkaban | Legacy job orchestrator being deprecated (go/bye-bye-az); Oklahoma oncall owner since 2025-05-01; cluster decommission runbook | [Azkaban](azkaban.md) |
| lipy-airflow-providers | LinkedIn's custom Airflow provider package: GridGateway operators, event listeners, Iris integration, hooks for internal systems | [Lipy Airflow Providers](lipy-airflow-providers.md) |

<!-- Add rows as new systems are discovered -->

## See Also
- [Architecture](../architecture.md)
- [Codebase Overview](../codebase/README.md)
