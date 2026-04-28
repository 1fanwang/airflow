# Oklahoma Team — Workstreams & Current Focus

> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

## Team Members

| Member | Primary Focus |
|--------|---------------|
| **Shawn Sun** | CRT→LCD migration for DAG MPs; Rdev auth (dedicated SPN, 24h token design) |
| **Jake Kandell** | Config Override (landed); Azkaban final-user migration outreach; scheduler correctness |
| **Sungho Park** | Alert noise reducer (Iris consecutive-failure config); image hygiene (2.5 removed) |
| **Stefan Wang** | Rdev agent capabilities — cluster distinguish, API calls, lifecycle management |
| **Arthur Chen** | Tradewind DAG Mover; system-initiated post-impact recovery research |
| **Yeni Bermudez Padron** | `max_active_runs` scheduler race; long-running MySQL queries (Roundup) |
| **Vinayak Agarwal** | Airflow DB maintenance DAGs; NFS cleanup DAGs; long-running MySQL queries |
| **Trevor Devore** | Data Sensors adoption (War Room); crew creation mapping bump to 500 |

---

## Sprint Themes Summary

| Theme | Owner(s) | Status |
|-------|----------|--------|
| **LCD Migration** (CRT→LCD for DAG MPs) | Shawn | POC done, E2E validation in flight, onboarding in review |
| **Rdev improvements** (auth, SPN, prod connection) | Shawn, Stefan | Dedicated SPN resolved; 24h token lifetime in design; lifecycle agent closed |
| **Alert Noise Reduction** (Iris callback config) | Sungho | Design in progress; feature tickets open |
| **Config Override** | Jake | Frontend + backend closed; parent epic reopened for follow-up |
| **Scheduler correctness** (max_active_runs race) | Jake, Yeni | Template fix closed; max_active_runs race in review |
| **DB maintenance** | Vinayak, Yeni | Long-running query investigations + maintenance DAG creation |
| **Tradewind / DAG Mover** | Arthur | In review |
| **Data Sensors adoption** | Trevor | War Room ongoing |
| **TLS security** | Shawn | Blocked |
| **Azkaban migration** | Jake | Final user outreach in review |
| **Image hygiene** | Sungho | Airflow 2.5 removed; RDev image migration resolved |

---

## See Also
- [Deployment](../references/deployment.md)
- [Oncall](../oncall/README.md)
- [Tradewind](../systems/tradewind.md)
