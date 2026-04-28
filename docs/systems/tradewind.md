> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# System — Tradewind

> Transparent proxy for Airflow API requests

## What It Is

Tradewind is a **federated orchestration platform** for Airflow clusters. It provides:

1. **Router API** (FastAPI) — a transparent reverse proxy that routes Airflow API calls to the correct physical cluster based on DAG ownership
2. **Airflow-style React UI** — a unified DAG list view that aggregates DAGs from multiple Airflow instances (`holdem`, `war`)
3. **Router Database** (MySQL) — stores DAG registrations, tenant routing information, and shard placement metadata

The system solves the problem of managing DAGs across multiple independent Airflow clusters in a federated architecture while presenting them as a unified interface to users and clients.

---

## Architecture

```
+----------------+     +----------------+     +----------------+
|  React UI      |---->|  FastAPI        |---->|  MySQL         |
|  (frontend/)   |     |  Router API     |     |  Router DB     |
+----------------+     |  (tradewind/)   |     +----------------+
                       +-------+--------+
                               |
                  +------------+------------+
                  v                         v
           Airflow holdem            Airflow war
```

### Components

**Router API** (`tradewind/src/tradewind/`)
- FastAPI application with three main router modules:
  - `routers/airflow.py` — Airflow DAG aggregation endpoints (`/api/v1/dags`, `/api/v1/variables`)
  - `routers/routing.py` — Router API for tenant placement and DAG registration
  - `routers/proxy.py` — Transparent reverse proxy for Airflow API calls
- Authentication via DataVault identity tokens (Trust Bridge in production, developer certs in dev)
- Lazy OTel metric initialization for request observability

**Router Database** (SQLAlchemy ORM + Alembic)
- `models/database.py` — ORM models (`DagRegistry`, `ShardPlacement`, `Tenant`)
- `models/schemas.py` — Pydantic request/response schemas
- Managed via Alembic migrations
- MySQL 8.0 (local dev via Docker)

**React Frontend** (`frontend/`)
- Vite + React + TypeScript + Ant Design
- Single Page Application styled after Airflow 2.9 DAG list
- Airflow-style pinwheel logo and navigation
- Dynamically aggregated DAG display with cluster/tag filtering
- Built and packaged into the Python wheel by `mint build`

**Cluster Config** (hardcoded constants)
- `models/cluster_config.py` — Single source of truth for physical cluster metadata
- Maps cluster IDs to logical clusters and Airflow URLs:
  - `holdem-1` -> `https://holdem.oklahoma-airflow.grid.linkedin.com`
  - `load-test` -> `https://airflow-load-test.grid.linkedin.com` (federated under `holdem`)
  - `war-1` -> `https://war.oklahoma-airflow.grid.linkedin.com`

---

## How Requests Route

### Transparent Proxy (`/api/v1/proxy`)

The proxy is a drop-in replacement for direct Airflow API calls. Clients add two headers:

```
X-Tradewind-Cluster: holdem          (required — logical cluster)
X-Tradewind-Dag-Id: my_dag           (optional — for non-/dags/ paths)
```

**Routing logic:**
1. Extract `dag_id` from the path (e.g., `/api/v1/dags/{dag_id}/...`) or from `X-Tradewind-Dag-Id` header
2. Look up DAG in the `dag_registry` table to find which physical cluster owns it
3. Forward request to the physical Airflow URL with headers filtered (strip Tradewind-specific and authorization headers, inject DataVault token)
4. Return upstream response to client

**Supported paths:**
- `/api/v1/proxy/dags/{dag_id}/...` — DAG-specific operations (dag_id extracted from path)
- `/api/v1/proxy/eventLogs?...` — DAG-scoped logs (requires `X-Tradewind-Dag-Id` header)

**Unsupported paths:**
- `/api/v1/proxy/variables/...` and other cluster-level operations return `501` (not implemented) because they require gathering responses from all physical clusters

### Tenant Routing (`/api/v1/routing`)

Used for deployment-time registration and operational monitoring:

- `GET /api/v1/routing/tenants/{mp}/{app}/{logical_cluster}` — Look up physical cluster for a tenant
- `PUT /api/v1/routing/tenants/{mp}/{app}/{logical_cluster}` — Create or get existing tenant routing
- `PUT /api/v1/routing/tenants/{mp}/{app}/{logical_cluster}/cluster` — Repin tenant to a different physical cluster
- `POST /api/v1/routing/tenants/{mp}/{app}/{logical_cluster}/dags` — Register DAGs for a tenant during deployment
- `GET /api/v1/routing/tenants/{mp}/{app}/{logical_cluster}/dags/{dag_id}` — Look up a specific DAG

### Airflow Aggregation (`/api/v1/dags`)

Server-side aggregation of DAGs from all physical clusters in a logical cluster:

- `GET /api/v1/dags?logical_cluster=holdem&tag=regression_test` — Fetch and aggregate DAGs from all physical clusters
- Supports filtering by `tag`, `paused` status, pagination (`page`, `page_size`)
- Canonical DAG grouping removes cluster-specific naming patterns (e.g., `_azkabanWar_war__`, `_holdem__`) to group logically identical DAGs
- Extracts MP (middle platform) and app name from DAG `file_loc` path pattern: `/opt/airflow/dags/{mp}/{app}/...`

---

## Authentication

**Token Resolution Order** (`auth.py`):

1. **Trust Bridge injection** (production) — `x-authn-datavault-token` header set by LinkedIn's service mesh
2. **Application service certificate** — via `lipy-datavault` (deployed app)
3. **Developer identity certificate** — `~/identity.cert` and `~/identity.key` (created by `id-tool grestin sign -o ~`)

Tokens are cached for 23 hours to avoid per-request lookups.

**Airflow Client**:
- Uses Bearer token authentication (Airflow 3.x compatible)
- DataVault token passed as `datavaultIdentityToken` header to upstream Airflow

---

## Configuration

### Deployment Config (`config/`)

- **External config** (`config/external/`) — Confetti/KSAP deployment files
- **App config** (`config/app/tradewind/`) — cfg2 configuration sources by fabric:
  - `dev/fabric.src` — Local Docker MySQL on port 3307, debug mode enabled
  - `prod/fabric.src` — Production database URL and settings
  - `ei-ltx1/fabric.src` — ei-ltx1 fabric config with SQLite fallback

Configuration is compiled to `application.cfg` at build time and loaded by `gunicorn_conf.py`.

### Cluster Configuration (hardcoded)

`models/cluster_config.py` defines the physical clusters in `CLUSTER_CONFIG` dict. Adding or removing a cluster requires a deploy (not runtime-configurable).

**Shard Placement** (runtime configurable)
- DB table tracks which physical cluster is the **active placement target** (absorbs new tenants)
- API endpoint `/api/v1/admin/clusters/{logical_cluster}/placement` allows updating placement
- OTel counter `Tradewind.Router.ShardPlacementFallback` emitted when placement table miss occurs

---

## Metrics / Observability

**OpenTelemetry Integration** (`metrics.py`)

Instruments initialized lazily to avoid startup ordering issues:

1. **Request Metrics** (emitted by `_ApiMetricsMiddleware` in `webapp.py`)
   - Counter: `Tradewind.Request.Count` — tagged by `Method`, `Endpoint`, `StatusCode`
   - Histogram: `Tradewind.Request.LatencyMs` — tagged by `Method`, `Endpoint`

2. **Routing Metrics**
   - Counter: `Tradewind.Router.ShardPlacementFallback` — emitted when shard_placement DB lookup fails; tagged by `LogicalCluster`

**Health Endpoints**
- `GET /health` — Returns `{"status":"healthy"}`
- `GET /admin` — Returns `GOOD` (LinkedIn standard health check)

---

## Recent Changes

Key commits (last 60):

| Commit | Message |
|--------|---------|
| `aa46e5f` | refactor: consolidate auth and routing helpers |
| `5d6d04b` | feat: header-based transparent proxy for Airflow API requests |
| `f5ad996` | Add OTel request metrics for all /api/v1/* endpoints |
| `375ea8b` | Frontend: redirect / to /airflow, fix nav links |
| `3103582` | Add /airflow/admin page with DAG lookup and repin |
| `6f17c97` | Add atomic deploy endpoint and worker placement API |
| `31abd22` | Add load-test cluster to holdem logical cluster federation |
| `31bd84c` | Add DB-driven shard placement with admin API |
| `1b64a8c` | Add OTel metrics infrastructure for routing observability |
| `b96f0cc` | Backend: server-side pagination, paused filtering, logical cluster routing |
| `b9deaee` | Add Airflow-style federated DAG UI at /airflow |
| `8a91d4a` | Add SSO integration for automatic Airflow authentication |
| `ac6f974` | Add prod-ltx1 KSAP chart for tradewind |

**Key features added recently:**
- Transparent reverse proxy for Airflow API calls (header-based routing)
- OTel request metrics for all API endpoints
- DB-driven shard placement with admin API
- Server-side pagination and filtering
- Atomic deploy endpoint with worker placement
- SSO integration

**Late April 2026 additions:**
- **Navbar user badge and timezone-aware clock** (PR #79, 2026-04-24): `/api/v1/logged-in-user` endpoint returning authenticated username from Trust Bridge `x-authn-username` header (falls back to `$USER` in dev). `ClockDropdown` in navbar: live `HH:MM TZ (+/-HH:MM)` clock with UTC / Local / custom IANA timezone picker; selection persists across sessions.
- **Announcement banners** (PR #80, 2026-04-24): Admin-controlled announcement banners at top of DAG list, mirroring Oklahoma Airflow's `OKLAHOMA_INFOS`/`OKLAHOMA_WARNINGS`/`OKLAHOMA_ALERTS` mechanism. Operators set `tradewind.announce_infos`, `tradewind.announce_warnings`, and `tradewind.announce_errors` in cfg2 config.
- **Airflow DB connections for holdem and load-test repin** (PR #82, 2026-04-24): Added `prod-ltx1` cfg2 entries for two Airflow clusters required for the holdem->load-test repin flow: `pxy-tradewind` (READ-only) on `holdem-1`, `pxy-tradewind-load-test` (READ-WRITE) on `load-test`.
- **Load-test Airflow DSN bypass ProxySQL** (PR #84, 2026-04-27): The previous DSN used `airflow-load-test.mysql.ltx1.prod.linkedin.com` which routes through ProxySQL. ProxySQL for load-test does not have `pxy-tradewind-load-test` configured as a user, so cert-based auth fails. Fix: point to direct MySQL host to bypass ProxySQL.
- **Use pxy-tradewind-rw for holdem-1 pause step** (PR #85, 2026-04-27): Switch holdem-1 Airflow DSN from `pxy-tradewind` (read-only) to `pxy-tradewind-rw` (read-write) so the repin poller can execute `UPDATE dag SET is_paused = 1` during the pause step. Also cleaned up completed backlog items (banner management, user account navbar) and removed duplicate-DAG backlog entry.

**April 2026 additions (cont'd):**
- **Backfill routing DB from Airflow clusters** (PR #56, 2026-04-16): New standalone operator script (`tradewind/src/tradewind/scripts/backfill_routing.py`) that populates Tradewind's router DB directly from Airflow metadata. Connects to the Airflow metadata DB (`SELECT dag_id, fileloc FROM dag WHERE is_active = 1`), computes which `mp_app_tag` entries are missing from the routing DB, and backfills them. Avoids REST API pagination — reads directly from the metadata DB for completeness.
- **DAG row hover links + Browse nav dropdown** (PR #78, 2026-04-15): Added hover-revealed quick-nav link bar to each DAG row — Code, Graph, Grid, Calendar, Gantt, Landing, Tries, Duration — each opens the physical-cluster Airflow URL in a new tab. CSS-only reveal (`.af-dag-row:hover`), no React state. Added Browse nav dropdown matching Airflow's native navigation. State Management page added to Browse dropdown for repin job visibility.
- **React version pin + npm ci** (PR #77, 2026-04-14): Fixed React error #527 in staging (v0.0.51) — `react-dom` 19.2.4 has a hardcoded version check and throws if `React.version` differs. Root cause: `npm install` re-resolves packages on each build, picking up react 19.2.5 while react-dom stayed at 19.2.4. Fix: pin both to 19.2.5 and switch build to `npm ci` for deterministic installs.

**April 2026 additions:**
- **Repin job data model** (PR #62, 2026-04-02): `RepinJob` ORM model with state machine for DAG cluster migrations; migration 004 (`repin_jobs` table); CRUD service and API endpoints
- **MySQL SSL cert auth** (PR #63, 2026-04-02): `pxy-tradewind` proxy user authenticates via x509 client cert; added `ssl_cert`/`ssl_key` to SQLAlchemy connection
- **DAG trigger Actions column** (PR #67, 2026-04-07): replaced Links column with trigger button (success/fail feedback) + open-in-Airflow external link; added Airflow pinwheel favicon and CSS-only InfoIcon tooltip
- **Owner filter, pause toggle, run history** (PR #68, 2026-04-08): server-side `?owner=` query param; pause/unpause DAGs with optimistic UI and error revert; expandable last-5-runs history column
- **Async repin state machine** (PR #70, 2026-04-13): `RepinPoller` state machine with `repin_jobs` table + `locked_by`/`locked_until` lease columns. `dag_mover_service.py` manages create/cancel repin jobs with atomic UPDATE + history INSERT + DELETE + audit log. Cancel only allowed from specific states. State transitions: `pending` -> `paused` -> `finalized` -> `completed` (not `draining`/`copying` as earlier described).
- **Project backlog** (PR #72, 2026-04-13): Added `BACKLOG.md` capturing open work across Performance, Correctness, Repinning, and UX categories. Fixes factual errors in repinning section: state names corrected from `draining`/`copying` to `paused`/`finalized`.
- **Navbar dropdowns and Browse page** (PR #73, 2026-04-13): `NavDropdown` component (hover-reveal CSS dropdown matching Holdem Bootstrap 3 style). Docs dropdown (Documentation, Airflow Website, GitHub Repo, REST API Reference), Backfill dropdown. Browse page added. Separator gap between Airflow brand logo and nav links.

---

## See Also

- [Architecture](../architecture.md)
- [Overview](../overview.md)
