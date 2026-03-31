# Security Architecture: LinkedIn Security Manager Integration with OSS Airflow

> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

## Overview

This document describes how LinkedIn's security layer integrates with Apache Airflow's Flask-AppBuilder (FAB) authentication and authorization system. It covers the full request lifecycle from DataVault token to API response, special handling for service accounts, and known limitations with non-DAG-specific API endpoints.

## Architecture Summary

LinkedIn replaces Airflow's default authentication with a custom security stack:

```
Request with DV Token
       │
       ▼
┌──────────────────────────────────┐
│  api_auth.py                     │
│  requires_authentication()       │
│  ├─ get_principal_from_dv_token()│
│  └─ get_principal_from_cert()    │
│       │                          │
│       ▼                          │
│  Principal (User or Service)     │
└──────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────┐
│  manager.py                      │
│  LinkedInAirflowSecurityManager  │
│  ├─ refresh_airflow_user()       │  ← UserPrincipal
│  └─ refresh_airflow_service_user()│ ← ServicePrincipal
│       │                          │
│       ▼                          │
│  FAB ab_user / ab_role tables    │
└──────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────┐
│  FAB Authorization (OSS Airflow) │
│  is_authorized_dag()             │
│  get_permitted_dag_ids()         │
│  Role → Permission → Resource    │
└──────────────────────────────────┘
```

## Key Source Files

| File | Location | Purpose |
|------|----------|---------|
| api_auth.py | `lipy-airflow-providers/.../linkedin/airflow/security/api_auth.py` | REST API authentication backend |
| manager.py | `lipy-airflow-providers/.../linkedin/airflow/security/manager.py` | `LinkedInAirflowSecurityManager` — custom FAB security manager |
| utils.py | `lipy-airflow-providers/.../linkedin/airflow/security/utils.py` | Service account detection helpers |
| constants.py | `lipy-airflow-providers/.../linkedin/airflow/security/constants.py` | Role definitions (`LI_BASE_USER`, permissions) |
| security.py | `airflow/airflow/api_connexion/security.py` | OSS Airflow API authorization decorators |
| fab_auth_manager.py | `airflow/airflow/providers/fab/auth_manager/fab_auth_manager.py` | OSS FAB auth manager (base class) |
| permissions.py | `airflow/airflow/security/permissions.py` | Resource and action constants |

## Authentication Flow (Step by Step)

### 1. Token Extraction (`api_auth.py:34-55`)

Every REST API call goes through `requires_authentication()`. The system first tries to extract a principal from the DataVault identity token in the request header. If no DV token is present, it falls back to client certificate (grestin cert).

```python
principal = get_principal_from_datavault_token() or get_principal_from_cert()
```

The DV token is parsed by `DatavaultIdentityToken(token)` and the identity field is mapped to either a `UserPrincipal` or `ServicePrincipal` via `Principal.get_principal_from_token_identity_field()`.

### 2. Principal Type Dispatch (`api_auth.py:102-128`)

The system handles three cases:

| Principal Type | Condition | Handler |
|---------------|-----------|---------|
| **UserPrincipal (human)** | `isinstance(principal, UserPrincipal)` and NOT `svc-`/`svc_` prefix | `_refresh_airflow_user_principal()` — always calls LDAP |
| **UserPrincipal (headless svc)** | `isinstance(principal, UserPrincipal)` and `svc-`/`svc_` prefix | `_refresh_airflow_service_user_principal()` — skips LDAP by default |
| **ServicePrincipal** | `isinstance(principal, ServicePrincipal)` | `refresh_airflow_service_user()` — uses `_service_calculate_user_roles()` |

**Critical distinction**: The principal type depends on HOW the DV token was generated:
- **LDAP_CREDENTIAL grant** (username/password) → creates **UserPrincipal**
- **Certificate-based auth** (service identity cert) → creates **ServicePrincipal**

### 3. User Registration and Role Assignment

#### Path A: Human Users (`_refresh_airflow_user_principal`, manager.py:308-363)

1. Query `ab_user` table for existing user
2. Call LDAP to get user's group memberships (always)
3. Calculate roles from LDAP groups + registration role (`LI_BASE_USER`)
4. Create user in `ab_user` if new, update roles if existing

#### Path B: ServicePrincipal (`refresh_airflow_service_user`, manager.py:477-550)

1. Convert `app_name` to username: `get_service_name("app-name")` → `"service-app-name"`
2. Query `ab_user` for existing service user
3. Calculate roles via `_service_calculate_user_roles()`:
   - **Always includes `LI_BASE_USER`** (the default registration role)
   - Also includes any role matching the service username (e.g., role `"service-app-name"`)
4. Create service user in `ab_user` if new

#### Path C: Headless Service UserPrincipal — svc-* accounts (`_refresh_airflow_service_user_principal`, manager.py:365-455)

This is the **problematic path**. When a UserPrincipal has a `svc-` or `svc_` prefix:

1. LDAP calls are **skipped** by default (performance optimization, `api_auth.py:107-111`)
2. `user_attributes` is set to just the username string (not a set of groups)
3. For NEW users: `roles = self.find_role("svc-username")` → looks for a role matching the exact username
4. If no matching role exists → `roles = None` → `add_user(role=None)` → **user created with `[None]` roles → SQLAlchemy error → registration fails → returns None → 401**

**The asymmetry**: Path B (`ServicePrincipal`) always includes `LI_BASE_USER` via `_service_calculate_user_roles()`. Path C (`UserPrincipal` with svc-*) does NOT — it only looks for a role matching the svc-* username.

#### The `refresh_role_override` mechanism (manager.py:377-392, 492-506)

Both Path B and Path C have a safety mechanism: if the user exists but has ONLY the `LI_BASE_USER` role (exactly 1 role), it forces a role refresh. However:
- If the user was **never created** (registration failed in Path C), this doesn't help
- If the user was created with **zero roles** (not 1), this check doesn't trigger
- The check compares `len(user.roles) == 1`, not `len(user.roles) <= 1`

## The `LI_BASE_USER` Role (constants.py)

This is the default role all authenticated users get:

```python
LINKEDIN_AIRFLOW_ROLES = {
    "LI_BASE_USER": {
        "can_read": [
            "Audit Logs", "DAGs", "DAG Code", "DAG Runs", "ImportError",
            "Jobs", "My Profile", "Plugins", "SLA Misses",
            "Task Instances", "Task Logs", "XComs", "Website",
        ],
        "can_edit": ["Task Instances", "DAG Runs"],
        "menu_access": [
            "Browse", "DAG Runs", "Documentation", "Docs", "Jobs",
            "Audit Logs", "Plugins", "SLA Misses", "Task Instances",
        ],
        "can_delete": ["Task Instances", "DAG Runs"],
        "can_create": ["Task Instances", "DAG Runs"],
    }
}
```

**Important**: `LI_BASE_USER` has `can_read` on the global `"DAGs"` resource. This means a user with this role can call `GET /api/v1/dags` — but the results are **filtered** to only include DAGs the user has specific access to.

## Authorization Flow for REST APIs

### `GET /api/v1/dags` (DAG listing)

The endpoint is decorated with `@security.requires_access_dag("GET")` (dag_endpoint.py:95).

The authorization check in `_requires_access` (security.py:84-99):

1. **`check_authentication()`** — calls `requires_authentication(Response)()` on each configured auth backend. Returns 200 if user authenticated, 401 if not.
2. **`is_authorized_callback()`** — calls `is_authorized_dag(method="GET", details=DagDetails(id=None))`:
   - `_is_authorized_dag()` first checks **global** authorization: does user have `can_read` on `"DAGs"`?
   - If yes (true for `LI_BASE_USER`) → returns True → endpoint proceeds
   - The endpoint then calls `get_permitted_dag_ids(user=g.user)` to filter results
3. **DAG filtering** (dag_endpoint.py:123-125): Only returns DAGs where user has `can_read` on `"DAG:<dag_id>"`

**Result for a service account with only `LI_BASE_USER`**: The API call succeeds (200 OK) but returns an **empty list** because the user has no DAG-specific permissions.

### DAG-Specific APIs (trigger, get runs, etc.)

For APIs targeting a specific DAG (e.g., `POST /api/v1/dags/{dag_id}/dagRuns`):
1. Authentication check (same as above)
2. Authorization: `is_authorized_dag(method="PUT", details=DagDetails(id=dag_id))`
   - First checks global `can_edit` on `"DAGs"` → False for `LI_BASE_USER` (it only has `can_edit` on Task Instances and DAG Runs, not DAGs globally)
   - Then checks DAG-specific: `can_edit` on `"DAG:<dag_id>"` → only if the service's role has been granted access via `access_control`
3. If unauthorized → 403 PermissionDenied

## DAG-Level Access Control (`sync_perm_for_dag`, manager.py:224-252)

When a DAG defines `access_control`:

```python
with DAG(
    'my_dag',
    access_control={
        "service-my_service": {"can_read", "can_edit"},
        "svc-my_account": {"can_read", "can_edit"},
        "SGP-ENG-my_team": {"can_read"},
    }
)
```

The `sync_perm_for_dag()` method:

1. For each entry in `access_control`:
   - If it starts with `"service-"`: creates a role named `"service-my_service"` (if not exists)
   - If it starts with `"svc-"` or `"svc_"`: creates a role named `"svc-my_account"` (if not exists)
   - If it starts with `"SGP-ENG"` or `"SGP-CREW"`: verifies the LDAP group exists, then creates a role
2. Calls OSS Airflow's `super().sync_perm_for_dag()` to sync the permissions (e.g., `can_read` on `"DAG:my_dag"`) into the FAB tables

This means **the role for a service account is created by the DAG's `access_control` attribute, not during authentication**.

## Service Account Detection (utils.py)

```python
SERVICE_USERNAME_PREFIX = "service-"          # ServicePrincipal path
HEADLESS_SERVICE_USER_PREFIXES = ("svc-", "svc_")  # UserPrincipal svc path

def is_service(name):           # Checks "service-" prefix
def is_headless_service_user_principal(name):  # Checks "svc-"/"svc_" prefix
def get_service_name(name):     # Adds "service-" prefix if not present
```

## Why Service Account API Calls Fail (Root Cause Analysis)

### The Specific Issue: `svc-git-dali` getting 401 on `GET /api/v1/dags`

**Token generation**: Vishnu generated a DV token using `LDAP_CREDENTIAL` grant:
```bash
curli -X POST -d '{"grantType":"LDAP_CREDENTIAL","username":"svc-git-dali","password":"..."}' \
  .../datavaultIdentityTokens?action=generateToken
```

**What happens**:

1. `LDAP_CREDENTIAL` grant produces a **UserPrincipal** (not ServicePrincipal), because it authenticates via LDAP username/password.
2. `UserPrincipal(user_name="svc-git-dali")` → `is_headless_service_user_principal("svc-git-dali")` = **True**
3. LDAP calls are skipped (performance optimization)
4. `_refresh_airflow_service_user_principal()` is called with `refresh_svc_account=False`
5. User not found in `ab_user` → attempts registration
6. `roles = self.find_role("svc-git-dali")` → **None** (no DAG has `access_control={"svc-git-dali": ...}` yet, or if it does, the scheduler hasn't synced permissions yet)
7. `self.add_user(..., role=None)` → FAB sets `user.roles = [None]` → **SQLAlchemy error** trying to persist invalid role relationship
8. Exception caught → returns **None** → `auth_current_request()` returns None → **401 Unauthorized**

**Why it works for personal tokens**: A regular `UserPrincipal(user_name="stewang")` takes the human user path, which calls LDAP, gets group memberships, and always assigns `LI_BASE_USER` role.

### Why Non-DAG-Specific APIs Are Problematic for Service Accounts

Even if the 401 issue is resolved (e.g., by pre-creating the role), there's a second issue:

1. A service account with `LI_BASE_USER` can call `GET /api/v1/dags` (passes global `can_read` on `"DAGs"`)
2. But the results are **filtered**: only DAGs where the account has `can_read` on `"DAG:<dag_id>"`
3. If no DAG has `access_control` listing the service account → **empty response** (not an error, just no data)
4. For DAG-specific operations (trigger, status), the service account needs explicit `access_control` grants

**The fundamental design**: Airflow's authorization model is DAG-centric. There is no concept of "open ACL" for service accounts. Every service account must be explicitly granted access per-DAG via `access_control` in the DAG definition.

## Database Tables (FAB Schema)

The FAB authorization tables in the Airflow metastore:

| Table | Purpose |
|-------|---------|
| `ab_user` | User records (both humans and service accounts) |
| `ab_role` | Role definitions (e.g., `LI_BASE_USER`, `service-my_app`, `SGP-ENG-my_team`) |
| `ab_user_role` | Maps users to roles |
| `ab_permission` | Actions (e.g., `can_read`, `can_edit`, `menu_access`) |
| `ab_view_menu` | Resources (e.g., `DAGs`, `DAG:my_dag_id`, `Task Instances`) |
| `ab_permission_view` | Maps permissions to resources |
| `ab_permission_view_role` | Maps permission-resource pairs to roles |

### Diagnostic Queries

To check if a service account exists and its roles:

```sql
-- Check if user exists
SELECT id, username, first_name, active, login_count, last_login
FROM ab_user WHERE username = 'svc-git-dali' OR username = 'service-git-dali';

-- Check user's roles
SELECT u.username, r.name as role_name
FROM ab_user u
JOIN ab_user_role ur ON u.id = ur.user_id
JOIN ab_role r ON ur.role_id = r.id
WHERE u.username LIKE '%git-dali%';

-- Check if the role exists (created by DAG access_control)
SELECT * FROM ab_role WHERE name LIKE '%git-dali%';

-- Check what permissions a role has
SELECT r.name as role_name, p.name as permission, vm.name as resource
FROM ab_role r
JOIN ab_permission_view_role pvr ON r.id = pvr.role_id
JOIN ab_permission_view pv ON pvr.permission_view_id = pv.id
JOIN ab_permission p ON pv.permission_id = p.id
JOIN ab_view_menu vm ON pv.view_menu_id = vm.id
WHERE r.name = 'LI_BASE_USER';

-- Check DAG-specific permissions for a service account
SELECT r.name as role_name, p.name as permission, vm.name as resource
FROM ab_role r
JOIN ab_permission_view_role pvr ON r.id = pvr.role_id
JOIN ab_permission_view pv ON pvr.permission_view_id = pv.id
JOIN ab_permission p ON pv.permission_id = p.id
JOIN ab_view_menu vm ON pv.view_menu_id = vm.id
WHERE r.name LIKE '%git-dali%' AND vm.name LIKE 'DAG:%';
```

## How to Fix Service Account Access

### For Vishnu's specific case (svc-git-dali)

1. **Ensure a DAG grants access to the service account**:
   ```python
   with DAG(
       'target_dag_id',
       access_control={
           "svc-git-dali": {"can_read", "can_edit"},
       }
   )
   ```

2. **Wait for the Airflow scheduler to sync permissions** — the scheduler calls `sync_perm_for_dag()` which creates the `svc-git-dali` role and its DAG-specific permissions.

3. **Then authenticate** — the next API call will find the role in the DB and assign it.

4. **Alternative: Use certificate-based auth** to get a `ServicePrincipal` token instead of `LDAP_CREDENTIAL`. The `ServicePrincipal` path always includes `LI_BASE_USER` and is more robust.

### For the "listing all DAGs" use case

There is no "open ACL" mode. To list all DAGs via API, the service account must either:
- Be listed in `access_control` of every DAG it needs to see
- Or use a more privileged role (Admin-level, requires Airflow team approval)

### Timing Issue

A common pitfall: the service account authenticates **before** any DAG has been deployed with the correct `access_control`. The first authentication creates the user with no matching role → registration may fail. The role only gets created later when the scheduler processes the DAG's `access_control`.

**Workaround**: Deploy the DAG with `access_control` first, wait for scheduler to sync, then authenticate.

## REFRESHED_SERVICE_ACCOUNTS Mechanism (utils.py:40-47)

An escape hatch exists in the code: the `REFRESHED_SERVICE_ACCOUNTS` environment variable (comma-separated list of usernames). Service accounts in this list would have their roles refreshed from LDAP on every authentication.

**However, this env var is NOT set on any cluster.** It exists in the provider code but was never configured in `oklahoma-airflow-deployment` Helm charts or Dockerfiles. So it is not a viable workaround today.

## Manual DB Fix for Blocked Service Accounts

When a `svc-*` service account hits the 401 bug described above, the immediate fix is to manually insert the user into the Airflow metastore with `LI_BASE_USER` role.

**Example: Unblocking `svc-git-dali` on holdem (2026-03-31)**

```sql
-- Get the LI_BASE_USER role id
SELECT @role_id := id FROM ab_role WHERE name = 'LI_BASE_USER';

-- Create the user
INSERT INTO ab_user (first_name, last_name, username, email, active, login_count)
VALUES ('Service (svc-git-dali)', 'Account', 'svc-git-dali', 'dali-alerts@linkedin.com', 1, 0);

-- Link user to LI_BASE_USER role
INSERT INTO ab_user_role (user_id, role_id)
VALUES (LAST_INSERT_ID(), @role_id);

-- Verify
SELECT u.username, r.name AS role_name
FROM ab_user u
JOIN ab_user_role ur ON u.id = ur.user_id
JOIN ab_role r ON ur.role_id = r.id
WHERE u.username = 'svc-git-dali';
```

**Important notes:**
- Must be run on each cluster's DB separately (holdem, corp, war, etc.)
- After this, `GET /api/v1/dags` returns 200 but with empty results until DAGs grant `access_control`
- The user's DL email should be used for the email field (from go/lidm), not the fake email pattern
- On next authentication, the code's `refresh_role_override` logic may trigger (user has exactly 1 role = LI_BASE_USER), which forces a role recalculation. If a matching `svc-git-dali` role exists from a DAG's `access_control`, it will be added automatically.
