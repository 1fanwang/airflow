> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

# Airflow — Security

## DataVault Token → FAB Authentication (API Access)

This section covers how LinkedIn's security layer integrates with Airflow's Flask-AppBuilder (FAB) auth system. Critical for debugging service account 401s and DAG-level permission errors.

### Architecture Summary

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

### Key Source Files

| File | Location | Purpose |
|------|----------|---------|
| `api_auth.py` | `lipy-airflow-providers/.../linkedin/airflow/security/api_auth.py` | REST API authentication backend |
| `manager.py` | `lipy-airflow-providers/.../linkedin/airflow/security/manager.py` | `LinkedInAirflowSecurityManager` |
| `utils.py` | `lipy-airflow-providers/.../linkedin/airflow/security/utils.py` | Service account detection helpers |
| `constants.py` | `lipy-airflow-providers/.../linkedin/airflow/security/constants.py` | Role definitions (`LI_BASE_USER`, permissions) |

### Principal Type Dispatch

Every API call goes through `requires_authentication()`. It extracts a DV token or falls back to grestin cert, then dispatches based on principal type:

| Principal Type | Condition | Handler |
|---------------|-----------|---------|
| **UserPrincipal (human)** | Username does NOT have `svc-`/`svc_` prefix | `_refresh_airflow_user_principal()` — always calls LDAP |
| **UserPrincipal (headless svc)** | Username has `svc-`/`svc_` prefix | `_refresh_airflow_service_user_principal()` — skips LDAP |
| **ServicePrincipal** | Token was cert-based (not LDAP_CREDENTIAL) | `refresh_airflow_service_user()` — uses `_service_calculate_user_roles()` |

**Critical distinction**: HOW the DV token was generated determines the principal type:
- `LDAP_CREDENTIAL` grant (username + password) → **UserPrincipal**
- Certificate-based auth (service identity cert) → **ServicePrincipal**

### The `svc-*` 401 Bug

When a `svc-*` account authenticates via `LDAP_CREDENTIAL` grant:

1. Token produces `UserPrincipal(user_name="svc-git-dali")` — NOT a ServicePrincipal
2. `is_headless_service_user_principal("svc-git-dali")` = True → LDAP calls skipped
3. User not found in `ab_user` → attempts registration
4. `roles = self.find_role("svc-git-dali")` → **None** (no DAG has `access_control={"svc-git-dali": ...}` yet, or scheduler hasn't synced)
5. `self.add_user(..., role=None)` → FAB sets `user.roles = [None]` → SQLAlchemy error
6. Exception caught → returns **None** → **401 Unauthorized**

**Why personal tokens work**: A regular `UserPrincipal(user_name="stewang")` always calls LDAP, gets group memberships, and assigns `LI_BASE_USER`.

**Why `service-*` accounts work**: `ServicePrincipal` path always includes `LI_BASE_USER` via `_service_calculate_user_roles()`. The `svc-*` UserPrincipal path does NOT.

### DAG-Level Access Control

DAG `access_control` is the source of truth for service account permissions. The scheduler calls `sync_perm_for_dag()` which creates FAB roles:

```python
with DAG(
    'my_dag',
    access_control={
        "service-my_service": {"can_read", "can_edit"},   # ServicePrincipal path
        "svc-my_account":     {"can_read", "can_edit"},   # UserPrincipal svc path
        "SGP-ENG-my_team":    {"can_read"},               # LDAP group
    }
)
```

**Important**: Airflow's auth model is DAG-centric. There is no "open ACL" for service accounts — every service account needs explicit `access_control` per-DAG. A service account with only `LI_BASE_USER` can call `GET /api/v1/dags` (returns 200) but gets an empty list because it has no DAG-specific permissions.

**Timing pitfall**: If the service account authenticates before any DAG with the correct `access_control` has been deployed, registration fails. Deploy the DAG first, wait for scheduler to sync, then authenticate.

### Fixing a Blocked `svc-*` Account (Manual DB Fix)

When a `svc-*` account hits the 401 bug, the immediate fix is to manually insert the user into the metastore. Run on each affected cluster's DB separately (holdem, corp, war, etc.):

```sql
-- Get the LI_BASE_USER role id
SELECT @role_id := id FROM ab_role WHERE name = 'LI_BASE_USER';

-- Create the user (use the DL email from go/lidm, not a fake pattern)
INSERT INTO ab_user (first_name, last_name, username, email, active, login_count)
VALUES ('Service (svc-git-dali)', 'Account', 'svc-git-dali', 'dali-alerts@linkedin.com', 1, 0);

-- Link user to LI_BASE_USER role
INSERT INTO ab_user_role (user_id, role_id)
VALUES (LAST_INSERT_ID(), @role_id);
```

After this fix: `GET /api/v1/dags` returns 200 with empty results until DAGs grant `access_control`. On next auth, `refresh_role_override` detects the single-role user and will add the DAG-specific role automatically if `sync_perm_for_dag()` has run.

### Diagnostic SQL Queries

```sql
-- Check if user exists and its roles
SELECT u.username, r.name as role_name
FROM ab_user u
JOIN ab_user_role ur ON u.id = ur.user_id
JOIN ab_role r ON ur.role_id = r.id
WHERE u.username LIKE '%git-dali%';

-- Check what permissions LI_BASE_USER has
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

### FAB Schema Tables

| Table | Purpose |
|-------|---------|
| `ab_user` | User records (humans and service accounts) |
| `ab_role` | Role definitions (`LI_BASE_USER`, `service-my_app`, `SGP-ENG-my_team`) |
| `ab_user_role` | Maps users to roles |
| `ab_permission` | Actions (`can_read`, `can_edit`, `menu_access`) |
| `ab_view_menu` | Resources (`DAGs`, `DAG:my_dag_id`, `Task Instances`) |
| `ab_permission_view` | Maps permissions to resources |
| `ab_permission_view_role` | Maps permission-resource pairs to roles |

### API Client — `OklahomaConfig` Auth Modes

`linkedin.oklahomaclientlibrary.oklahoma_client.OklahomaConfig` (in the `airflow-client` MP) is the standard Python client for prod Airflow API access. It supports three auth modes — **only the first two work on prod clusters**:

| Mode | How to enable | Where it works |
|------|---------------|----------------|
| **Cert-based DV mint (recommended)** | Pass `certificate_filepath` + `key_filepath`, leave `username`/`password` unset | All prod clusters (Holdem/War/Faro/Corp) — produces a `ServicePrincipal` token, auto-grants `LI_BASE_USER` |
| **Pre-minted DV token** | Pass `api_key={'DataVaultAuth': '<token>'}`, leave `username`/`password` unset | All prod clusters — caller is responsible for minting and refreshing |
| **Username/password (rdev only)** | Pass `username` + `password` | rdev only (DB-fallback auth). **Silently 401s on prod** — see below. |

**The username/password trap**: at `oklahoma_client.py:51`, `useDVAuth` is True only when *both* `username` and `password` are `None`:

```python
self.useDVAuth: bool = self.username is None and self.password is None
```

When `useDVAuth` is False, the client never mints a DV token and never sets the `datavaultIdentityToken` header. The OpenAPI base class falls back to HTTP Basic auth (`Authorization: Basic <base64>`), which `linkedin.airflow.security.api_auth` does not read. Result: every prod request 401s, regardless of how correct the rest of the configuration is. This is the `OklahomaConfig` half of the same trap as the headless-`svc-*` UserPrincipal path documented above.

**Working prod example (cert-based)**:

```python
from linkedin.oklahomaclientlibrary.oklahoma_client import OklahomaConfig, OklahomaClient

config = OklahomaConfig(
    host="https://holdem.oklahoma-airflow.grid.linkedin.com",
    certificate_filepath="/path/to/svc-account.cert",  # service identity cert (Grestin-provisioned)
    key_filepath="/path/to/svc-account.key",
)
# client.update_params_for_auth() will mint and inject the DV token on each request
```

If your service identity cert isn't already provisioned at your runtime, file a Grestin request — paths typically land under `/var/cluster/oklahoma/identity.{cert,key}` for Oklahoma-deployed services, or wherever your service ops mounts them. Default lookup if `certificate_filepath`/`key_filepath` are unset is `~/identity.cert` / `~/identity.key` (`utils.py:38-40`).

**Reference**: APA-145542 (2026-05-07) — service account stuck on 401 because its MP was passing `username`/`password` to `OklahomaConfig` against Holdem.

---

## Authentication

LinkedIn Airflow uses **SSO (Azure AD) as the primary authentication method**, with optional LDAP fallback.

### SSO (Azure AD)
- **Provider**: Azure AD (AAD)
- **Tenant**: `lnkdprod.com`
- **Client ID**: `82c36929-64e6-4d1d-b609-3c6e00abd720`
- **Proxy**: HTTP proxy configured for `login.microsoftonline.com` access (default: `ltx1-kraken-vip-1.prod.linkedin.com:10339`)
- **Session Lifetime**: 8 hours (configurable)
- **Session Cookie**: Secure, HttpOnly, SameSite=Lax

### LDAP Fallback
- **Backend**: LinkedIn VDS (LDAP)
- **Group Field**: `memberof`
- **2FA Support**: Symantec VIP token (integrated into password field)
- **User Registration**: Automatic on first login
- **Default Role**: `LI_BASE_USER`
- **Auth View**: `CustomAuthViewLDAP` (not recommended for production)

### Implementation Details
- Authentication handled by `linkedin.web.authn` and `linkedin.web.session_manager`
- Custom views: `CustomAuthViewSSO` and `CustomAuthViewLDAP`
- CSRF protection: Disabled by default (`CSRF_PROTECTION_ENABLED: False`) — TODO: enable in production
- Auto-sync LDAP groups to Airflow roles on every login

---

## RBAC Roles

Airflow uses **Kubernetes RBAC** for service account permissions and **Flask-AppBuilder (FAB)** roles for user permissions.

### Kubernetes RBAC Roles

#### Pod Launcher Role
Allows Airflow scheduler/workers to launch task pods:
```yaml
- create, list, get, patch, watch, delete pods
- get pods/log (read logs)
- create, get pods/exec (task execution)
- list events
```
Enabled when: `rbac.create=true` AND `allowPodLaunching=true`

#### Pod Cleanup Role
Cleanup job permissions:
```yaml
- list, delete pods
```

#### Pod Log Reader Role
Allows webserver/triggerer to read pod logs:
```yaml
- list, get, watch pods
- get, list pods/log
```

### Airflow User Roles (FAB-based)

#### Granting Admin Role via kubectl

To grant a user the Admin role (e.g. for a new cluster or escalation):

```bash
kubectl exec -it airflow-holdem-scheduler-<pod> -n airflow -c scheduler -- bash

export AIRFLOW_HOME=/opt/airflow
airflow users add-role -u <username> -r Admin
```

Replace `holdem` with the target cluster name. Run from any scheduler pod.

#### LI_BASE_USER (Default)
Auto-assigned to new users.

**Can Read:**
- Audit Logs, DAG Code, DAG Runs, ImportError, Jobs, My Profile, Plugins, SLA Misses, Task Instances, Task Logs, XComs, Website

**Can Edit/Create/Delete:**
- Task Instances, DAG Runs

**Menu Access:**
- Browse, DAG Runs, Documentation, Docs, Jobs, Audit Logs, Plugins, SLA Misses, Task Instances

---

## DAG-Level ACLs

### DAG Identity
- **DAG Principal**: `oklahoma-dag-<DAG_ID>`
- **MP Principal**: `oklahoma-mp-<MP_NAME>`
- **Certificate Type**: Grestin certificates
- **ACL Tool**: `acl-tool` CLI

### DAG Naming Convention
Format: `<DAG_NAME>__<MP_NAME>` (double underscore)
Example: `my_pipeline__data_processing`

### Proxy User ACL Validation
- **Default Timeout**: 30 seconds (validation), 10 seconds (DataVault)
- **EI Cluster Exception**: Disabled (timeout = 0) because EI cannot reach corp DataVault service
- **Configuration**: Stored in `config.oklahoma.proxy_user_acl_*` in Helm values

---

## Service Accounts

### Kubernetes Service Accounts

Each Airflow component has a dedicated K8s service account:
- `webserver`
- `scheduler`
- `worker`
- `triggerer`
- `dag-processor`
- `flower`
- `cleanup` (for pod cleanup jobs)
- `statsd`
- `redis`

### Service Account Configuration
- `automountServiceAccountToken`: Controlled per component
- Service accounts bound to component-specific roles via RoleBindings
- `runAsUser: 50000` (Airflow user UID)

### Example: Worker Service Account
Workers need pod launcher permissions to execute tasks:
```yaml
apiVersion: v1
kind: ServiceAccount
automountServiceAccountToken: true
metadata:
  name: airflow-worker
  namespace: airflow
```

Bound via RoleBinding:
```yaml
kind: RoleBinding
roleRef:
  kind: Role
  name: airflow-pod-launcher-role
subjects:
- kind: ServiceAccount
  name: airflow-worker
```

---

## Secrets Management

### Kubernetes Secrets

All sensitive data is stored as K8s Secrets and injected at pod runtime:

#### Fernet Key
- **Secret Name**: `<release>-fernet-key`
- **Key**: `fernet-key` (base64-encoded)
- **Purpose**: Encrypts DAG configurations and connections in metadata DB
- **Generated**: Automatically if not provided (pre-install Helm hook)

#### Webserver Secret Key
- **Secret Name**: `<release>-webserver-secret`
- **Purpose**: Flask session encryption
- **Source**: Can be provided via `webserverSecretKey` or `webserverSecretKeySecretName`

#### Database Connection
- **Secret Names**: `<release>-metadata-connection`, `<release>-result-backend-connection`
- **Connection Format**: SQLAlchemy connection string
- **Injected As**: Environment variables (`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`)

#### Registry Credentials
- **Secret Name**: `<release>-registry-secret`
- **Purpose**: Container image registry authentication
- **Type**: `kubernetes.io/dockercfg`

#### SSO Client Credential
- **Environment Variable**: `OKLAHOMA_AIRFLOW_WEBSERVER_SSO_CLIENT_CREDENTIAL`
- **URN**: Stored in SSO config
- **Managed By**: cfg2 framework

#### Kerberos Keytab (Optional)
- **Secret Name**: `<release>-kerberos-keytab`
- **Key**: `kerberos.keytab` (base64-encoded)
- **Optional**: Enable via `kerberos.enabled=true`

### Secret Injection Patterns

#### Environment Variables
```yaml
env:
- name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
  valueFrom:
    secretKeyRef:
      name: airflow-metadata-connection
      key: connection
```

#### Volume Mounts
```yaml
volumeMounts:
- name: kerberos-keytab
  mountPath: /etc/airflow.keytab
  readOnly: true

volumes:
- name: kerberos-keytab
  secret:
    secretName: airflow-kerberos-keytab
    items:
    - key: kerberos.keytab
      path: keytab
```

### Extra Secrets
- **Template**: `extra-secrets.yaml`
- **Format**: Helm values → K8s Secrets
- **Example**: Database credentials, API keys, service account credentials

---

## Network Security

### Kubernetes Network Policies

All Airflow components have network policies to restrict pod-to-pod communication.

#### Webserver Network Policy
- **Ingress Allowed From**: Defined in `webserver.networkPolicy.ingress.from`
- **Ports**: HTTP (8080), HTTPS (443) as configured
- **Purpose**: Control external access to Airflow UI

#### Worker Network Policy
- **Ingress Required From**: Scheduler (for task execution)
- **Purpose**: Prevent unauthorized task execution

#### Scheduler Network Policy
- **Egress Allowed To**: Database, Redis, workers
- **Purpose**: Ensure scheduler can only reach necessary services

#### Enablement
- **Flag**: `networkPolicies.enabled`
- **Scope**: All components (workers, scheduler, webserver, etc.)

### TLS/SSL Configuration
- **Session Cookie Secure**: `True`
- **ProxySQL Migration**: All clusters use ProxySQL for encrypted DB connections
- **CA Bundle**: `/etc/riddler/gaap-bundle.crt` (internal LinkedIn CA)

---

## Common Permission Errors

### "Proxy User ACL validation failed"
**Cause**: User doesn't have ACL permissions to submit DAGs with a specific proxy user.
**Resolution**:
- Check `proxy_user_acl_validation_timeout_sec` (if 0, validation is skipped)
- Ensure DAG principal has DataVault ACLs set via `acl-tool`
- On EI cluster: ACLs are in corp DataVault (unreachable); timeout is intentionally set to 0

### "Access Denied to DAG"
**Cause**: User's role doesn't have permission to view/edit DAG.
**Resolution**:
- Check user's Airflow role (default: `LI_BASE_USER`)
- Verify user is in correct LDAP groups
- Admin can assign additional roles via Airflow UI (Admin → Users)

### "Impersonation Not Allowed"
**Cause**: User cannot run tasks as another user (proxy user).
**Resolution**:
- Verify `RBAC_IMPERSONATE_USER` config is enabled
- Check user has `can_impersonate` permission
- Validate proxy user exists in metastore

### "Session Expired"
**Cause**: Default 8-hour session lifetime exceeded.
**Resolution**:
- Re-authenticate via SSO or LDAP
- Increase `PERMANENT_SESSION_LIFETIME` if needed (currently 8 hours = 28800 seconds)

---

## Security Best Practices

### Authentication
- Use SSO (Azure AD) in production — LDAP is fallback only
- Enable CSRF protection in production (`CSRF_PROTECTION_ENABLED: true`)
- Use static webserver secret key (not dynamic)

### Secrets
- Never hardcode credentials in DAGs or configs
- Use K8s Secrets + environment variable injection
- Rotate Fernet key periodically
- Use Kerberos keytab only in krb5-enabled environments

### RBAC
- Assign minimal necessary roles to users
- Regularly audit user permissions
- Use LDAP group membership for role assignment
- Enable `AUTH_ROLES_SYNC_AT_LOGIN: true` (already enabled)

### Network
- Enable network policies on all clusters
- Restrict webserver ingress to authorized IPs/services
- Use TLS for all external connections
- Validate ProxySQL certificates

### Monitoring
- Enable action logging via `@action_logging` decorator
- Audit all user/role changes
- Monitor proxy user ACL validation timeouts
- Log all authentication failures

---

## See Also
- [Architecture](architecture.md) — Cluster topology and component layout
- [DAG Authoring](dag-authoring.md) — DAG naming, operators, and deployment
- Documentation: DAG naming conventions, Grestin certs, acl-tool CLI
- Commit: `1e8afe4` — Proxy user ACL validation configuration
