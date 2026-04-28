> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

# Codebase — Patterns

## DAG Authoring Patterns

### Multi-Tenant DAG Uploads
- **User-isolated DAGs**: DAGs uploaded by users are stored in user-specific subdirectories (`/dags/<username>/`) by default
- **Root-level DAGs**: Allowlisted users can bypass validation and write directly to the DAG root via `DAG_ROOT_UPLOAD_ALLOW_LIST` environment variable
- **Archive Support**: DAG uploads support both `.zip` and `.tar.gz` archives with automatic extraction and macOS metadata cleanup

### DAG Validation on Upload
DAGs go through multi-stage validation before acceptance:
1. **Import validation**: Using Airflow's `DagBag` to ensure no Python import errors
2. **Deduplication checks**: Prevents overwriting other users' DAGs unless uploading to user's own subdirectory
3. **Proxy user validation**: Ensures uploading user has permissions to impersonate any proxy users specified in Spark operators or Darwin tasks
4. **Grid User Manager integration**: Cross-references proxy users with the Grid User Manager API (fabric-aware: EI vs. prod endpoints)

### Testing Patterns
**Canonical Testing Flows:**
- **Grid User (venv)**: Upload DAG changes + workspace resources → Test run cycle
- **ML User (Docker image)**: DAG changes + Docker build (with workspace/app code) → Push → Test run
- **ML User (Fast Registration)**: DAG changes + tar/zip workspace to blob store → Test run
- **Automation (KPO)**: Similar to ML user with Docker images
- **Provider Testing**: Modify `lipy-airflow-providers` → Build Oklahoma image → Deploy to dev cluster → Test

**Test Infrastructure:**
- **RDev instances**: Personal ephemeral Airflow clusters for testing (preferred for workspace testing)
- **Shared test clusters**: Suitable for DAG.py-only changes (e.g., `go/test-airflow`)
- **Local setup**: Docker-compose or Airflow Breeze for local development

---

## Plugin Architecture

### Upload Plugin System
The Oklahoma-specific upload plugin (`upload_plugin`) provides DAG management through the Airflow webserver UI and API.

**Components:**
- **`upload_plugin.py`**: Main Flask blueprint with both UI (`/upload`) and API (`/api/v1/plugins/upload/upload_dag`) endpoints
- **`dag_upload_handler.py`**: Orchestrates DAG processing (extract, validate, save)
- **`dag_validations.py`**: Validation logic including proxy user checks and deduplication

**Key Features:**
- Web UI under "Test > Upload DAG" menu (configurable via `AIRFLOW_UPLOAD_PLUGIN_ENABLED`)
- REST API for programmatic DAG uploads
- Permission-based access control (default role: `LI_BASE_USER`)
- Temporary staging directory for processing uploads (`/tmp/upload_plugin`)

**Plugin Loading Pattern:**
```python
# Registered via AirflowPlugin.on_load()
class AirflowUploadPlugin(AirflowPlugin):
    name = "upload_plugin"
    
    @classmethod
    def on_load(cls, *args, **kwargs):
        UploadAppBuilderBaseView.init()  # Initialize permissions
        os.makedirs(TEMP_UPLOAD_LOCATION, exist_ok=True)
    
    flask_blueprints = [upload_api_bp]  # Always registered
    appbuilder_views = [upload_package]  # Only if UPLOAD_PLUGIN_ENABLED=true
```

### External Providers
Custom operators, sensors, and hooks come from **`lipy-airflow-providers`** multiproduct with multiple subprojects:
- **`apache-airflow-providers-lnkd`**: Primary provider with LinkedIn-internal operators
- **`oklahoma-helpers`**: Utilities (KafkaHelper, Config loader, venv path resolution)
- **`oklahoma-listener`**: Event listener components
- **`in-dbt`**: DBT macros (managed by DBT team)
- **`oklahoma-backfill`**: (Deprecated) Backfill plugin now consumed from `li-airflow-backfill-plugin`

---

## Config Management

### Environment-Driven Configuration
Configuration is fabric-aware and environment-specific:

**Fabric Detection:**
- Environment variable: `FABRIC` (e.g., "ei1", "prod")
- Used to determine Grid User Manager endpoint (EI vs. PROD URLs)
- Also determines SSO proxy behavior

**Configuration Layers:**

1. **Flask/Airflow Config** (`webserver_app.py`):
   - Session lifetime: 8 hours (configurable)
   - Cookie security: HTTPS-only, HttpOnly, Lax SameSite
   - CSRF protection: Disabled by default (TODO: enable)
   - MAX_CONTENT_LENGTH: 20MB (supports large DAG uploads)
   - SSO proxy via Kraken VIP (`OKLAHOMA_AIRFLOW_WEBSERVER_SSO_PROXY_URL`)

2. **Plugin Configurations** (via `linkedin.config.base.framework`):
   - SSO (Azure AD: tenant=lnkdprod.com)
   - Session Manager (DB connection, cookie settings, CSRF)
   - Datavault integration (ACL service, identity tokens)
   - Cache settings (Redis, Memcached, or null)

3. **Startup Configs** (`env.cfg`, `env-dev.cfg`):
   - LinkedIn cfg2 metadata (app name, fabric, instance, container port)
   - Product tag and payload information

4. **Environment Variables**:
   - `UPLOAD_PLUGIN_ENABLED`: Enable/disable upload UI plugin
   - `DAG_ROOT_UPLOAD_ALLOW_LIST`: Comma-separated list of users allowed to write to DAG root
   - `DATAVAULT_TOKEN_FABRIC`: Fabric for Datavault token generation
   - `ENABLE_SPIFFE_CERT`: Use SPIFFE certificates instead of URN certificates
   - `FABRIC`: Current cluster fabric (ei1, prod-ltx1, etc.)

### JSONC Config Support
DAGs can load JSONC config files via the Config utility from `oklahoma-helpers`. Config files are loaded per-fabric, enabling DAG portability across environments.

---

## Shared Utilities

### From `oklahoma-helpers`:
- **`Config`**: Load fabric-specific JSONC configurations in DAGs
- **`KafkaHelper`**: Avro client wrapper for Kafka interactions
- **`get_cert_path()`**: Retrieve Grestin certificate paths for current Oklahoma identity
- **`get_venv()`**: Resolve Python binary path from DAG's virtual environment

### LinkedIn Internal Libraries:
- **`linkedin.config.base`**: Framework-level configuration and plugin registration
- **`linkedin.web`**: Flask extensions (caching, session management, authentication)
- **`linkedin.websso.cfg2`**: CFG2-based SSO configuration
- **`linkedin.airflow.security`**: LDAP-based authentication and role-based access control
- **`linkedin.metrics`**: AMF (Application Metrics Framework) integration for monitoring
- **`linkedin.pki`**: PKI, Datavault, and SPIFFE certificate handling

### Internal Operators (from `lipy-airflow-providers`):
- **SparkOperator**: Distributed computing with proxy user support
- **DarwinOperator**: Job submission with submit_user parameter
- **Custom Sensors/Hooks**: Product-specific integrations

---

## Testing Patterns

### DAG Validation Framework
Comprehensive multi-stage validation on upload:

```python
# dag_validations.py pattern
def check_proxy_users(dag_bag, username):
    """Gather all proxy users from DAG and validate permissions"""
    proxy_users = set()
    for dag in dag_bag.dags.values():
        proxy_users.update(_collect_proxy_users(dag))
    return _validate_proxy_users(username, proxy_users)

def _collect_proxy_users(dag):
    """Extract proxy users from Spark and Darwin operators"""
    proxy_users = set()
    for task in dag.tasks:
        if hasattr(task, "spark_params") and "proxy_user" in task.spark_params:
            proxy_users.add(task.spark_params["proxy_user"])
        if hasattr(task, "darwin_params") and hasattr(task, "submit_user"):
            proxy_users.add(task.submit_user)
    return proxy_users
```

### Startup/Scheduler Testing
- **`test_webserver_app_configs.py`**: Validates plugin configuration loading
- **`start_scheduler.py`** / **`start_webserver.py`**: Production startup scripts with role initialization

### Test Infrastructure
- **RDev**: Recommended for full workspace testing (includes scheduler + webserver)
- **Local Airflow**: For quick DAG syntax/import validation
- **Shared Test Clusters**: Minimal dependency testing

---

## Linting / Validation

### DAG Upload Validation
- **Import validation**: Python syntax/import errors caught by `DagBag`
- **Deduplication**: Prevents DAG ID collisions across users
- **Proxy user validation**: Grid User Manager API integration with Datavault tokens
- **Archive validation**: Supports ZIP and TAR.GZ with error handling

### Security Validation
- **SPIFFE/URN certificates**: Dynamic certificate resolution for API calls
- **Datavault tokens**: Identity tokens for Grid User Manager API calls
- **LDAP sync**: User roles synced from LDAP on login (configurable)

### Plugin Testing
- Built-in `install_plugin.sh` for quick plugin deployment to K8s pods
- Supports hot-reload via `AIRFLOW__WEBSERVER__RELOAD_ON_PLUGIN_CHANGE=True`

---

## Security & Authentication

### Single Sign-On (SSO)
- **Provider**: Azure AD (AAD)
- **Tenant**: `lnkdprod.com`
- **Proxy**: Kraken VIP for OIDC endpoint access
- **Session**: 8-hour lifetime with remember-me cookie support (2 days)

### Authorization
- **LDAP-based RBAC**: Role-based access control via LDAP groups
- **Plugin permissions**: Per-role access to Upload DAG feature
- **Proxy user ACLs**: Grid User Manager validates impersonation permissions

### Certificate Handling
- **Primary**: URN-based certificates (`DATAVAULT_CLIENT_CERTIFICATE_FILEPATH`, `DATAVAULT_CLIENT_KEY_FILEPATH`)
- **Secondary**: SPIFFE certificates via `linkedin.datavault.utils.get_app_spiffe_certs()`
- **Trust store**: `/etc/lipki/ca-bundle.crt` for CA validation

---

## Git Patterns

### Release Strategy
**Recurring patterns in commits:**
- **Version bumps**: Frequent provider and Airflow core version upgrades (daily/weekly cadence)
- **Dependency management**: Automated dependency upgrades via ADU (Automated Dependency Upgrade)
- **Image builds**: Docker image version pins and base image updates
- **Emergency fixes**: Quick hot-fixes for deployment issues (e.g., TLS workarounds via LD_PRELOAD)
- **Infrastructure updates**: Gradle version updates, workflow automation improvements

**Recent changes (last 80 commits):**
- Bump to Airflow 2.9.2.151 (core version tracking)
- Provider version 10.0.41 (frequent updates)
- Dependency upgrade automation
- E2E test infrastructure improvements
- Docker base image management
- RDev image testing workflows

---

## See Also
- [DAG Authoring](dag-authoring.md)
- [Codebase Overview](codebase/README.md)
- [Gotchas](codebase/gotchas.md)
- [lipy-airflow-providers documentation](../code/lipy-airflow-providers.md)
- [Airflow Testing Experience](../testing/Airflow-testing-experience.md)
