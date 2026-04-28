> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# System — GGW (Grid Gateway Worker)

> LinkedIn's unified job execution layer that Airflow tasks delegate to via GGW operator/hook. Provides abstraction over diverse compute targets (Spark, Hadoop, Flink, Flyte, Darwin notebooks, etc.) with gRPC-based communication and DataVault token authentication.

## What It Is

**Grid Gateway (GGW)** is LinkedIn's internal job orchestration platform that Airflow uses to submit and monitor jobs across multiple compute engines. Rather than Airflow tasks directly executing code, GGW operators delegate execution to Grid Gateway's control plane, which manages:

- Job submission and lifecycle (start, poll, stop)
- Multi-tenant execution contexts with Azkaban-style proxy users
- Job-type plugins (Spark, Hadoop, Flink, Flyte, Darwin, SQL/Trino, etc.)
- Resource allocation and queue management
- Output/artifact capture
- Logs and metrics collection

From an Airflow perspective, GGW is a **remote execution engine**. A DAG author writes a `SparkBatchOperator` (or other GGW operator), which talks to the Grid Gateway control plane via gRPC, polls for job status, and captures outputs via XCom.

**Locations in repo:**
- Operators: `/apache-airflow-providers-lnkd/src/airflow/providers/lnkd/gridgateway/operators/`
- Hooks: `/apache-airflow-providers-lnkd/src/airflow/providers/lnkd/gridgateway/hooks/`

---

## How Airflow Uses It

### Operator Hierarchy

All GGW operators inherit from `GridGatewayBaseOperator`:

```
GridGatewayBaseOperator (base class)
├── SparkBatchOperator          (Spark jobs)
├── HadoopJavaOperator          (Hadoop MapReduce)
├── HadoopShellOperator         (Hadoop shell scripts)
├── JavaOperator                (Raw Java execution)
├── CommandOperator             (Shell commands)
├── FlyteOperator               (Flyte jobs)
├── DarwinOperator              (Jupyter notebooks)
├── DataQualityJobOperator      (DQ assertions)
├── PinotPushOperator           (Pinot data loads)
├── VenicePushOperator          (Venice data loads)
├── WormholePushOperator        (Wormhole data fabric)
├── KafkaPushOperator           (Kafka topic writes)
├── AmbryPushOperator           (Ambry blob store)
├── SQLOperator                 (Trino SQL)
├── FlinkBatchOperator          (Flink streaming)
├── CarbonOperator              (Carbon analytics)
└── GridGatewayOperator         (Generic, any function namespace/name)
```

### Execution Flow

1. DAG author instantiates an operator (e.g., `SparkBatchOperator(proxy_user="alice", ...)`)
2. `execute()` method calls `_execute()` in base class, which:
   - Builds gRPC `StartExecutionRequest` with function namespace, name, version, params, tags
   - Creates a `GridGatewayHook` to handle gRPC communication
   - Calls `hook.start_execution()` → returns **execution URN** (unique identifier)
   - Polls `hook.get_execution(urn)` until terminal state (SUCCEEDED/FAILED/STOPPED)
   - Captures outputs to XCom if `do_xcom_push=True`
   - Raises `GridGatewayExecutionError` if job fails
3. Task succeeds or fails based on Grid Gateway job result

**Key advantage:** Airflow doesn't block on compute—it just polls remotely.

---

## Trustbridge Integration

### What Is Trustbridge?

Trustbridge is LinkedIn's **RDev gateway** for accessing production services from development environments. In RDev (remote dev), direct mTLS access to production services is blocked, so Trustbridge provides a proxy tunnel.

### When Trustbridge Is Used

Trustbridge is activated when **all three conditions** are met:

```python
def _use_trustbridge(self) -> bool:
    return (
        is_rdev_env(self.okl_env)                                      # Running in RDev
        and os.getenv("USE_TB_GRID_GATEWAY", "false").lower() == "true"  # Explicitly enabled
        and os.getenv("DATAVAULT_TOKEN_FABRIC", "None").lower() != "ei-ltx1"  # Not LTX1 fabric
    )
```

### Trustbridge Channel Setup

**Normal prod:**
- Direct secure channel to Grid Gateway: `<host>:<port>` with mTLS certificates
- Credentials: cert chain from `/var/cluster/oklahoma/identity.cert/key`
- Authority header: `"mufn-control-service"`

**Via Trustbridge (RDev):**
- Secure channel to: `grpc.prod.linkedin.com:443`
- Credentials: standard SSL (no client certs)
- Authority header: NOT sent (Trustbridge handles it)
- Special headers added:
  - `x-li-grpc-fabric: prod-ltx1` (recent change from `grid1`)
  - `x-li-grpc-app-tag: prod-1`
  - `x-li-grpc-app: mufn-control-service`

### DataVault Token in Trustbridge Mode

**Normal:** Token fetched from DataVault service via `TokenClient.generate_token_from_certificate()`

**Trustbridge:** Token read from file defined by `DATAVAULT_TOKEN_PATH` environment variable
- Rationale: RDev can't reach DataVault service, so token is pre-injected into pod
- If not set, raises `AirflowException("DATAVAULT_TOKEN_PATH environment variable must be set when using Trustbridge.")`

---

## GGW Hook

**Location:** `/apache-airflow-providers-lnkd/src/airflow/providers/lnkd/gridgateway/hooks/grid_gateway_hooks.py`

### Endpoints & Communication

| Operation | Method | gRPC Call | Auth |
|-----------|--------|-----------|------|
| Submit job | `start_execution()` | `FunctionExecutionStub.StartExecution` | DataVault token |
| Poll status | `get_execution()` | `FunctionExecutionStub.GetExecution` | DataVault token |
| Cancel job | `stop_execution()` | `FunctionExecutionStub.StopExecution` | DataVault token |

All calls include metadata header `("dvtoken", <token>)` or `("ststoken", <token>)` if STS enabled.

### Channel Creation

```python
@cached_property
def get_secure_channel(self) -> grpc.Channel:
    if self._use_trustbridge():
        # RDev path: connect via TB proxy
        channel = grpc.secure_channel(
            "grpc.prod.linkedin.com:443",
            grpc.ssl_channel_credentials(),  # standard SSL
            options=[("grpc.connect_timeout_ms", 30000)],
        )
    else:
        # Prod path: direct mTLS connection
        channel = grpc.secure_channel(
            target=f"{conn.host}:{conn.port}",
            credentials=self.get_channel_cerds(use_spiffe=self._use_spiffe),
            options=[("grpc.default_authority", authority), ("grpc.connect_timeout_ms", 30000)],
        )
    # Add interceptors (mock, persist context) if present
    if self._grpc_interceptors:
        channel = grpc.intercept_channel(channel, *self._grpc_interceptors)
    return channel
```

### Key Parameters (Constructor)

```python
GridGatewayHook(
    grid_gateway_service_conn_id="grid_gateway_service_default",  # Airflow connection
    oklahoma_identity=OklahomaIdentity.MP,                         # Identity for token
    enable_sts_token=False,                                        # Use STS tokens
    grpc_interceptors=None,                                        # Mocking/persistence
    use_spiffe=False,                                              # SPIFFE certs
)
```

### Connection Configuration

Airflow connection `grid_gateway_service_default`:
- **Type:** `grid_gateway_service`
- **Host:** Grid Gateway server hostname
- **Port:** Grid Gateway server port
- **Extras JSON:** Can set `"authority"` to override default authority (for Nimbus/other control plane)

### Mu Functions Service Connection (`mufn_service_default`)

The **`mufn_service_default`** connection (type: `Mu Functions Service`) is the standard GGW connection for Airflow → Grid Gateway communication.

| Environment | Host:Port |
|-------------|-----------|
| Production | `k8s-0.apiserver.grid1.atd.grid.linkedin.com:31234` |
| Local testing / EI | `k8s-0.apiserver.ei-ltx1.atd.stg.linkedin.com:31234` |

Connection ID: `mufn_service_default`

This connection must be present in every cluster's Airflow connections store. Import via `airflow connections import` during cluster bootstrap (see [Cluster Creation](../cluster-creation.md)).

### Timeouts

- `DEFAULT_CONNECT_TIMEOUT_MS = 30000` (30s to establish channel)
- `DEFAULT_READ_TIMEOUT_SEC = 10` (polling operations)
- `DEFAULT_WRITE_TIMEOUT_SEC = 30` (start/stop operations)

### Retry Policy

Built-in exponential backoff for transient errors:
- `DEFAULT_GRPC_RETRY_ATTEMPTS = 3`
- `DEFAULT_GRPC_RETRY_MULTIPLIER = 1`
- `DEFAULT_GRPC_RETRY_WAIT_MIN_SEC = 2`
- Retries on: `DEADLINE_EXCEEDED`, `UNAVAILABLE`
- No retry on: `PERMISSION_DENIED`, `INVALID_ARGUMENT`

---

## GGW Operator

**Base class:** `GridGatewayBaseOperator` (all concrete operators inherit)

**Location:** `/apache-airflow-providers-lnkd/src/airflow/providers/lnkd/gridgateway/operators/grid_gateway_base.py`

### Common Parameters

All GGW operators accept:

```python
GridGatewayBaseOperator(
    grid_gateway_conn_id="grid_gateway_service_default",          # Airflow connection
    polling_interval=30,                                           # Seconds between status checks (min 30)
    retries=3,                                                     # Airflow retries
    dependency_ivy=None,                                           # Ivy coordinates for JARs
    grid_gateway_dependencies=None,                                # GGW-format archive paths
    grid_gateway_params=None,                                      # Dict of GGW params
    grid_gateway_function_overrides=None,                          # Function spec overrides
    target_grid_cluster=None,                                      # Cluster target (for multi-cluster)
    identity=OklahomaIdentity.MP,                                  # Identity for DataVault token
    enable_sts_token=False,                                        # Enable STS token auth
    image_url=None,                                                # Docker image override
    disruption_ready=False,                                        # Chaos/disruption readiness
    allow_rdev_runs=True,                                          # Skip job in RDev if False
    enable_job_checkpoint=True,                                    # Pod disruption checkpointing
)
```

### Execution Parameters

Each operator subclass adds specific params:

**SparkBatchOperator:**
```python
SparkBatchOperator(
    proxy_user="alice",                    # Proxy user for execution
    execution_target="jar",                # "jar" or "python"
    job_class="com.example.Main",          # Main class
    execution_jars=["app.jar"],            # JARs to run
    spark_version="3.3.0",                 # Spark version
    driver_memory="4g",                    # Driver heap size
    executor_memory="8g",                  # Executor heap size
    executor_num=10,                       # Number of executors
    executor_cores=4,                      # Cores per executor
    spark_confs={"spark.executor.instances": "10"},  # Spark config
    job_queue="default",                   # YARN queue
)
```

**DarwinOperator (Jupyter notebooks):**
```python
DarwinOperator(
    proxy_user="alice",
    darwin_image="jupyter-image:latest",   # Notebook image
    git_repo_url="https://github.com/...", # Code repo
    git_resource_path="notebooks/run.ipynb",  # Notebook path
    action="run",                          # Action: "run", "render", etc.
    entity="my-notebook",                  # Entity identifier
)
```

**DataQualityJobOperator:**
```python
DataQualityJobOperator(
    proxy_user="dataquality",
    cluster="holdem",
    job_queue="dm_dataquality",
    assertions=r"""assertions dsl...""",   # DQ expressions
    ml_context={...},                      # ML monitoring config
)
```

### Disruption Readiness

For job types supporting disruption tolerance (hadoopJava, java, javaprocess, command, hadoopShell):

```python
disruption_ready=True  # Applies automatic retry policy for ENVIRONMENT_.* errors
```

Default retry policy:
```python
{"maxRetries": 3, "rules": [{"onErrorCode": "ENVIRONMENT_.*"}]}
```

---

## Common Failure Modes

### 1. Certificate Provisioning Error

**Symptom:**
```
GridGatewayCertificateProvisioningError: An error occurred while provisioning Grid Gateway certificates
```

**Root causes:**
- `/etc/riddler/ca-bundle.crt` not present (missing CA bundle)
- `/var/cluster/oklahoma/identity.cert` or `.key` not present (identity certs)
- In RDev: `grid_setup.sh` script didn't run

**Fix:**
- In prod: Contact ops to provision certs
- In RDev: Run `grid_setup.sh` and ensure SPIFFE certs are available

---

### 2. DataVault Token Failure

**Symptom:**
```
GridGatewayDataVaultTokenException: Failed to acquire DataVault token from <fabric>: <error>
```

**Root causes:**
- DataVault service unreachable (network issue)
- Wrong fabric env variable
- Invalid identity for token acquisition

**Fix:**
- Check `DATAVAULT_TOKEN_FABRIC` env var (should be `ei-prod-prod`)
- Verify DataVault service is up
- Check identity cert is valid

---

### 3. Timeout on Execution

**Symptom:**
```
GridGatewayTimeoutException: Grid Gateway start execution request timed out after 30 seconds
```

**Root causes:**
- Grid Gateway service slow/overloaded
- Network connectivity issue
- gRPC channel timeout (default 30s connection, 30s write)

**Fix:**
- Check Grid Gateway service health
- Increase timeout (not recommended, check underlying issue first)
- Check network connectivity via `telnet <host> <port>`

---

### 4. Proxy User Permission Denied

**Symptom:**
```
GridGatewayProxyUserPermissionException: Grid Gateway start execution permission denied.
Details: urn:li:servicePrincipal:(identity,...) is not allowed to impersonate as proxy_user
```

**Root causes:**
- DAG/MP identity lacks proxy permission for `proxy_user`
- ACL not configured in Grid Gateway

**Fix:**
```bash
picli airflow proxy \
  --proxy-users 'proxy_user' \
  --impersonating-identities 'urn:li:servicePrincipal:...' \
  --fabric-groups 'corp,prod'
```

---

### 5. RDev Execution Skipped

**Symptom:**
```
AirflowSkipException: This execution is being skipped only for rdev executions...
```

**Cause:** `allow_rdev_runs=False` on operator, running in RDev

**Fix:**
```python
MyGGWOperator(
    ...,
    allow_rdev_runs=True,  # Allow execution in RDev
)
```

---

### 6. Trustbridge Configuration Issues

**Symptom:**
```
AirflowException: DATAVAULT_TOKEN_PATH environment variable must be set when using Trustbridge.
```

**Conditions:** Only occurs when:
- Running in RDev
- `USE_TB_GRID_GATEWAY=true` in environment
- `DATAVAULT_TOKEN_FABRIC != ei-ltx1`

**Fix:**
- Ensure RDev environment injects `DATAVAULT_TOKEN_PATH`
- Or set `USE_TB_GRID_GATEWAY=false` to disable Trustbridge

---

## Usage Pattern (DAG Example)

```python
from datetime import datetime
from airflow import DAG
from airflow.providers.lnkd.gridgateway.operators.spark_batch import SparkBatchOperator
from airflow.providers.lnkd.gridgateway.operators.hadoop_java import HadoopJavaOperator
from airflow.providers.lnkd.gridgateway.operators.sql import SQLOperator

with DAG(
    dag_id="ggw_example_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    tags=["gridgateway", "data-processing"],
) as dag:
    
    # Spark job: process raw data
    spark_process = SparkBatchOperator(
        task_id="process_data_spark",
        proxy_user="data_engineer",
        execution_target="jar",
        job_class="com.example.DataProcessing",
        execution_jars=["s3://my-bucket/app.jar"],
        spark_version="3.3.0",
        driver_memory="4g",
        executor_memory="8g",
        executor_num=20,
        executor_cores=4,
        spark_confs={
            "spark.shuffle.partitions": "200",
            "spark.sql.adaptive.enabled": "true",
        },
        job_queue="standard",
        polling_interval=60,
        do_xcom_push=True,
    )
    
    # SQL job: validate results with Trino
    sql_validate = SQLOperator(
        task_id="validate_sql",
        proxy_user="analytics",
        sql="SELECT COUNT(*) FROM my_schema.processed_data",
        db="trino",
        polling_interval=45,
    )
    
    # Hadoop Java: legacy job type
    hadoop_legacy = HadoopJavaOperator(
        task_id="legacy_hadoop_job",
        proxy_user="legacy_user",
        job_class="com.old.HadoopJob",
        job_jar="hdfs:///user/legacy/app.jar",
        job_args="-input hdfs:///data/input -output hdfs:///data/output",
    )
    
    # Fan-out: both jobs run after Spark
    spark_process >> [sql_validate, hadoop_legacy]
```

### XCom Output Capture

If `do_xcom_push=True` (default), operator returns execution result dict:

```python
# In downstream task
{{ task_instance.xcom_pull(task_ids='process_data_spark') }}
# Returns: {
#   'state': 'SUCCEEDED',
#   'output_key_1': 'value_1',
#   'mufn.log.url': 'https://logs.grid.linkedin.com/...'
# }
```

---

## Recent Changes

### March 2026 - Trustbridge Logging

**Commit:** `1e6beace` (2026-03-11)
- Added logging of Trustbridge request headers (keys only, not sensitive tokens)
- Helps debug TB communication issues

### March 2026 - TB Endpoint Update

**Commit:** `17d10509` (2026-03-11)
- Fixed TB endpoint fabric header: changed from `grid1` to `prod-ltx1`
- Aligns with newer TB routing policy

### March 2026 - Operator Overridable Attributes

**Commit:** `cdb20193` (2026-03-31)
- All GGW operators now expose overridable attributes
- `get_overridable_attrs()` allows runtime config override via Airflow UI
- Null values skip override (preserves DAG defaults)
- Darwin operator added 5 overridable attrs: `darwin_image`, `git_resource_path`, `git_repo_url`, `action`, `entity`

### June 2025 - Authority Parameter Customization

**Commit:** `544ab5cd` (2025-06-13)
- Users can now override authority in connection extras
- Enables flexibility for different control planes (Nimbus vs. legacy)
- Default still `"mufn-control-service"`

### June 2025 - Disruption Readiness & Job Checkpointing

**Commit:** `87d6db65` (2025-06-13)
- Integrated external job checkpointing for pod disruptions
- `enable_job_checkpoint` flag (default True)
- Auto-retry on `ENVIRONMENT_.*` errors for disruption-ready job types

### April 2026 - Darwin Execution Result Logging

**Commit:** `9dfdbe6d` (2026-04-08)
- Darwin operator now logs execution result URL
- Surfaces direct link to notebook output in task logs

### Persistent Themes in Recent Commits

- **RDev/Trustbridge robustness:** Multiple fixes for TB communication (headers, fabric, token path)
- **Operator flexibility:** Overridable attributes, config overrides, authority customization
- **Error handling:** Clearer error banners, permission-denied diagnostics, DataVault token isolation
- **Job resilience:** Disruption readiness, checkpointing, retry policies for environment errors
- **Observability:** Better logging, log URL capture, result surfacing

---

## CLI: Interacting with GGW via grpcurli

The GGW control plane is **not in D2** — use direct disco hostnames. Do NOT use `d2://mufnControlService` or similar; it will fail with `Unavailable`.

### gRPC Method Reference

| Operation | Method | Request field |
|-----------|--------|---------------|
| Submit job | `FunctionExecution/StartExecution` | see StartExecutionRequest |
| Poll status | `FunctionExecution/GetExecution` | `{"executionUrn": "<urn>"}` |
| **Kill job** | `FunctionExecution/StopExecution` | `{"executionUrn": "<urn>"}` |

Proto source: `com/linkedin/protobuf/mufn/controlplane/core-operations.proto` (repo: `linkedin-multiproduct/mufn-service`)
`StopExecutionRequest` has exactly one field: `executionUrn` (string).

### Endpoints by Fabric

| Fabric | Cluster | Direct hostname |
|--------|---------|-----------------|
| `ei-ltx1` | faro | `prod-1.mufn-control-service.ei-ltx1.atd.stg.linkedin.com:31201` |
| `prod-ltx1` | holdem | `prod-1.mufn-control-service.prod-ltx1.atd.disco.linkedin.com:31201` |
| `prod-lva1` | war | `prod-1.mufn-control-service.prod-lva1.atd.disco.linkedin.com:31201` |

### Kill a job — from prod host (simplest)

```bash
# SSH to any prod host first
ssh ltx1-holdemgw01

grpcurli -f prod-ltx1 --dv-auth SELF \
  prod-1.mufn-control-service.prod-ltx1.atd.disco.linkedin.com:31201 \
  FunctionExecution/StopExecution \
  -d '{"executionUrn": "urn:li:mu:prod-ltx1:function(spark):execution(<uuid>)"}'
```

### Kill a job — from Mac via Trustbridge

**Step 1** — get a DV token (run from eng-portal):
```bash
ssh eng-portal
id-tool grestin sign -f prod-ltx1
curli -X POST -H "Content-Type: application/json" \
  -d '{"grantType":"CLIENT_CERTIFICATE"}' \
  https://1.datavault-token-service.prod-ltx1.atd-ds.disco.linkedin.com:4019/datavault-token-service/datavaultIdentityTokens?action=generateToken \
  --cacert /etc/riddler/ca-bundle.crt --cert ./identity.cert --key ./identity.key
```

**Step 2** — back on Mac:
```bash
token=<token_from_step_1>
grpcurli \
  -H "dvtoken:$token" \
  -H 'X-Li-grpc-fabric: prod-ltx1' \
  -H 'X-Li-grpc-app-tag: prod-1' \
  -H 'X-Li-grpc-app: mufn-control-service' \
  grpc.prod.linkedin.com:443 \
  FunctionExecution/StopExecution \
  -d '{"executionUrn": "urn:li:mu:prod-ltx1:function(spark):execution(<uuid>)"}'
```

Use `-H 'X-Li-grpc-app-tag: beta-1'` to target the GGW Beta-1 instance instead.

### Finding the execution URN

The URN is logged in the Airflow task log:
```
Execution urn:li:mu:prod-ltx1:function(spark):execution(<uuid>) ...
```
Also available in XCom key `mufn.log.url` (the log URL embeds the URN).

---

## See Also

- [Lipy Airflow Providers](lipy-airflow-providers.md) — Overview of entire provider library
- [Troubleshooting](../troubleshooting.md) — General Airflow debugging
- [DAG Authoring](../dag-authoring.md) — Writing DAGs with Airflow best practices
- Grid Gateway Example APIs: https://congenial-adventure-r4qn544.pages.github.io/docs/user/example-apis (repo: linkedin-multiproduct/mufn-service, path: docs/docs/user/example-apis.md)
- Grid Gateway Docs: https://congenial-adventure-r4qn544.pages.github.io/docs/user/onboarding
- Airflow Docs: https://didactic-umbrella-wlp475l.pages.github.io/docs/users/dag-authoring/write-run-dags
- Grid Gateway Support: https://engx.corp.linkedin.com/products/100/support
- Grid Gateway On-Call: https://oncall.prod.linkedin.com/team/team/Grid%20Jobs%20Platform

## Critical Gotcha: Marking Task Failed ≠ Killing Job

Marking a task as failed in the Airflow UI does NOT kill the underlying GGW job. It only marks Airflow's task state as failed. The GGW job continues running independently on the compute cluster.

To actually stop a running GGW job, use `GridGatewayHook.stop_execution()` or invoke the GGW gRPC `StopExecution` API directly.

## Stopping/Killing Jobs

### Programmatic (GridGatewayHook)

Use `GridGatewayHook.stop_execution()` to stop a running GGW job.

### Via gRPC (grpcurli)

The GGW service exposes a `StopExecution` proto method callable via `grpcurli`. This is the preferred method for direct service interaction.
