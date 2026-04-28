> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

# Airflow — Deployment

## CRT -> LCD Migration (Active — EKG Disabled)

As of late April 2026 (BDP-98089), Oklahoma-Airflow has migrated from **CRT** (Continuous Release Train) to **LCD** (LinkedIn Continuous Delivery) for infrastructure deployments. The migration had a bumpy rollout:

1. **PR #1060** (Apr 23): Initial LCD onboarding — added LCD pipeline config alongside CRT.
2. **PR #1064** (Apr 24): **Reverted** — LCD deployments failed in the EKG step because EKG/Canary is not supported for MPs deployed with ArgoCD. Reverted to unblock weekend deployments.
3. **PR #1066** (Apr 27): **Re-onboarded** to LCD with simpler config to validate migration.
4. **PR #1067** (Apr 27): Fixed LCD pipeline creation failure from PR #1066 (comment trigger).
5. **PR #1070** (Apr 27): **Disabled EKG/canary steps** in LCD configuration — the definitive fix. ArgoCD-deployed MPs cannot use EKG/Canary.

**Current state**: LCD is the active deployment path with EKG/canary steps disabled. The CRT flow documented below remains as reference and may still be used for some operations during the transition period.

---

## CRT Flow and Promotion Pipeline

Oklahoma Airflow infrastructure (Helm charts, images, configuration) is deployed via LinkedIn's **CRT (Continuous Release Train)** system. The deployment MP is `oklahoma-airflow-deployment`.

**CRT page:** `https://crt.prod.linkedin.com/#/deployment/actions?pathName=DEFAULT&productName=oklahoma-airflow-deployment`

### Infrastructure Deployment Order

Every merge to `oklahoma-airflow-deployment` goes through clusters in this fixed order:

```
grid1-test (airflow-test) -> ei-ltx1 (faro) -> corp-lva1 (corp) -> grid1-prod (holdem + dbt) -> grid2-prod (war) -> [Manual] Tag RDev stable image
```

- **grid1-test** is the regression gate. Run the `alwaysOk` DAG plus any DAGs tagged `regression_test` before proceeding.
- **ei-ltx1 (faro)** is the EI/staging environment — the first real fabric.
- **corp-lva1 (corp)** is the corporate cluster.
- **grid1-prod (holdem/dbt)** and **grid2-prod (war)** are the production clusters.
- After the War deployment, the RDev `stable` image tag must be manually updated (see [RDev Testing](#rdev-testing) below).

**CRT strategies:** All clusters use `ARGO` as the deployment strategy (`"action_name": "ARGO"`), meaning ArgoCD drives the actual Kubernetes rollout.

### Nominate and Deploy Steps

1. Merge your PR into `oklahoma-airflow-deployment`.
2. On the CRT page, click **Nominate** for `grid1-test`.
3. Click the ArgoCD link shown in the deployment info to watch the Kubernetes rollout.
4. Verify regression DAGs pass on the test cluster.
5. Repeat — nominate, deploy, verify — for each subsequent cluster in order.

**Do not deploy after 2pm on Fridays.** This is a standing team policy.

### Config Promotion (EI -> Prod)

Cluster-specific configuration lives in `.linkedin/kube/airflow/values/` inside `oklahoma-airflow-deployment`. The hierarchy is:

```
deployment/helm-values/clusters/
  dev/          <- personal dev clusters
  grid1/
    values/
      grid1/    <- prod values per product tag (holdem, dbt, test)
      ei-ltx1/  <- faro values
  grid2/        <- war values
```

For Airflow image version bumps, update the tag in `.linkedin/kube/airflow/values/grid1/values.yaml`. Config differences between EI and prod (e.g., `IGNORE_IMPORT_ERRORS_MP_ALLOW_LIST`) live in the per-fabric values files and are promoted by merging them through CRT in order.

---

## DAG Deployment — Upload Plugin Flow (CRT DAG Deployment)

User DAGs are deployed via the **Upload Plugin**, not through infrastructure CRT. The plugin exposes a REST API endpoint that is called by the `airflow-crt-action` MP as part of each user's own CRT pipeline.

### Normal Flow (airflow-crt-action)

When a user MP has type `airflow-workflow`, LinkedIn's deployment-scheduler invokes `airflow-crt-action`, which:

1. Builds the DAG ZIP artifact (e.g., `jkandell_airflow_application-0.0.44.zip`).
2. POSTs it to the upload endpoint:
   ```
   POST /api/v1/plugins/upload/upload_dags
   ```
   with fields: `subpaths`, `version`, `grid_cluster`, `file`, optionally `ignore_import_errors`.
3. The webserver unpacks the ZIP into `/opt/airflow/dags/<mp_name>/<app_name>/`.
4. The scheduler picks up the new DAGs within ~15 minutes.

The `subpaths` field must be `mp_name/application_name` (e.g., `jkandell_airflow_mp/jkandell_airflow_application`). DAG IDs must follow the convention `<dag_name>__<mp_name>` — validated by `picli airflow validate_dags`.

### Import Error Blocking

Deployments are blocked if any DAG in the upload has import errors. Two ways to override:

**Option 1 — Allow-list in helm values:**
Add the MP to `IGNORE_IMPORT_ERRORS_MP_ALLOW_LIST` in the cluster's values file and redeploy.

**Option 2 — Direct API call:**
```bash
# Get a datavault token from eng-portal, then:
curl -X POST -m 900 \
  -H "datavaultIdentityToken: ${TOKEN}" \
  http://<cluster-url>/api/v1/plugins/upload/upload_dags \
  -F "subpaths=<mp_name>/<app_name>" \
  -F "version=<version>" \
  -F "grid_cluster=<holdem|war|faro>" \
  -F "file=@/path/to/dags.zip" \
  -F "ignore_import_errors=true"
```

Response `{"result": "Deployment is in progress."}` -> poll until `{"result": "Done!"}`.

Cluster URLs:
- Faro (staging): `http://faro.oklahoma-airflow.stg.linkedin.com`
- Holdem (prod): `http://holdem.oklahoma-airflow.grid.linkedin.com`
- War (prod): use the war cluster URL

---

## picli — The DAG Developer CLI

`picli` (Pipelines CLI) is the primary developer-facing tool for Airflow DAG development. Deployed on LCD at `go/lcd picli`.

### Command Reference

```bash
picli airflow --help          # Airflow command group

# RDev testing
picli test create             # Create a new RDev test instance for the current MP
picli test create --branch <branch> --mp-name <mp> --name <instance-name>
picli test login <instance>   # SSH in and start Airflow (scheduler + webserver)
picli test upload <instance>  # Upload build artifacts to test instance
picli test delete <instance>  # Tear down the test instance

# RDev setup
picli airflow rdev-init --path-to-mp /path/to/mp
picli airflow rdev-init --path-to-dag-src /path/to/mp/app/src

# Validation
picli airflow validate_dags --application-path /path/to/app
  # Checks: DAG IDs follow <name>__<mp_name> convention, no import errors

# Policy enforcement
picli airflow enforce_policy <app_src_path>

# CRT sync
picli airflow crt-sync

# Azkaban migration
picli airflow migrate
picli airflow refactor
```

### rdev-init

`picli airflow rdev-init` scaffolds the `.devcontainer/devcontainer.json` and `.devcontainer/rdev-init.sh` for a DAG MP. The default RDev image is:
```
lnkdin.cr/lps-image/linkedin/oklahoma-airflow/airflow-main-airflow-rdev:stable
```
The `stable` tag is a floating tag controlled by the Oklahoma team (see [RDev Stable Tagging](#rdev-stable-tagging)).

### validate_dags

Runs a `DagBag` import against your local DAGs src directory and checks that every DAG ID matches the `<dag_name>__<mp_name>` convention. Expected output: `"All DAG IDs are valid!"`.

### Recent picli Improvements (Mar-Apr 2026)

| PR | Change | Notes |
|----|--------|-------|
| #656 | Remove default grid cluster `holdem` from `picli test login` | Venice team was hitting prod stores via rdev; now users must explicitly specify `--grid-cluster` |
| #665 | Remember last-used login method as default | Saves `--method` and `--grid-cluster` to `~/.picli/preferences.json` |
| #670 | Deploy all LCD targets in parallel | Fixes ~11h sequential deployments caused by 300-minute UCM agent waits; added `deployment_order` to LCD pipeline YAML |
| #675 | Fix CLI commands failing to handle `.` as a relative path input | Makes invocation easier when shell is in MP root folder |
| #676 | Fix `enforce_policy` satellite venv syspath | Discovers Python version dynamically instead of hardcoding `python3.10/site-packages` |
| #677 | Silence noisy third-party library logs from CLI output | Every command (including `--help`) was flooding stdout with third-party log noise |
| #678 | Initialize Airflow as isolated library using `settings.initialize()` | Proper initialization removes fragile behavior, maintenance burden, and broken local dev |

### LCD Migration Gotchas

When migrating a DAG MP from CRT to LCD (LinkedIn Continuous Delivery):

- **`picli airflow crt-sync` generates old-schema CRT files**: Running `picli airflow crt-sync` scaffolds CRT workflow configuration files. These files use the legacy CRT schema format and are not directly compatible with the LCD migration tooling. If your MP is being migrated to LCD, do not use `crt-sync` output as the LCD config source — use the LCD migration tool directly with your existing CRT pipeline YAML.

- **LCD migration tool fails if MP has no active instances**: The LCD migration tool requires at least one deployed MP instance to exist at migration time. If the MP was previously disabled or has zero active instances, the tool exits with an error and cannot complete the migration. Workaround: temporarily create a dummy placeholder deployment, run the LCD migration, then clean up.

Source: #ask_airflow Slack

---

## RDev Testing

RDev is the preferred pre-production testing environment. Each user gets an isolated Airflow instance (scheduler + webserver) running in a LinkedIn RDev container.

### Creating and Using an RDev

```bash
# Scaffold devcontainer config in your MP
picli airflow rdev-init --path-to-mp /path/to/your_mp

# Create the RDev (spins up the container, ~5 min)
picli test create

# Log in — this triggers ~/.okl_rdev/okl-rdev-init.sh which starts MySQL + Airflow
picli test login <mp_name/instance-name>

# Upload your build artifacts (run mint build && mint release first)
picli test upload <mp_name/instance-name>

# Delete when done
picli test delete <mp_name/instance-name>
```

**Customizable Navbar Header** (deployment PR #1043, 2026-04-14): Downstream rdev images (e.g., `darwin-rdev-airflow-image`) can now customize the Airflow nav header to visually distinguish their modified version from the base Oklahoma rdev. Addresses incident-10325 / ACTIONITEM-16458.

**SMTP for EmailOperator** (deployment PR #1038, 2026-04-11): RDev environment now has SMTP configuration, enabling `EmailOperator` usage in test instances. Previously, email sending was not available on rdev.

**fsGroup Fix** (deployment PRs #1035/#1036, 2026-04-10): Airflow scheduler/webserver/worker pods had incorrect `fsGroup` in their security context. On new NKS nodes, the wrong GID caused NFS mount hangs (processes blocked in uninterruptible sleep). Fix: set `fsGroup` correctly for all pod types.

After `picli test login`, look for:
- `"Scheduler has been started successfully!"`
- `"Webserver has been started successfully!"`

Check `tmux ls` to see `airflow_scheduler` and `airflow_webserver` sessions.

### RDev Stable Tagging

After every War deployment, the oncall must manually update the `stable` image tag so all user RDev instances pull the new validated image:

1. Go to the `oklahoma-airflow-deployment` GitHub -> **Actions** tab.
2. Click **"Tag stable image"** workflow -> **Run workflow** -> select `master` branch.
3. Enter the exact image version that was in the War deployment (e.g., `0.0.581`).
4. Confirm the workflow turns green.

The current `stable` version can be found in the most recent successful execution of this workflow under the `STABLE_VERSION` environment variable log.

### Snapshot Versions for RDev Image Testing

To test changes to the RDev image itself before merging:

```bash
# 1. Build locally
mint image build   # from oklahoma-airflow root

# 2. Tag and push to temp registry
docker tag <image-id> lnkdin.cr/temp/<username>/oklahoma-airflow/airflow-main-airflow-rdev:<ver>-SNAPSHOT
docker push lnkdin.cr/temp/<username>/oklahoma-airflow/airflow-main-airflow-rdev:<ver>-SNAPSHOT

# 3. Update .devcontainer/devcontainer.json in your test MP to use the snapshot image

# 4. Create RDev from that branch
picli test create --branch <your-branch>
```

---

## Dev Cluster (Helm-based) Testing

For testing changes to Airflow core, providers, or Helm charts, engineers spin up a personal dev cluster in the `airflow-test` Kubernetes namespace.

### Deploy a Dev Cluster

```bash
# Substitute your LOGNAME into the values files
envsubst '$LOGNAME' < ./deployment/helm-values/clusters/dev/ei-ltx1/values.yaml > \
    ./deployment/helm-values/clusters/dev/ei-ltx1/values-airflow-$LOGNAME.yaml

# Install
helm install airflow-$LOGNAME ./.linkedin/kube/airflow \
    --namespace airflow-test \
    --values ./deployment/helm-values/clusters/dev/ei-ltx1/values-airflow-$LOGNAME.yaml

# Check pods
kubectl get pods -n airflow-test | grep airflow-$LOGNAME

# Port-forward to access UI
kubectl port-forward -n airflow-test svc/airflow-$LOGNAME-webserver 8080:8080
# -> http://localhost:8080

# Upgrade (after making changes)
helm upgrade airflow-$LOGNAME ./.linkedin/kube/airflow \
    --namespace airflow-test \
    --values ./deployment/helm-values/clusters/dev/ei-ltx1/values-airflow-$LOGNAME.yaml

# Tear down
helm uninstall airflow-$LOGNAME --namespace airflow-test
```

### Fast Dev Testing (Without Rebuilding Image)

For quick iteration on provider or Airflow source files, use `fastAirflowDevTesting.sh`:

```bash
# From oklahoma-airflow-deployment directory:
bash deployment/scripts/fastAirflowDevTesting.sh <file1.py> <file2.py>

# Creates configmaps in airflow-test namespace, restarts scheduler + webserver
# Multiple configmap groups: separate with --
bash deployment/scripts/fastAirflowDevTesting.sh file1.py file2.py -- file3.py file4.py

# Clean up
kubectl delete configmap $LOGNAME-airflow-test-map-0 -n airflow-test
```

The script creates Kubernetes ConfigMaps from your local files and mounts them into the pods via the `volumes`/`volumeMounts` section in your dev `values.yaml`.

---

## Backfill Process

Backfills are triggered through the Airflow UI or via the Airflow CLI/API:

- **UI:** Go to the DAG page -> **Graph** view -> select a past date range -> **Trigger DAG** with `execution_date`.
- **API:** `POST /api/v1/dags/{dag_id}/dagRuns` with a historical `execution_date`.
- **CLI (inside a pod):** `airflow dags backfill -s <start_date> -e <end_date> <dag_id>`

For large backfills affecting prod, coordinate with the oncall — backfills can stress the scheduler and DB.

---

## Rollback

### Rolling Back Infrastructure (Helm/Image)

1. Identify the last known good version in CRT.
2. On the CRT page, navigate to that version and re-nominate + deploy it in order through the clusters.
3. Mark the bad version as deprecated immediately:
   ```bash
   mint catalog deprecate oklahoma-airflow-deployment <bad_version>
   ```
   This prevents accidental re-deployment.

ArgoCD can also be used for fast rollback on a single cluster:
```bash
kubectl in argocd login -f grid1 -t k8s-0
argocd app rollback airflow-holdem <revision>
```

### Rolling Back a DAG Deployment

Re-upload the previous ZIP artifact version via the upload API or CRT for the user's MP. The scheduler will pick up the old version within ~15 minutes.

If a stale Airflow application directory is causing duplicate DAG issues (e.g., after an app rename):
```bash
kubectl exec -it <scheduler-pod> -n airflow -- bash
cd /opt/airflow/dags/<mp_name>/
rmdir -r <stale_app_dir>
rmdir -r <stale_app_dir>.dir
# Scheduler reconciles within ~15 minutes
```

---

## DAG Naming Conventions and Their Role in Deployment

DAG IDs must follow: `<dag_name>__<mp_name>` (double underscore separator).

This convention matters for deployment because:
- The upload plugin uses the MP name (derived from the `subpaths` field) to route DAGs to the correct directory: `/opt/airflow/dags/<mp_name>/<app_name>/`.
- The scheduler uses the file path (which includes `<mp_name>`) to scope DAGs per tenant.
- Validation (`picli airflow validate_dags`) enforces this before the ZIP is uploaded to CRT.
- Duplicate DAG problems arise when application names change but old directories are not cleaned up (the `fileloc` column in the `dag` table oscillates between two paths).

---

## Helm Chart Deployment and Image Build

### Image Build Flow

1. Airflow source lives in `oklahoma-airflow` (the `airflow-main` module).
2. Image is built via `mint image build` or `mint buildImage` (the latter also builds the RDev image).
3. Images are pushed to `container-image-registry.corp.linkedin.com/lps-image/linkedin/oklahoma-airflow/`.
4. The image tag in `oklahoma-airflow-deployment` Helm values is bumped, a PR is raised, and CRT drives the rollout.

Key Dockerfiles:
- `airflow-main/airflow-oklahoma.Dockerfile` — production image
- `airflow-main/airflow-rdev.Dockerfile` — RDev image (uses `python-base-rdev-imagemysql8` base, installs as `coder` user)

Build script: `airflow-main/scripts/buildAirflowImage.sh` — set `IS_LOCAL_PACKAGE=0` for source-only changes, `IS_LOCAL_AIRFLOW_CORE=0` for provider-only changes.

### Helm Chart Structure

Charts live in `.linkedin/kube/airflow/` in `oklahoma-airflow-deployment`. The chart is the `oklahoma-airflow` Helm chart. Key config patterns:

- `defaultAirflowTag` — controls which image tag all pods use.
- `otelMetrics.enabled` / `otelMetrics.account` — toggles OpenTelemetry metrics to Geneva/MDM.
- `IGNORE_IMPORT_ERRORS_MP_ALLOW_LIST` — comma-separated list of MPs that bypass import error blocking.
- `INLOGS_SECRET` / `LOGGING_FALLBACK_TO_FILE_ENABLED` — controls InLogs API log reading vs. NFS fallback.

### Verifying a Deployment with ArgoCD

```bash
# Login to ArgoCD on grid1
kubectl in argocd login -f grid1 -t k8s-0

# List all Airflow app instances
argocd app list -l product=oklahoma-airflow-deployment

# Preview what a PR will change before merging
argocd app diff airflow-test --revision <your-branch>
argocd app diff airflow-holdem --revision <your-branch>
argocd app diff airflow-war --revision <your-branch>
```

### Helm Chart Testing Requirements

All Helm chart PRs must:
1. Have the PR owner spin up a dev cluster for every changed chart.
2. Run at least one successful regression DAG (tests Grid Gateway connectivity).
3. Exception: image-only bumps (assume PR owner tested the underlying change).

---

## See Also
- [Clusters](clusters.md)
- [Oncall](oncall/README.md)
- [DAG Authoring](dag-authoring.md)
- [Architecture](architecture.md)
- [Troubleshooting](troubleshooting.md)
- [Codebase Overview](codebase/README.md)

## Pull Request Standards (oklahoma-airflow-deployment)

The oklahoma-airflow-deployment repository enforces a PR description validation check that requires specific section headers:

- `## Problem & Solution Overview` — describe the issue and solution (not `## Summary`)
- `## Testing Done` — describe how changes were tested (not `## Test plan`)

If these exact section headers are missing or incorrectly named, the "Pull Request Description" check will fail and must be fixed before the PR can be merged.
