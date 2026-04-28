> Part of the workspace knowledge base. See [CLAUDE.md](../CLAUDE.md) for the navigation map.

# Airflow — Cluster Creation

## Prerequisites

| What | Where |
|------|-------|
| K8s namespace | `go/k8s/namespace` self-service portal |
| NFS provisioning | `go/k8s-nfs` self-service portal |
| MySQL cert-based | Nuage form (password auth deprecated) |
| Trust Bridge onboarding | `captain setup trustbridge` |
| CRT registration | `crt-workflows/workflows/individual/o/oklahoma-airflow-deployment.json` |
| AAD redirect URI | Azure app `spi-oklahoma-airflow-prod` App Registrations |

---

## 1. Kubernetes Namespace

1. Request namespace via `go/k8s/namespace`
2. Apply Kyverno policies (auto-applied on namespace creation in NKS)
3. Provision Oklahoma identity/Grestin certs for the namespace
   - Grestin cert is the cluster's mTLS identity used for DataVault token acquisition and GGW calls
   - Mounted at `/var/cluster/identity.cert`, `.key`, `.p12` (via K8s-LARE init container)

---

## 2. NFS (Shared DAG Storage)

1. Request NFS via `go/k8s-nfs`
2. Create a PersistentVolume (PV) + PersistentVolumeClaim (PVC) pointing to the NFS share
3. Use a unique **subpath per cluster** (e.g. `airflow-holdem/`) to isolate DAG files

DAGs are served from NFS to schedulers, workers, and DAG processors. The PVC is named `airflow-{cluster}-dags-pvc`.

---

## 3. MySQL (Metadata Database)

> **Password auth is deprecated.** Use cert-based auth exclusively.

### Provisioning

- Fill in the Nuage form to provision a cert-based MySQL instance
- Certs are mounted by K8s-LARE (same mechanism as Grestin certs)

### Connection String Format

```
mysql+mysqldb://<user>@<CNAME>:<port>/<db>?ssl_ca=%2Fetc%2Friddler%2Fca-bundle.crt&ssl_cert=<cert_path>&ssl_key=<key_path>&ssl_mode=VERIFY_CA
```

Example for a `holdem` cluster:
```
mysql+mysqldb://airflow_holdem@holdem-mysql.prod-ltx1.corp.linkedin.com:3306/airflow_holdem?ssl_ca=%2Fetc%2Friddler%2Fca-bundle.crt&ssl_cert=%2Fvar%2Fcluster%2Fdb.cert&ssl_key=%2Fvar%2Fcluster%2Fdb.key&ssl_mode=VERIFY_CA
```

---

## 4. Kubernetes Secrets

Create the following secrets in the cluster's namespace:

| Secret name | Content | How to create |
|-------------|---------|---------------|
| `airflow-{cluster}-fernet-key` | Fernet key for variable/connection encryption | Generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `airflow-{cluster}-webserver-secret` | Flask session signing key | Random 32+ byte secret |
| SSO client credential | Azure AD client secret for `spi-oklahoma-airflow-prod` | From Azure App Registration -> Certificates & Secrets |
| InLogs API secret | API key for InLogs lifecycle reporting | From InLogs team |

---

## 5. DNS — NKS Topology-Based Discovery

> **DNS Range is deprecated.** Use NKS topology-based disco hostnames.

### Hostname Format

```
{product-tag}.{application}.{fabric}.atd.disco.linkedin.com:{port}
```

Example (Lasso cluster, prod-ltx1):
```
lasso.airflow-main-airflow-oklahoma.prod-ltx1.atd.disco.linkedin.com:31119
```

### SSO Proxy URLs by Fabric

SSO (Azure AD) proxy is fabric-specific. Use the correct entry for each cluster:

| Fabric | Proxy URL |
|--------|-----------|
| PROD grid1 (ltx1) | `ltx1-kraken-vip-1.prod.linkedin.com:10339` |
| PROD grid2 (lva1) | `lva1-kraken-vip-1.prod.linkedin.com:10339` |
| EI (staging) | `ltx1-kraken-vip-1.stg.linkedin.com:10339` |
| CORP | `lva1-kraken-vip-8.corp.linkedin.com:10339` |

### New Cluster DNS

For a new cluster, the domain CNAME can point directly to the NKS disco hostname (no ATS intermediary needed for NKS-based clusters).

---

## 6. Trust Bridge

All browser traffic to Airflow UI goes through Trust Bridge (reverse proxy / auth gateway).

1. Self-onboard via `captain setup trustbridge`
2. For NKS-based clusters: use the **pseudo-backend pattern** — the Trust Bridge backend points to the NKS disco hostname
3. Existing clusters migrated from legacy (ATS) to Trust Bridge as part of Nimbus migration

---

## 7. K8s-LARE (Identity Certificates)

K8s-LARE is an init container that provisions cluster identity certificates.

| Config | Value |
|--------|-------|
| Init container name | `k8s-lare` |
| Application | `airflow-main-airflow-oklahoma` |
| Certs written | `/var/cluster/identity.cert`, `/var/cluster/identity.key`, `/var/cluster/identity.p12` |

K8s-LARE runs before the main container, mounts the certs, and the main Airflow process uses them for DataVault token acquisition and mTLS GGW connections.

---

## 8. SSO — Azure Active Directory

All clusters use the shared Azure AD app **`spi-oklahoma-airflow-prod`** (tenant: `lnkdprod.com`).

When adding a new cluster:
1. Go to App Registrations -> `spi-oklahoma-airflow-prod` -> Authentication
2. Add the new cluster's redirect URI: `https://{cluster-url}/oauth-authorized/azure`
3. Save

---

## 9. InLogs / Lifecycle Env Vars

These env vars enable DAG/task lifecycle event reporting:

| Component | Env Var | Value |
|-----------|---------|-------|
| Webserver, Scheduler, Worker | `AIRFLOW__LISTENER_PLUGIN__ENABLED` | `True` |
| Worker only | `AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS` | `airflow.providers.lnkd.log.oklahoma_logging_config.OKLAHOMA_LOGGING_CONFIG` |

Both are set in the Helm values per cluster. The listener plugin emits lifecycle events to InLogs (airflow task start/complete/failure events).

---

## 10. Connection Export/Import

When bootstrapping a new cluster, copy Airflow connections from an existing cluster:

```bash
# Export from source cluster
kubectl exec -it airflow-holdem-scheduler-<pod> -n airflow -c scheduler -- \
  airflow connections export /tmp/connections.json

kubectl cp airflow-holdem-scheduler-<pod>:/tmp/connections.json ./connections.json -n airflow -c scheduler

# Import into new cluster
kubectl cp ./connections.json airflow-newcluster-scheduler-<pod>:/tmp/connections.json -n airflow -c scheduler

kubectl exec -it airflow-newcluster-scheduler-<pod> -n airflow -c scheduler -- \
  airflow connections import /tmp/connections.json
```

---

## 11. CRT Registration

Register the new cluster in two places:

**1. CRT workflow file** — add cluster to:
```
crt-workflows/workflows/individual/o/oklahoma-airflow-deployment.json
```

**2. Cluster config** — add cluster URL to:
```
lipy-oklahoma-airflow/.../oklahoma_cluster_config.json
```

This enables the CRT/LCD deployment pipeline to deploy DAGs to the new cluster.

---

## 12. ATS Traffic

For ATS (Application Traffic Shaping) integration with the new cluster, follow the Self-Service Traffic Routing Guide (internal link, search for it in Confluence).

---

## See Also

- [Clusters](clusters.md) — Per-cluster config reference (resource sizing, env vars, URLs)
- [Architecture](architecture.md) — K8s components, executor, PGBouncer
- [Deployment](deployment.md) — CRT/LCD flow, picli commands, upload plugin
- [Security](security-architecture.md) — DataVault/FAB auth flow, grestin certs, service account 401 bug
- [Trust Bridge](systems/trustbridge.md) — Trust Bridge auth and connectivity
- [Codebase Overview](codebase/README.md) — 27-repo workspace map, policy enforcement
