> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# JKS / Truststore

## Existing JKS / Truststore Implementations at LinkedIn

### id-tools Repository
- **Location**: `linkedin-multiproduct/id-tools`
- **File**: `id-tools/src/linkedin/trustcatool/truststore.py`
- **Details**: Contains implementations for JKS truststore parsing and certificate handling via the `pyjks` / `jks` library
- **Module**: `trustcatool` handles trust store configuration and certificate parsing

### lipy-truststore Repository
- **Location**: `linkedin-multiproduct/lipy-truststore`
- **File**: `lipy-truststore/src/linkedin/truststore/trust_config.py`
- **Purpose**: Provides trust store configuration management and schema (`TrustStoreConfig.schema.json`)
- **Capability**: Handles multiple trust store configs within a single configuration

### Related Projects
- **airflow-oc-image**: PR #179 adds JKS trusted certs parsing in `airflow/flyte/token_fetcher.py`

### Recommendation
Before implementing new JKS parsing functionality, check existing implementations in `id-tools` and `lipy-truststore` to avoid code duplication and leverage existing patterns.
