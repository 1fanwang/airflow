> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# Trust Bridge

## Connectivity Checks

**Primary (recommended):**
```bash
captain setup trustbridge
```
Authenticates and verifies connectivity. Opens browser prompt for SSO re-auth if needed.

**Via curli:**
```bash
curli --tb-auth -X GET 'https://<endpoint>'
```

## DataVault Token Auth

`dvtoken` is NOT a standalone CLI tool — it refers to a **DataVault identity token** (credential artifact).

To authenticate using DataVault token:
```bash
curli --dv-auth SELF <url>
```
