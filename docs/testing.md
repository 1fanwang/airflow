# Testing

Last updated: auto | Sources: update_context

---

## Running Tests with tox

tox is available via `uv tool run` (not installed directly in the environment).

```bash
# List available tox environments
uv tool run tox list

# Run a specific test environment
uv tool run tox run -e test312

# Check tox configuration for an environment
uv tool run tox config -e test312
```

### Available Environments

- `prepare` — setup environment
- `test310`, `test311`, `test312` — test against Python 3.10, 3.11, 3.12 respectively
- `ci` — CI environment

### Configuration Notes

- Base Python: 3.12
- Runner: virtualenv
- `skip_install = true` — tox does not install the package
- External tools allowed: bash, uv, pytest, ./scripts/ci/install_breeze.sh
