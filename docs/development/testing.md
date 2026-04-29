# Testing

Last updated: auto | Sources: update_context

---

## Running Tests with tox

The codebase includes a `tox.ini` configuration file, but tox is not installed in the system Python environment. To run tox, use `uv tool run`:

```bash
uv tool run tox list          # List available environments
uv tool run tox run -e test310 # Run a specific test environment
```

**Important:** The test environments (test310, test311, test312) are defined in `tox.ini` with `base_python = python3.12` and `skip_install = true`, but currently have **no commands configured**. This means `tox run -e test310` completes successfully but does not actually execute any tests. The test environments exist as placeholders and their commands need to be defined in the tox configuration before they can be used to run actual test suites.
