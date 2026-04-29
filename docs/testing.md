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

## Running Tests with Tox

Tox is available via `uv tool run tox` (version 4.53.0 available at runtime).

**Note**: Test environments (`test310`, `test311`, `test312`, `ci`) are configured in `tox.ini` but currently have empty `commands` sections. Running `uv tool run tox run -e test310` succeeds but performs no actual test execution. The environments inherit base settings:
- `base_python = python3.12`
- `skip_install = True`
- Standard environment variables set (PIP_DISABLE_PIP_VERSION_CHECK, PYTHONHASHSEED, etc.)

Test commands need to be added to the environment definitions in `tox.ini` before tox will actually run tests.

## Running Tests with tox

tox is available in this codebase but must be invoked via uv:

```bash
uv tool run tox list          # List available environments
uv tool run tox run -e test310 # Run test310 environment
```

### Current Test Environment Status

The tox configuration defines test environments (test310, test311, test312, prepare, ci) but they currently have **no commands configured**. Running `uv tool run tox run -e test310` will succeed without executing any actual tests:

- `skip_install = True`
- `commands =` (empty)
- `deps =` (empty)

This means test environments need command definitions in tox.ini before they can actually run tests. Check tox.ini for commented-out or incomplete command definitions.

## Tox Test Infrastructure

**Status**: tox is configured but non-functional.

- **Availability**: Not directly installed. Run via `uv tool run tox` (v4.53.0 available)
- **Environments defined**: `prepare`, `test310`, `test311`, `test312`, `ci`
- **Issue**: test310/311/312 environments have **no commands defined** — they are empty shells that pass trivially without executing any tests
- **Impact**: `uv tool run tox run -e test310` succeeds (0.32s) but doesn't run any actual tests

These environments need command definitions in `tox.ini` to function as test runners.

## tox Test Runner

The repository includes a `tox.ini` configuration file with test environments (prepare, test310, test311, test312, ci), but the test environments (test310, test311, test312) are currently empty shells with no commands or dependencies defined.

**To run tox:**
```bash
uv tool run tox --version
uv tool run tox list          # List available environments
uv tool run tox run -e test310  # Run a specific environment (will succeed but do nothing)
```

tox is not installed as a system dependency but can be executed via `uv tool run` (uv manages the tox installation). However, the actual test environments lack command definitions, so running them will complete successfully without executing any tests.

For now, use the testing infrastructure outside of tox directly.

## Tox Test Environments - Incomplete Configuration

**Status**: The tox test environments (`test310`, `test311`, `test312`) are defined in `tox.ini` but are not fully configured.

**Finding**: When attempting to run tox test environments, they complete immediately without executing any tests because:
- No `commands` are defined in the test environment configuration
- No dependencies (`deps`) are specified
- `skip_install = True` is set

**Availability**: Tox 4.53.0 is available via `uv tool run tox`, but the test environments will not run actual tests without proper configuration.

**Workaround**: Tests should be run using the project's primary test runner (likely pytest or similar) directly, not through tox environments, until the tox test configuration is completed.

## Running Tests with Tox

Tox is available via `uv tool run tox` (not installed directly). However, the configured test environments (test310, test311, test312) currently have empty command definitions (`skip_install=True`, `commands=<empty>`), making them no-ops. The actual test execution may be defined elsewhere (e.g., Breeze scripts, pytest configuration, or CI workflows). Use `uv tool run tox list` to see available environments and `uv tool run tox config -e <env>` to inspect environment details.
