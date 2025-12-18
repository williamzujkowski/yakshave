# Live Integration Test Fixtures

This document describes the live integration test fixtures in `tests/conftest.py` for testing against real GitHub API.

## Overview

The live fixtures enable integration tests that make real GitHub API calls while:
- Minimizing API usage via session-scoped caching
- Providing test isolation with temporary directories
- Automatically skipping tests when credentials are missing
- Using stable, historical data for reproducibility

## Fixtures

### `github_token`
**Scope**: Session
**Purpose**: Provide GitHub API token from environment

Gets `GITHUB_TOKEN` from environment. Automatically skips test if not set.

```python
@pytest.mark.live_api
def test_something(github_token: str) -> None:
    # Will skip if GITHUB_TOKEN not set
    assert github_token
```

### `live_config`
**Scope**: Session
**Purpose**: Provide Config for live testing

Creates a Config instance targeting the `github` org with:
- Year 2024 (stable historical data)
- Conservative rate limiting (concurrency=1, min_sleep=2.0s)
- Public repos only
- All collectors enabled
- Session-scoped temp directory

```python
@pytest.mark.live_api
def test_something(live_config: Config) -> None:
    assert live_config.github.target.name == "github"
    assert live_config.github.windows.year == 2024
```

### `live_paths`
**Scope**: Session
**Purpose**: Provide PathManager for test isolation

Creates PathManager using `live_config` with session-scoped temp directories.

```python
@pytest.mark.live_api
def test_something(live_paths: PathManager) -> None:
    assert live_paths.year == 2024
    assert live_paths.raw_root.exists()
```

### `cached_raw_data`
**Scope**: Session
**Purpose**: Run collection once, cache for all tests

**Important**: This fixture makes real GitHub API calls and runs collection ONCE per pytest session. All tests using this fixture share the cached data.

```python
@pytest.mark.live_api
@pytest.mark.slow
def test_something(cached_raw_data: dict, live_paths: PathManager) -> None:
    # Collection already ran, data is cached
    assert "discovery" in cached_raw_data
    assert live_paths.manifest_path.exists()
```

### `live_test_config_path`
**Scope**: Session
**Purpose**: Provide config file path for CLI testing

Creates `live_test_config.yaml` in temp directory for CLI integration tests.

```python
@pytest.mark.live_api
def test_cli(live_test_config_path: Path) -> None:
    result = subprocess.run(
        ["gh-year-end", "plan", "--config", str(live_test_config_path)],
        capture_output=True
    )
    assert result.returncode == 0
```

## Usage

### Marking Tests

All tests using live fixtures must be marked with `@pytest.mark.live_api`:

```python
@pytest.mark.live_api
def test_live_collection(cached_raw_data: dict) -> None:
    assert "discovery" in cached_raw_data
```

For tests that run collection (slow), also use `@pytest.mark.slow`:

```python
@pytest.mark.live_api
@pytest.mark.slow
def test_full_pipeline(cached_raw_data: dict) -> None:
    # This test uses cached data, but mark it slow
    # since cached_raw_data runs collection on first use
    pass
```

### Running Tests

Run all live API tests:
```bash
uv run pytest -m live_api
```

Skip live API tests:
```bash
uv run pytest -m "not live_api"
```

Run with token (makes real API calls):
```bash
GITHUB_TOKEN=ghp_xxx uv run pytest -m live_api
```

Run without token (tests skip):
```bash
GITHUB_TOKEN="" uv run pytest -m live_api
# Output: tests/test_live_fixtures_demo.py::test_github_token_fixture SKIPPED
```

### Example Test

```python
"""Test using live fixtures."""
import pytest
from gh_year_end.config import Config
from gh_year_end.storage.paths import PathManager


@pytest.mark.live_api
def test_live_collection(
    cached_raw_data: dict,
    live_config: Config,
    live_paths: PathManager
) -> None:
    """Test that collection ran successfully.

    Args:
        cached_raw_data: Cached collection stats (runs once per session).
        live_config: Config for live testing.
        live_paths: PathManager for live tests.
    """
    # Verify collection completed
    assert "discovery" in cached_raw_data
    assert "duration_seconds" in cached_raw_data

    # Verify files exist
    assert live_paths.manifest_path.exists()
    assert live_paths.repos_raw_path.exists()

    # Verify config settings
    assert live_config.github.target.name == "github"
    assert live_config.github.windows.year == 2024
```

## Configuration Details

### Target Repository

Tests use the `github` org:
- **Mode**: org
- **Name**: github
- **Visibility**: public repos only
- **Year**: 2024 (stable historical data)
- **Forks**: excluded
- **Archived**: excluded

### Rate Limiting

Conservative settings to avoid secondary limits:
- **Strategy**: adaptive
- **Max concurrency**: 1 (sequential requests)
- **Min sleep**: 2.0 seconds between requests
- **Max sleep**: 60 seconds
- **Sample rate limit endpoint**: every 50 requests

### Collection Settings

All collectors enabled:
- Pull requests
- Issues
- Reviews
- Comments (issue + review)
- Commits (with files and classification)
- Hygiene (file presence, branch protection, security features)

Branch protection uses sampling (5 repos) to minimize API usage.

## Best Practices

1. **Use session-scoped fixtures**: Reuse data across tests
2. **Mark with `@pytest.mark.live_api`**: Enable filtering
3. **Skip gracefully**: Tests auto-skip without token
4. **Minimize API calls**: Use `cached_raw_data` to share collection results
5. **Test isolation**: Each session gets its own temp directory
6. **Conservative rate limiting**: Avoid hitting secondary limits
7. **Stable data**: Use historical data (2024) for reproducibility

## Troubleshooting

### Tests skip with "GITHUB_TOKEN not set"
Set token in environment:
```bash
export GITHUB_TOKEN=ghp_xxxxxxxxxxxxx
uv run pytest -m live_api
```

### Rate limit errors
The fixtures use conservative rate limiting. If you still hit limits:
1. Check `live_config` rate limit settings
2. Increase `min_sleep_seconds`
3. Ensure `max_concurrency=1`
4. Add delays between test runs

### Fixture not found
Ensure you're importing from `conftest.py`:
- Fixtures in `tests/conftest.py` are auto-discovered
- No import needed in test files
- Just use fixture as function parameter

### Collection fails
Check:
1. Token has required permissions (repo, read:org)
2. Network connectivity
3. GitHub API status (status.github.com)
4. Rate limit hasn't been hit elsewhere

## Files

- **`tests/conftest.py`**: Fixture definitions
- **`tests/fixtures/live_test_config.yaml`**: Static config file
- **`tests/test_live_fixtures_demo.py`**: Example usage
- **`pyproject.toml`**: Pytest markers configuration

## Related

- Issue #70: Live integration tests
- `tests/test_integration.py`: Integration tests with mocked API
- `tests/test_end_to_end.py`: End-to-end tests with fixture data
