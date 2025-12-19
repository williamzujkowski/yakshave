# Testing Guide

Comprehensive guide to running and writing tests for gh-year-end.

## Quick Reference

```bash
# Run all unit tests (fast, no API calls)
uv run pytest

# Run with coverage
uv run pytest --cov=src/gh_year_end --cov-report=term-missing

# Run specific test file
uv run pytest tests/test_config.py -v

# Run specific test class
uv run pytest tests/test_config.py::TestConfigLoading -v

# Run specific test function
uv run pytest tests/test_config.py::TestConfigLoading::test_load_valid_config -v

# Run tests matching pattern
uv run pytest -k "test_config" -v

# Run integration tests (requires GITHUB_TOKEN)
GITHUB_TOKEN=ghp_xxx uv run pytest -m integration

# Run live API tests (requires GITHUB_TOKEN, makes real API calls)
GITHUB_TOKEN=ghp_xxx uv run pytest -m live_api

# Skip slow tests
uv run pytest -m "not slow"

# Stop on first failure
uv run pytest -x

# Show local variables on failure
uv run pytest -l

# Run in parallel (requires pytest-xdist)
uv run pytest -n auto
```

## Test Organization

### Test Files (51 total)

**Configuration & Setup**
- `test_config.py` - Configuration loading and validation
- `test_paths.py` - Path management
- `test_logging.py` - Logging utilities
- `test_auth.py` - GitHub authentication
- `test_manifest.py` - Manifest tracking

**GitHub API & Collection**
- `test_http.py` - HTTP client
- `test_writer.py` - JSONL writer
- `test_ratelimit.py` - Rate limiting
- `test_ratelimit_enhanced.py` - Enhanced rate limiting
- `test_discovery.py` - Repository discovery
- `test_filters.py` - Repository filtering
- `test_checkpoint.py` - Collection checkpoint/resume
- `test_orchestrator.py` - Collection orchestrator
- `test_integration.py` - Integration tests (requires token)
- `test_github_integration.py` - GitHub integration tests
- `test_large_org_integration.py` - Large organization tests

**Normalization (Phase 4)**
- `test_normalize_common.py` - Common normalization utilities
- `test_normalize_repos.py` - Repository normalization
- `test_normalize_commits.py` - Commit normalization
- `test_normalize_issues.py` - Issue normalization
- `test_normalize_pulls.py` - Pull request normalization
- `test_normalize_reviews.py` - Review normalization
- `test_normalize_comments.py` - Comment normalization
- `test_normalize_users.py` - User normalization
- `test_normalize_hygiene.py` - Hygiene normalization
- `test_parquet_writer.py` - Parquet file writing

**Metrics (Phase 5)**
- `test_metrics_orchestrator.py` - Metrics orchestrator
- `test_metrics_awards.py` - Awards computation
- `test_metrics_hygiene.py` - Hygiene metrics
- `test_metrics_leaderboards.py` - Leaderboard computation
- `test_metrics_repo_health.py` - Repository health metrics
- `test_metrics_timeseries.py` - Time series metrics

**Reports (Phase 6)**
- `test_report_build.py` - Report building
- `test_report_export.py` - Metrics export to JSON
- `test_templates.py` - Template rendering
- `test_views_engineer.py` - Engineer view rendering
- `test_views_exec.py` - Executive view rendering
- `test_smoke_site.py` - Site smoke tests
- `test_cli_report.py` - CLI report command

**End-to-End & Pipeline**
- `test_end_to_end.py` - End-to-end tests with fixtures
- `test_pipeline_live_collect.py` - Live collection pipeline
- `test_pipeline_live_normalize.py` - Live normalization pipeline
- `test_pipeline_live_metrics.py` - Live metrics pipeline
- `test_pipeline_live_report.py` - Live report pipeline
- `test_pipeline_live_e2e.py` - Live end-to-end pipeline

**Utilities**
- `test_hygiene.py` - Hygiene collector utilities
- `test_identity.py` - Identity resolution
- `test_live_fixtures_demo.py` - Live fixtures demo

**Fixtures & Config**
- `conftest.py` - Shared test fixtures
- `fixtures/sample_org/` - Sample organization data

### Test Count

**771 total tests** across 51 test files

### Test Categories

#### Unit Tests (Default)
Fast tests with mocked dependencies. No external API calls or network access.

```bash
# Run all unit tests
uv run pytest

# Explicitly exclude integration and live_api tests
uv run pytest -m "not integration and not live_api"
```

#### Integration Tests (`@pytest.mark.integration`)
Tests that require `GITHUB_TOKEN` and make real GitHub API calls. Automatically skip if token not set.

**Test Files:**
- `test_integration.py` - Core integration tests
- `test_smoke_site.py` - Some tests marked integration

```bash
# Run only integration tests
GITHUB_TOKEN=ghp_xxx uv run pytest -m integration

# Skip integration tests
uv run pytest -m "not integration"
```

#### Live API Tests (`@pytest.mark.live_api`)
Tests using session-scoped fixtures that make real API calls against stable test data (github org, 2024).

**Test Files:**
- `test_pipeline_live_collect.py`
- `test_pipeline_live_normalize.py`
- `test_pipeline_live_metrics.py`
- `test_pipeline_live_report.py`
- `test_pipeline_live_e2e.py`
- `test_live_fixtures_demo.py`

**Key Features:**
- Session-scoped data collection (runs once per pytest session)
- Cached raw data shared across all tests
- Conservative rate limiting (concurrency=1, min_sleep=2.0s)
- Targets stable historical data (year 2024)
- Uses `github` org as stable test target

```bash
# Run only live API tests
GITHUB_TOKEN=ghp_xxx uv run pytest -m live_api

# Skip live API tests
uv run pytest -m "not live_api"
```

#### Slow Tests (`@pytest.mark.slow`)
Tests that take significant time (collection, large data processing).

```bash
# Skip slow tests
uv run pytest -m "not slow"

# Run only slow tests
uv run pytest -m slow
```

## Pytest Markers

Defined in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
markers = [
    "integration: marks tests as integration tests (require GITHUB_TOKEN)",
    "live_api: marks tests that make real GitHub API calls",
    "slow: marks tests as slow",
]
```

View all markers:
```bash
uv run pytest --markers
```

## Coverage Configuration

Defined in `pyproject.toml`:

```toml
[tool.coverage.run]
source = ["src/gh_year_end"]
branch = true
omit = [
    "src/gh_year_end/cli.py",  # CLI tested via integration tests
]

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "def __repr__",
    "raise NotImplementedError",
    "if TYPE_CHECKING:",
    "if __name__ == .__main__.:",
]
fail_under = 45  # Temporarily lowered for Phase 2
```

**Coverage Commands:**

```bash
# Run with coverage report
uv run pytest --cov=src/gh_year_end --cov-report=term-missing

# Generate HTML coverage report
uv run pytest --cov=src/gh_year_end --cov-report=html
open htmlcov/index.html

# Generate XML coverage report (for CI)
uv run pytest --cov=src/gh_year_end --cov-report=xml

# Show missing line numbers
uv run pytest --cov=src/gh_year_end --cov-report=term-missing

# Fail if coverage below threshold
uv run pytest --cov=src/gh_year_end --cov-fail-under=45
```

**Coverage Threshold:** 45% (temporarily lowered, target is 80%)

## Test Fixtures

### Shared Fixtures (`conftest.py`)

Session-scoped fixtures for live API testing:

#### `github_token`
**Scope:** Session
**Purpose:** Provide GitHub token from environment
**Behavior:** Skips test if `GITHUB_TOKEN` not set

```python
@pytest.mark.live_api
def test_something(github_token: str) -> None:
    # Will skip if GITHUB_TOKEN not set
    assert github_token
```

#### `live_config`
**Scope:** Session
**Purpose:** Config targeting stable test data
**Target:** github org, year 2024, public repos only

```python
@pytest.mark.live_api
def test_something(live_config: Config) -> None:
    assert live_config.github.target.name == "github"
    assert live_config.github.windows.year == 2024
```

#### `live_paths`
**Scope:** Session
**Purpose:** PathManager for test isolation
**Storage:** Session-scoped temp directories

```python
@pytest.mark.live_api
def test_something(live_paths: PathManager) -> None:
    assert live_paths.year == 2024
    assert live_paths.raw_root.exists()
```

#### `cached_raw_data`
**Scope:** Session
**Purpose:** Run collection once, cache for all tests
**WARNING:** Makes real GitHub API calls on first use

```python
@pytest.mark.live_api
@pytest.mark.slow
def test_something(cached_raw_data: dict, live_paths: PathManager) -> None:
    # Collection already ran, data is cached
    assert "discovery" in cached_raw_data
    assert live_paths.manifest_path.exists()
```

#### `live_test_config_path`
**Scope:** Session
**Purpose:** Config file path for CLI testing
**Output:** `live_test_config.yaml` in temp directory

```python
@pytest.mark.live_api
def test_cli(live_test_config_path: Path) -> None:
    result = subprocess.run(
        ["gh-year-end", "plan", "--config", str(live_test_config_path)],
        capture_output=True
    )
    assert result.returncode == 0
```

### Per-Test Fixtures

Many test files define their own fixtures for specific test scenarios. Examples:

- `temp_data_dir` - Temporary directories with auto-cleanup
- `mock_config` - Mocked configuration objects
- `sample_data` - Sample test data
- Mocked API responses using `respx`

## Common Test Patterns

### Testing with Mocked API

Using `respx` to mock HTTP responses:

```python
import respx
from httpx import Response

@respx.mock
async def test_api_call():
    respx.get("https://api.github.com/repos/owner/repo").mock(
        return_value=Response(200, json={"id": 123, "name": "repo"})
    )

    # Test code that makes API call
    result = await client.get_repo("owner", "repo")
    assert result["name"] == "repo"
```

### Testing with Temp Directories

```python
from pathlib import Path
import pytest

@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Pytest provides tmp_path fixture automatically."""
    return tmp_path

def test_file_operations(temp_dir: Path):
    test_file = temp_dir / "test.txt"
    test_file.write_text("hello")
    assert test_file.read_text() == "hello"
```

### Testing Async Functions

Using `pytest-asyncio`:

```python
import pytest

@pytest.mark.asyncio
async def test_async_function():
    result = await some_async_function()
    assert result == expected
```

### Parametrized Tests

```python
import pytest

@pytest.mark.parametrize("input,expected", [
    ("valid_input", True),
    ("invalid_input", False),
])
def test_validation(input: str, expected: bool):
    assert validate(input) == expected
```

## Running Tests in CI/CD

### GitHub Actions Example

```yaml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v1

      - name: Install dependencies
        run: uv sync

      - name: Run unit tests
        run: uv run pytest -v -m "not integration and not live_api"

      - name: Run integration tests
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: uv run pytest -v -m integration

      - name: Upload coverage
        uses: codecov/codecov-action@v3
        with:
          files: ./coverage.xml
```

## Writing Tests

### Test Structure

Follow the Arrange-Act-Assert pattern:

```python
def test_feature():
    # Arrange - set up test data and mocks
    config = Config.model_validate({...})

    # Act - execute the code being tested
    result = function_under_test(config)

    # Assert - verify the results
    assert result == expected_value
```

### Test Naming

Use descriptive names that explain what is being tested:

```python
# Good
def test_config_validates_year_boundaries():
    ...

def test_collector_skips_when_disabled():
    ...

# Bad
def test_config():
    ...

def test_collector():
    ...
```

### Test Documentation

Add docstrings to complex tests:

```python
def test_complex_scenario():
    """Test collection with multiple repos and rate limiting.

    This test verifies that:
    1. Rate limiting is respected
    2. Multiple repos are collected sequentially
    3. Errors are handled gracefully
    4. Manifest is updated correctly
    """
    ...
```

### Test Organization

Group related tests in classes:

```python
class TestConfigValidation:
    """Tests for configuration validation logic."""

    def test_valid_config(self):
        ...

    def test_invalid_config(self):
        ...

    def test_missing_required_fields(self):
        ...
```

## Debugging Tests

### Show Captured Output

```bash
# Show print statements and logging
uv run pytest -v -s

# Show captured output even for passing tests
uv run pytest -v --capture=no
```

### Debug with PDB

```bash
# Drop into debugger on failure
uv run pytest --pdb

# Drop into debugger on first failure
uv run pytest -x --pdb
```

### Verbose Output

```bash
# Show full diff on assertion failures
uv run pytest -vv

# Show local variables on failure
uv run pytest -l

# Show test names as they run
uv run pytest -v
```

### Run Specific Tests

```bash
# Run tests matching keyword
uv run pytest -k "normalize"

# Run tests NOT matching keyword
uv run pytest -k "not slow"

# Run specific test file
uv run pytest tests/test_config.py

# Run specific test class
uv run pytest tests/test_config.py::TestConfigLoading

# Run specific test function
uv run pytest tests/test_config.py::TestConfigLoading::test_load_valid_config
```

## Troubleshooting

### Tests Skip with "GITHUB_TOKEN not set"

Set token in environment:

```bash
export GITHUB_TOKEN=ghp_xxxxxxxxxxxxx
uv run pytest -m integration
```

Or inline:

```bash
GITHUB_TOKEN=ghp_xxx uv run pytest -m integration
```

### Import Errors

Ensure package is installed in development mode:

```bash
uv sync
```

### Fixture Not Found

Fixtures in `tests/conftest.py` are auto-discovered. No import needed.

For fixtures in other files, ensure the fixture file is imported or placed in `conftest.py`.

### Rate Limit Errors (Live API Tests)

Live API tests use conservative rate limiting. If you still hit limits:

1. Check `live_config` rate limit settings in `conftest.py`
2. Increase `min_sleep_seconds`
3. Ensure `max_concurrency=1`
4. Add delays between test runs
5. Check GitHub API status: https://www.githubstatus.com/

### Async Tests Not Running

Ensure `pytest-asyncio` is installed and test is marked:

```python
import pytest

@pytest.mark.asyncio
async def test_async_function():
    result = await async_function()
    assert result
```

### Collection Failures (Live Tests)

Check:
1. Token has required permissions (repo, read:org)
2. Network connectivity
3. GitHub API status
4. Rate limit hasn't been hit elsewhere

## Best Practices

### Unit Tests

1. **Mock external dependencies** - No real API calls, file I/O, or network
2. **Test one thing** - Each test should verify a single behavior
3. **Fast execution** - Unit tests should complete in milliseconds
4. **No side effects** - Tests should not modify global state
5. **Deterministic** - Tests should produce same results every run

### Integration Tests

1. **Mark appropriately** - Use `@pytest.mark.integration` or `@pytest.mark.live_api`
2. **Auto-skip gracefully** - Skip if credentials not available
3. **Minimize API calls** - Use cached data when possible
4. **Test isolation** - Use temporary directories
5. **Conservative rate limiting** - Respect API limits

### Test Data

1. **Use fixtures** - Centralize test data in fixtures
2. **Realistic data** - Test data should mirror production data structure
3. **Edge cases** - Include empty lists, None values, boundary conditions
4. **Clean up** - Remove test data after tests complete

### Test Coverage

1. **Focus on critical paths** - Prioritize testing core functionality
2. **Don't chase 100%** - Focus on meaningful coverage, not metrics
3. **Test error paths** - Verify error handling and edge cases
4. **Document gaps** - Use `pragma: no cover` with explanatory comments

## Quick Commands Summary

```bash
# Development workflow
uv run pytest                    # Run all unit tests
uv run pytest -x                 # Stop on first failure
uv run pytest -k "test_name"     # Run specific test
uv run pytest --lf               # Run last failed tests
uv run pytest --ff               # Run failed first, then rest

# Coverage
uv run pytest --cov=src/gh_year_end --cov-report=term-missing
uv run pytest --cov=src/gh_year_end --cov-report=html

# Markers
uv run pytest -m integration                    # Integration tests
uv run pytest -m live_api                       # Live API tests
uv run pytest -m "not integration"              # Skip integration
uv run pytest -m "not slow"                     # Skip slow tests
uv run pytest -m "integration and not slow"     # Combine markers

# Debugging
uv run pytest -v -s              # Verbose with output
uv run pytest -l                 # Show local variables on failure
uv run pytest --pdb              # Drop into debugger on failure
uv run pytest -vv                # Very verbose

# Pre-commit checks
uv run pytest -x -q              # Quick check, stop on first failure
```

## References

- **Pytest Documentation:** https://docs.pytest.org/
- **Pytest Best Practices:** https://docs.pytest.org/en/latest/goodpractices.html
- **pytest-asyncio:** https://pytest-asyncio.readthedocs.io/
- **respx (HTTP mocking):** https://lundberg.github.io/respx/
- **Coverage.py:** https://coverage.readthedocs.io/

## Related Documentation

- `/home/william/git/yakshave/CLAUDE.md` - Project standards and rules
- `/home/william/git/yakshave/tests/INTEGRATION_TESTS.md` - Integration test details
- `/home/william/git/yakshave/tests/LIVE_FIXTURES.md` - Live fixture documentation
- `/home/william/git/yakshave/tests/TESTING_SUMMARY.md` - Testing implementation summary
