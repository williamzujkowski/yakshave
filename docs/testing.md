# Testing Guide

**Status**: Active
**Coverage**: 45% (target: 80%)
**Test Count**: 771 tests across 51 files

---

## Quick Start

```bash
# Run all tests
uv run pytest

# Verbose output
uv run pytest -v

# With coverage report
uv run pytest --cov=src/gh_year_end --cov-report=term-missing

# Parallel execution (faster)
uv run pytest -n auto
```

---

## Test Categories

### Unit Tests (Default)

Fast, mocked, no external dependencies. Run by default.

```bash
uv run pytest
```

- Mock all external APIs
- No GITHUB_TOKEN required
- Execution time: < 1 second per test
- Use `respx` for HTTP mocking

### Integration Tests

Real API calls, requires authentication. Marked with `@pytest.mark.integration`.

```bash
GITHUB_TOKEN=ghp_xxx uv run pytest -m integration
```

- Makes actual GitHub API calls
- Requires valid GITHUB_TOKEN
- May consume API rate limits
- Use sparingly in CI

### Live API Tests

Real API calls with session-level caching. Marked with `@pytest.mark.live_api`.

```bash
GITHUB_TOKEN=ghp_xxx uv run pytest -m live_api
```

- Uses `pytest-recording` for VCR-style caching
- First run records responses
- Subsequent runs replay from cache
- Balance between unit and integration

### Slow Tests

Long-running tests (>1s). Marked with `@pytest.mark.slow`.

```bash
# Skip slow tests
uv run pytest -m "not slow"

# Run only slow tests
uv run pytest -m slow
```

- End-to-end pipeline tests
- Large data processing
- Multi-step workflows

---

## Running Specific Tests

### By File

```bash
# Single test file
uv run pytest tests/test_config.py

# Multiple files
uv run pytest tests/test_config.py tests/test_cli.py
```

### By Class or Function

```bash
# Single test class
uv run pytest tests/test_config.py::TestConfigValidation

# Single test function
uv run pytest tests/test_config.py::test_load_valid_config

# Class method
uv run pytest tests/test_config.py::TestConfigValidation::test_missing_required_field
```

### By Pattern

```bash
# Match test names
uv run pytest -k "test_ratelimit"

# Multiple patterns (OR)
uv run pytest -k "test_ratelimit or test_retry"

# Negative match
uv run pytest -k "not slow"
```

### By Directory

```bash
# Phase-specific tests
uv run pytest tests/test_github_*.py  # Phase 1
uv run pytest tests/test_normalize.py  # Phase 4
uv run pytest tests/test_metrics.py    # Phase 5
uv run pytest tests/test_report_*.py   # Phase 6
```

---

## Markers

Pytest markers control test selection and behavior.

### Available Markers

| Marker | Purpose | Usage |
|--------|---------|-------|
| `integration` | Real API calls | `GITHUB_TOKEN=xxx uv run pytest -m integration` |
| `live_api` | Cached API calls | `GITHUB_TOKEN=xxx uv run pytest -m live_api` |
| `slow` | Long-running tests | `uv run pytest -m slow` |
| `asyncio` | Async tests | Automatic via pytest-asyncio |

### Combining Markers

```bash
# Skip integration AND slow
uv run pytest -m "not integration and not slow"

# Run integration OR live_api
uv run pytest -m "integration or live_api"

# Complex logic
uv run pytest -m "not (integration or slow)"
```

### Skip Integration Tests

Most common during development:

```bash
uv run pytest -m "not integration"
```

---

## Coverage

### Generate Coverage Reports

```bash
# Terminal report
uv run pytest --cov=src/gh_year_end --cov-report=term

# Terminal with missing lines
uv run pytest --cov=src/gh_year_end --cov-report=term-missing

# HTML report (opens in browser)
uv run pytest --cov=src/gh_year_end --cov-report=html
open htmlcov/index.html

# XML report (for CI)
uv run pytest --cov=src/gh_year_end --cov-report=xml

# Multiple formats
uv run pytest --cov=src/gh_year_end --cov-report=term --cov-report=html
```

### Coverage Thresholds

```bash
# Fail if coverage < 80%
uv run pytest --cov=src/gh_year_end --cov-fail-under=80
```

**Current**: 45%
**Target**: 80%
**Blocker**: P1 (next release)

### Coverage by Phase

| Phase | Module | Coverage |
|-------|--------|----------|
| Phase 1 | `github/` | ~70% |
| Phase 2 | `collector/` | ~60% |
| Phase 3 | `hygiene/` | ~55% |
| Phase 4 | `normalize/` | ~50% |
| Phase 5 | `metrics/` | ~40% |
| Phase 6 | `report/` | ~30% |

---

## Test Organization

### Phase 1: GitHub Client & Discovery

```
tests/test_github_client.py       # API client
tests/test_github_ratelimit.py    # Rate limiting
tests/test_github_retry.py        # Retry logic
tests/test_github_discovery.py    # Repo discovery
```

**Run**: `uv run pytest tests/test_github_*.py`

### Phase 2: Data Collection

```
tests/test_collector_base.py      # Base collector
tests/test_collector_commits.py   # Commit collection
tests/test_collector_prs.py       # PR collection
tests/test_collector_issues.py    # Issue collection
tests/test_collector_reviews.py   # Review collection
```

**Run**: `uv run pytest tests/test_collector_*.py`

### Phase 3: Hygiene Snapshots

```
tests/test_hygiene_snapshot.py    # Snapshot logic
tests/test_hygiene_storage.py     # Storage format
```

**Run**: `uv run pytest tests/test_hygiene_*.py`

### Phase 4: Normalization

```
tests/test_normalize.py           # Main normalization
tests/test_normalize_commits.py   # Commit normalization
tests/test_normalize_prs.py       # PR normalization
tests/test_normalize_identity.py  # Identity resolution
```

**Run**: `uv run pytest tests/test_normalize*.py`

### Phase 5: Metrics

```
tests/test_metrics.py             # Metrics engine
tests/test_metrics_engineer.py    # Engineer metrics
tests/test_metrics_exec.py        # Executive metrics
tests/test_metrics_aggregate.py   # Aggregations
```

**Run**: `uv run pytest tests/test_metrics*.py`

### Phase 6: Report Generation

```
tests/test_report_build.py        # Report builder
tests/test_report_export.py       # Export formats
tests/test_cli_report.py          # CLI integration
tests/test_templates.py           # Template rendering
tests/test_views_engineer.py      # Engineer views
tests/test_views_exec.py          # Executive views
tests/test_smoke_site.py          # Site validation
```

**Run**: `uv run pytest tests/test_report_*.py tests/test_*_*.py`

### End-to-End Tests

```
tests/test_end_to_end.py          # Full pipeline
tests/test_github_integration.py  # GitHub integration
```

**Run**: `uv run pytest tests/test_end_to_end.py tests/test_github_integration.py`

---

## Writing Tests

### Test Structure

```python
# tests/test_example.py
import pytest
from gh_year_end.example import function_to_test


class TestExample:
    """Group related tests in classes."""

    def test_basic_case(self):
        """Test the happy path."""
        result = function_to_test("input")
        assert result == "expected"

    def test_edge_case(self):
        """Test edge cases."""
        result = function_to_test("")
        assert result is None

    def test_error_case(self):
        """Test error handling."""
        with pytest.raises(ValueError, match="invalid input"):
            function_to_test(None)
```

### Async Tests

Use `pytest-asyncio` for async functions:

```python
import pytest


@pytest.mark.asyncio
async def test_async_function():
    """Test async code."""
    result = await async_function()
    assert result == "expected"
```

### HTTP Mocking with respx

Mock external API calls with `respx`:

```python
import httpx
import pytest
import respx


@pytest.mark.asyncio
@respx.mock
async def test_api_call():
    """Mock HTTP requests."""
    # Mock the response
    respx.get("https://api.github.com/user").mock(
        return_value=httpx.Response(200, json={"login": "testuser"})
    )

    # Test code that makes the request
    async with httpx.AsyncClient() as client:
        response = await client.get("https://api.github.com/user")
        data = response.json()

    assert data["login"] == "testuser"
```

### Fixtures

Use fixtures for common test data:

```python
import pytest


@pytest.fixture
def sample_config():
    """Provide sample config for tests."""
    return {
        "github": {
            "org": "test-org",
            "year": 2024,
        }
    }


def test_with_fixture(sample_config):
    """Use fixture in test."""
    assert sample_config["github"]["org"] == "test-org"
```

### Parametrized Tests

Test multiple cases efficiently:

```python
import pytest


@pytest.mark.parametrize(
    "input_value,expected",
    [
        ("abc", 3),
        ("", 0),
        ("hello", 5),
    ],
)
def test_length(input_value, expected):
    """Test multiple inputs."""
    assert len(input_value) == expected
```

### Temporary Files

Use `tmp_path` fixture for file operations:

```python
def test_file_write(tmp_path):
    """Test file writing."""
    file_path = tmp_path / "test.txt"
    file_path.write_text("content")
    assert file_path.read_text() == "content"
```

### Environment Variables

Use `monkeypatch` for environment variables:

```python
def test_with_env_var(monkeypatch):
    """Test with environment variable."""
    monkeypatch.setenv("GITHUB_TOKEN", "test_token")
    # Test code that reads GITHUB_TOKEN
    assert os.getenv("GITHUB_TOKEN") == "test_token"
```

---

## Patterns from Existing Tests

### Pattern 1: Collector Tests

```python
import pytest
import respx
from gh_year_end.collector.commits import CommitCollector


@pytest.mark.asyncio
@respx.mock
async def test_collect_commits(tmp_path):
    """Test commit collection."""
    # Mock API response
    respx.get("https://api.github.com/repos/org/repo/commits").mock(
        return_value=httpx.Response(200, json=[{"sha": "abc123"}])
    )

    # Run collector
    collector = CommitCollector(config, client)
    await collector.collect(tmp_path)

    # Verify output
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["count"] == 1
```

### Pattern 2: Normalization Tests

```python
import duckdb
import pytest
from gh_year_end.normalize import normalize_commits


def test_normalize_commits(tmp_path):
    """Test commit normalization."""
    # Create raw data
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    raw_file = raw_dir / "commits.jsonl"
    raw_file.write_text('{"sha": "abc123", "commit": {"author": {"date": "2024-01-01T00:00:00Z"}}}\n')

    # Run normalization
    output_file = tmp_path / "commits.parquet"
    normalize_commits(raw_dir, output_file)

    # Verify output
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{output_file}')").df()
    assert len(df) == 1
    assert df.iloc[0]["sha"] == "abc123"
```

### Pattern 3: Metrics Tests

```python
import duckdb
import pytest
from gh_year_end.metrics import calculate_engineer_metrics


def test_engineer_metrics(tmp_path):
    """Test engineer metric calculation."""
    # Create normalized data
    commits_file = tmp_path / "commits.parquet"
    # ... write test data to parquet

    # Calculate metrics
    output_file = tmp_path / "engineer_metrics.parquet"
    calculate_engineer_metrics(commits_file, output_file)

    # Verify metrics
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{output_file}')").df()
    assert "commit_count" in df.columns
    assert df.iloc[0]["commit_count"] > 0
```

### Pattern 4: CLI Tests

```python
from click.testing import CliRunner
from gh_year_end.cli import cli


def test_cli_command(tmp_path):
    """Test CLI command."""
    runner = CliRunner()
    result = runner.invoke(cli, ["plan", "--config", str(tmp_path / "config.yaml")])
    assert result.exit_code == 0
    assert "Execution Plan" in result.output
```

---

## Continuous Integration

### GitHub Actions Workflow

Tests run automatically on:
- Push to any branch
- Pull request creation/update
- Manual workflow dispatch

```yaml
# .github/workflows/ci.yml
- name: Run tests
  run: |
    uv run pytest --cov=src/gh_year_end --cov-report=xml
```

### Pre-commit Checks

Run before every commit:

```bash
# Format
ruff format .

# Lint
ruff check .

# Type check
mypy src/

# Tests
uv run pytest -m "not integration"
```

---

## Troubleshooting

### Tests Fail with "No module named X"

Ensure dependencies are installed:

```bash
uv sync
```

### Integration Tests Fail with 401

Set GITHUB_TOKEN:

```bash
export GITHUB_TOKEN=ghp_xxxxx
uv run pytest -m integration
```

### Tests Hang Indefinitely

Kill hung processes:

```bash
pkill -f pytest
```

Check for:
- Infinite loops
- Missing timeouts on async operations
- Deadlocks in concurrent code

### Coverage Report Not Generated

Ensure coverage package is installed:

```bash
uv add --dev pytest-cov
```

### Flaky Tests

- Add retries for network tests
- Use deterministic test data
- Avoid time-dependent assertions
- Mock external dependencies

---

## Best Practices

1. **One assertion per test** (or closely related assertions)
2. **Clear test names** describing what is tested
3. **Arrange-Act-Assert** structure
4. **Mock external dependencies** in unit tests
5. **Use fixtures** for common setup
6. **Parametrize** similar test cases
7. **Test edge cases** and error paths
8. **Keep tests fast** (<100ms for unit tests)
9. **Avoid test interdependencies**
10. **Document complex test logic**

---

## Resources

- [pytest documentation](https://docs.pytest.org/)
- [pytest-asyncio](https://pytest-asyncio.readthedocs.io/)
- [respx documentation](https://lundberg.github.io/respx/)
- [Coverage.py](https://coverage.readthedocs.io/)
- [CLAUDE.md testing standards](../CLAUDE.md#testing)
