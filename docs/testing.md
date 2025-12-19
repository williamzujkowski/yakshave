# Testing Guide

**Status**: Authoritative
**Last Updated**: 2025-12-18
**Total Tests**: 811
**Coverage**: 45% (target: 80%)

This document provides comprehensive guidance for testing gh-year-end.

## Quick Start

```bash
# Run all unit tests (fast, no GitHub token needed)
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/test_config.py -v

# Run with coverage report
uv run pytest --cov=src/gh_year_end --cov-report=term-missing

# Run integration tests (requires GITHUB_TOKEN)
GITHUB_TOKEN=ghp_xxx uv run pytest -m integration

# Run live API tests (requires GITHUB_TOKEN)
GITHUB_TOKEN=ghp_xxx uv run pytest -m live_api
```

## Test Commands and Options

### Basic Commands

| Command | Description |
|---------|-------------|
| `uv run pytest` | Run all unit tests (excludes integration) |
| `uv run pytest -v` | Verbose output with test names |
| `uv run pytest -x` | Stop on first failure |
| `uv run pytest -k "test_name"` | Run tests matching pattern |
| `uv run pytest tests/test_config.py` | Run specific file |
| `uv run pytest tests/test_config.py::TestClass::test_method` | Run specific test |

### Advanced Options

| Option | Description |
|--------|-------------|
| `-v` or `--verbose` | Show detailed test output |
| `-s` | Show print statements (disable capture) |
| `-x` | Stop on first failure |
| `--tb=short` | Short traceback format |
| `--tb=long` | Full traceback format |
| `-q` | Quiet output (less verbose) |
| `--collect-only` | Show which tests would run |
| `--lf` | Run last failed tests |
| `--ff` | Run failed tests first |

### Parallel Execution

```bash
# Install pytest-xdist
uv pip install pytest-xdist

# Run tests in parallel (4 workers)
uv run pytest -n 4
```

## Test Markers

Markers categorize tests for selective execution. Configure via `pyproject.toml` and use with `@pytest.mark.marker_name`.

### Available Markers

| Marker | Description | Usage | Requires |
|--------|-------------|-------|----------|
| `integration` | Integration tests with mocked APIs | `@pytest.mark.integration` | None |
| `live_api` | Tests making real GitHub API calls | `@pytest.mark.live_api` | GITHUB_TOKEN |
| `slow` | Slow-running tests (>5s) | `@pytest.mark.slow` | None |

### Running by Marker

```bash
# Run only integration tests
uv run pytest -m integration

# Run only live API tests
GITHUB_TOKEN=ghp_xxx uv run pytest -m live_api

# Exclude slow tests
uv run pytest -m "not slow"

# Run integration OR live_api tests
uv run pytest -m "integration or live_api"

# Run integration tests that are NOT slow
uv run pytest -m "integration and not slow"
```

### Test Counts by Marker

```bash
# Total tests
uv run pytest --collect-only -q | tail -1
# Output: 811 tests collected

# Integration tests
uv run pytest -m integration --collect-only -q | tail -1
# Output: 10 tests collected

# Live API tests
uv run pytest -m live_api --collect-only -q | tail -1
# Output: 54 tests collected

# Slow tests
uv run pytest -m slow --collect-only -q | tail -1
# Output: 1 test collected
```

## Coverage

### Running Coverage

```bash
# Basic coverage report
uv run pytest --cov=src/gh_year_end

# Coverage with missing lines
uv run pytest --cov=src/gh_year_end --cov-report=term-missing

# Generate HTML coverage report
uv run pytest --cov=src/gh_year_end --cov-report=html

# Open HTML report in browser
open htmlcov/index.html

# Generate XML report (for CI)
uv run pytest --cov=src/gh_year_end --cov-report=xml
```

### Coverage Configuration

Coverage settings in `pyproject.toml`:

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
fail_under = 45  # Current: 45%, Target: 80%
```

### Coverage Targets

| Category | Current | Target | Priority |
|----------|---------|--------|----------|
| Overall | 45% | 80% | P1 |
| Phase 1 (GitHub Client) | ~60% | 80% | P1 |
| Phase 2 (Collectors) | ~30% | 70% | P0 |
| Phase 3 (Hygiene) | ~50% | 75% | P2 |
| Phase 4 (Normalization) | ~60% | 85% | P1 |
| Phase 5 (Metrics) | ~70% | 90% | P1 |
| Phase 6 (Report) | ~55% | 80% | P1 |

### Improving Coverage

To identify untested code:

```bash
# Show missing lines in terminal
uv run pytest --cov=src/gh_year_end --cov-report=term-missing | grep "^src"

# Generate HTML report and review
uv run pytest --cov=src/gh_year_end --cov-report=html
open htmlcov/index.html
```

## Writing Tests

### Test File Structure

```python
"""Test description.

Brief overview of what this test module covers.
"""

import pytest
from pathlib import Path

from gh_year_end.module import function_to_test


class TestFeatureName:
    """Tests for specific feature."""

    def test_basic_case(self) -> None:
        """Test basic functionality."""
        result = function_to_test("input")
        assert result == "expected"

    def test_edge_case(self) -> None:
        """Test edge case handling."""
        with pytest.raises(ValueError):
            function_to_test(None)

    @pytest.mark.integration
    def test_integration(self, github_token: str) -> None:
        """Test with external dependencies."""
        # Test code using real API
        pass
```

### Naming Conventions

| Item | Convention | Example |
|------|------------|---------|
| Test files | `test_*.py` | `test_config.py` |
| Test classes | `TestFeatureName` | `TestConfigLoading` |
| Test functions | `test_description` | `test_load_valid_config` |
| Fixtures | `descriptive_name` | `github_token`, `live_config` |

### Test Guidelines

1. **One assertion per test** (when possible)
   ```python
   # Good
   def test_config_loads_org_name(self) -> None:
       config = load_config(path)
       assert config.github.target.name == "test-org"

   # Avoid
   def test_config_loads(self) -> None:
       config = load_config(path)
       assert config.github.target.name == "test-org"
       assert config.github.target.mode == "org"
       assert config.github.windows.year == 2024
   ```

2. **Descriptive test names** - Test name should describe what it tests
   ```python
   # Good
   def test_load_missing_file_raises_file_not_found_error(self) -> None:

   # Avoid
   def test_load_error(self) -> None:
   ```

3. **Use fixtures for setup** - Avoid repetitive setup code
   ```python
   @pytest.fixture
   def sample_config(tmp_path: Path) -> Config:
       """Create test config."""
       return Config.model_validate({...})

   def test_with_config(self, sample_config: Config) -> None:
       assert sample_config.github.target.mode == "org"
   ```

4. **Test both success and failure cases**
   ```python
   def test_valid_input(self) -> None:
       result = parse("valid")
       assert result is not None

   def test_invalid_input_raises(self) -> None:
       with pytest.raises(ValueError):
           parse("invalid")
   ```

5. **Use parametrize for multiple similar tests**
   ```python
   @pytest.mark.parametrize("mode,expected", [
       ("org", True),
       ("user", True),
       ("invalid", False),
   ])
   def test_target_mode_validation(self, mode: str, expected: bool) -> None:
       # Test implementation
       pass
   ```

### Async Tests

For async functions, use `@pytest.mark.asyncio`:

```python
import pytest

@pytest.mark.asyncio
async def test_async_function(self) -> None:
    """Test async functionality."""
    result = await async_function()
    assert result == expected
```

### Testing Exceptions

```python
# Test exception is raised
def test_raises_value_error(self) -> None:
    with pytest.raises(ValueError):
        function_that_raises()

# Test exception message
def test_raises_with_message(self) -> None:
    with pytest.raises(ValueError, match="invalid input"):
        function_that_raises()

# Test exception with inspection
def test_exception_details(self) -> None:
    with pytest.raises(ValueError) as exc_info:
        function_that_raises()

    assert "invalid" in str(exc_info.value)
```

### Testing File Operations

Use `tmp_path` fixture for temporary files:

```python
def test_writes_file(self, tmp_path: Path) -> None:
    """Test file writing."""
    output_file = tmp_path / "output.json"
    write_json(output_file, {"key": "value"})

    assert output_file.exists()
    assert output_file.read_text() == '{"key": "value"}'
```

## Test Fixtures

Fixtures provide reusable test setup and data. Defined in `tests/conftest.py`.

### Session-Scoped Fixtures

Run once per test session (expensive operations).

| Fixture | Scope | Description | Dependencies |
|---------|-------|-------------|--------------|
| `github_token` | session | GitHub API token from env | GITHUB_TOKEN env var |
| `live_config` | session | Config for live API tests | `tmp_path_factory` |
| `live_paths` | session | PathManager for live tests | `live_config` |
| `cached_raw_data` | session | Cached collection data | `github_token`, `live_config`, `live_paths` |
| `live_test_config_path` | session | YAML config file for CLI | `tmp_path_factory` |

### Function-Scoped Fixtures

Run once per test function (fast operations).

| Fixture | Scope | Description | Dependencies |
|---------|-------|-------------|--------------|
| `sample_metrics_dir` | function | Path to sample metrics fixtures | None |
| `sample_metrics_config` | function | Config using sample data | `tmp_path`, `sample_metrics_dir` |
| `sample_metrics_paths` | function | PathManager for sample tests | `sample_metrics_config` |

### Using Fixtures

```python
def test_with_fixture(self, github_token: str) -> None:
    """Test using github_token fixture."""
    # Test will be skipped if GITHUB_TOKEN not set
    assert github_token.startswith("ghp_")

def test_with_multiple_fixtures(
    self,
    live_config: Config,
    live_paths: PathManager
) -> None:
    """Test using multiple fixtures."""
    assert live_config.github.target.name == "github"
    assert live_paths.root.exists()
```

### Built-in Pytest Fixtures

| Fixture | Description | Example |
|---------|-------------|---------|
| `tmp_path` | Temporary directory (per test) | `tmp_path / "file.txt"` |
| `tmp_path_factory` | Temporary directory factory (session) | `tmp_path_factory.mktemp("data")` |
| `monkeypatch` | Mock/patch objects | `monkeypatch.setenv("VAR", "value")` |
| `capsys` | Capture stdout/stderr | `captured = capsys.readouterr()` |
| `caplog` | Capture log messages | `assert "error" in caplog.text` |

### Creating Custom Fixtures

```python
# In tests/conftest.py or test file
@pytest.fixture
def sample_data() -> dict[str, Any]:
    """Create sample test data."""
    return {
        "user": "test-user",
        "repos": ["repo1", "repo2"],
    }

@pytest.fixture
def temp_config(tmp_path: Path) -> Path:
    """Create temporary config file."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("github:\n  target:\n    mode: org\n")
    return config_path
```

## CI/CD Integration

### GitHub Actions Workflow

Tests run automatically on push and pull requests via `.github/workflows/ci.yml`.

#### Jobs

| Job | Description | Runs On | Required |
|-----|-------------|---------|----------|
| `lint` | Ruff linting | All commits | Yes (P0) |
| `typecheck` | MyPy type checking | All commits | Yes (P0) |
| `security` | Bandit security scan | All commits | No (P1) |
| `dependency-check` | Safety vulnerability check | All commits | No (P1) |
| `test` | Unit tests (Python 3.11, 3.12) | All commits | Yes (P0) |
| `integration-test` | Integration tests | All commits | No (P1) |
| `live-integration-test` | Live API tests | Push/same-repo PRs | No (P1) |
| `site-validation` | Site generation checks | After tests | No (P1) |
| `quality-gate` | Overall quality check | After all jobs | Yes (P0) |

#### Test Job Configuration

```yaml
test:
  name: Test (Python ${{ matrix.python-version }})
  runs-on: ubuntu-latest
  strategy:
    matrix:
      python-version: ["3.11", "3.12"]
  steps:
    - uses: actions/checkout@v4
    - name: Install uv
      uses: astral-sh/setup-uv@v4
    - name: Set up Python
      run: uv python install ${{ matrix.python-version }}
    - name: Install dependencies
      run: uv sync --all-extras
    - name: Run unit tests with coverage
      run: uv run pytest -m "not integration" --cov=src/gh_year_end --cov-report=xml
    - name: Upload coverage
      uses: codecov/codecov-action@v4
```

#### Live API Tests

```yaml
live-integration-test:
  name: Live API Integration Tests
  runs-on: ubuntu-latest
  if: github.event_name == 'push' || github.event.pull_request.head.repo.full_name == github.repository
  steps:
    - name: Run live integration tests
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      run: |
        uv run pytest -m "live_api" \
          --tb=short \
          -v \
          --timeout=300 \
          tests/test_pipeline_live*.py
      timeout-minutes: 10
```

### Local Pre-Commit Checks

Run before committing:

```bash
# Full pre-commit check
ruff check . && ruff format --check . && mypy src/ && uv run pytest

# Quick check (format + unit tests)
ruff format . && uv run pytest -x

# With integration tests
ruff check . && ruff format --check . && mypy src/ && \
  GITHUB_TOKEN=ghp_xxx uv run pytest
```

### Quality Gates

| Priority | Blocks Merge | Checks |
|----------|-------------|--------|
| P0 | Yes | Lint errors, type errors, test failures |
| P1 | No (warns) | Security issues, coverage <80%, integration test failures |
| P2 | No (tracks) | Complexity warnings, documentation gaps |

## Test Organization

### Test Distribution

```
Total Tests: 811
By Type:
  - Unit Tests: ~757 (93%)
  - Integration Tests: ~10 (1%)
  - Live API Tests: ~54 (7%)

By Phase:
  - Phase 1 (GitHub Client): ~150 tests
  - Phase 2 (Collectors): ~200 tests
  - Phase 3 (Hygiene): ~50 tests
  - Phase 4 (Normalization): ~150 tests
  - Phase 5 (Metrics): ~150 tests
  - Phase 6 (Report): ~70 tests
  - End-to-End: ~40 tests
```

### Test Files

```
tests/
├── conftest.py                          # Shared fixtures
├── fixtures/                            # Test data
│   ├── sample_metrics/                  # Pre-generated test data
│   ├── valid_config.yaml                # Example configs
│   └── invalid_date_config.yaml
├── test_config.py                       # Configuration tests
├── test_auth.py                         # Authentication tests
├── test_http.py                         # HTTP client tests
├── test_ratelimit.py                    # Rate limiting tests
├── test_discovery.py                    # Repository discovery
├── test_orchestrator.py                 # Collection orchestration
├── test_normalize_*.py                  # Normalization tests (8 files)
├── test_metrics_*.py                    # Metrics tests (5 files)
├── test_report_*.py                     # Report generation tests
├── test_pipeline_live_*.py              # Live API integration (5 files)
├── test_integration.py                  # Integration tests
├── test_end_to_end.py                   # End-to-end tests
└── test_smoke_site.py                   # Site generation smoke tests
```

## Common Testing Scenarios

### Testing Configuration

```python
def test_load_valid_config(self) -> None:
    """Test loading valid configuration."""
    config = load_config(Path("fixtures/valid_config.yaml"))
    assert config.github.target.mode == "org"

def test_invalid_config_raises(self) -> None:
    """Test invalid config raises ValidationError."""
    with pytest.raises(ValidationError):
        load_config(Path("fixtures/invalid_config.yaml"))
```

### Testing API Clients

```python
@pytest.mark.asyncio
async def test_fetch_repos(self, respx_mock: Any) -> None:
    """Test repository fetching with mocked API."""
    respx_mock.get("https://api.github.com/orgs/test/repos").mock(
        return_value=httpx.Response(200, json=[{"name": "repo1"}])
    )

    repos = await client.fetch_repos("test")
    assert len(repos) == 1
    assert repos[0]["name"] == "repo1"
```

### Testing Data Processing

```python
def test_normalize_pull_request(self) -> None:
    """Test PR normalization."""
    raw_pr = {"number": 123, "title": "Test", "state": "open"}
    normalized = normalize_pull_request(raw_pr)

    assert normalized["pr_number"] == 123
    assert normalized["pr_title"] == "Test"
    assert normalized["pr_state"] == "open"
```

### Testing Report Generation

```python
def test_build_summary_report(
    self,
    sample_metrics_config: Config,
    sample_metrics_paths: PathManager
) -> None:
    """Test summary report generation."""
    report = build_summary_report(sample_metrics_config, sample_metrics_paths)

    assert "total_prs" in report
    assert "total_contributors" in report
    assert report["total_prs"] > 0
```

## Troubleshooting

### Common Issues

#### Tests Not Found

```bash
# Problem: pytest can't find tests
# Solution: Ensure test files match pattern
uv run pytest --collect-only  # See what pytest finds

# Check naming
ls tests/test_*.py  # Test files
grep -r "def test_" tests/  # Test functions
```

#### Import Errors

```bash
# Problem: ImportError: cannot import name 'X'
# Solution: Ensure package installed in editable mode
uv sync --all-extras

# Verify installation
uv pip list | grep gh-year-end
```

#### Fixture Not Found

```bash
# Problem: fixture 'X' not found
# Solution: Check fixture definition in conftest.py
grep -r "@pytest.fixture" tests/conftest.py

# List all fixtures
uv run pytest --fixtures
```

#### Tests Hang

```bash
# Problem: Tests hang indefinitely
# Solution: Add timeout
uv run pytest --timeout=30

# Or use pytest-timeout
uv pip install pytest-timeout
```

#### Coverage Not Working

```bash
# Problem: Coverage shows 0%
# Solution: Ensure source path correct
uv run pytest --cov=src/gh_year_end --cov-report=term

# Check coverage config
grep -A 5 "\[tool.coverage" pyproject.toml
```

### GitHub Token Issues

```bash
# Problem: Tests skip with "GITHUB_TOKEN not set"
# Solution: Set token in environment
export GITHUB_TOKEN=ghp_your_token_here

# Verify
echo $GITHUB_TOKEN

# Run integration tests
uv run pytest -m integration
```

### Rate Limiting

```bash
# Problem: Live tests hit rate limits
# Solution: Reduce concurrency in live_config
# Edit tests/conftest.py, set max_concurrency: 1

# Or space out test runs
sleep 60 && uv run pytest -m live_api
```

## Best Practices

### Test Independence

Tests must be independent and not rely on execution order.

```python
# Good - each test creates own data
def test_a(self, tmp_path: Path) -> None:
    data = tmp_path / "data.json"
    write_data(data)
    assert data.exists()

def test_b(self, tmp_path: Path) -> None:
    data = tmp_path / "data.json"
    write_data(data)
    assert data.exists()

# Avoid - tests share state
class TestShared:
    data_file = Path("shared_data.json")

    def test_a(self) -> None:
        write_data(self.data_file)  # Creates file

    def test_b(self) -> None:
        # Assumes test_a ran first!
        assert self.data_file.exists()
```

### Fast Tests

Keep unit tests fast (<100ms each) by:

1. **Mock external dependencies**
   ```python
   @pytest.fixture
   def mock_api(monkeypatch: pytest.MonkeyPatch) -> None:
       monkeypatch.setattr("module.api_call", lambda: {"mock": "data"})
   ```

2. **Use session-scoped fixtures for expensive setup**
   ```python
   @pytest.fixture(scope="session")
   def expensive_data() -> dict:
       # Runs once per test session
       return load_large_dataset()
   ```

3. **Avoid unnecessary I/O**
   ```python
   # Good - in-memory
   def test_process(self) -> None:
       result = process_data({"key": "value"})

   # Avoid - file I/O when not needed
   def test_process(self, tmp_path: Path) -> None:
       file = tmp_path / "data.json"
       file.write_text('{"key": "value"}')
       result = process_file(file)
   ```

### Clear Assertions

```python
# Good - specific assertion
assert len(results) == 3
assert results[0]["name"] == "expected"

# Avoid - generic assertion
assert results

# Good - clear error message
assert config.year == 2024, f"Expected year 2024, got {config.year}"

# Good - multiple assertions for clarity
assert "error" in response
assert response["error"]["code"] == 404
```

### Test Documentation

```python
def test_edge_case(self) -> None:
    """Test handling of edge case X.

    This test verifies that when condition Y occurs,
    the system correctly handles it by doing Z.

    Regression test for issue #123.
    """
    # Test implementation
```

## Additional Resources

### Documentation

- [CLAUDE.md](/home/william/git/yakshave/CLAUDE.md) - Project instructions
- [tests/TESTING_SUMMARY.md](/home/william/git/yakshave/tests/TESTING_SUMMARY.md) - Testing summary
- [pytest documentation](https://docs.pytest.org/)
- [pytest-asyncio](https://pytest-asyncio.readthedocs.io/)
- [coverage.py](https://coverage.readthedocs.io/)

### Commands Reference

```bash
# List all available markers
uv run pytest --markers

# List all fixtures
uv run pytest --fixtures

# Show test collection tree
uv run pytest --collect-only

# Run tests matching pattern
uv run pytest -k "config or auth"

# Verbose output with captured stdout
uv run pytest -v -s

# Stop after N failures
uv run pytest --maxfail=3

# Re-run failed tests from last run
uv run pytest --lf

# Run tests in random order (requires pytest-random-order)
uv pip install pytest-random-order
uv run pytest --random-order
```

### Environment Variables

| Variable | Purpose | Example |
|----------|---------|---------|
| `GITHUB_TOKEN` | GitHub API authentication | `export GITHUB_TOKEN=ghp_xxx` |
| `PYTEST_CURRENT_TEST` | Current test name (auto-set) | Used by pytest internally |

## Summary

Key testing principles:

1. **Run tests frequently** - Before every commit
2. **Write tests first** - TDD approach for new features
3. **Keep tests fast** - Unit tests <100ms each
4. **Test edge cases** - Not just happy path
5. **Use fixtures** - Avoid repetitive setup
6. **Clear assertions** - Specific, descriptive checks
7. **Independent tests** - No shared state
8. **Document tests** - Clear docstrings
9. **Track coverage** - Aim for 80%+
10. **CI/CD integration** - Automated testing on every push

For questions or issues, see [GitHub Issues](https://github.com/williamzujkowski/yakshave/issues).
