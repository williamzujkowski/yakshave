# Testing Quick Reference

One-page reference for common testing commands and patterns.

## Essential Commands

```bash
# Basic test runs
uv run pytest                                    # All unit tests (fast)
uv run pytest -v                                 # Verbose output
uv run pytest -x                                 # Stop on first failure
uv run pytest -q                                 # Quiet mode

# Run specific tests
uv run pytest tests/test_config.py               # Single file
uv run pytest tests/test_config.py::TestClass   # Single class
uv run pytest tests/test_config.py::test_func   # Single function
uv run pytest -k "test_name"                     # Match by keyword

# Test markers
uv run pytest -m integration                     # Integration tests only
uv run pytest -m live_api                        # Live API tests only
uv run pytest -m "not integration"               # Skip integration
uv run pytest -m "not slow"                      # Skip slow tests
uv run pytest -m "integration and not slow"      # Combine markers

# Coverage
uv run pytest --cov=src/gh_year_end              # With coverage
uv run pytest --cov=src/gh_year_end --cov-report=term-missing  # Show missing lines
uv run pytest --cov=src/gh_year_end --cov-report=html          # HTML report

# Debugging
uv run pytest -s                                 # Show print/log output
uv run pytest -l                                 # Show locals on failure
uv run pytest --pdb                              # Drop to debugger on failure
uv run pytest -vv                                # Very verbose
uv run pytest --lf                               # Run last failed
uv run pytest --ff                               # Run failed first

# With GitHub token
GITHUB_TOKEN=ghp_xxx uv run pytest -m integration
GITHUB_TOKEN=ghp_xxx uv run pytest -m live_api
```

## Test Markers

| Marker | Description | Requires | Files |
|--------|-------------|----------|-------|
| `integration` | Real API calls, mocked responses | GITHUB_TOKEN | test_integration.py |
| `live_api` | Real API calls, live data | GITHUB_TOKEN | test_pipeline_live_*.py |
| `slow` | Long-running tests | - | Various |
| `asyncio` | Async test functions | - | Various |

## Test Categories

### Unit Tests (Default)
- **Count:** ~700+ tests
- **Speed:** Fast (milliseconds)
- **API calls:** No (mocked)
- **Token required:** No
- **Run:** `uv run pytest`

### Integration Tests
- **Marker:** `@pytest.mark.integration`
- **Speed:** Medium (seconds)
- **API calls:** Yes (minimal)
- **Token required:** Yes
- **Run:** `GITHUB_TOKEN=ghp_xxx uv run pytest -m integration`

### Live API Tests
- **Marker:** `@pytest.mark.live_api`
- **Speed:** Slow (minutes)
- **API calls:** Yes (extensive, cached)
- **Token required:** Yes
- **Run:** `GITHUB_TOKEN=ghp_xxx uv run pytest -m live_api`

## Key Fixtures (`conftest.py`)

| Fixture | Scope | Purpose | Requires |
|---------|-------|---------|----------|
| `github_token` | session | GitHub token from env | GITHUB_TOKEN |
| `live_config` | session | Config for live tests | - |
| `live_paths` | session | PathManager for tests | - |
| `cached_raw_data` | session | Cached collection data | GITHUB_TOKEN |
| `live_test_config_path` | session | Config file path | - |

## Test File Organization

```
tests/
├── conftest.py                      # Shared fixtures
│
├── Configuration (5 files)
│   ├── test_config.py
│   ├── test_paths.py
│   ├── test_logging.py
│   ├── test_auth.py
│   └── test_manifest.py
│
├── Collection (11 files)
│   ├── test_http.py
│   ├── test_writer.py
│   ├── test_ratelimit.py
│   ├── test_ratelimit_enhanced.py
│   ├── test_discovery.py
│   ├── test_filters.py
│   ├── test_checkpoint.py
│   ├── test_orchestrator.py
│   ├── test_integration.py         # @pytest.mark.integration
│   ├── test_github_integration.py
│   └── test_large_org_integration.py
│
├── Normalization (9 files)
│   ├── test_normalize_common.py
│   ├── test_normalize_repos.py
│   ├── test_normalize_commits.py
│   ├── test_normalize_issues.py
│   ├── test_normalize_pulls.py
│   ├── test_normalize_reviews.py
│   ├── test_normalize_comments.py
│   ├── test_normalize_users.py
│   ├── test_normalize_hygiene.py
│   └── test_parquet_writer.py
│
├── Metrics (6 files)
│   ├── test_metrics_orchestrator.py
│   ├── test_metrics_awards.py
│   ├── test_metrics_hygiene.py
│   ├── test_metrics_leaderboards.py
│   ├── test_metrics_repo_health.py
│   └── test_metrics_timeseries.py
│
├── Reports (7 files)
│   ├── test_report_build.py
│   ├── test_report_export.py
│   ├── test_templates.py
│   ├── test_views_engineer.py
│   ├── test_views_exec.py
│   ├── test_smoke_site.py
│   └── test_cli_report.py
│
├── Live Pipeline (5 files)         # @pytest.mark.live_api
│   ├── test_pipeline_live_collect.py
│   ├── test_pipeline_live_normalize.py
│   ├── test_pipeline_live_metrics.py
│   ├── test_pipeline_live_report.py
│   └── test_pipeline_live_e2e.py
│
├── End-to-End (2 files)
│   ├── test_end_to_end.py
│   └── test_live_fixtures_demo.py
│
└── Utilities (6 files)
    ├── test_hygiene.py
    └── test_identity.py
```

## Coverage Configuration

```toml
# pyproject.toml
[tool.coverage.run]
source = ["src/gh_year_end"]
branch = true
omit = ["src/gh_year_end/cli.py"]

[tool.coverage.report]
fail_under = 45  # Temporarily lowered, target 80%
```

**Commands:**
```bash
# Coverage report
uv run pytest --cov=src/gh_year_end --cov-report=term-missing

# HTML report
uv run pytest --cov=src/gh_year_end --cov-report=html
open htmlcov/index.html

# XML report (for CI)
uv run pytest --cov=src/gh_year_end --cov-report=xml
```

## Common Test Patterns

### Async Test
```python
import pytest

@pytest.mark.asyncio
async def test_async_function():
    result = await async_function()
    assert result == expected
```

### Parametrized Test
```python
@pytest.mark.parametrize("input,expected", [
    ("valid", True),
    ("invalid", False),
])
def test_validation(input: str, expected: bool):
    assert validate(input) == expected
```

### Mocked HTTP
```python
import respx
from httpx import Response

@respx.mock
async def test_api():
    respx.get("https://api.github.com/repos/o/r").mock(
        return_value=Response(200, json={"name": "repo"})
    )
    result = await client.get_repo("o", "r")
    assert result["name"] == "repo"
```

### Temp Directory
```python
def test_file_ops(tmp_path):
    file = tmp_path / "test.txt"
    file.write_text("hello")
    assert file.read_text() == "hello"
```

### Integration Test
```python
@pytest.mark.integration
def test_with_token(github_token: str):
    # Auto-skips if GITHUB_TOKEN not set
    client = GitHubClient(token=github_token)
    result = await client.get_user("octocat")
    assert result["login"] == "octocat"
```

### Live API Test
```python
@pytest.mark.live_api
def test_with_live_data(cached_raw_data: dict, live_paths: PathManager):
    # Uses cached collection data (runs once per session)
    assert "discovery" in cached_raw_data
    assert live_paths.manifest_path.exists()
```

## Pre-Commit Workflow

```bash
# Quick check (fast feedback)
uv run pytest -x -q

# Full check (before push)
ruff check .
ruff format --check .
mypy src/
uv run pytest --cov=src/gh_year_end --cov-report=term-missing

# With integration tests
GITHUB_TOKEN=ghp_xxx uv run pytest
```

## CI/CD Example

```yaml
# .github/workflows/test.yml
- name: Run unit tests
  run: uv run pytest -v -m "not integration and not live_api"

- name: Run integration tests
  env:
    GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
  run: uv run pytest -v -m integration

- name: Coverage
  run: uv run pytest --cov=src/gh_year_end --cov-report=xml
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Tests skip "GITHUB_TOKEN not set" | `export GITHUB_TOKEN=ghp_xxx` or `GITHUB_TOKEN=ghp_xxx pytest ...` |
| Import errors | `uv sync` |
| Fixture not found | Check `conftest.py`, fixtures auto-discovered |
| Rate limit errors | Increase `min_sleep_seconds` in `live_config` |
| Async tests not running | Add `@pytest.mark.asyncio` |
| Collection failures | Check token permissions, network, GitHub API status |

## Useful Links

- **Full Testing Guide:** `/home/william/git/yakshave/docs/TESTING_GUIDE.md`
- **Integration Tests:** `/home/william/git/yakshave/tests/INTEGRATION_TESTS.md`
- **Live Fixtures:** `/home/william/git/yakshave/tests/LIVE_FIXTURES.md`
- **Project Rules:** `/home/william/git/yakshave/CLAUDE.md`
- **Pytest Docs:** https://docs.pytest.org/
