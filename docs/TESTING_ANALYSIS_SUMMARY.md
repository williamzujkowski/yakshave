# Testing Setup Analysis Summary

Analysis of the complete testing infrastructure for gh-year-end.

**Date:** 2025-12-18
**Total Tests:** 771
**Test Files:** 51
**Coverage Target:** 45% (temporarily lowered from 80%)

## Documentation Created

1. **TESTING_GUIDE.md** (691 lines)
   - Comprehensive guide to all testing commands and options
   - Test organization and categories
   - Fixtures and patterns
   - Writing and debugging tests
   - CI/CD integration
   - Best practices

2. **TESTING_QUICK_REFERENCE.md** (296 lines)
   - One-page quick reference card
   - Essential commands
   - Test markers table
   - File organization diagram
   - Common patterns
   - Troubleshooting table

## Test Infrastructure

### Configuration (`pyproject.toml`)

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_functions = ["test_*"]
addopts = "-v --tb=short"
markers = [
    "integration: marks tests as integration tests (require GITHUB_TOKEN)",
    "live_api: marks tests that make real GitHub API calls",
    "slow: marks tests as slow",
]

[tool.coverage.run]
source = ["src/gh_year_end"]
branch = true
omit = ["src/gh_year_end/cli.py"]

[tool.coverage.report]
fail_under = 45  # Temporarily lowered
exclude_lines = [
    "pragma: no cover",
    "def __repr__",
    "raise NotImplementedError",
    "if TYPE_CHECKING:",
    "if __name__ == .__main__.:",
]
```

## Test Categories

### Unit Tests (Default)
- **Count:** ~650+ tests
- **Speed:** Fast (milliseconds)
- **API Calls:** No (all mocked)
- **Token Required:** No
- **Files:** Most test files
- **Command:** `uv run pytest`

### Integration Tests (`@pytest.mark.integration`)
- **Count:** ~10 tests
- **Speed:** Medium (seconds to minutes)
- **API Calls:** Yes (minimal, real API)
- **Token Required:** Yes (auto-skips if missing)
- **Files:**
  - `test_integration.py` (7 tests)
  - `test_smoke_site.py` (1 test)
  - `test_github_integration.py`
- **Command:** `GITHUB_TOKEN=ghp_xxx uv run pytest -m integration`

### Live API Tests (`@pytest.mark.live_api`)
- **Count:** ~100+ tests
- **Speed:** Slow (minutes)
- **API Calls:** Yes (extensive, cached per session)
- **Token Required:** Yes (auto-skips if missing)
- **Target:** github org, year 2024 (stable historical data)
- **Files:**
  - `test_pipeline_live_collect.py` (9 tests)
  - `test_pipeline_live_normalize.py` (multiple tests)
  - `test_pipeline_live_metrics.py` (multiple tests)
  - `test_pipeline_live_report.py` (5 tests)
  - `test_pipeline_live_e2e.py` (full pipeline test)
  - `test_live_fixtures_demo.py` (5 demo tests)
- **Command:** `GITHUB_TOKEN=ghp_xxx uv run pytest -m live_api`

### Slow Tests (`@pytest.mark.slow`)
- Tests marked as time-consuming
- Often overlaps with `live_api` tests
- Command: `uv run pytest -m "not slow"` (to skip)

## Test File Organization

### By Phase

**Phase 1-3: Collection (16 files)**
- Configuration: 5 files (config, paths, logging, auth, manifest)
- HTTP/API: 6 files (http, writer, ratelimit, ratelimit_enhanced, discovery, filters)
- Orchestration: 5 files (checkpoint, orchestrator, integration, github_integration, large_org_integration)

**Phase 4: Normalization (10 files)**
- Common utilities: test_normalize_common.py
- Entity normalization: 8 files (repos, commits, issues, pulls, reviews, comments, users, hygiene)
- Parquet writing: test_parquet_writer.py

**Phase 5: Metrics (6 files)**
- Orchestrator: test_metrics_orchestrator.py
- Metrics: 5 files (awards, hygiene, leaderboards, repo_health, timeseries)

**Phase 6: Reports (7 files)**
- Building: test_report_build.py, test_report_export.py
- Templating: test_templates.py
- Views: test_views_engineer.py, test_views_exec.py
- Testing: test_smoke_site.py
- CLI: test_cli_report.py

**End-to-End (7 files)**
- test_end_to_end.py (with fixture data)
- test_pipeline_live_collect.py
- test_pipeline_live_normalize.py
- test_pipeline_live_metrics.py
- test_pipeline_live_report.py
- test_pipeline_live_e2e.py
- test_live_fixtures_demo.py

**Utilities (5 files)**
- test_hygiene.py
- test_identity.py
- conftest.py (shared fixtures)
- fixtures/sample_org/ (sample data)

## Key Fixtures (conftest.py)

### Session-Scoped Fixtures

All fixtures are session-scoped for efficiency:

1. **github_token**
   - Gets `GITHUB_TOKEN` from environment
   - Skips test if not set
   - Used by all integration/live_api tests

2. **live_config**
   - Config targeting github org, year 2024
   - Conservative rate limiting (concurrency=1, min_sleep=2.0s)
   - Public repos only, no forks or archived
   - All collectors enabled

3. **live_paths**
   - PathManager using session-scoped temp directory
   - Isolated test data storage
   - Auto-cleanup

4. **cached_raw_data**
   - Runs collection ONCE per pytest session
   - Caches data for all tests
   - Makes real GitHub API calls
   - Returns collection statistics

5. **live_test_config_path**
   - Creates live_test_config.yaml in temp directory
   - Used for CLI integration tests

### Test Configuration

**Target:** github org (stable, well-known test target)
**Year:** 2024 (historical, stable data)
**Rate Limiting:**
- Strategy: adaptive
- Max concurrency: 1 (sequential)
- Min sleep: 2.0 seconds
- Max sleep: 60 seconds
- Sample rate limit endpoint: every 50 requests

## Test Commands Summary

### Basic Commands
```bash
# All unit tests
uv run pytest

# With coverage
uv run pytest --cov=src/gh_year_end --cov-report=term-missing

# Specific file
uv run pytest tests/test_config.py -v

# Specific test
uv run pytest tests/test_config.py::TestConfigLoading::test_load_valid_config -v

# Pattern matching
uv run pytest -k "normalize" -v
```

### Marker-Based Commands
```bash
# Integration tests only
GITHUB_TOKEN=ghp_xxx uv run pytest -m integration

# Live API tests only
GITHUB_TOKEN=ghp_xxx uv run pytest -m live_api

# Skip integration tests
uv run pytest -m "not integration"

# Skip slow tests
uv run pytest -m "not slow"

# Skip both integration and live_api
uv run pytest -m "not integration and not live_api"
```

### Debugging Commands
```bash
# Stop on first failure
uv run pytest -x

# Show output
uv run pytest -s

# Show locals on failure
uv run pytest -l

# Drop to debugger on failure
uv run pytest --pdb

# Very verbose
uv run pytest -vv

# Run last failed
uv run pytest --lf

# Run failed first
uv run pytest --ff
```

### Coverage Commands
```bash
# Terminal report with missing lines
uv run pytest --cov=src/gh_year_end --cov-report=term-missing

# HTML report
uv run pytest --cov=src/gh_year_end --cov-report=html
open htmlcov/index.html

# XML report (for CI)
uv run pytest --cov=src/gh_year_end --cov-report=xml

# Fail if under threshold
uv run pytest --cov=src/gh_year_end --cov-fail-under=45
```

## Test Dependencies

From `pyproject.toml`:

```toml
[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",           # Test framework
    "pytest-cov>=4.1.0",       # Coverage plugin
    "pytest-asyncio>=0.23.0",  # Async test support
    "ruff>=0.3.0",             # Linting
    "mypy>=1.8.0",             # Type checking
    "types-PyYAML>=6.0.0",     # Type stubs
    "respx>=0.21.0",           # HTTP mocking
]
```

**Additional plugins (auto-loaded):**
- pytest-anyio: Anyio async support
- pytest-respx: HTTP request mocking

## Coverage Configuration

**Source:** `src/gh_year_end`
**Branch Coverage:** Enabled
**Threshold:** 45% (temporarily lowered)
**Target:** 80% (future goal)

**Omitted Files:**
- `src/gh_year_end/cli.py` (tested via integration tests)

**Excluded Lines:**
- `pragma: no cover`
- `def __repr__`
- `raise NotImplementedError`
- `if TYPE_CHECKING:`
- `if __name__ == .__main__.:`

**Note:** Coverage lowered for Phase 2-3 because collectors require integration tests with mocked HTTP or real API access. Phase 4+ coverage is higher due to better unit test coverage.

## Live API Test Strategy

### Session-Scoped Caching
1. First `live_api` test runs collection
2. Data cached for entire pytest session
3. All subsequent tests reuse cached data
4. No re-collection unless session restarted

### Benefits
- Minimizes API calls (collection runs once)
- Faster test execution (cached data)
- Respects rate limits (conservative settings)
- Test isolation (session-scoped temp dirs)
- Reproducibility (stable historical data)

### Trade-offs
- First test run is slow (minutes)
- Subsequent runs are fast (seconds)
- Requires valid GITHUB_TOKEN
- Makes real API calls
- Tests against live data (github org)

## Common Test Patterns

### 1. Async Tests
```python
import pytest

@pytest.mark.asyncio
async def test_async_function():
    result = await async_function()
    assert result == expected
```

### 2. Parametrized Tests
```python
@pytest.mark.parametrize("input,expected", [
    ("valid", True),
    ("invalid", False),
])
def test_validation(input: str, expected: bool):
    assert validate(input) == expected
```

### 3. Mocked HTTP (respx)
```python
import respx
from httpx import Response

@respx.mock
async def test_api():
    respx.get("https://api.github.com/users/octocat").mock(
        return_value=Response(200, json={"login": "octocat"})
    )
    result = await client.get_user("octocat")
    assert result["login"] == "octocat"
```

### 4. Temp Directories
```python
def test_file_operations(tmp_path: Path):
    file = tmp_path / "test.txt"
    file.write_text("hello")
    assert file.read_text() == "hello"
```

### 5. Integration Tests
```python
@pytest.mark.integration
def test_with_token(github_token: str):
    # Auto-skips if GITHUB_TOKEN not set
    client = GitHubClient(token=github_token)
    result = await client.get_user("octocat")
    assert result["login"] == "octocat"
```

### 6. Live API Tests
```python
@pytest.mark.live_api
def test_collection(cached_raw_data: dict, live_paths: PathManager):
    # Uses cached collection data
    assert "discovery" in cached_raw_data
    assert live_paths.manifest_path.exists()
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v1

      - name: Install dependencies
        run: uv sync

      - name: Run unit tests
        run: uv run pytest -v -m "not integration and not live_api"

      - name: Run integration tests
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: uv run pytest -v -m integration

      - name: Coverage
        run: uv run pytest --cov=src/gh_year_end --cov-report=xml

      - name: Upload coverage
        uses: codecov/codecov-action@v3
        with:
          files: ./coverage.xml
```

## Pre-Commit Workflow

```bash
# Quick check (fast)
uv run pytest -x -q

# Full local check
ruff check .
ruff format --check .
mypy src/
uv run pytest --cov=src/gh_year_end --cov-report=term-missing

# With integration tests (optional)
export GITHUB_TOKEN=ghp_xxx
uv run pytest
```

## Troubleshooting

### Tests Skip with "GITHUB_TOKEN not set"
**Solution:** Set token in environment
```bash
export GITHUB_TOKEN=ghp_xxxxxxxxxxxxx
uv run pytest -m integration
```

### Import Errors
**Solution:** Ensure package installed
```bash
uv sync
```

### Fixture Not Found
**Solution:** Fixtures in `conftest.py` are auto-discovered. No import needed.

### Rate Limit Errors
**Solution:** Live API tests use conservative settings. If still hitting limits:
1. Increase `min_sleep_seconds` in `live_config`
2. Ensure `max_concurrency=1`
3. Add delays between test runs
4. Check GitHub API status

### Async Tests Not Running
**Solution:** Add marker
```python
@pytest.mark.asyncio
async def test_async_function():
    ...
```

## Best Practices

### Unit Tests
1. Mock external dependencies
2. Test one behavior per test
3. Fast execution (milliseconds)
4. No side effects
5. Deterministic results

### Integration Tests
1. Mark with `@pytest.mark.integration` or `@pytest.mark.live_api`
2. Auto-skip gracefully if credentials missing
3. Minimize API calls
4. Use temporary directories
5. Conservative rate limiting

### Test Data
1. Use fixtures for test data
2. Realistic data structures
3. Include edge cases (empty, None, boundaries)
4. Clean up after tests

### Test Coverage
1. Focus on critical paths
2. Don't chase 100% (focus on meaningful coverage)
3. Test error paths
4. Document gaps with `pragma: no cover`

## Documentation Files

1. **TESTING_GUIDE.md** - Comprehensive testing guide (691 lines)
2. **TESTING_QUICK_REFERENCE.md** - One-page reference card (296 lines)
3. **tests/INTEGRATION_TESTS.md** - Integration test details
4. **tests/LIVE_FIXTURES.md** - Live fixture documentation
5. **tests/TESTING_SUMMARY.md** - Implementation summary

## Related Files

- `/home/william/git/yakshave/pyproject.toml` - Pytest configuration
- `/home/william/git/yakshave/tests/conftest.py` - Shared fixtures
- `/home/william/git/yakshave/CLAUDE.md` - Project standards
- `.github/workflows/ci.yml` - CI pipeline

## References

- **Pytest:** https://docs.pytest.org/
- **pytest-asyncio:** https://pytest-asyncio.readthedocs.io/
- **respx:** https://lundberg.github.io/respx/
- **Coverage.py:** https://coverage.readthedocs.io/
- **Project repo:** https://github.com/williamzujkowski/yakshave

## Key Findings

### Strengths
1. Comprehensive test coverage (771 tests across 51 files)
2. Well-organized test structure by phase
3. Smart session-scoped fixtures for live API tests
4. Auto-skip behavior when credentials missing
5. Multiple test categories (unit, integration, live_api)
6. Good use of pytest markers for selective execution
7. Extensive documentation (3 MD files in tests/)

### Opportunities
1. Coverage at 45% (target 80%)
2. More collector tests needed (tracked in #54)
3. Could add more parametrized tests
4. Could benefit from test data generators
5. Performance benchmarks could be added

### Recommendations
1. Continue adding unit tests for collectors
2. Work toward 80% coverage target
3. Keep using session-scoped fixtures for API tests
4. Maintain separation of unit/integration/live_api tests
5. Add performance regression tests
6. Consider adding contract tests for API responses
