# Testing Summary

**Last Updated**: 2025-12-18
**Total Tests**: 811
**Test Files**: 51 (48 test files + 3 utility/fixture files)
**Coverage**: 45% (target: 80%)

This document summarizes the testing infrastructure for gh-year-end.

## Test Statistics

```
Total Tests: 811 across 48 files
By Category:
  - Unit Tests: ~757
  - Integration Tests: ~10
  - Live API Tests: ~54

By Phase:
  - Phase 1 (GitHub Client): ~150
  - Phase 2 (Collectors): ~200
  - Phase 3 (Hygiene): ~50
  - Phase 4 (Normalization): ~150
  - Phase 5 (Metrics): ~150
  - Phase 6 (Report): ~70
  - End-to-End: ~40
```

## Running Tests

### Unit Tests Only (Default)
```bash
uv run pytest -v
# or
uv run pytest -v -m "not integration"
```

### Integration Tests Only
```bash
GITHUB_TOKEN=ghp_your_token_here uv run pytest -v -m integration
```

### All Tests
```bash
GITHUB_TOKEN=ghp_your_token_here uv run pytest -v
```

### With Coverage
```bash
uv run pytest --cov=src/gh_year_end --cov-report=term-missing
```

### Specific Test File
```bash
uv run pytest tests/test_orchestrator.py -v
```

## Documentation

See [docs/testing.md](../docs/testing.md) for comprehensive testing guide including:
- Quick start commands
- Test categories and markers
- Running specific tests
- Coverage reporting
- Writing tests
- Best practices

## Integration Test Coverage

### What's Tested
1. **Repository Discovery**
   - Real API calls to GitHub
   - JSONL file creation
   - Data structure validation
   - Manifest tracking

2. **Data Structure**
   - Envelope structure (timestamp, source, endpoint, request_id, page, data)
   - Field types and format
   - Deterministic ordering

3. **Error Handling**
   - Invalid token handling
   - Rate limit tracking
   - Missing token errors

4. **Cleanup**
   - Temporary directory cleanup
   - Data removal verification

### What's NOT Tested (Future Work)
- Full collector pipeline (pulls, issues, reviews, comments, commits)
- GraphQL queries
- Rate limit stress testing
- Error recovery and retry logic
- Concurrent collection scenarios

## Orchestrator Test Coverage

### What's Tested
1. **Main Collection Flow**
   - Successful collection with all collectors
   - No repositories scenario
   - Missing token error
   - Disabled collectors
   - Force flag behavior

2. **Data Extraction**
   - Issue number extraction from JSONL
   - PR number extraction from JSONL
   - Empty file handling
   - Duplicate handling

3. **Configuration**
   - Collector enable/disable logic
   - Storage path configuration
   - Manifest creation and tracking

## Key Patterns and Best Practices

### Integration Tests
1. **Fixture Design**
   - `github_token`: Skips test if not set
   - `temp_data_dir`: Auto-cleanup temp directories
   - `integration_config`: Minimal config for fast tests

2. **Test Isolation**
   - Each test uses temporary directories
   - Auto-cleanup after test completion
   - No shared state between tests

3. **Real API Usage**
   - Tests against actual GitHub API
   - Uses williamzujkowski/yakshave repo
   - Minimal API calls for speed
   - Low concurrency to avoid rate limits

### Unit Tests
1. **Comprehensive Mocking**
   - All collectors mocked
   - All clients mocked
   - File I/O mocked where appropriate

2. **Edge Case Testing**
   - Empty results
   - Missing files
   - Disabled collectors
   - Error conditions

## Marker Configuration

Test markers are configured in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
markers = [
    "integration: marks tests as integration tests (require GITHUB_TOKEN)",
    "live_api: marks tests that make real GitHub API calls",
    "slow: marks tests as slow",
]
```

Current test counts by marker:
- Total: 811 tests
- Integration (`-m integration`): 10 tests
- Live API (`-m live_api`): 54 tests
- Slow (`-m slow`): 1 test

## CI/CD Recommendations

### GitHub Actions Example
```yaml
- name: Run Unit Tests
  run: uv run pytest -v -m "not integration"

- name: Run Integration Tests
  env:
    GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
  run: uv run pytest -v -m integration
```

### Local Development
```bash
# Quick check (unit tests only)
uv run pytest -x -q

# Full check with integration
export GITHUB_TOKEN=ghp_...
uv run pytest -v
```

## Code Quality

### Linting
```bash
ruff check tests/test_integration.py tests/test_orchestrator.py
```

### Type Checking
```bash
mypy tests/test_integration.py tests/test_orchestrator.py
```

### Format
```bash
ruff format tests/test_integration.py tests/test_orchestrator.py
```

## Future Enhancements

1. **Extended Integration Tests**
   - Test all collectors (pulls, issues, reviews, comments, commits)
   - Test against dedicated test organization
   - Add performance benchmarks

2. **Enhanced Orchestrator Tests**
   - Test concurrent collection scenarios
   - Test rate limit backoff strategies
   - Test partial failure recovery

3. **Test Utilities**
   - Create test data generators
   - Add mock GitHub API server
   - Add test fixtures for common scenarios

4. **Documentation**
   - Add video walkthrough
   - Create testing best practices guide
   - Document common issues and solutions

## References

- Project documentation: `/home/william/git/yakshave/CLAUDE.md`
- Integration test docs: `/home/william/git/yakshave/tests/INTEGRATION_TESTS.md`
- Pytest markers: `uv run pytest --markers`
- Test coverage: `uv run pytest --cov=src/gh_year_end --cov-report=term-missing`
