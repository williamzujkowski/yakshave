# Integration Tests

This document describes the integration tests for gh-year-end and how to run them.

## Overview

Integration tests make real API calls to GitHub and verify the complete data collection pipeline. They are marked with `@pytest.mark.integration` and require a valid `GITHUB_TOKEN`.

## Test Files

- **`tests/test_integration.py`**: Integration tests that make real API calls
- **`tests/test_orchestrator.py`**: Unit tests for the orchestrator (mocked, no API calls)

## Running Tests

### Unit Tests Only (Default)

Run all tests except integration tests:

```bash
uv run pytest -v
```

Or explicitly exclude integration tests:

```bash
uv run pytest -v -m "not integration"
```

### Integration Tests Only

Integration tests require a valid GitHub token. Set the `GITHUB_TOKEN` environment variable:

```bash
export GITHUB_TOKEN=ghp_your_token_here
uv run pytest -v -m integration
```

Or run inline:

```bash
GITHUB_TOKEN=ghp_your_token_here uv run pytest -v -m integration
```

### All Tests (Unit + Integration)

```bash
export GITHUB_TOKEN=ghp_your_token_here
uv run pytest -v
```

## Test Categories

### Integration Test Classes

#### `TestRepositoryDiscovery`
- `test_discover_repos_real_api`: Tests repository discovery with real GitHub API
- `test_discover_repos_with_manifest`: Tests manifest tracking during discovery

#### `TestJSONLDataStructure`
- `test_jsonl_envelope_structure`: Verifies JSONL envelope structure
- `test_jsonl_deterministic_ordering`: Verifies deterministic ordering of results

#### `TestErrorHandling`
- `test_invalid_token_error`: Tests invalid token error handling
- `test_rate_limit_handling`: Tests rate limit tracking

#### `TestDataCleanup`
- `test_cleanup_removes_all_data`: Tests data cleanup functionality

### Orchestrator Unit Tests

#### `TestRunCollection`
- `test_run_collection_success`: Tests successful collection orchestration
- `test_run_collection_no_repos`: Tests behavior with no repositories
- `test_run_collection_missing_token`: Tests error handling for missing token
- `test_run_collection_disabled_collectors`: Tests with disabled collectors
- `test_run_collection_force_flag`: Tests force re-collection

#### `TestExtractIssueNumbers`
- `test_extract_issue_numbers_success`: Tests extracting issue numbers from JSONL
- `test_extract_issue_numbers_no_files`: Tests behavior with no files

#### `TestExtractPRNumbers`
- `test_extract_pr_numbers_success`: Tests extracting PR numbers from JSONL
- `test_extract_pr_numbers_duplicate_numbers`: Tests deduplication of PR numbers

## Skip Behavior

Integration tests will automatically skip if `GITHUB_TOKEN` is not set:

```bash
$ uv run pytest tests/test_integration.py -v
...
tests/test_integration.py::TestRepositoryDiscovery::test_discover_repos_real_api SKIPPED
```

Tests that don't require a token (like `test_invalid_token_error`) will still run.

## Test Data

Integration tests use controlled test data:
- **Target**: `williamzujkowski` (user's own repositories)
- **Year**: 2024
- **Scope**: Public repositories only, no forks or archived repos
- **Collectors**: Most collectors disabled for speed (only discovery enabled by default)

All test data is written to temporary directories that are automatically cleaned up.

## Verifying Data Structure

Integration tests verify:
1. JSONL files are created in expected locations
2. Envelope structure includes: `timestamp`, `source`, `endpoint`, `request_id`, `page`, `data`
3. Data content includes required fields
4. Manifest tracking works correctly
5. Deterministic ordering is maintained

## Coverage

Run tests with coverage:

```bash
uv run pytest --cov=src/gh_year_end --cov-report=term-missing
```

Include integration tests in coverage:

```bash
export GITHUB_TOKEN=ghp_your_token_here
uv run pytest --cov=src/gh_year_end --cov-report=term-missing
```

## Troubleshooting

### Tests Skip Unexpectedly

Ensure `GITHUB_TOKEN` is set:
```bash
echo $GITHUB_TOKEN
```

### Rate Limit Errors

Integration tests use low concurrency (2) to minimize rate limit issues. If you hit rate limits:
1. Wait for rate limit reset (check response headers)
2. Use a token with higher rate limits (authenticated vs unauthenticated)
3. Run fewer tests at once

### Temporary Directory Cleanup

If tests fail and leave temporary data, it will be cleaned up automatically on next run. To manually clean:

```bash
find /tmp -type d -name "gh_year_end_test_*" -exec rm -rf {} +
```

## CI/CD Integration

In CI pipelines, add the GitHub token as a secret and run:

```bash
pytest -v -m integration
```

For local development without a token:

```bash
pytest -v -m "not integration"
```

## Future Enhancements

Potential improvements for integration tests:
- Add tests for all collectors (pulls, issues, reviews, comments, commits)
- Test error recovery and retry logic
- Test rate limit handling under stress
- Add tests for GraphQL queries
- Test against a dedicated test organization with known data
