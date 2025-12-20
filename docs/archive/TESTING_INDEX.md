# Testing Documentation Index

Central index for all testing documentation in gh-year-end.

## Quick Start

**New to the project?** Start here:
1. Read [TESTING_QUICK_REFERENCE.md](/home/william/git/yakshave/docs/TESTING_QUICK_REFERENCE.md) - One-page command reference
2. Run basic tests: `uv run pytest`
3. Check coverage: `uv run pytest --cov=src/gh_year_end --cov-report=term-missing`

**Need details?** Read [TESTING_GUIDE.md](/home/william/git/yakshave/docs/TESTING_GUIDE.md)

## Documentation Structure

### Main Documentation (docs/)

1. **[TESTING_GUIDE.md](/home/william/git/yakshave/docs/TESTING_GUIDE.md)** (691 lines)
   - Comprehensive testing guide
   - All commands and options
   - Test organization and categories
   - Fixtures, patterns, and best practices
   - Writing tests and debugging
   - CI/CD integration

2. **[TESTING_QUICK_REFERENCE.md](/home/william/git/yakshave/docs/TESTING_QUICK_REFERENCE.md)** (296 lines)
   - One-page quick reference card
   - Essential commands table
   - Test markers and categories
   - File organization diagram
   - Common patterns
   - Troubleshooting table

3. **[TESTING_ANALYSIS_SUMMARY.md](/home/william/git/yakshave/docs/TESTING_ANALYSIS_SUMMARY.md)** (500+ lines)
   - Complete analysis of testing setup
   - Test infrastructure details
   - Coverage configuration
   - Live API test strategy
   - CI/CD examples
   - Key findings and recommendations

### Implementation Documentation (tests/)

4. **[tests/INTEGRATION_TESTS.md](/home/william/git/yakshave/tests/INTEGRATION_TESTS.md)**
   - Integration test details
   - Running instructions
   - Test categories and descriptions
   - Skip behavior
   - Troubleshooting

5. **[tests/LIVE_FIXTURES.md](/home/william/git/yakshave/tests/LIVE_FIXTURES.md)**
   - Live integration test fixtures
   - Session-scoped caching strategy
   - Fixture documentation
   - Usage examples
   - Configuration details

6. **[tests/TESTING_SUMMARY.md](/home/william/git/yakshave/tests/TESTING_SUMMARY.md)**
   - Implementation summary for GitHub issue #23
   - Test results
   - Coverage information
   - CI/CD recommendations

## Quick Links by Use Case

### I want to...

**Run tests**
- Basic: [TESTING_QUICK_REFERENCE.md - Essential Commands](/home/william/git/yakshave/docs/TESTING_QUICK_REFERENCE.md#essential-commands)
- Advanced: [TESTING_GUIDE.md - Quick Reference](/home/william/git/yakshave/docs/TESTING_GUIDE.md#quick-reference)

**Understand test organization**
- Overview: [TESTING_GUIDE.md - Test Organization](/home/william/git/yakshave/docs/TESTING_GUIDE.md#test-organization)
- File structure: [TESTING_QUICK_REFERENCE.md - Test File Organization](/home/william/git/yakshave/docs/TESTING_QUICK_REFERENCE.md#test-file-organization)

**Use test markers**
- Quick ref: [TESTING_QUICK_REFERENCE.md - Test Markers](/home/william/git/yakshave/docs/TESTING_QUICK_REFERENCE.md#test-markers)
- Details: [TESTING_GUIDE.md - Pytest Markers](/home/william/git/yakshave/docs/TESTING_GUIDE.md#pytest-markers)

**Write tests**
- Patterns: [TESTING_GUIDE.md - Common Test Patterns](/home/william/git/yakshave/docs/TESTING_GUIDE.md#common-test-patterns)
- Best practices: [TESTING_GUIDE.md - Writing Tests](/home/william/git/yakshave/docs/TESTING_GUIDE.md#writing-tests)

**Debug failing tests**
- Quick: [TESTING_QUICK_REFERENCE.md - Troubleshooting](/home/william/git/yakshave/docs/TESTING_QUICK_REFERENCE.md#troubleshooting)
- Detailed: [TESTING_GUIDE.md - Debugging Tests](/home/william/git/yakshave/docs/TESTING_GUIDE.md#debugging-tests)

**Run integration tests**
- Quick: [tests/INTEGRATION_TESTS.md - Running Tests](/home/william/git/yakshave/tests/INTEGRATION_TESTS.md#running-tests)
- Details: [TESTING_GUIDE.md - Integration Tests](/home/william/git/yakshave/docs/TESTING_GUIDE.md#integration-tests-pytestmarkintegration)

**Use live API fixtures**
- Overview: [tests/LIVE_FIXTURES.md](/home/william/git/yakshave/tests/LIVE_FIXTURES.md)
- Fixtures: [TESTING_GUIDE.md - Test Fixtures](/home/william/git/yakshave/docs/TESTING_GUIDE.md#test-fixtures)

**Check coverage**
- Commands: [TESTING_QUICK_REFERENCE.md - Coverage](/home/william/git/yakshave/docs/TESTING_QUICK_REFERENCE.md#coverage-configuration)
- Config: [TESTING_GUIDE.md - Coverage Configuration](/home/william/git/yakshave/docs/TESTING_GUIDE.md#coverage-configuration)

**Set up CI/CD**
- Examples: [TESTING_GUIDE.md - Running Tests in CI/CD](/home/william/git/yakshave/docs/TESTING_GUIDE.md#running-tests-in-cicd)
- Strategy: [TESTING_ANALYSIS_SUMMARY.md - CI/CD Integration](/home/william/git/yakshave/docs/TESTING_ANALYSIS_SUMMARY.md#cicd-integration)

## Test Statistics

- **Total Tests:** 771
- **Test Files:** 51
- **Unit Tests:** ~650+
- **Integration Tests:** ~10
- **Live API Tests:** ~100+
- **Coverage:** 45% (target: 80%)
- **Fixtures:** 106 across 35 files

## Test Categories

### Unit Tests (Default)
- **Run:** `uv run pytest`
- **Speed:** Fast (milliseconds)
- **API Calls:** No (mocked)
- **Token Required:** No

### Integration Tests (`@pytest.mark.integration`)
- **Run:** `GITHUB_TOKEN=ghp_xxx uv run pytest -m integration`
- **Speed:** Medium (seconds)
- **API Calls:** Yes (minimal)
- **Token Required:** Yes (auto-skips if missing)

### Live API Tests (`@pytest.mark.live_api`)
- **Run:** `GITHUB_TOKEN=ghp_xxx uv run pytest -m live_api`
- **Speed:** Slow (minutes)
- **API Calls:** Yes (extensive, cached)
- **Token Required:** Yes (auto-skips if missing)

### Slow Tests (`@pytest.mark.slow`)
- **Run:** `uv run pytest -m slow`
- **Skip:** `uv run pytest -m "not slow"`

## Essential Commands

```bash
# Run all unit tests (fast)
uv run pytest

# Run with coverage
uv run pytest --cov=src/gh_year_end --cov-report=term-missing

# Run integration tests
GITHUB_TOKEN=ghp_xxx uv run pytest -m integration

# Run live API tests
GITHUB_TOKEN=ghp_xxx uv run pytest -m live_api

# Skip integration and live API tests
uv run pytest -m "not integration and not live_api"

# Run specific file
uv run pytest tests/test_config.py -v

# Stop on first failure
uv run pytest -x

# Show output and locals on failure
uv run pytest -s -l
```

## Configuration Files

- **pyproject.toml** - Pytest configuration, markers, coverage settings
- **tests/conftest.py** - Shared fixtures (session-scoped for live API tests)
- **.github/workflows/ci.yml** - CI/CD pipeline

## Key Concepts

### Test Markers
- `@pytest.mark.integration` - Real API calls, requires token
- `@pytest.mark.live_api` - Live API with cached data
- `@pytest.mark.slow` - Long-running tests
- `@pytest.mark.asyncio` - Async test functions

### Session-Scoped Fixtures
- `github_token` - Token from environment
- `live_config` - Config for live tests
- `live_paths` - PathManager with temp dirs
- `cached_raw_data` - Cached collection data (runs once)
- `live_test_config_path` - Config file path

### Coverage Targets
- **Current:** 45% (temporarily lowered)
- **Target:** 80% (long-term goal)
- **Omitted:** CLI module (tested via integration)

## Related Documentation

- [CLAUDE.md](/home/william/git/yakshave/CLAUDE.md) - Project standards and rules
- [README.md](/home/william/git/yakshave/README.md) - Project overview
- [CI_WORKFLOW.md](/home/william/git/yakshave/.github/workflows/CI_WORKFLOW.md) - CI/CD documentation

## External Resources

- **Pytest:** https://docs.pytest.org/
- **pytest-asyncio:** https://pytest-asyncio.readthedocs.io/
- **respx (HTTP mocking):** https://lundberg.github.io/respx/
- **Coverage.py:** https://coverage.readthedocs.io/
- **Project repo:** https://github.com/williamzujkowski/yakshave

## Maintenance

### When to Update This Documentation

1. **New test markers added** - Update marker tables
2. **New test categories** - Update category descriptions
3. **New fixtures added** - Update fixture documentation
4. **Coverage threshold changed** - Update coverage stats
5. **New test files added** - Update file counts and organization
6. **CI/CD changes** - Update CI/CD examples

### Documentation Audit Schedule

- **Last Audit:** 2025-12-18
- **Next Review:** 2026-03-18 (quarterly)
- **Owner:** Engineering team

## Contributing

When adding new tests:

1. Follow naming conventions (`test_*.py`, `test_*()`)
2. Add appropriate markers (`@pytest.mark.integration`, etc.)
3. Write docstrings for complex tests
4. Update documentation if adding new categories or fixtures
5. Ensure tests are isolated and repeatable
6. Clean up test data after completion

## Support

**Questions?** Check:
1. This index for relevant documentation
2. TESTING_GUIDE.md for detailed information
3. TESTING_QUICK_REFERENCE.md for commands
4. tests/INTEGRATION_TESTS.md for integration test help
5. tests/LIVE_FIXTURES.md for fixture help

**Issues?** File a bug report with:
- Test command used
- Error message
- Expected vs actual behavior
- Environment details (OS, Python version, uv version)
