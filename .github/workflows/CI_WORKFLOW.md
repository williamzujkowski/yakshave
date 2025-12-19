# CI/CD Workflow Documentation

## Overview

The enhanced CI/CD workflow provides comprehensive validation of code quality, security, and site generation. The workflow is organized into distinct jobs with clear dependencies and quality gates.

## Workflow Jobs

### 1. Lint (`lint`)
**Purpose**: Enforce code style and formatting standards.

**Tools**:
- `ruff check`: Linting with multiple rule sets (pycodestyle, Pyflakes, isort, etc.)
- `ruff format --check`: Formatting validation

**Quality Gate**: P0 (blocks merge)

**Caching**: Enabled via `enable-cache: true` on uv setup

### 2. Type Check (`typecheck`)
**Purpose**: Ensure type safety and catch type-related bugs.

**Tools**:
- `mypy`: Strict type checking on `src/` directory

**Quality Gate**: P0 (blocks merge)

**Caching**: Enabled

### 3. Security Scan (`security`)
**Purpose**: Identify security vulnerabilities in source code.

**Tools**:
- `bandit`: Security linter for Python code
- Scans `src/` directory recursively
- Generates JSON report for artifact storage

**Configuration**: `.bandit` file in project root

**Quality Gate**: P1 (warns but doesn't block)

**Artifacts**:
- `bandit-report.json` (retained for 30 days)

**Behavior**:
- Continues on error to allow other checks to run
- Results displayed in console output
- Full report uploaded as artifact

### 4. Dependency Check (`dependency-check`)
**Purpose**: Check dependencies for known security vulnerabilities.

**Tools**:
- `safety`: Scans Python dependencies against vulnerability database

**Quality Gate**: P1 (warns but doesn't block)

**Artifacts**:
- `safety-report.json` (retained for 30 days)

**Behavior**:
- Continues on error
- Results displayed and uploaded as artifact

### 5. Unit Tests (`test`)
**Purpose**: Run comprehensive test suite with coverage reporting.

**Matrix Strategy**: Tests run on Python 3.11 and 3.12

**Test Scope**: Excludes integration tests (`-m "not integration"`)

**Coverage**:
- Source: `src/gh_year_end`
- Output: XML and terminal formats
- Uploaded to Codecov

**Quality Gate**: P0 (blocks merge)

**Caching**: Enabled

### 6. Integration Tests (`integration-test`)
**Purpose**: Run integration tests that require GitHub API access.

**Test Scope**: Only tests marked with `-m integration`

**Requirements**:
- `GITHUB_TOKEN` environment variable
- Only runs on push to main or PRs from the same repository

**Quality Gate**: P1 (warns but doesn't block)

**Behavior**: Continues on error to allow other checks to complete

### 7. Site Generation Validation (`site-validation`)
**Purpose**: Validate static site generation and HTML output.

**Dependencies**: Runs after `test` job completes

**Validation Steps**:

#### a. Run Site Smoke Tests
- Executes `tests/test_smoke_site.py` and `tests/test_templates.py`
- Validates site structure and template rendering

#### b. Generate Test Site
- Creates minimal test site with sample data
- Output directory: `test_site_output`

#### c. HTML Validation
- Uses `html-validate` npm package
- Configuration: `.htmlvalidate.json`
- Validates all generated HTML files
- Checks for:
  - Valid HTML5 syntax
  - Proper element nesting
  - Required attributes
  - No duplicate IDs

#### d. Asset Check
- Verifies presence of required template files:
  - `base.html`, `index.html`, `summary.html`, `engineers.html`
- Verifies presence of required assets:
  - `style.css`, `app.js`

#### e. Accessibility Check
- Uses BeautifulSoup4 for parsing
- Checks for:
  - `lang` attribute on `<html>` tag
  - `alt` text on images
  - Labels for form inputs
- Non-blocking (warns only)

**Quality Gate**: P1 (warns but doesn't block)

**Artifacts**:
- `test-site-output/` directory (retained for 7 days)
- Contains generated HTML and assets for inspection

### 8. Quality Gate (`quality-gate`)
**Purpose**: Final validation gate that enforces quality standards.

**Dependencies**: Runs after all other jobs (uses `needs: [...]`)

**Behavior**: Always runs regardless of previous job outcomes (`if: always()`)

**Gate Logic**:

#### P0 Gates (Must Pass):
- Lint
- Type Check
- Unit Tests

**Result**: Workflow fails if any P0 gate fails

#### P1 Gates (Warn Only):
- Security Scan
- Dependency Check
- Site Validation

**Result**: Warnings displayed but workflow continues

## Workflow Efficiency

### Caching Strategy
All jobs use `enable-cache: true` for uv, which caches:
- Python installations
- Virtual environments
- Dependency downloads

### Parallel Execution
Jobs run in parallel where possible:
- `lint`, `typecheck`, `security`, `dependency-check`, and `test` run concurrently
- `site-validation` runs after tests complete
- `integration-test` runs independently
- `quality-gate` waits for all to complete

### Job Dependencies
```
lint ─────────┐
typecheck ────┤
security ─────┤
dep-check ────┼──> quality-gate
test ─────────┤
site-val ─────┤
integration ──┘
```

## Configuration Files

### `.htmlvalidate.json`
HTML validation rules for site templates and generated HTML.

**Key Rules**:
- HTML5 doctype required
- Double quotes for attributes
- No duplicate IDs or attributes
- Required element attributes enforced

### `.bandit`
Security scanning configuration.

**Settings**:
- Excludes: `/tests`, `/.venv`, `/__pycache__`
- Targets: `src/` directory
- Severity: LOW
- Confidence: MEDIUM

## Running Checks Locally

### Pre-commit Checks
```bash
# Full pre-commit validation
ruff check . && ruff format --check . && mypy src/ && pytest -m "not integration"

# Security scan
bandit -r src/

# Dependency check
safety check
```

### Site Validation
```bash
# Run smoke tests
pytest tests/test_smoke_site.py tests/test_templates.py -v

# Generate and validate HTML
npm install -g html-validate
uv run gh-year-end report --config config/config.example.yaml
html-validate site/templates/*.html
```

### Integration Tests
```bash
# Requires GITHUB_TOKEN
export GITHUB_TOKEN=ghp_xxxxx
pytest -m integration
```

## Artifact Management

### Security Reports
- **Retention**: 30 days
- **Format**: JSON
- **Access**: Download from GitHub Actions UI

### Site Output
- **Retention**: 7 days
- **Contents**: Generated HTML, CSS, JS, data files
- **Purpose**: Manual inspection and debugging

## Troubleshooting

### Job Failures

#### Lint Failures
```bash
# Auto-fix formatting
ruff format .

# Auto-fix linting issues
ruff check . --fix
```

#### Type Check Failures
```bash
# Run with verbose output
mypy src/ --show-error-codes
```

#### Test Failures
```bash
# Run with verbose output
pytest -vv --tb=long

# Run specific test
pytest tests/test_file.py::test_function -v
```

#### Security Issues
```bash
# Review bandit report
bandit -r src/ -f screen

# Check specific issue
bandit -r src/ -t B201
```

#### Site Validation Failures
```bash
# Run smoke tests locally
pytest tests/test_smoke_site.py -v

# Validate templates
html-validate site/templates/*.html --config .htmlvalidate.json
```

## Best Practices

### Before Committing
1. Run local pre-commit checks
2. Ensure tests pass
3. Run security scan if touching sensitive code
4. Validate site generation if modifying templates

### During PR Review
1. Check quality-gate job status
2. Review any P1 warnings (security, dependencies, site)
3. Download artifacts for manual inspection if needed
4. Verify coverage metrics in Codecov

### Maintaining the Workflow
1. Keep uv version up to date
2. Update security tools monthly
3. Review and update validation rules quarterly
4. Monitor artifact storage usage

## Quality Gate Priorities

| Priority | Blocks Merge? | Examples | Action |
|----------|--------------|----------|--------|
| P0 | Yes | Lint, type check, tests, secrets | Fix immediately |
| P1 | No | Security warnings, coverage <80% | Fix in next release |
| P2 | No | Complexity warnings, doc gaps | Track for improvement |

## References

- [Project Standards (CLAUDE.md)](/home/william/git/yakshave/CLAUDE.md)
- [GitHub Actions Documentation](https://docs.github.com/actions)
- [Ruff Documentation](https://docs.astral.sh/ruff/)
- [Bandit Documentation](https://bandit.readthedocs.io/)
- [html-validate Documentation](https://html-validate.org/)
