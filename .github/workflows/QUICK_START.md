# CI/CD Quick Start Guide

## Overview
This document provides quick commands for common CI/CD tasks. For detailed information, see [CI_WORKFLOW.md](CI_WORKFLOW.md).

## Pre-Commit Checks (Run Before Push)

### Essential (P0)
```bash
# Format code
ruff format .

# Lint code
ruff check .

# Type check
mypy src/

# Run unit tests
pytest -m "not integration"
```

### One-Liner
```bash
ruff format . && ruff check . && mypy src/ && pytest -m "not integration"
```

## Security Checks (Recommended)

```bash
# Install security tools
uv pip install bandit[toml] safety

# Run security scan
bandit -r src/

# Check dependencies
safety check
```

## Site Validation (Before Releasing)

```bash
# Install validator
npm install -g html-validate

# Run smoke tests
pytest tests/test_smoke_site.py tests/test_templates.py -v

# Validate templates
html-validate site/templates/*.html
```

## Integration Tests (Requires GITHUB_TOKEN)

```bash
# Set token
export GITHUB_TOKEN=ghp_xxxxxxxxxxxxx

# Run integration tests
pytest -m integration
```

## Full Local CI Simulation

```bash
# Run all checks in order
echo "=== Linting ==="
ruff check . && ruff format --check .

echo "=== Type Checking ==="
mypy src/

echo "=== Security ==="
bandit -r src/

echo "=== Dependencies ==="
safety check

echo "=== Unit Tests ==="
pytest -m "not integration" --cov=src/gh_year_end --cov-report=term-missing

echo "=== Site Tests ==="
pytest tests/test_smoke_site.py tests/test_templates.py -v

echo "=== Integration Tests ==="
pytest -m integration --tb=short
```

## Quick Fixes

### Format Issues
```bash
# Auto-fix formatting
ruff format .
```

### Lint Issues
```bash
# Auto-fix linting
ruff check . --fix
```

### Coverage Too Low
```bash
# See which files need tests
pytest --cov=src/gh_year_end --cov-report=html
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

## CI Job Status

After pushing, check job status on GitHub:
- **P0 Gates** (must pass): lint, typecheck, test
- **P1 Gates** (should pass): security, dependency-check, site-validation

## Troubleshooting

### "Lint failed"
```bash
ruff check . --fix
ruff format .
```

### "Type check failed"
```bash
mypy src/ --show-error-codes
```

### "Tests failed"
```bash
pytest -vv --tb=long
```

### "Security scan warnings"
```bash
# View detailed report
bandit -r src/ -f screen
```

### "Site validation failed"
```bash
# Run locally to debug
pytest tests/test_smoke_site.py -v
html-validate site/templates/*.html --config .htmlvalidate.json
```

## When to Run What

| Action | Run Before |
|--------|------------|
| Format + Lint | Every commit |
| Type Check | Every commit |
| Unit Tests | Every commit |
| Security | Touching auth/API/secrets |
| Dependency Check | Updating dependencies |
| Site Validation | Modifying templates |
| Integration Tests | Modifying collectors/API |

## Viewing Reports

### Coverage Report
```bash
pytest --cov=src/gh_year_end --cov-report=html
open htmlcov/index.html
```

### Security Report
```bash
bandit -r src/ -f html -o bandit-report.html
open bandit-report.html
```

## GitHub Actions Artifacts

Download from Actions tab:
- `bandit-report.json` (30 day retention)
- `safety-report.json` (30 day retention)
- `test-site-output/` (7 day retention)

## Common Issues

### "uv sync fails"
```bash
# Clear cache and retry
rm -rf .venv
uv sync --all-extras
```

### "pytest can't find modules"
```bash
# Ensure package is installed
uv sync --all-extras
uv run pytest
```

### "mypy too slow"
```bash
# Use incremental mode
mypy src/ --incremental
```

## References

- Full workflow documentation: [CI_WORKFLOW.md](CI_WORKFLOW.md)
- Project standards: [CLAUDE.md](/home/william/git/yakshave/CLAUDE.md)
- Testing guide: [tests/TESTING_SUMMARY.md](/home/william/git/yakshave/tests/TESTING_SUMMARY.md)
