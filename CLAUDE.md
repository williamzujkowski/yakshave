# CLAUDE.md — Project Instructions

```
STATUS: AUTHORITATIVE
VERSION: 1.0.0
LAST_AUDIT: 2025-12-20
NEXT_REVIEW: 2026-03-20
SCOPE: gh-year-end development standards
```

## Purpose

This file defines **project-specific rules** for AI assistants. It enforces:

* Config-first: all tunables in one config file + schema validation
* Deterministic: stable ordering, stable IDs, repeatable outputs
* Pull once: never re-fetch if raw snapshot exists unless explicitly requested
* Safe file operations and quality gates

If any rule conflicts with other documentation, **this file wins**.

---

## Project Overview

**Name**: gh-year-end
**Type**: CLI tool + static site generator
**Status**: active development (MVP)

### Tech Stack

```yaml
languages:
  primary: Python
  secondary: [JavaScript, HTML, CSS]

frameworks:
  frontend: D3.js (static site)
  backend: N/A (CLI tool)
  testing: pytest

infrastructure:
  cloud: N/A
  container: N/A
  ci_cd: GitHub Actions

database: JSON files (site data)
package_manager: uv
```

### Key Directories

```
gh-year-end/
├── src/gh_year_end/      # Main Python package
├── tests/                # pytest test suite
├── config/               # Config schema and examples
├── site/                 # Static site templates and assets
└── data/                 # Generated data (gitignored)
```

---

## Communication Style

**Be direct. Be precise. No filler.**

| Rule | Avoid | Prefer |
|------|-------|--------|
| No preamble | "I'd be happy to help..." | Jump to the answer |
| No hedging | "It seems like maybe..." | "Caused by X" or "Unknown—check Y" |
| No apologizing | "Sorry, but..." | State the constraint directly |
| Challenge errors | "Interesting approach..." | "Won't work. X guarantees not Y." |

**Format**: One idea per sentence. Bullets over prose. Code over explanation.

---

## Safety Rules

**Accuracy**
- Never fabricate versions, APIs, or flags
- Never guess technical specs—mark unknown as `[TODO: verify]`
- Never approximate metrics without source + method

**Security**
- Never commit secrets (API keys, passwords, tokens)
- Never include PII in code or comments
- Never log tokens or request auth headers
- Run security scans before committing: `bandit -r src/`

---

## Development Setup

```bash
# Clone
git clone git@github.com:williamzujkowski/yakshave.git
cd yakshave

# Install (using uv)
uv sync

# Verify
uv run gh-year-end --help
```

### Environment Variables

```bash
GITHUB_TOKEN=ghp_xxxxx  # Required for GitHub API access
```

---

## Testing

```bash
# Full suite
uv run pytest

# With coverage
uv run pytest --cov=src/gh_year_end --cov-report=term-missing

# Single file
uv run pytest tests/test_config.py

# Integration tests (requires GITHUB_TOKEN)
uv run pytest -m integration

# Live API tests (requires GITHUB_TOKEN, makes real API calls)
uv run pytest -m live_api
```

**Coverage threshold**: 45% (enforced), 80% (goal)

---

## Code Quality

### Python Standards

| Tool | Command | Config |
|------|---------|--------|
| Format | `ruff format .` | pyproject.toml |
| Lint | `ruff check .` | pyproject.toml |
| Type check | `mypy src/` | pyproject.toml |

```bash
# Pre-commit
ruff check . && ruff format --check . && mypy src/ && pytest
```

### Quality Gates

| Priority | Blocks Merge? | Examples |
|----------|--------------|----------|
| P0 | Yes | Lint errors, test failures, security vulns, secrets |
| P1 | Next release | Coverage <80%, medium vulns |
| P2 | Track | Complexity warnings, doc gaps |

---

## Git Workflow

### Commit Format

`type(scope): description`

**Types**: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`, `perf`
**Scopes**: `collect`, `collector`, `report`, `build`, `hygiene`, `identity`, `cli`, `config`, `tests`, `ci`

### PR Requirements

1. All tests pass
2. Lint/format checks pass
3. Type checking passes
4. Coverage >= 80%
5. At least one approval

### PR Template

```markdown
## Summary
[Brief description]

## Type
- [ ] Feature
- [ ] Bug fix
- [ ] Refactor
- [ ] Documentation

## Testing
[How was this tested?]

## Checklist
- [ ] Tests added/updated
- [ ] Docs updated (if needed)
```

---

## Architecture

### Key Patterns

* **Config-first**: Single `config.yaml` validated via Pydantic models
* **Simplified pipeline**: raw (JSONL) -> metrics (JSON) -> site (HTML/JS)
* **In-memory aggregation**: Metrics computed during collection, exported to JSON
* **Deterministic**: Stable ordering, stable IDs, repeatable outputs
* **Report generation**: Jinja2 template rendering with D3.js visualizations

### Important Files

| File | Purpose |
|------|---------|
| src/gh_year_end/cli.py | CLI entrypoint and commands |
| src/gh_year_end/config.py | Config loading and Pydantic validation |
| src/gh_year_end/github/ratelimit.py | Adaptive rate limiting |
| src/gh_year_end/storage/checkpoint.py | Checkpoint/resume functionality for collection |
| src/gh_year_end/report/build.py | Report generation and site building |
| src/gh_year_end/report/export.py | Metrics export to JSON |

---

## Project-Specific Rules

### Data Collection Rules
- Do not re-fetch data if cached data exists unless `--force`
- Always write `manifest.json` with counts/errors
- Always store headers relevant to rate limiting
- Always keep identity/bot filtering explainable
- Always ensure deterministic ordering and stable IDs in outputs
- Checkpoint support: collection can be interrupted and resumed using `--resume`, `--retry-failed`, or `--from-repo` flags
- Checkpoints track per-repo progress and errors, enabling granular retry strategies
- Metrics are computed in-memory during collection and exported to JSON files

### Module Size Limits
- Modules: prefer <= 400 lines. Complex collectors/orchestrators may exceed this.
- Functions: prefer <= 50 lines
- DRY/KISS principles

### Rate Limiting Requirements
- If `retry-after` exists, sleep that many seconds
- If `x-ratelimit-remaining == 0`, sleep until `x-ratelimit-reset`
- Avoid secondary limits by controlling concurrency and request pacing
- Record periodic snapshots of the `/rate_limit` endpoint response

---

## Quick Commands

```bash
# Complete pipeline (collect + build)
uv run gh-year-end all --config config/config.yaml

# Individual steps
uv run gh-year-end collect --config config/config.yaml  # Collect data and generate metrics JSON
uv run gh-year-end build --config config/config.yaml    # Build static site from JSON

# Force re-collection
uv run gh-year-end collect --config config/config.yaml --force
uv run gh-year-end all --config config/config.yaml --force
```

---

## Standards Reference

| Category | Standard | Reference |
|----------|----------|-----------|
| Code | PEP 8 + Ruff | https://docs.astral.sh/ruff/ |
| Testing | pytest best practices | https://docs.pytest.org/ |
| Security | OWASP Top 10 | https://owasp.org/Top10/ |
| Metrics | CHAOSS | https://chaoss.community/ |

Full standards: https://github.com/williamzujkowski/standards
