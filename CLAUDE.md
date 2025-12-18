# CLAUDE.md — Project Instructions

```
STATUS: AUTHORITATIVE
VERSION: 1.0.0
LAST_AUDIT: 2025-12-18
NEXT_REVIEW: 2026-03-17
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

database: DuckDB + Parquet (local analytics)
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
```

**Coverage threshold**: 80%

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

**Types**: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`
**Scopes**: `collector`, `normalize`, `metrics`, `report`, `hygiene`, `identity`, `cli`, `config`

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

* **Config-first**: Single `config.yaml` validated against JSON schema
* **Immutable snapshots**: Raw data in JSONL with request/response envelope
* **Layered data**: raw -> curated (Parquet) -> metrics (Parquet) -> report (JSON/HTML)
* **Deterministic**: Stable ordering, stable IDs, repeatable outputs
* **Report generation**: Phase 6 report module with exec/engineer views, Jinja2 template rendering, and D3.js visualizations

### Important Files

| File | Purpose |
|------|---------|
| src/gh_year_end/cli.py | CLI entrypoint and commands |
| src/gh_year_end/config.py | Config loading and validation |
| src/gh_year_end/github/ratelimit.py | Adaptive rate limiting |
| config/schema.json | Config schema for validation |

---

## Project-Specific Rules

### Data Collection Rules
- Do not re-fetch data if `data/raw/year=YYYY/...` exists unless `--force`
- Always write `manifest.json` with counts/errors
- Always store headers relevant to rate limiting
- Always keep identity/bot filtering explainable (`dim_identity_rule`, `bot_reason`)
- Always ensure deterministic ordering and stable IDs in normalized outputs

### Module Size Limits
- Modules <= 300-400 lines (some modules like checkpoint.py exceed this due to complexity, which is acceptable for cohesive functionality)
- Functions <= 50 lines
- DRY/KISS principles

### Rate Limiting Requirements
- If `retry-after` exists, sleep that many seconds
- If `x-ratelimit-remaining == 0`, sleep until `x-ratelimit-reset`
- Avoid secondary limits by controlling concurrency and request pacing
- Record periodic snapshots of the `/rate_limit` endpoint response

---

## Quick Commands

```bash
# Development
uv run gh-year-end plan --config config/config.yaml

# Check collection progress
uv run gh-year-end status --config config/config.yaml

# Build (collect all data and generate report)
uv run gh-year-end all --config config/config.yaml

# Individual steps
uv run gh-year-end collect --config config/config.yaml
uv run gh-year-end normalize --config config/config.yaml
uv run gh-year-end metrics --config config/config.yaml
uv run gh-year-end report --config config/config.yaml

# Collection with checkpoints
# Resume interrupted collection
uv run gh-year-end collect --config config/config.yaml --resume

# Retry failed repos only
uv run gh-year-end collect --config config/config.yaml --retry-failed

# Start from specific repo
uv run gh-year-end collect --config config/config.yaml --from-repo owner/repo
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
