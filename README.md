# gh-year-end

CLI tool for generating GitHub year-end community health reports.

Collects activity data from GitHub organizations or users, runs analytics, and builds a static D3-powered site with executive summary and engineer drilldown views.

## Quick Start

Install with uv:

```bash
git clone https://github.com/williamzujkowski/yakshave.git
cd yakshave
uv sync
```

Set your GitHub token:

```bash
export GITHUB_TOKEN=ghp_your_token_here
```

Configure for your org or user:

```bash
cp config/config.example.yaml config/config.yaml
# Edit config.yaml - set target.mode (org|user) and target.name
```

Run the pipeline:

```bash
uv run gh-year-end all --config config/config.yaml
```

View the report:

```bash
python -m http.server -d site/2025
# Open http://localhost:8000
```

## CLI Commands

### plan

Shows what will be collected without making changes. Dry run.

```bash
gh-year-end plan --config config.yaml
```

### collect

Fetches raw data from GitHub API and stores as JSONL. Supports checkpoint/resume for interrupted collections.

```bash
gh-year-end collect --config config.yaml

# Flags:
#   --force            Delete checkpoint and start fresh
#   --resume           Continue from last checkpoint (fail if none exists)
#   --from-repo NAME   Resume starting from specific repo
#   --retry-failed     Only retry repos marked as failed
```

### normalize

Converts raw JSONL to curated Parquet tables with bot detection and identity resolution.

```bash
gh-year-end normalize --config config.yaml
```

### metrics

Calculates leaderboards, time series, repository health scores, and hygiene scores from curated data.

```bash
gh-year-end metrics --config config.yaml
```

### report

Exports metrics to JSON and builds static D3 site with exec and engineer views.

```bash
gh-year-end report --config config.yaml

# Flags:
#   --force    Rebuild site even if it already exists
```

### all

Runs the complete pipeline: collect, normalize, metrics, report.

```bash
gh-year-end all --config config.yaml

# Flags:
#   --force    Re-fetch data even if raw files exist
```

### status

Shows current collection progress from checkpoint. Displays completed phases, repo counts, failed repos with errors, and estimated time remaining.

```bash
gh-year-end status --config config.yaml
```

All commands support `--verbose` for detailed output.

## Features

**Checkpoint/Resume**: Collection saves progress at the repo level. Interrupt with Ctrl+C and resume later with `--resume`. The system tracks completed repos, pages fetched, and errors, allowing fine-grained restart control.

**Pre-filtering for Large Orgs**: If enabled, uses GitHub Search API to pre-filter repos based on activity, size, language, topics, and name patterns. Reduces API calls for orgs with 150+ repositories.

**Adaptive Rate Limiting**: Respects `retry-after` headers, sleeps when `x-ratelimit-remaining` hits zero, and samples `/rate_limit` endpoint periodically. Avoids secondary rate limits with configurable concurrency and pacing.

**Bot Detection**: Filters bot accounts from leaderboards based on regex patterns (`.*\[bot\]$`, `dependabot`, etc.). Supports override list for bot accounts that should count as human contributors.

**Multiple Report Views**: Executive summary (high-level metrics) and engineer drilldown (detailed activity, commit analysis, file changes).

## Data Pipeline

```
GitHub API → Raw JSONL → Curated Parquet → Metrics Parquet → Static Site
             (collect)    (normalize)        (metrics)        (report)
```

### collect

Fetches repos, pull requests, issues, reviews, comments, commits, and hygiene data (file presence, branch protection, security features). Writes JSONL with request/response envelope including headers.

### normalize

Converts raw JSONL to star schema:
- Dimension tables: `dim_user`, `dim_repo`, `dim_identity_rule`
- Fact tables: `fact_pull_request`, `fact_issue`, `fact_review`, `fact_commit`, `fact_commit_file`, `fact_issue_comment`, `fact_review_comment`, `fact_repo_files_presence`, `fact_repo_hygiene`, `fact_repo_security_features`

Applies bot detection, identity resolution, and stable ID generation. All Parquet files have deterministic row ordering.

### metrics

Computes:
- Leaderboards (PRs merged, reviews submitted, docs changes, etc.)
- Time series (weekly activity)
- Repository health (PR review coverage, staleness, response times)
- Hygiene scores (SECURITY.md, CODEOWNERS, CI workflows, branch protection)
- Awards (top contributors with customizable categories)

### report

Exports metrics to JSON and renders Jinja2 templates with D3.js visualizations. Copies static assets (CSS, JS, fonts). Final site is self-contained HTML/CSS/JS with no external dependencies.

## Configuration

See `config/config.example.yaml` for all options.

Key settings:

```yaml
github:
  target:
    mode: org          # org | user
    name: your-org
  windows:
    year: 2025
    since: "2025-01-01T00:00:00Z"
    until: "2026-01-01T00:00:00Z"

identity:
  humans_only: true    # Filter bots from leaderboards

collection:
  enable:
    pulls: true
    issues: true
    reviews: true
    comments: true
    commits: true
    hygiene: true
```

Optional pre-filtering (reduces API calls for large orgs):

```yaml
github:
  discovery:
    quick_scan:
      enabled: true    # Use Search API for pre-filtering
    filters:
      activity:
        enabled: true
        min_pushed_at_days_ago: 365
      size:
        enabled: true
        min_kb: 1
```

## Development

Run tests:

```bash
uv run pytest
```

Run with coverage:

```bash
uv run pytest --cov=src/gh_year_end --cov-report=term-missing
```

Lint and format:

```bash
uv run ruff check .
uv run ruff format .
```

Type check:

```bash
uv run mypy src/
```

Pre-commit checks:

```bash
ruff check . && ruff format --check . && mypy src/ && pytest
```

## License

MIT
