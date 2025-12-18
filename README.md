# gh-year-end

GitHub Year-End Community Health Report Generator

Generate comprehensive year-end reports for GitHub organizations or users, including activity metrics, leaderboards, repository health scores, and awards.

## Features

- **Immutable Data Snapshots**: Raw data stored locally for reproducibility
- **Leaderboards**: PRs merged, reviews submitted, docs changes, and more
- **Repository Health**: Activity metrics, review coverage, stale PRs/issues
- **Hygiene Scores**: SECURITY.md, CODEOWNERS, CI workflows, branch protection
- **Awards**: Recognize top contributors with fun, customizable awards
- **Static Report**: D3-powered HTML site with exec and engineer views

## Installation

```bash
# Clone the repository
git clone https://github.com/williamzujkowski/yakshave.git
cd yakshave

# Install with uv
uv sync

# Or with pip
pip install -e ".[dev]"
```

## Quick Start

1. **Set up your GitHub token:**
   ```bash
   export GITHUB_TOKEN=ghp_your_token_here
   ```

2. **Copy and customize the config:**
   ```bash
   cp config/config.example.yaml config/config.yaml
   # Edit config.yaml with your org/user
   ```

3. **Generate the report:**
   ```bash
   gh-year-end all --config config/config.yaml
   ```

4. **View the report:**
   ```bash
   cd site/year=2025
   python -m http.server 8000
   # Open http://localhost:8000
   ```

## CLI Commands

```bash
# Show what will be collected (dry run)
gh-year-end plan --config config.yaml

# Collect raw data from GitHub
gh-year-end collect --config config.yaml

# Normalize raw data to Parquet tables
gh-year-end normalize --config config.yaml

# Compute metrics
gh-year-end metrics --config config.yaml

# Generate static report
gh-year-end report --config config.yaml

# Run the complete pipeline
gh-year-end all --config config.yaml
```

## Configuration

See `config/config.example.yaml` for all options. Key settings:

```yaml
github:
  target:
    mode: org          # org | user
    name: your-org     # Organization or username

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
    commits: true
    hygiene: true
```

## Data Pipeline

```
GitHub API → Raw JSONL → Curated Parquet → Metrics Parquet → Static Site
              (collect)    (normalize)        (metrics)        (report)
```

### Storage Structure

```
data/
├── raw/year=2025/source=github/target=<name>/
│   ├── manifest.json
│   ├── repos.jsonl
│   ├── pulls/<repo>.jsonl
│   ├── issues/<repo>.jsonl
│   └── ...
├── curated/year=2025/
│   ├── dim_user.parquet
│   ├── dim_repo.parquet
│   ├── fact_pull_request.parquet
│   └── ...
└── metrics/year=2025/
    ├── metrics_leaderboard.parquet
    ├── metrics_repo_health.parquet
    └── ...
```

## Development

```bash
# Install dev dependencies
uv sync

# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov=src/gh_year_end

# Lint and format
uv run ruff check .
uv run ruff format .

# Type check
uv run mypy src/
```

## License

MIT

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Run `ruff check . && ruff format . && mypy src/ && pytest`
5. Submit a pull request
