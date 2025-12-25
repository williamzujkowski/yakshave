# Multi-Org Deployment Guide

Deploy gh-year-end for your GitHub organization or personal account.

## Quick Start

### 1. Clone and Install

```bash
git clone https://github.com/williamzujkowski/yakshave.git
cd yakshave
uv sync
```

### 2. Configure for Your Org

Copy the example config:

```bash
cp config/config.example.yaml config/config.yaml
```

Edit `config/config.yaml`:

```yaml
github:
  target:
    mode: org              # Use "org" for organizations, "user" for personal
    name: your-org-name    # Your GitHub org or username

report:
  title: "Your Org Year in Review 2025"
  base_path: "/your-repo-name"    # For GitHub Pages subdirectory
  organization_name: "Your Org"   # Display name (optional)
```

### 3. Set Token

```bash
# Using GitHub CLI (recommended)
export GITHUB_TOKEN=$(gh auth token)

# Or use a Personal Access Token
export GITHUB_TOKEN=ghp_your_token_here
```

### 4. Generate Report

```bash
uv run gh-year-end all --config config/config.yaml
```

### 5. View Locally

```bash
python -m http.server -d site/2025
# Open http://localhost:8000
```

## GitHub Token Requirements

### For Organizations

Required scopes for org analysis:
- `repo` - Full repository access
- `read:org` - Read org membership

### For Personal Accounts

Required scopes:
- `repo` - Full repository access

### Using GitHub CLI Token

The simplest approach - uses your existing GitHub CLI authentication:

```bash
export GITHUB_TOKEN=$(gh auth token)
```

Verify token has required scopes:

```bash
gh auth status
```

## GitHub Pages Deployment

### Option 1: Manual Push

```bash
# Build the site
uv run gh-year-end build --config config/config.yaml

# Push to gh-pages branch
git worktree add gh-pages-branch gh-pages
cp -r site/* gh-pages-branch/
cd gh-pages-branch
git add -A
git commit -m "Deploy year-end report"
git push origin gh-pages
```

### Option 2: GitHub Actions

Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy Year-End Report

on:
  push:
    branches: [main]
  workflow_dispatch:

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      pages: write
      id-token: write

    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v4

      - name: Install dependencies
        run: uv sync

      - name: Build report
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: uv run gh-year-end all --config config/config.yaml

      - name: Upload artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: site/

      - name: Deploy to GitHub Pages
        uses: actions/deploy-pages@v4
```

### Config for GitHub Pages

**Subdirectory deployment** (e.g., `username.github.io/repo-name`):

```yaml
report:
  base_path: "/repo-name"
  base_url: "https://username.github.io/repo-name"
```

**Root domain** (e.g., `username.github.io`):

```yaml
report:
  base_path: ""
  base_url: "https://username.github.io"
```

**Custom domain** (e.g., `year-in-review.example.com`):

```yaml
report:
  base_path: ""
  base_url: "https://year-in-review.example.com"
```

## Rate Limiting for Large Orgs

### Estimated API Usage

| Org Size | Repos | Est. API Calls | Est. Time |
|----------|-------|----------------|-----------|
| Small | <50 | ~5,000 | 10-20 min |
| Medium | 50-200 | ~20,000 | 30-60 min |
| Large | 200+ | ~50,000+ | 2-4 hours |

### Tuning for Large Orgs

```yaml
rate_limit:
  strategy: adaptive
  max_concurrency: 2        # Reduce for very large orgs
  min_sleep_seconds: 2      # Increase delay between requests

  secondary:
    max_requests_per_minute: 60   # Lower than GitHub's 90 limit
    threshold: 0.7                # Start throttling at 70%
```

### Resume After Interruption

Collection supports checkpoints:

```bash
# Resume from last checkpoint
uv run gh-year-end collect --config config/config.yaml --resume

# Retry only failed repos
uv run gh-year-end collect --config config/config.yaml --retry-failed

# Resume from specific repo
uv run gh-year-end collect --config config/config.yaml --from-repo org/repo-name
```

## Customization

### Awards Configuration

Create `config/awards.yaml`:

```yaml
awards:
  - id: pr_champion
    name: "PR Champion"
    description: "Most pull requests merged"
    metric: prs_merged
    icon: "trophy"

  - id: review_hero
    name: "Review Hero"
    description: "Most code reviews submitted"
    metric: reviews_submitted
    icon: "eye"

  - id: doc_writer
    name: "Documentation Champion"
    description: "Most documentation contributions"
    metric: docs_changes
    icon: "book"
```

Reference in config:

```yaml
report:
  awards_config: "./config/awards.yaml"
```

### Theme Options

```yaml
report:
  theme: "engineer_exec_toggle"   # Default theme with view toggle
  theme_color: "#667eea"          # PWA theme color
```

### Display Settings

```yaml
thresholds:
  leaderboard_top_n: 10          # Contributors shown in leaderboards
  contributor_chart_top_n: 10    # Contributors in charts

  # Health indicators
  hygiene_healthy: 50            # Score >= this is green
  hygiene_warning: 30            # Score >= this is yellow
  review_coverage_good: 50       # Good review coverage %
  stale_pr_days: 30              # Days before PR is stale
```

### Bot Filtering

```yaml
identity:
  humans_only: true
  bots:
    exclude_patterns:
      - ".*\\[bot\\]$"           # GitHub App bots
      - "^dependabot$"           # Dependabot
      - "^renovate\\[bot\\]$"    # Renovate
      - "^your-ci-bot$"          # Custom CI bots
    include_overrides:
      - "human-looking-bot"      # Force include as human
```

## Multi-Year Reports

Generate reports for multiple years:

```bash
# Generate 2024
# Edit config.yaml: year: 2024
uv run gh-year-end all --config config/config.yaml

# Generate 2025
# Edit config.yaml: year: 2025
uv run gh-year-end all --config config/config.yaml
```

Each year gets its own directory (`site/2024/`, `site/2025/`). The root `index.html` redirects to the most recent year.

## Troubleshooting

### Token Issues

```bash
# Verify token is set
echo $GITHUB_TOKEN | head -c 10

# Check token scopes
gh auth status
```

### Rate Limit Errors

If you hit rate limits:

1. Wait for reset (check `gh api rate_limit`)
2. Reduce concurrency in config
3. Use `--resume` to continue

### Empty Data

If reports show zeros:

1. Verify token has correct permissions
2. Check org/user name is correct
3. Verify date window includes activity
4. Run with `--verbose` for debugging

### Build Errors

```bash
# Regenerate from scratch
uv run gh-year-end all --config config/config.yaml --force
```

## Example Configs

### Personal Account

```yaml
github:
  target:
    mode: user
    name: myusername
  windows:
    year: 2025

report:
  title: "My Year in Review 2025"
  base_path: "/my-year-end"
```

### Organization

```yaml
github:
  target:
    mode: org
    name: my-org
  windows:
    year: 2025

report:
  title: "My Org Year in Review 2025"
  organization_name: "My Organization"
  base_path: "/year-end-report"
```

### Large Organization with Pre-filtering

```yaml
github:
  target:
    mode: org
    name: large-org
  discovery:
    quick_scan:
      enabled: true
    activity_filter:
      enabled: true
      min_pushed_within_days: 365
    size_filter:
      enabled: true
      min_kb: 10

rate_limit:
  max_concurrency: 2
  min_sleep_seconds: 2
```
