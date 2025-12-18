# CLI Reference

Command-line interface for gh-year-end.

## Entry Point

```bash
gh-year-end [OPTIONS] COMMAND [ARGS]...
```

GitHub Year-End Community Health Report Generator. Generates comprehensive year-end reports for GitHub organizations or users, including activity metrics, leaderboards, and repository health scores.

## Global Options

| Option | Short | Description |
|--------|-------|-------------|
| `--version` | | Show version and exit |
| `--verbose` | `-v` | Enable verbose output (includes tracebacks on errors) |
| `--help` | | Show help message and exit |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GITHUB_TOKEN` | Yes | GitHub personal access token for API authentication |

Generate token at: https://github.com/settings/tokens

Required scopes: `repo`, `read:org`, `read:user`

## Commands

### plan

Preview collection plan without making changes.

```bash
gh-year-end plan --config CONFIG
```

**Options:**

| Option | Short | Required | Description |
|--------|-------|----------|-------------|
| `--config` | `-c` | Yes | Path to config.yaml file |

**Output:**

- Target org/user
- Year and time window
- Enabled data collectors
- Storage root path

**Example:**

```bash
gh-year-end plan -c config/config.yaml
```

---

### collect

Collect raw data from GitHub API.

```bash
gh-year-end collect --config CONFIG [OPTIONS]
```

Fetches all configured data types (PRs, issues, reviews, comments, commits, hygiene) from the target org/user and stores as raw JSONL files.

**Options:**

| Option | Short | Required | Description |
|--------|-------|----------|-------------|
| `--config` | `-c` | Yes | Path to config.yaml file |
| `--force` | `-f` | No | Delete checkpoint and start fresh (re-fetches all data) |
| `--resume` | `-r` | No | Require existing checkpoint (fails if none exists) |
| `--from-repo` | | No | Resume starting from specific repo (e.g., 'owner/repo') |
| `--retry-failed` | | No | Only retry repos marked as failed in checkpoint |

**Checkpoint/Resume Behavior:**

- Default: Creates checkpoint, resumes automatically if interrupted
- `--force`: Deletes checkpoint, re-fetches everything
- `--resume`: Requires checkpoint to exist (fails otherwise)
- `--from-repo`: Skips to specific repository in collection order
- `--retry-failed`: Only processes failed repositories

**Output:**

Raw JSONL files in `data/raw/year=YYYY/source=github/target=<name>/`:

- `manifest.json` - Collection summary
- `checkpoint.json` - Resume state
- `repos.jsonl` - Repository metadata
- `rate_limit_samples.jsonl` - API rate limit samples
- `pulls/*.jsonl` - Pull requests per repo
- `issues/*.jsonl` - Issues per repo
- `reviews/*.jsonl` - PR reviews per repo
- `issue_comments/*.jsonl` - Issue comments per repo
- `review_comments/*.jsonl` - Review comments per repo
- `commits/*.jsonl` - Commits per repo
- `repo_tree/*.jsonl` - File tree snapshots per repo
- `branch_protection/*.jsonl` - Branch protection rules per repo
- `security_features/*.jsonl` - Security features per repo

**Example:**

```bash
# Fresh collection
gh-year-end collect -c config/config.yaml

# Force re-collection
gh-year-end collect -c config/config.yaml --force

# Resume from checkpoint
gh-year-end collect -c config/config.yaml --resume

# Resume from specific repo
gh-year-end collect -c config/config.yaml --from-repo myorg/myrepo

# Retry only failed repos
gh-year-end collect -c config/config.yaml --retry-failed
```

**Interrupt Handling:**

Press Ctrl+C to interrupt. Checkpoint is saved automatically. Resume with `--resume`.

---

### status

Show current collection status from checkpoint.

```bash
gh-year-end status --config CONFIG
```

**Options:**

| Option | Short | Required | Description |
|--------|-------|----------|-------------|
| `--config` | `-c` | Yes | Path to config.yaml file |

**Output:**

- Run information (target, year, timestamps)
- Phase progress (current phase, completed phases)
- Repository progress (complete, in-progress, failed, pending)
- Failed repositories with error details
- ETA for completion (if in progress)
- Next steps hints

**Example:**

```bash
gh-year-end status -c config/config.yaml
```

---

### normalize

Normalize raw data to curated Parquet tables.

```bash
gh-year-end normalize --config CONFIG
```

Converts raw JSONL files to normalized Parquet tables with consistent schemas, bot detection, and identity resolution.

**Options:**

| Option | Short | Required | Description |
|--------|-------|----------|-------------|
| `--config` | `-c` | Yes | Path to config.yaml file |

**Input:**

Raw JSONL files from `data/raw/year=YYYY/`

**Output:**

Curated Parquet tables in `data/curated/year=YYYY/`:

**Dimension Tables:**
- `dim_user.parquet` - User identities with bot classification
- `dim_identity_rule.parquet` - Bot detection rules applied
- `dim_repo.parquet` - Repository metadata

**Fact Tables:**
- `fact_pull_request.parquet` - Pull requests
- `fact_issue.parquet` - Issues
- `fact_review.parquet` - PR reviews
- `fact_issue_comment.parquet` - Issue comments
- `fact_review_comment.parquet` - Review comments
- `fact_commit.parquet` - Commits
- `fact_commit_file.parquet` - Files changed per commit
- `fact_repo_files_presence.parquet` - Hygiene file presence
- `fact_repo_hygiene.parquet` - Branch protection rules
- `fact_repo_security_features.parquet` - Security features

**Example:**

```bash
gh-year-end normalize -c config/config.yaml
```

---

### metrics

Compute metrics from curated data.

```bash
gh-year-end metrics --config CONFIG
```

Calculates leaderboards, time series, repository health scores, hygiene scores, and awards from normalized Parquet tables.

**Options:**

| Option | Short | Required | Description |
|--------|-------|----------|-------------|
| `--config` | `-c` | Yes | Path to config.yaml file |

**Input:**

Curated Parquet tables from `data/curated/year=YYYY/`

**Output:**

Metrics Parquet tables in `data/metrics/year=YYYY/`:

- `metrics_leaderboard.parquet` - User activity rankings
- `metrics_repo_health.parquet` - Repository health scores
- `metrics_time_series.parquet` - Weekly/monthly activity trends
- `metrics_repo_hygiene_score.parquet` - Repository hygiene scores
- `metrics_awards.parquet` - Special achievement awards

**Example:**

```bash
gh-year-end metrics -c config/config.yaml
```

---

### report

Generate static HTML report.

```bash
gh-year-end report --config CONFIG [OPTIONS]
```

Exports metrics to JSON and builds a static D3-powered site with exec summary and engineer drilldown views.

**Options:**

| Option | Short | Required | Description |
|--------|-------|----------|-------------|
| `--config` | `-c` | Yes | Path to config.yaml file |
| `--force` | `-f` | No | Rebuild site even if it already exists |

**Input:**

Metrics Parquet tables from `data/metrics/year=YYYY/`

**Output:**

Static HTML site in `site/year=YYYY/`:

- `index.html` - Main landing page
- `exec.html` - Executive summary view
- `engineer.html` - Engineer drilldown view
- `data/*.json` - Exported metrics as JSON
- `assets/` - CSS, JavaScript, D3 visualizations

**Example:**

```bash
gh-year-end report -c config/config.yaml

# Force rebuild
gh-year-end report -c config/config.yaml --force
```

**Viewing the report:**

```bash
python -m http.server -d site/year=2025
# Open http://localhost:8000 in browser
```

---

### all

Run complete pipeline: collect → normalize → metrics → report.

```bash
gh-year-end all --config CONFIG [OPTIONS]
```

Main command to generate a complete year-end report from scratch.

**Options:**

| Option | Short | Required | Description |
|--------|-------|----------|-------------|
| `--config` | `-c` | Yes | Path to config.yaml file |
| `--force` | `-f` | No | Re-fetch data even if raw files exist |

**Example:**

```bash
# First-time full report
gh-year-end all -c config/config.yaml

# Force re-collection and regeneration
gh-year-end all -c config/config.yaml --force
```

---

## Typical Workflows

### First-Time Full Report

```bash
# 1. Set GitHub token
export GITHUB_TOKEN=ghp_xxxxxxxxxxxxx

# 2. Preview plan
gh-year-end plan -c config/config.yaml

# 3. Run full pipeline
gh-year-end all -c config/config.yaml

# 4. View report
python -m http.server -d site/year=2025
```

### Incremental Development

```bash
# Collect once
gh-year-end collect -c config/config.yaml

# Iterate on normalization/metrics/report
gh-year-end normalize -c config/config.yaml
gh-year-end metrics -c config/config.yaml
gh-year-end report -c config/config.yaml

# Rebuild report after template changes
gh-year-end report -c config/config.yaml --force
```

### Resume Interrupted Collection

```bash
# Collection interrupted (Ctrl+C or error)
gh-year-end collect -c config/config.yaml
# ^C

# Check status
gh-year-end status -c config/config.yaml

# Resume from checkpoint
gh-year-end collect -c config/config.yaml --resume

# Or retry only failed repos
gh-year-end collect -c config/config.yaml --retry-failed
```

---

## Data Pipeline Flow

```
┌─────────────────┐
│  GitHub API     │
└────────┬────────┘
         │
         ▼ collect
┌─────────────────┐
│  Raw JSONL      │  data/raw/year=YYYY/
│  - repos        │  - checkpoint.json (resume state)
│  - pulls        │  - manifest.json (summary)
│  - issues       │  - rate_limit_samples.jsonl
│  - reviews      │  - repos.jsonl
│  - comments     │  - pulls/*.jsonl
│  - commits      │  - issues/*.jsonl
│  - hygiene      │  - reviews/*.jsonl
└────────┬────────┘  - [comments|commits|hygiene]/*.jsonl
         │
         ▼ normalize
┌─────────────────┐
│  Curated        │  data/curated/year=YYYY/
│  Parquet        │  - dim_user.parquet
│  - Dimensions   │  - dim_identity_rule.parquet
│  - Facts        │  - dim_repo.parquet
│                 │  - fact_pull_request.parquet
│                 │  - fact_issue.parquet
└────────┬────────┘  - fact_[review|comment|commit]*.parquet
         │
         ▼ metrics
┌─────────────────┐
│  Metrics        │  data/metrics/year=YYYY/
│  Parquet        │  - metrics_leaderboard.parquet
│  - Leaderboards │  - metrics_repo_health.parquet
│  - Time series  │  - metrics_time_series.parquet
│  - Health       │  - metrics_repo_hygiene_score.parquet
│  - Awards       │  - metrics_awards.parquet
└────────┬────────┘
         │
         ▼ report
┌─────────────────┐
│  Static Site    │  site/year=YYYY/
│  - HTML         │  - index.html
│  - JSON         │  - exec.html
│  - D3 viz       │  - engineer.html
│                 │  - data/*.json
│                 │  - assets/[css|js|images]/
└─────────────────┘
```

---

## Storage Structure

```
.
├── config/
│   ├── config.yaml          # Main configuration
│   └── awards.yaml          # Awards definitions
│
├── data/                    # Configured via storage.root
│   ├── raw/
│   │   └── year=2025/
│   │       └── source=github/
│   │           └── target=myorg/
│   │               ├── checkpoint.json
│   │               ├── manifest.json
│   │               ├── rate_limit_samples.jsonl
│   │               ├── repos.jsonl
│   │               ├── pulls/
│   │               │   └── myorg__repo1.jsonl
│   │               ├── issues/
│   │               ├── reviews/
│   │               ├── issue_comments/
│   │               ├── review_comments/
│   │               ├── commits/
│   │               ├── repo_tree/
│   │               ├── branch_protection/
│   │               └── security_features/
│   │
│   ├── curated/
│   │   └── year=2025/
│   │       ├── dim_user.parquet
│   │       ├── dim_identity_rule.parquet
│   │       ├── dim_repo.parquet
│   │       ├── fact_pull_request.parquet
│   │       ├── fact_issue.parquet
│   │       ├── fact_review.parquet
│   │       ├── fact_issue_comment.parquet
│   │       ├── fact_review_comment.parquet
│   │       ├── fact_commit.parquet
│   │       ├── fact_commit_file.parquet
│   │       ├── fact_repo_files_presence.parquet
│   │       ├── fact_repo_hygiene.parquet
│   │       └── fact_repo_security_features.parquet
│   │
│   └── metrics/
│       └── year=2025/
│           ├── metrics_leaderboard.parquet
│           ├── metrics_repo_health.parquet
│           ├── metrics_time_series.parquet
│           ├── metrics_repo_hygiene_score.parquet
│           └── metrics_awards.parquet
│
└── site/                    # Configured via report.output_dir
    └── year=2025/
        ├── index.html
        ├── exec.html
        ├── engineer.html
        ├── data/
        │   ├── leaderboard.json
        │   ├── repo_health.json
        │   ├── time_series.json
        │   ├── hygiene_scores.json
        │   └── awards.json
        └── assets/
            ├── css/
            ├── js/
            └── images/
```

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error (exception during execution) |
| 130 | Interrupted by user (Ctrl+C) |

**Note:** When collection is interrupted (exit 130), checkpoint is saved automatically. Resume with `--resume` flag.

---

## Troubleshooting

### No checkpoint found

**Error:**
```
No checkpoint found
```

**Solution:**

Run `collect` command without `--resume` flag to start new collection.

---

### Checkpoint config mismatch

**Error:**
```
Error: Checkpoint config mismatch. Use --force to restart.
```

**Cause:**

Config file changed since checkpoint was created (different target, year, or enabled collectors).

**Solution:**

```bash
# Option 1: Use --force to delete checkpoint and start fresh
gh-year-end collect -c config/config.yaml --force

# Option 2: Restore original config
# Edit config.yaml to match checkpoint settings
```

---

### Resume requires existing checkpoint

**Error:**
```
Error: --resume requires existing checkpoint
```

**Cause:**

Used `--resume` flag but no checkpoint exists.

**Solution:**

```bash
# Remove --resume flag
gh-year-end collect -c config/config.yaml
```

---

### No raw data found

**Error:**
```
Error: No raw data found. Run 'collect' command first.
```

**Cause:**

Attempted to run `normalize` before running `collect`.

**Solution:**

```bash
# Run collection first
gh-year-end collect -c config/config.yaml

# Then normalize
gh-year-end normalize -c config/config.yaml
```

---

### No curated data found

**Error:**
```
Error: No curated data found. Run 'normalize' command first.
```

**Cause:**

Attempted to run `metrics` before running `normalize`.

**Solution:**

```bash
# Run normalization first
gh-year-end normalize -c config/config.yaml

# Then metrics
gh-year-end metrics -c config/config.yaml
```

---

### No metrics data found

**Error:**
```
Error: No metrics data found. Run 'metrics' command first.
```

**Cause:**

Attempted to run `report` before running `metrics`.

**Solution:**

```bash
# Run metrics first
gh-year-end metrics -c config/config.yaml

# Then report
gh-year-end report -c config/config.yaml
```

---

### GitHub API rate limit exceeded

**Symptoms:**

- Collection slows down significantly
- Many 403 responses in verbose output
- Rate limit samples show remaining=0

**Solution:**

Collection handles rate limiting automatically:

1. Sleeps until rate limit resets
2. Uses adaptive concurrency to avoid secondary limits
3. Samples rate limit endpoint periodically

No action required. Wait for collection to complete.

To reduce wait time:

- Use a GitHub App token (higher rate limits)
- Reduce `rate_limit.max_concurrency` in config
- Increase `rate_limit.min_sleep_seconds` to be more conservative

---

### Authentication failure

**Error:**
```
401 Unauthorized
```

**Cause:**

Missing or invalid `GITHUB_TOKEN` environment variable.

**Solution:**

```bash
# Generate token at https://github.com/settings/tokens
# Required scopes: repo, read:org, read:user

export GITHUB_TOKEN=ghp_xxxxxxxxxxxxx

# Verify token
curl -H "Authorization: token $GITHUB_TOKEN" https://api.github.com/user
```

---

### Permission denied (403)

**Error:**
```
403 Forbidden
```

**Cause:**

GitHub token lacks required permissions for target org/user.

**Solution:**

For organization targets:

1. Ensure token has `read:org` scope
2. Ensure user is member of organization
3. Check organization SSO requirements (may need token authorization)

For repository-specific errors:

1. Ensure token has `repo` scope (or `public_repo` for public repos only)
2. Ensure user has read access to repository

---

### Out of memory

**Symptoms:**

- Process killed during normalization or metrics
- `MemoryError` exception

**Solution:**

```bash
# Process repos in smaller batches by targeting specific repos
# Edit config.yaml to exclude large/inactive repos

# Or increase system memory/swap
# Or use a machine with more RAM
```

---

### Disk space full

**Error:**
```
OSError: [Errno 28] No space left on device
```

**Solution:**

```bash
# Check disk usage
df -h

# Check data directory size
du -sh data/

# Clean up old data
rm -rf data/raw/year=2024/
rm -rf data/curated/year=2024/
rm -rf data/metrics/year=2024/
rm -rf site/year=2024/

# Or change storage.root in config to different volume
```

---

### Template or asset errors

**Error:**
```
FileNotFoundError: [template or asset file]
```

**Cause:**

Missing template files or assets in repository.

**Solution:**

```bash
# Ensure complete checkout
git status
git pull

# Verify templates exist
ls -la site/

# Reinstall package
uv sync --refresh
```

---

## See Also

- [config/schema.json](../config/schema.json) - Configuration schema
- [IMPLEMENTATION_SUMMARY_PHASE5.md](../IMPLEMENTATION_SUMMARY_PHASE5.md) - Metrics implementation details
- [docs/](../docs/) - Additional documentation
