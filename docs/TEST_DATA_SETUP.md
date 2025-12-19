# Test Data Setup Guide

This guide explains how to use the test data generation system for rapid local development and testing of the gh-year-end website.

## Overview

The test data system provides minimal but realistic sample data for website development without requiring full data collection from GitHub. This is useful for:

- Frontend development and iteration
- Template testing
- Visualization debugging
- CI/CD pipeline validation
- Quick local demos

## Quick Start

### 1. Generate Test Data to Default Location

```bash
# Generate sample metrics for year 2024
uv run python scripts/setup_test_data.py

# Output: data/metrics/year=2024/*.parquet
```

### 2. Build Website with Test Data

```bash
# Export metrics to JSON and build site
uv run gh-year-end report --config config/config.yaml
```

### 3. View Website

```bash
# Open in browser
open site/2024/index.html

# Or use a local server
python -m http.server --directory site/2024 8000
# Visit http://localhost:8000
```

## Test Data Contents

The generated test data includes:

### Metrics Files

| File | Records | Description |
|------|---------|-------------|
| `metrics_leaderboard.parquet` | 225 | Contributor rankings (10 users, 9 metrics, org + repo scopes) |
| `metrics_time_series.parquet` | 855 | Weekly/monthly activity (52 weeks + 12 months) |
| `metrics_repo_health.parquet` | 5 | Repository health metrics |
| `metrics_repo_hygiene_score.parquet` | 5 | Repository hygiene scores |
| `metrics_awards.parquet` | 5 | Sample awards (individual, repository, risk) |

### Sample Data Characteristics

**Users (10 total)**:
- alice, bob, charlie, diana, eve, frank, grace, henry, iris, jack
- Varied contribution levels to test leaderboards
- Realistic activity patterns

**Repositories (5 total)**:
- test-org/backend-api
- test-org/frontend-web
- test-org/mobile-app
- test-org/data-pipeline
- test-org/docs-site

**Metrics Coverage**:
- Full year of weekly data (52 weeks)
- Monthly aggregates (12 months)
- Org-wide and per-repo breakdowns
- Varied hygiene scores (85 to 45)

## Advanced Usage

### Custom Year

```bash
# Generate data for different year
uv run python scripts/setup_test_data.py --year 2025 --output data/metrics/year=2025
```

### Custom Output Directory

```bash
# Generate to custom location
uv run python scripts/setup_test_data.py --output /tmp/test_metrics
```

### Update Test Fixtures

```bash
# Regenerate fixtures used by pytest
uv run python scripts/setup_test_data.py --year 2024 --output tests/fixtures/sample_metrics
```

## Using in Tests

### Pytest Fixtures

The test suite includes fixtures for using sample data:

```python
def test_my_feature(sample_metrics_config, sample_metrics_paths):
    """Test using sample metrics data."""
    # Fixtures provide:
    # - sample_metrics_config: Config with test data paths
    # - sample_metrics_paths: PathManager instance
    # - sample_metrics_dir: Path to fixtures directory

    # Run export
    stats = export_metrics(sample_metrics_config, sample_metrics_paths)
    assert len(stats["files_written"]) > 0

    # Build site
    build_stats = build_site(sample_metrics_config, sample_metrics_paths)
    assert len(build_stats["templates_rendered"]) > 0
```

### Available Fixtures

- `sample_metrics_dir`: Path to fixtures directory
- `sample_metrics_config`: Config instance with test data
- `sample_metrics_paths`: PathManager for test data

See `tests/test_sample_metrics_fixtures.py` for usage examples.

## Data Schema Reference

### metrics_leaderboard.parquet

```python
{
    "year": int32,                  # Year (e.g., 2024)
    "metric_key": string,           # Metric identifier (e.g., "prs_merged")
    "scope": string,                # "org" or "repo"
    "repo_id": string | null,       # Repository ID (null for org scope)
    "user_id": string,              # User ID
    "value": int64,                 # Metric value
    "rank": int32,                  # Rank within scope (1-based)
}
```

**Metrics included**:
- prs_opened, prs_merged
- reviews_submitted, approvals, changes_requested
- issues_opened, issues_closed
- comments_total, review_comments_total

### metrics_time_series.parquet

```python
{
    "year": int32,                  # Year
    "period_type": string,          # "week" or "month"
    "period_start": date,           # Period start date
    "period_end": date,             # Period end date
    "scope": string,                # "org" or "repo"
    "repo_id": string | null,       # Repository ID (null for org)
    "metric_key": string,           # Metric identifier
    "value": int64,                 # Count for period
}
```

### metrics_repo_health.parquet

```python
{
    "repo_id": string,                          # Repository ID
    "repo_full_name": string,                   # Full name (org/repo)
    "year": int32,                              # Year
    "active_contributors_30d": int32,           # Contributors (30d)
    "active_contributors_90d": int32,           # Contributors (90d)
    "active_contributors_365d": int32,          # Contributors (365d)
    "prs_opened": int32,                        # PRs opened
    "prs_merged": int32,                        # PRs merged
    "issues_opened": int32,                     # Issues opened
    "issues_closed": int32,                     # Issues closed
    "review_coverage": float32,                 # % PRs with reviews
    "median_time_to_first_review": float32,     # Hours to review
    "median_time_to_merge": float32,            # Hours to merge
    "stale_pr_count": int32,                    # Stale PRs (>30d)
    "stale_issue_count": int32,                 # Stale issues (>30d)
}
```

### metrics_repo_hygiene_score.parquet

```python
{
    "repo_id": string,                          # Repository ID
    "repo_full_name": string,                   # Full name
    "year": int32,                              # Year
    "score": int32,                             # Score 0-100
    "has_readme": bool,                         # Has README
    "has_license": bool,                        # Has LICENSE
    "has_contributing": bool,                   # Has CONTRIBUTING
    "has_code_of_conduct": bool,                # Has CODE_OF_CONDUCT
    "has_security_md": bool,                    # Has SECURITY.md
    "has_codeowners": bool,                     # Has CODEOWNERS
    "has_ci_workflows": bool,                   # Has CI workflows
    "branch_protection_enabled": bool | null,   # Branch protection
    "requires_reviews": bool | null,            # Requires reviews
    "dependabot_enabled": bool | null,          # Dependabot enabled
    "secret_scanning_enabled": bool | null,     # Secret scanning
    "notes": string,                            # Issues/warnings
}
```

### metrics_awards.parquet

```python
{
    "award_key": string,                # Unique identifier
    "title": string,                    # Display title
    "description": string,              # Award description
    "category": string,                 # "individual", "repository", or "risk"
    "winner_user_id": string | null,    # User ID (for individual)
    "winner_repo_id": string | null,    # Repo ID (for repository)
    "winner_name": string,              # Display name
    "supporting_stats": string,         # JSON stats blob
}
```

## Modifying Test Data

To customize the generated data, edit `scripts/setup_test_data.py`:

1. **Add more users**: Append to `SAMPLE_USERS` list
2. **Add more repos**: Append to `SAMPLE_REPOS` list
3. **Change metrics values**: Modify value arrays in generator functions
4. **Add new metrics**: Extend metric lists in generator functions

After modifications:

```bash
# Regenerate test data
uv run python scripts/setup_test_data.py

# Regenerate pytest fixtures
uv run python scripts/setup_test_data.py --output tests/fixtures/sample_metrics

# Run tests to validate
uv run pytest tests/test_sample_metrics_fixtures.py -v
```

## Troubleshooting

### Missing Fixtures Error

```
FileNotFoundError: Sample metrics fixtures not found at tests/fixtures/sample_metrics
```

**Solution**: Generate the fixtures:
```bash
uv run python scripts/setup_test_data.py --year 2024 --output tests/fixtures/sample_metrics
```

### Empty or Missing Data Files

**Solution**: Regenerate all test data:
```bash
# Remove old data
rm -rf data/metrics/year=2024

# Regenerate
uv run python scripts/setup_test_data.py --year 2024
```

### Schema Mismatch Errors

If metrics schema has changed:

1. Update generator functions in `scripts/setup_test_data.py`
2. Regenerate all fixtures and test data
3. Update this documentation

### Report Build Fails with Test Data

Check that config.yaml points to the correct data directory:

```yaml
storage:
  root: "./data"  # Should contain metrics/year=2024/

report:
  output_dir: "./site"
```

## Related Files

- `scripts/setup_test_data.py` - Generator script
- `tests/conftest.py` - Pytest fixtures
- `tests/test_sample_metrics_fixtures.py` - Fixture tests
- `tests/fixtures/sample_metrics/` - Fixture files
- `tests/fixtures/sample_metrics/README.md` - Fixtures documentation

## See Also

- [Architecture Documentation](../IMPLEMENTATION_SUMMARY.md)
- [Testing Guide](../tests/README.md)
- [Report Building](../src/gh_year_end/report/README.md)
