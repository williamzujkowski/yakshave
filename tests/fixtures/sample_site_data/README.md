# Sample Site Data - Test Fixtures

Minimal test dataset for rapid website iteration and visualization validation.

## Purpose

This directory contains a small, representative dataset for testing the gh-year-end website without running full data collection. The data is small but complete, covering all visualization types.

## Files

| File | Purpose | Test Coverage |
|------|---------|---------------|
| `summary.json` | Overall stats | Summary dashboard, header stats |
| `leaderboards.json` | Contributor rankings | Leaderboard tables, org/repo views |
| `timeseries.json` | Activity over time | Time series charts, trend analysis |
| `repo_health.json` | Repository health metrics | Repo health dashboard, KPIs |
| `hygiene_scores.json` | Repository hygiene scores | Hygiene gauge charts, checklist displays |
| `awards.json` | Fun awards | Awards page, winner displays |

## Dataset Characteristics

### Scale
- **Contributors**: 3 users (alice, bob, charlie)
- **Repositories**: 2 repos (backend, frontend)
- **Time Period**: 4 months (Jan-Apr 2024)
- **Total PRs**: 15 merged
- **Total Issues**: 8 opened, 7 closed
- **Total Reviews**: 12 submitted

### Test IDs
- User IDs: `U_001`, `U_002`, `U_003`
- Repo IDs: `R_001`, `R_002`
- Repo names: `test-org/backend`, `test-org/frontend`

### Data Quality
All files follow the exact schema produced by `gh-year-end report` command:
- Proper field types (int, float, string, bool, null)
- Correct nested structures (org/repo scopes, period types)
- Representative edge cases (null values, missing data, zero counts)

## Usage

### 1. Copy to Site Directory (Manual Testing)

Use the utility script to copy test data to the site directory:

```bash
# Copy test data for year 2024
python scripts/copy_test_site_data.py --year 2024

# Force overwrite without confirmation
python scripts/copy_test_site_data.py --year 2024 --force
```

Then open `site/2024/index.html` in a browser to test visualizations.

### 2. Use in Pytest Tests

Import fixtures in your test files:

```python
from tests.fixtures.site_data_fixtures import (
    load_sample_summary,
    load_sample_leaderboards,
    all_sample_data,
)

def test_summary_data(load_sample_summary):
    """Test summary data structure."""
    assert load_sample_summary["year"] == 2024
    assert load_sample_summary["total_contributors"] == 3

def test_all_data_files(all_sample_data):
    """Test all data files are loadable."""
    assert len(all_sample_data) == 6
    assert "summary" in all_sample_data
    assert "leaderboards" in all_sample_data
```

### 3. Direct JSON Loading

Load files directly for testing:

```python
import json
from pathlib import Path

fixtures_dir = Path("tests/fixtures/sample_site_data")

with (fixtures_dir / "summary.json").open() as f:
    summary = json.load(f)
```

## Extending the Dataset

To add more test data:

1. Follow existing schema patterns from `src/gh_year_end/report/export.py`
2. Keep data minimal (3-5 users, 2-3 repos max)
3. Update this README with new characteristics
4. Ensure all visualizations render correctly

## Schema Reference

### Summary
- Overall statistics aggregated across org
- Includes total counts and hygiene score summary
- Used for dashboard header and overview

### Leaderboards
- Nested structure: `leaderboards[metric_key][org|repos]`
- Org scope: flat list of ranked contributors
- Repo scope: grouped by repo_id
- Metrics: prs_merged, reviews_submitted, issues_opened, etc.

### Timeseries
- Nested structure: `timeseries[period_type][metric_key][org|repos]`
- Period types: month, week, day
- Each data point has period_start, period_end, value
- Used for line charts and trend analysis

### Repo Health
- Dictionary indexed by repo_id
- Contains PR/issue metrics, review coverage, staleness
- Used for repository health dashboard

### Hygiene Scores
- Dictionary indexed by repo_id
- Boolean flags for documentation, security, CI/CD
- Numeric score (0-100) calculated from flags
- Used for hygiene gauge charts

### Awards
- Grouped by category (individual, repository, risk)
- Each award has winner info and supporting stats
- Used for awards display page

## Related Files

- `/scripts/copy_test_site_data.py` - Utility to copy data to site/
- `/tests/fixtures/site_data_fixtures.py` - Pytest fixture definitions
- `/src/gh_year_end/report/export.py` - Data export logic and schemas

## Maintenance

Update this dataset when:
- Report schema changes in `export.py`
- New metric types are added
- New visualizations require additional test cases

Keep data minimal and representative. Do not duplicate production data.
