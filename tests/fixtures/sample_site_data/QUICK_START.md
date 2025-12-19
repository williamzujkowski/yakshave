# Quick Start: Using Sample Site Data

Fast setup guide for testing website visualizations with minimal data.

## TL;DR

```bash
# Copy test data to site directory
python scripts/copy_test_site_data.py --year 2024 --force

# Open site in browser
open site/2024/index.html  # macOS
xdg-open site/2024/index.html  # Linux
start site/2024/index.html  # Windows
```

## What You Get

- 3 test users (alice, bob, charlie)
- 2 test repos (backend, frontend)
- 4 months of activity data
- All 6 visualization types populated

## Files Included

1. `summary.json` - Dashboard stats
2. `leaderboards.json` - Top contributors
3. `timeseries.json` - Activity charts
4. `repo_health.json` - Repository metrics
5. `hygiene_scores.json` - Code quality scores
6. `awards.json` - Fun achievements

## Use in Tests

```python
def test_my_visualization(load_sample_summary):
    """Test with sample data."""
    assert load_sample_summary["total_contributors"] == 3
```

See `tests/test_site_data_fixtures.py` for examples.

## More Info

Full documentation: [README.md](./README.md)
