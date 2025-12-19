# Sample Metrics Fixtures

This directory contains minimal test data for rapid website development and testing.

## Contents

- `metrics_leaderboard.parquet` - Sample contributor leaderboards (10 users, 9 metrics)
- `metrics_time_series.parquet` - Weekly/monthly activity data (52 weeks, 12 months)
- `metrics_repo_health.parquet` - Repository health metrics (5 repos)
- `metrics_repo_hygiene_score.parquet` - Hygiene scores (5 repos)
- `metrics_awards.parquet` - Sample awards (5 awards across categories)

## Usage

### Generate Test Data

```bash
# Generate to default location (data/metrics/year=2024)
python scripts/setup_test_data.py

# Generate to custom location
python scripts/setup_test_data.py --year 2025 --output tests/fixtures/sample_metrics
```

### Use in Tests

```python
@pytest.fixture
def sample_metrics_path(tmp_path):
    """Provide path to sample metrics data."""
    # Copy fixtures to tmp_path or use directly
    return Path("tests/fixtures/sample_metrics")
```

## Data Characteristics

- **10 sample users**: alice, bob, charlie, diana, eve, frank, grace, henry, iris, jack
- **5 sample repos**: backend-api, frontend-web, mobile-app, data-pipeline, docs-site
- **Full year coverage**: Weekly and monthly time series for 2024
- **Realistic variation**: Staggered values to test visualizations
- **All categories**: Individual, repository, and risk awards

## Updating Fixtures

To regenerate fixtures with current schema:

```bash
python scripts/setup_test_data.py --year 2024 --output tests/fixtures/sample_metrics
```

This ensures fixtures stay in sync with the latest metrics schemas.
