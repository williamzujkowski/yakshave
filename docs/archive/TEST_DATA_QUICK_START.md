# Test Data Quick Start

Quick reference for using test data to develop and test the gh-year-end website.

## One-Command Setup

```bash
# Generate test data and build site (default: year 2024)
./scripts/quick_test_site.sh

# For a different year
./scripts/quick_test_site.sh 2025
```

This will:
1. Generate sample metrics data
2. Export to JSON
3. Build the static site
4. Show you how to view it

## View the Test Site

**Option 1: Direct file access**
```bash
open site/2024/index.html
```

**Option 2: Local HTTP server**
```bash
python -m http.server --directory site/2024 8000
# Visit http://localhost:8000
```

## Manual Steps

If you prefer step-by-step control:

```bash
# 1. Generate test data
uv run python scripts/setup_test_data.py --year 2024

# 2. Build site
uv run gh-year-end report --config config/config.yaml

# 3. View
open site/2024/index.html
```

## Test Data Contents

The generated test data includes:

- **10 sample contributors**: alice, bob, charlie, diana, eve, frank, grace, henry, iris, jack
- **5 sample repositories**: backend-api, frontend-web, mobile-app, data-pipeline, docs-site
- **Full year of activity**: 52 weeks + 12 months of time series data
- **All metric types**: Leaderboards, time series, repo health, hygiene scores, awards

## For Testing/Development

### Run Tests with Sample Data

```bash
# All fixture tests
uv run pytest tests/test_sample_metrics_fixtures.py -v

# Specific test
uv run pytest tests/test_sample_metrics_fixtures.py::test_build_site_with_sample_data -v
```

### Use in Your Tests

```python
def test_my_feature(sample_metrics_config, sample_metrics_paths):
    """Test using sample metrics."""
    # Your test code here
    stats = build_site(sample_metrics_config, sample_metrics_paths)
    assert len(stats["templates_rendered"]) > 0
```

### Regenerate Fixtures

```bash
# Update pytest fixtures with latest schema
uv run python scripts/setup_test_data.py --year 2024 --output tests/fixtures/sample_metrics
```

## Customization

To modify test data characteristics, edit `scripts/setup_test_data.py`:

- **SAMPLE_USERS**: Add/modify test users
- **SAMPLE_REPOS**: Add/modify test repositories
- **Metric values**: Change contribution counts and patterns

## Directory Structure

```
gh-year-end/
├── scripts/
│   ├── setup_test_data.py       # Data generator
│   └── quick_test_site.sh       # One-command builder
├── tests/
│   ├── fixtures/
│   │   └── sample_metrics/      # Pytest fixtures
│   ├── conftest.py              # Fixture definitions
│   └── test_sample_metrics_fixtures.py  # Tests
├── data/
│   └── metrics/
│       └── year=2024/           # Generated test data
└── site/
    └── 2024/                    # Built website
```

## Troubleshooting

**Problem**: Fixtures not found error

```bash
# Solution: Generate fixtures
uv run python scripts/setup_test_data.py --year 2024 --output tests/fixtures/sample_metrics
```

**Problem**: Empty site or missing data

```bash
# Solution: Regenerate everything
rm -rf data/metrics/year=2024 site/2024
./scripts/quick_test_site.sh 2024
```

**Problem**: Schema changes

1. Update `scripts/setup_test_data.py`
2. Regenerate all data:
   ```bash
   uv run python scripts/setup_test_data.py --year 2024
   uv run python scripts/setup_test_data.py --year 2024 --output tests/fixtures/sample_metrics
   ```
3. Run tests to validate

## Full Documentation

For detailed information, see:
- [docs/TEST_DATA_SETUP.md](docs/TEST_DATA_SETUP.md) - Complete guide
- [tests/fixtures/sample_metrics/README.md](tests/fixtures/sample_metrics/README.md) - Fixture details
- [tests/test_sample_metrics_fixtures.py](tests/test_sample_metrics_fixtures.py) - Usage examples

## Related Commands

```bash
# Generate custom year/location
uv run python scripts/setup_test_data.py --year 2025 --output /tmp/test

# Just export (if you have data already)
uv run gh-year-end report --config config/config.yaml

# Run all tests
uv run pytest tests/test_sample_metrics_fixtures.py -v

# View generated JSON
ls -lh site/2024/data/*.json
```
