# Testing with Real GitHub Data

Quick reference guide for using real GitHub data to test the gh-year-end website.

## Quick Start

### Option 1: Use Cached Real Data (Recommended)

**Zero API calls. Works offline.**

```bash
# One-time setup: Capture the fixture
export GITHUB_TOKEN=ghp_xxxxx
./scripts/capture_real_fixture.sh

# Use the fixture for testing (unlimited times, no API calls)
uv run gh-year-end normalize --config tests/fixtures/real_williamzujkowski_2024_config.yaml
uv run gh-year-end metrics --config tests/fixtures/real_williamzujkowski_2024_config.yaml
uv run gh-year-end report --config tests/fixtures/real_williamzujkowski_2024_config.yaml

# View the site
python -m http.server -d site/2024
```

### Option 2: Fresh Live Data (Development)

**~50 API calls. Requires token.**

```bash
export GITHUB_TOKEN=ghp_xxxxx

# Collect fresh data from your user account
uv run gh-year-end all --config config/demo-williamzujkowski-2024.yaml --force
```

## Comparison

| Method | API Calls | Speed | Use Case |
|--------|-----------|-------|----------|
| Cached Fixture | 0 | Instant | Daily testing, CI/CD |
| Live Collection | 150-300 | 2-5 min | Fixture refresh, debugging |
| Synthetic Data | 0 | Instant | Unit tests, edge cases |

## Testing Workflow

### Testing Website Changes

```bash
# 1. Normalize cached real data
uv run gh-year-end normalize --config tests/fixtures/real_williamzujkowski_2024_config.yaml

# 2. Generate metrics
uv run gh-year-end metrics --config tests/fixtures/real_williamzujkowski_2024_config.yaml

# 3. Build and view site
uv run gh-year-end report --config tests/fixtures/real_williamzujkowski_2024_config.yaml
python -m http.server -d site/2024
```

### Testing Data Pipeline

```bash
# Run pytest with real data fixtures
uv run pytest tests/test_real_data_smoke.py -v

# Or run end-to-end with cached data
uv run pytest tests/test_end_to_end.py -v
```

### Refreshing Fixtures (Quarterly)

```bash
# Capture fresh data
export GITHUB_TOKEN=ghp_xxxxx
./scripts/capture_real_fixture.sh

# Commit updated fixture
git add tests/fixtures/real_williamzujkowski_2024/
git commit -m "chore: refresh real data fixture"
```

## Fixture Details

### What's Included

- Public repositories from williamzujkowski (2024)
- Pull requests, issues, reviews, comments
- Commit history with file changes
- Repository hygiene data (branch protection, security features)

### What's NOT Included

- Private repositories
- API tokens or secrets
- Data from other users/orgs
- Real-time updates (snapshot is frozen)

### Storage

- **Location**: `tests/fixtures/real_williamzujkowski_2024/`
- **Size**: ~2-5 MB
- **Format**: JSONL files (same as live collection)

## Troubleshooting

### Fixture not found

```bash
# Ensure fixture exists
ls tests/fixtures/real_williamzujkowski_2024/raw

# If missing, capture it
./scripts/capture_real_fixture.sh
```

### API rate limit errors

Switch to cached fixture - no API calls needed:

```bash
# Use cached data instead of live API
uv run gh-year-end normalize --config tests/fixtures/real_williamzujkowski_2024_config.yaml
```

### Stale fixture data

Refresh quarterly or when testing new features:

```bash
export GITHUB_TOKEN=ghp_xxxxx
./scripts/capture_real_fixture.sh
```

## See Also

- [REAL_DATA_TESTING_STRATEGY.md](../REAL_DATA_TESTING_STRATEGY.md) - Detailed strategy document
- [tests/fixtures/real_williamzujkowski_2024/README.md](../tests/fixtures/real_williamzujkowski_2024/README.md) - Fixture metadata
- [config/demo-williamzujkowski-2024.yaml](../config/demo-williamzujkowski-2024.yaml) - Live collection config
