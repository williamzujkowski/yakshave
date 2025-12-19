# Scripts

Collection and utility scripts for gh-year-end.

## williamzujkowski Collection

### Quick Start

```bash
# Set GitHub token
export GITHUB_TOKEN=ghp_xxxxx

# Run collection for both 2024 and 2025
./scripts/collect_williamzujkowski.sh
```

### Individual Year Collection

```bash
# 2024 only
uv run gh-year-end all --config config/williamzujkowski_2024.yaml

# 2025 only
uv run gh-year-end all --config config/williamzujkowski_2025.yaml
```

### Configuration Details

Both configs use:
- Target: `user/williamzujkowski`
- Discovery: No forks, no archived repos
- Rate limiting: Adaptive with max_concurrency=6
- All collectors enabled
- Output: `./site`

### Rate Limit Settings

```yaml
rate_limit:
  strategy: adaptive
  max_concurrency: 6              # Increased from default 4
  min_sleep_seconds: 0.5          # Decreased from default 1.0
  burst:
    capacity: 30
    sustained_rate: 10.0
  secondary:
    max_requests_per_minute: 90
    threshold: 0.8
```

These settings balance throughput with GitHub API limits:
- Higher concurrency (6 vs 4) for faster collection
- Lower min_sleep (0.5s vs 1.0s) for better efficiency
- Burst capacity of 30 requests
- Secondary rate limit protection at 90 req/min

### Troubleshooting

**Rate limit errors**: Reduce `max_concurrency` to 4 or lower in config.

**Secondary rate limit violations**: Already configured conservatively at 90 req/min threshold.

**Missing data**: Check `data/raw/year=YYYY/manifest.json` for collection errors.

**Re-run collection**: Add `--force` flag to re-fetch existing data.

```bash
uv run gh-year-end collect --config config/williamzujkowski_2024.yaml --force
```
