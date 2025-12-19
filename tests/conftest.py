"""Test fixtures for gh-year-end.

Provides fixtures for:
- Live integration tests (marked with @pytest.mark.live_api)
- Sample metrics data for website testing
- Test configurations and path managers
"""

import asyncio
import json
import os
import shutil
from pathlib import Path
from typing import Any

import pytest

from gh_year_end.collect.orchestrator import run_collection
from gh_year_end.config import Config
from gh_year_end.storage.paths import PathManager


@pytest.fixture(scope="session")
def github_token() -> str:
    """Get GitHub token from environment.

    Skips test if GITHUB_TOKEN is not set.

    Returns:
        GitHub API token.
    """
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        pytest.skip("GITHUB_TOKEN not set - skipping live API test")
    return token


@pytest.fixture(scope="session")
def live_config(tmp_path_factory: pytest.TempPathFactory) -> Config:
    """Create config targeting stable public repo (github/hotkey).

    Uses year 2024 for stable, historical data.
    Conservative rate limiting to avoid secondary limits.

    Args:
        tmp_path_factory: Pytest temp path factory for session scope.

    Returns:
        Config instance for live testing.
    """
    # Create session-scoped temp directory
    temp_dir = tmp_path_factory.mktemp("live_test_data")

    return Config.model_validate(
        {
            "github": {
                "target": {"mode": "org", "name": "github"},
                "auth": {"token_env": "GITHUB_TOKEN"},
                "discovery": {
                    "include_forks": False,
                    "include_archived": False,
                    "visibility": "public",
                },
                "windows": {
                    "year": 2024,
                    "since": "2024-01-01T00:00:00Z",
                    "until": "2025-01-01T00:00:00Z",
                },
            },
            "rate_limit": {
                "strategy": "adaptive",
                "max_concurrency": 1,
                "min_sleep_seconds": 2.0,
                "max_sleep_seconds": 60.0,
                "sample_rate_limit_endpoint_every_n_requests": 50,
            },
            "identity": {
                "bots": {
                    "exclude_patterns": [r".*\[bot\]$", r"^dependabot$", r"^renovate\[bot\]$"],
                    "include_overrides": [],
                },
                "humans_only": True,
            },
            "collection": {
                "enable": {
                    "pulls": True,
                    "issues": True,
                    "reviews": True,
                    "comments": True,
                    "commits": True,
                    "hygiene": True,
                },
                "commits": {"include_files": True, "classify_files": True},
                "hygiene": {
                    "paths": [
                        "SECURITY.md",
                        "README.md",
                        "LICENSE",
                        "CONTRIBUTING.md",
                        "CODE_OF_CONDUCT.md",
                        "CODEOWNERS",
                        ".github/CODEOWNERS",
                    ],
                    "workflow_prefixes": [".github/workflows/"],
                    "branch_protection": {
                        "mode": "sample",
                        "sample_top_repos_by": "prs_merged",
                        "sample_count": 5,
                    },
                    "security_features": {"best_effort": True},
                },
            },
            "storage": {
                "root": str(temp_dir / "data"),
                "raw_format": "jsonl",
                "curated_format": "parquet",
                "dataset_version": "v1",
            },
            "report": {
                "title": "GitHub Organization 2024 Year in Review",
                "output_dir": str(temp_dir / "site"),
                "theme": "engineer_exec_toggle",
            },
        }
    )


@pytest.fixture(scope="session")
def live_paths(live_config: Config) -> PathManager:
    """Create PathManager for live tests.

    Uses session-scoped temp directory for test isolation.

    Args:
        live_config: Live test configuration.

    Returns:
        PathManager instance.
    """
    return PathManager(live_config)


@pytest.fixture(scope="session")
def cached_raw_data(github_token: str, live_config: Config, live_paths: PathManager) -> dict:
    """Run collection ONCE per session and cache data.

    This fixture:
    1. Runs data collection against github/hotkey repo (2024 data)
    2. Caches the result for reuse across all tests in session
    3. Skips if GITHUB_TOKEN is missing

    Note: This makes real GitHub API calls. Use sparingly.

    Args:
        github_token: GitHub API token (triggers skip if missing).
        live_config: Live test configuration.
        live_paths: Path manager for live tests.

    Returns:
        Collection statistics dictionary.
    """
    # Ensure directories exist
    live_paths.ensure_directories()

    # Run collection (async)
    stats = asyncio.run(run_collection(live_config, force=False))

    # Verify collection succeeded
    assert "discovery" in stats, "Collection should include discovery stats"
    assert "duration_seconds" in stats, "Collection should track duration"

    return stats


@pytest.fixture(scope="session")
def live_test_config_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create live test config file.

    Writes config.yaml to temp directory for CLI testing.

    Args:
        tmp_path_factory: Pytest temp path factory for session scope.

    Returns:
        Path to config.yaml file.
    """
    config_dir = tmp_path_factory.mktemp("config")
    config_path = config_dir / "live_test_config.yaml"

    config_content = """github:
  target:
    mode: org
    name: github
  auth:
    token_env: GITHUB_TOKEN
  discovery:
    include_forks: false
    include_archived: false
    visibility: public
  windows:
    year: 2024
    since: "2024-01-01T00:00:00Z"
    until: "2025-01-01T00:00:00Z"

rate_limit:
  strategy: adaptive
  max_concurrency: 1
  min_sleep_seconds: 2.0
  max_sleep_seconds: 60.0
  sample_rate_limit_endpoint_every_n_requests: 50

identity:
  bots:
    exclude_patterns:
      - ".*\\\\[bot\\\\]$"
      - "^dependabot$"
      - "^renovate\\\\[bot\\\\]$"
    include_overrides: []
  humans_only: true

collection:
  enable:
    pulls: true
    issues: true
    reviews: true
    comments: true
    commits: true
    hygiene: true
  commits:
    include_files: true
    classify_files: true
  hygiene:
    paths:
      - SECURITY.md
      - README.md
      - LICENSE
      - CONTRIBUTING.md
      - CODE_OF_CONDUCT.md
      - CODEOWNERS
      - .github/CODEOWNERS
    workflow_prefixes:
      - .github/workflows/
    branch_protection:
      mode: sample
      sample_top_repos_by: prs_merged
      sample_count: 5
    security_features:
      best_effort: true

storage:
  root: "./data"
  raw_format: jsonl
  curated_format: parquet
  dataset_version: v1

report:
  title: "GitHub Organization 2024 Year in Review"
  output_dir: "./site"
  theme: engineer_exec_toggle
"""

    config_path.write_text(config_content)
    return config_path


# Sample metrics fixtures for website testing


@pytest.fixture
def sample_metrics_dir() -> Path:
    """Path to sample metrics fixtures directory.

    Contains pre-generated Parquet files with minimal realistic test data:
    - metrics_leaderboard.parquet
    - metrics_time_series.parquet
    - metrics_repo_health.parquet
    - metrics_repo_hygiene_score.parquet
    - metrics_awards.parquet

    Returns:
        Path to tests/fixtures/sample_metrics directory.
    """
    fixtures_path = Path(__file__).parent / "fixtures" / "sample_metrics"
    if not fixtures_path.exists():
        pytest.fail(
            f"Sample metrics fixtures not found at {fixtures_path}. "
            "Run: python scripts/setup_test_data.py --year 2024 --output tests/fixtures/sample_metrics"
        )
    return fixtures_path


@pytest.fixture
def sample_metrics_config(tmp_path: Path, sample_metrics_dir: Path) -> Config:
    """Create config that uses sample metrics data.

    Sets up paths to use sample metrics fixtures for report building.

    Args:
        tmp_path: Pytest temp directory.
        sample_metrics_dir: Path to sample metrics fixtures.

    Returns:
        Config instance configured to use sample data.
    """
    # Copy sample metrics to expected location
    metrics_dest = tmp_path / "data" / "metrics" / "year=2024"
    metrics_dest.mkdir(parents=True, exist_ok=True)

    for parquet_file in sample_metrics_dir.glob("*.parquet"):
        shutil.copy2(parquet_file, metrics_dest / parquet_file.name)

    return Config.model_validate(
        {
            "github": {
                "target": {"mode": "org", "name": "test-org"},
                "auth": {"token_env": "GITHUB_TOKEN"},
                "discovery": {
                    "include_forks": False,
                    "include_archived": False,
                    "visibility": "public",
                },
                "windows": {
                    "year": 2024,
                    "since": "2024-01-01T00:00:00Z",
                    "until": "2025-01-01T00:00:00Z",
                },
            },
            "rate_limit": {
                "strategy": "adaptive",
                "max_concurrency": 5,
                "min_sleep_seconds": 1.0,
                "max_sleep_seconds": 60.0,
                "sample_rate_limit_endpoint_every_n_requests": 100,
            },
            "identity": {
                "bots": {
                    "exclude_patterns": [r".*\[bot\]$"],
                    "include_overrides": [],
                },
                "humans_only": True,
            },
            "collection": {
                "enable": {
                    "pulls": True,
                    "issues": True,
                    "reviews": True,
                    "comments": True,
                    "commits": True,
                    "hygiene": True,
                },
                "commits": {"include_files": True, "classify_files": True},
                "hygiene": {
                    "paths": ["README.md", "LICENSE", "SECURITY.md"],
                    "workflow_prefixes": [".github/workflows/"],
                    "branch_protection": {
                        "mode": "sample",
                        "sample_top_repos_by": "prs_merged",
                        "sample_count": 5,
                    },
                    "security_features": {"best_effort": True},
                },
            },
            "storage": {
                "root": str(tmp_path / "data"),
                "raw_format": "jsonl",
                "curated_format": "parquet",
                "dataset_version": "v1",
            },
            "report": {
                "title": "Test Organization 2024 Year in Review",
                "output_dir": str(tmp_path / "site"),
                "theme": "engineer_exec_toggle",
            },
        }
    )


@pytest.fixture
def sample_metrics_paths(sample_metrics_config: Config) -> PathManager:
    """Create PathManager for sample metrics tests.

    Args:
        sample_metrics_config: Config using sample data.

    Returns:
        PathManager instance.
    """
    return PathManager(sample_metrics_config)


# Sample site data fixtures for website validation


@pytest.fixture
def sample_site_data_dir() -> Path:
    """Return path to sample site data fixtures directory.

    Returns:
        Path to tests/fixtures/sample_site_data/
    """
    return Path(__file__).parent / "fixtures" / "sample_site_data"


@pytest.fixture
def load_sample_summary(sample_site_data_dir: Path) -> dict[str, Any]:
    """Load sample summary.json fixture.

    Args:
        sample_site_data_dir: Path to sample site data fixtures

    Returns:
        Parsed summary.json data
    """
    summary_path = sample_site_data_dir / "summary.json"
    with summary_path.open("r") as f:
        return json.load(f)


@pytest.fixture
def load_sample_leaderboards(sample_site_data_dir: Path) -> dict[str, Any]:
    """Load sample leaderboards.json fixture.

    Args:
        sample_site_data_dir: Path to sample site data fixtures

    Returns:
        Parsed leaderboards.json data
    """
    leaderboards_path = sample_site_data_dir / "leaderboards.json"
    with leaderboards_path.open("r") as f:
        return json.load(f)


@pytest.fixture
def load_sample_timeseries(sample_site_data_dir: Path) -> dict[str, Any]:
    """Load sample timeseries.json fixture.

    Args:
        sample_site_data_dir: Path to sample site data fixtures

    Returns:
        Parsed timeseries.json data
    """
    timeseries_path = sample_site_data_dir / "timeseries.json"
    with timeseries_path.open("r") as f:
        return json.load(f)


@pytest.fixture
def load_sample_repo_health(sample_site_data_dir: Path) -> dict[str, Any]:
    """Load sample repo_health.json fixture.

    Args:
        sample_site_data_dir: Path to sample site data fixtures

    Returns:
        Parsed repo_health.json data
    """
    repo_health_path = sample_site_data_dir / "repo_health.json"
    with repo_health_path.open("r") as f:
        return json.load(f)


@pytest.fixture
def load_sample_hygiene_scores(sample_site_data_dir: Path) -> dict[str, Any]:
    """Load sample hygiene_scores.json fixture.

    Args:
        sample_site_data_dir: Path to sample site data fixtures

    Returns:
        Parsed hygiene_scores.json data
    """
    hygiene_scores_path = sample_site_data_dir / "hygiene_scores.json"
    with hygiene_scores_path.open("r") as f:
        return json.load(f)


@pytest.fixture
def load_sample_awards(sample_site_data_dir: Path) -> dict[str, Any]:
    """Load sample awards.json fixture.

    Args:
        sample_site_data_dir: Path to sample site data fixtures

    Returns:
        Parsed awards.json data
    """
    awards_path = sample_site_data_dir / "awards.json"
    with awards_path.open("r") as f:
        return json.load(f)


@pytest.fixture
def setup_test_site_data(tmp_path: Path, sample_site_data_dir: Path) -> Path:
    """Copy sample site data to a temporary site directory structure.

    Creates a complete site/YYYY/data/ directory with all test data files.

    Args:
        tmp_path: Pytest temporary directory
        sample_site_data_dir: Path to sample site data fixtures

    Returns:
        Path to temporary site/YYYY/data/ directory
    """
    # Create site structure
    site_data_dir = tmp_path / "site" / "2024" / "data"
    site_data_dir.mkdir(parents=True, exist_ok=True)

    # Copy all JSON files
    for json_file in sample_site_data_dir.glob("*.json"):
        shutil.copy(json_file, site_data_dir / json_file.name)

    return site_data_dir


@pytest.fixture
def all_sample_data(
    load_sample_summary: dict[str, Any],
    load_sample_leaderboards: dict[str, Any],
    load_sample_timeseries: dict[str, Any],
    load_sample_repo_health: dict[str, Any],
    load_sample_hygiene_scores: dict[str, Any],
    load_sample_awards: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """Load all sample site data files at once.

    Args:
        load_sample_summary: Summary data fixture
        load_sample_leaderboards: Leaderboards data fixture
        load_sample_timeseries: Timeseries data fixture
        load_sample_repo_health: Repo health data fixture
        load_sample_hygiene_scores: Hygiene scores data fixture
        load_sample_awards: Awards data fixture

    Returns:
        Dictionary mapping filename to parsed JSON data
    """
    return {
        "summary": load_sample_summary,
        "leaderboards": load_sample_leaderboards,
        "timeseries": load_sample_timeseries,
        "repo_health": load_sample_repo_health,
        "hygiene_scores": load_sample_hygiene_scores,
        "awards": load_sample_awards,
    }
