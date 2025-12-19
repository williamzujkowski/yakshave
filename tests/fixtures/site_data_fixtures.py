"""Pytest fixtures for sample site data.

Provides fixtures for loading and setting up minimal test data for website validation.
"""

import json
import shutil
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def sample_site_data_dir() -> Path:
    """Return path to sample site data fixtures directory.

    Returns:
        Path to tests/fixtures/sample_site_data/
    """
    return Path(__file__).parent / "sample_site_data"


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
