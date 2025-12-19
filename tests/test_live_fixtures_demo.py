"""Demo test showing how to use live integration test fixtures.

This test demonstrates the usage of fixtures from conftest_live.py.
It's a simple validation test to ensure the fixtures work correctly.

Run with:
    uv run pytest tests/test_live_fixtures_demo.py -m live_api
    uv run pytest tests/test_live_fixtures_demo.py -m live_api -v

Skip with:
    uv run pytest -m "not live_api"
"""

import pytest

from gh_year_end.config import Config
from gh_year_end.storage.paths import PathManager


@pytest.mark.live_api
def test_github_token_fixture(github_token: str) -> None:
    """Test that github_token fixture provides valid token.

    Args:
        github_token: GitHub API token from fixture.
    """
    assert github_token, "Token should not be empty"
    assert isinstance(github_token, str), "Token should be string"
    # GitHub tokens typically start with ghp_, gho_, or other prefixes
    assert len(github_token) > 20, "Token should be reasonably long"


@pytest.mark.live_api
def test_live_config_fixture(live_config: Config) -> None:
    """Test that live_config fixture provides valid config.

    Args:
        live_config: Config from fixture.
    """
    assert isinstance(live_config, Config), "Should be Config instance"
    assert live_config.github.target.name == "github", "Should target github org"
    assert live_config.github.target.mode == "org", "Should be org mode"
    assert live_config.github.windows.year == 2024, "Should use 2024 data"
    assert live_config.rate_limit.max_concurrency == 1, "Should use conservative concurrency"
    assert live_config.rate_limit.min_sleep_seconds == 2.0, "Should use conservative sleep"


@pytest.mark.live_api
def test_live_paths_fixture(live_paths: PathManager) -> None:
    """Test that live_paths fixture provides valid PathManager.

    Args:
        live_paths: PathManager from fixture.
    """
    assert isinstance(live_paths, PathManager), "Should be PathManager instance"
    assert live_paths.year == 2024, "Should use 2024"
    assert live_paths.target == "github", "Should target github org"

    # Verify path structure
    assert "year=2024" in str(live_paths.raw_root), "Raw path should include year"
    assert "source=github" in str(live_paths.raw_root), "Raw path should include source"
    assert "target=github" in str(live_paths.raw_root), "Raw path should include target"


@pytest.mark.live_api
@pytest.mark.slow
def test_cached_raw_data_fixture(cached_raw_data: dict, live_paths: PathManager) -> None:
    """Test that cached_raw_data fixture runs collection and caches results.

    This test runs ONCE per session and is cached for other tests.
    Subsequent tests using this fixture will reuse the collected data.

    Args:
        cached_raw_data: Collection stats from fixture.
        live_paths: PathManager from fixture.
    """
    # Verify stats structure
    assert "discovery" in cached_raw_data, "Should include discovery stats"
    assert "duration_seconds" in cached_raw_data, "Should track duration"
    assert cached_raw_data["duration_seconds"] > 0, "Duration should be positive"

    # Verify raw data was collected
    assert live_paths.raw_root.exists(), "Raw data directory should exist"
    assert live_paths.manifest_path.exists(), "Manifest should exist"

    # Verify repos were discovered
    if "discovery" in cached_raw_data:
        repos_discovered = cached_raw_data["discovery"].get("repos_discovered", 0)
        assert repos_discovered > 0, "Should discover at least one repo"

    # Note: The actual collection happens only ONCE per pytest session
    # All tests using this fixture share the same cached data


@pytest.mark.live_api
def test_live_test_config_path_fixture(live_test_config_path) -> None:
    """Test that live_test_config_path fixture creates valid config file.

    Args:
        live_test_config_path: Path to config file from fixture.
    """
    assert live_test_config_path.exists(), "Config file should exist"
    assert live_test_config_path.suffix == ".yaml", "Should be YAML file"

    # Verify can load config
    from gh_year_end.config import load_config

    cfg = load_config(live_test_config_path)
    assert cfg.github.target.name == "github", "Should target github org"
    assert cfg.github.windows.year == 2024, "Should use 2024"
