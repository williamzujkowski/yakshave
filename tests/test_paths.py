"""Tests for path management."""

from pathlib import Path

import pytest

from gh_year_end.config import Config
from gh_year_end.storage.paths import PathManager


@pytest.fixture
def config() -> Config:
    """Create a test configuration."""
    return Config.model_validate(
        {
            "github": {
                "target": {"mode": "org", "name": "test-org"},
                "windows": {
                    "year": 2025,
                    "since": "2025-01-01T00:00:00Z",
                    "until": "2026-01-01T00:00:00Z",
                },
            },
            "storage": {"root": "/tmp/test-data"},
            "report": {"output_dir": "/tmp/test-site"},
        }
    )


@pytest.fixture
def path_manager(config: Config) -> PathManager:
    """Create a path manager with test config."""
    return PathManager(config)


class TestPathManagerRoots:
    """Tests for root path generation."""

    def test_raw_root(self, path_manager: PathManager) -> None:
        """Test raw root path generation."""
        expected = Path("/tmp/test-data/raw/year=2025/source=github/target=test-org")
        assert path_manager.raw_root == expected

    def test_curated_root(self, path_manager: PathManager) -> None:
        """Test curated root path generation."""
        expected = Path("/tmp/test-data/curated/year=2025")
        assert path_manager.curated_root == expected

    def test_metrics_root(self, path_manager: PathManager) -> None:
        """Test metrics root path generation."""
        expected = Path("/tmp/test-data/metrics/year=2025")
        assert path_manager.metrics_root == expected

    def test_site_root(self, path_manager: PathManager) -> None:
        """Test site root path generation."""
        expected = Path("/tmp/test-site/2025")
        assert path_manager.site_root == expected


class TestPathManagerRawPaths:
    """Tests for raw data path generation."""

    def test_manifest_path(self, path_manager: PathManager) -> None:
        """Test manifest path."""
        assert path_manager.manifest_path.name == "manifest.json"
        assert path_manager.manifest_path.parent == path_manager.raw_root

    def test_repos_raw_path(self, path_manager: PathManager) -> None:
        """Test repos raw path."""
        assert path_manager.repos_raw_path.name == "repos.jsonl"

    def test_pulls_raw_path(self, path_manager: PathManager) -> None:
        """Test pulls raw path for a repo."""
        path = path_manager.pulls_raw_path("owner/repo")
        assert path.name == "owner__repo.jsonl"
        assert "pulls" in path.parts

    def test_issues_raw_path(self, path_manager: PathManager) -> None:
        """Test issues raw path for a repo."""
        path = path_manager.issues_raw_path("owner/repo")
        assert path.name == "owner__repo.jsonl"
        assert "issues" in path.parts

    def test_reviews_raw_path(self, path_manager: PathManager) -> None:
        """Test reviews raw path for a repo."""
        path = path_manager.reviews_raw_path("owner/repo")
        assert path.name == "owner__repo.jsonl"
        assert "reviews" in path.parts


class TestPathManagerCuratedPaths:
    """Tests for curated data path generation."""

    def test_dim_user_path(self, path_manager: PathManager) -> None:
        """Test dim_user curated path."""
        path = path_manager.curated_path("dim_user")
        assert path.name == "dim_user.parquet"
        assert path.parent == path_manager.curated_root

    def test_fact_pull_request_path(self, path_manager: PathManager) -> None:
        """Test fact_pull_request curated path."""
        path = path_manager.curated_path("fact_pull_request")
        assert path.name == "fact_pull_request.parquet"


class TestPathManagerMetricsPaths:
    """Tests for metrics path generation."""

    def test_leaderboard_path(self, path_manager: PathManager) -> None:
        """Test metrics_leaderboard path."""
        path = path_manager.metrics_path("metrics_leaderboard")
        assert path.name == "metrics_leaderboard.parquet"
        assert path.parent == path_manager.metrics_root

    def test_repo_health_path(self, path_manager: PathManager) -> None:
        """Test metrics_repo_health path."""
        path = path_manager.metrics_path("metrics_repo_health")
        assert path.name == "metrics_repo_health.parquet"


class TestPathManagerSafeName:
    """Tests for safe name conversion."""

    def test_safe_name_with_slash(self) -> None:
        """Test that slashes are replaced with double underscores."""
        assert PathManager._safe_name("owner/repo") == "owner__repo"

    def test_safe_name_no_slash(self) -> None:
        """Test that names without slashes are unchanged."""
        assert PathManager._safe_name("simple") == "simple"

    def test_safe_name_multiple_slashes(self) -> None:
        """Test handling of multiple slashes."""
        assert PathManager._safe_name("a/b/c") == "a__b__c"
