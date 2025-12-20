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

    def test_rate_limit_samples_path(self, path_manager: PathManager) -> None:
        """Test rate limit samples path."""
        assert path_manager.rate_limit_samples_path.name == "rate_limit_samples.jsonl"
        assert path_manager.rate_limit_samples_path.parent == path_manager.raw_root

    def test_checkpoint_path(self, path_manager: PathManager) -> None:
        """Test checkpoint path."""
        assert path_manager.checkpoint_path.name == "checkpoint.json"
        assert path_manager.checkpoint_path.parent == path_manager.raw_root

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

    def test_issue_comments_raw_path(self, path_manager: PathManager) -> None:
        """Test issue comments raw path for a repo."""
        path = path_manager.issue_comments_raw_path("owner/repo")
        assert path.name == "owner__repo.jsonl"
        assert "issue_comments" in path.parts

    def test_review_comments_raw_path(self, path_manager: PathManager) -> None:
        """Test review comments raw path for a repo."""
        path = path_manager.review_comments_raw_path("owner/repo")
        assert path.name == "owner__repo.jsonl"
        assert "review_comments" in path.parts

    def test_commits_raw_path(self, path_manager: PathManager) -> None:
        """Test commits raw path for a repo."""
        path = path_manager.commits_raw_path("owner/repo")
        assert path.name == "owner__repo.jsonl"
        assert "commits" in path.parts

    def test_repo_tree_raw_path(self, path_manager: PathManager) -> None:
        """Test repo tree raw path for a repo."""
        path = path_manager.repo_tree_raw_path("owner/repo")
        assert path.name == "owner__repo.jsonl"
        assert "repo_tree" in path.parts

    def test_branch_protection_raw_path(self, path_manager: PathManager) -> None:
        """Test branch protection raw path for a repo."""
        path = path_manager.branch_protection_raw_path("owner/repo")
        assert path.name == "owner__repo.jsonl"
        assert "branch_protection" in path.parts

    def test_security_features_raw_path(self, path_manager: PathManager) -> None:
        """Test security features raw path for a repo."""
        path = path_manager.security_features_raw_path("owner/repo")
        assert path.name == "owner__repo.jsonl"
        assert "security_features" in path.parts


class TestPathManagerCuratedPaths:
    """Tests for curated data path generation."""

    def test_curated_path(self, path_manager: PathManager) -> None:
        """Test curated path method."""
        path = path_manager.curated_path("dim_user")
        assert path.name == "dim_user.parquet"
        assert path.parent == path_manager.curated_root

    def test_dim_user_path(self, path_manager: PathManager) -> None:
        """Test dim_user convenience property."""
        assert path_manager.dim_user_path == path_manager.curated_path("dim_user")

    def test_dim_repo_path(self, path_manager: PathManager) -> None:
        """Test dim_repo convenience property."""
        assert path_manager.dim_repo_path == path_manager.curated_path("dim_repo")

    def test_dim_identity_rule_path(self, path_manager: PathManager) -> None:
        """Test dim_identity_rule convenience property."""
        assert path_manager.dim_identity_rule_path == path_manager.curated_path("dim_identity_rule")

    def test_fact_pull_request_path(self, path_manager: PathManager) -> None:
        """Test fact_pull_request convenience property."""
        assert path_manager.fact_pull_request_path == path_manager.curated_path("fact_pull_request")

    def test_fact_issue_path(self, path_manager: PathManager) -> None:
        """Test fact_issue convenience property."""
        assert path_manager.fact_issue_path == path_manager.curated_path("fact_issue")

    def test_fact_review_path(self, path_manager: PathManager) -> None:
        """Test fact_review convenience property."""
        assert path_manager.fact_review_path == path_manager.curated_path("fact_review")

    def test_fact_issue_comment_path(self, path_manager: PathManager) -> None:
        """Test fact_issue_comment convenience property."""
        assert path_manager.fact_issue_comment_path == path_manager.curated_path(
            "fact_issue_comment"
        )

    def test_fact_review_comment_path(self, path_manager: PathManager) -> None:
        """Test fact_review_comment convenience property."""
        assert path_manager.fact_review_comment_path == path_manager.curated_path(
            "fact_review_comment"
        )

    def test_fact_commit_path(self, path_manager: PathManager) -> None:
        """Test fact_commit convenience property."""
        assert path_manager.fact_commit_path == path_manager.curated_path("fact_commit")

    def test_fact_commit_file_path(self, path_manager: PathManager) -> None:
        """Test fact_commit_file convenience property."""
        assert path_manager.fact_commit_file_path == path_manager.curated_path("fact_commit_file")

    def test_fact_repo_files_presence_path(self, path_manager: PathManager) -> None:
        """Test fact_repo_files_presence convenience property."""
        assert path_manager.fact_repo_files_presence_path == path_manager.curated_path(
            "fact_repo_files_presence"
        )

    def test_fact_repo_hygiene_path(self, path_manager: PathManager) -> None:
        """Test fact_repo_hygiene convenience property."""
        assert path_manager.fact_repo_hygiene_path == path_manager.curated_path("fact_repo_hygiene")

    def test_fact_repo_security_features_path(self, path_manager: PathManager) -> None:
        """Test fact_repo_security_features convenience property."""
        assert path_manager.fact_repo_security_features_path == path_manager.curated_path(
            "fact_repo_security_features"
        )


class TestPathManagerMetricsPaths:
    """Tests for metrics path generation."""

    def test_metrics_path(self, path_manager: PathManager) -> None:
        """Test metrics path method."""
        path = path_manager.metrics_path("metrics_leaderboard")
        assert path.name == "metrics_leaderboard.parquet"
        assert path.parent == path_manager.metrics_root

    def test_metrics_leaderboard_path(self, path_manager: PathManager) -> None:
        """Test metrics_leaderboard convenience property."""
        assert path_manager.metrics_leaderboard_path == path_manager.metrics_path(
            "metrics_leaderboard"
        )

    def test_metrics_repo_health_path(self, path_manager: PathManager) -> None:
        """Test metrics_repo_health convenience property."""
        assert path_manager.metrics_repo_health_path == path_manager.metrics_path(
            "metrics_repo_health"
        )

    def test_metrics_time_series_path(self, path_manager: PathManager) -> None:
        """Test metrics_time_series convenience property."""
        assert path_manager.metrics_time_series_path == path_manager.metrics_path(
            "metrics_time_series"
        )

    def test_metrics_repo_hygiene_score_path(self, path_manager: PathManager) -> None:
        """Test metrics_repo_hygiene_score convenience property."""
        assert path_manager.metrics_repo_hygiene_score_path == path_manager.metrics_path(
            "metrics_repo_hygiene_score"
        )

    def test_metrics_awards_path(self, path_manager: PathManager) -> None:
        """Test metrics_awards convenience property."""
        assert path_manager.metrics_awards_path == path_manager.metrics_path("metrics_awards")


class TestPathManagerSitePaths:
    """Tests for site path generation."""

    def test_site_data_path(self, path_manager: PathManager) -> None:
        """Test site data path."""
        assert path_manager.site_data_path == path_manager.site_root / "data"

    def test_site_assets_path(self, path_manager: PathManager) -> None:
        """Test site assets path."""
        assert path_manager.site_assets_path == path_manager.site_root / "assets"


class TestPathManagerEnsureDirectories:
    """Tests for directory creation."""

    def test_ensure_directories(self, tmp_path: Path) -> None:
        """Test that ensure_directories creates all required directories."""
        # Create a path manager with tmp_path
        test_config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test-org"},
                    "windows": {
                        "year": 2025,
                        "since": "2025-01-01T00:00:00Z",
                        "until": "2026-01-01T00:00:00Z",
                    },
                },
                "storage": {"root": str(tmp_path / "data")},
                "report": {"output_dir": str(tmp_path / "site")},
            }
        )
        pm = PathManager(test_config)

        # Ensure directories don't exist yet
        assert not pm.raw_root.exists()
        assert not pm.curated_root.exists()
        assert not pm.metrics_root.exists()
        assert not pm.site_root.exists()

        # Call ensure_directories
        pm.ensure_directories()

        # Verify all main directories exist
        assert pm.raw_root.exists()
        assert (pm.raw_root / "pulls").exists()
        assert (pm.raw_root / "issues").exists()
        assert (pm.raw_root / "reviews").exists()
        assert (pm.raw_root / "issue_comments").exists()
        assert (pm.raw_root / "review_comments").exists()
        assert (pm.raw_root / "commits").exists()
        assert (pm.raw_root / "repo_tree").exists()
        assert (pm.raw_root / "branch_protection").exists()
        assert (pm.raw_root / "security_features").exists()
        assert pm.curated_root.exists()
        assert pm.metrics_root.exists()
        assert pm.site_root.exists()
        assert pm.site_data_path.exists()
        assert pm.site_assets_path.exists()

    def test_ensure_directories_idempotent(self, tmp_path: Path) -> None:
        """Test that ensure_directories can be called multiple times safely."""
        # Create a path manager with tmp_path
        test_config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test-org"},
                    "windows": {
                        "year": 2025,
                        "since": "2025-01-01T00:00:00Z",
                        "until": "2026-01-01T00:00:00Z",
                    },
                },
                "storage": {"root": str(tmp_path / "data")},
                "report": {"output_dir": str(tmp_path / "site")},
            }
        )
        pm = PathManager(test_config)

        # Call multiple times - should not raise
        pm.ensure_directories()
        pm.ensure_directories()
        pm.ensure_directories()

        # Verify directories still exist
        assert pm.raw_root.exists()
        assert pm.curated_root.exists()


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


class TestPathManagerYearConfiguration:
    """Tests for different year configurations."""

    def test_different_years(self) -> None:
        """Test that paths change based on year configuration."""
        config_2024 = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test-org"},
                    "windows": {
                        "year": 2024,
                        "since": "2024-01-01T00:00:00Z",
                        "until": "2025-01-01T00:00:00Z",
                    },
                },
                "storage": {"root": "/tmp/test-data"},
                "report": {"output_dir": "/tmp/test-site"},
            }
        )
        config_2025 = Config.model_validate(
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

        pm_2024 = PathManager(config_2024)
        pm_2025 = PathManager(config_2025)

        # Verify year is in paths
        assert "year=2024" in str(pm_2024.raw_root)
        assert "year=2025" in str(pm_2025.raw_root)
        assert "year=2024" in str(pm_2024.curated_root)
        assert "year=2025" in str(pm_2025.curated_root)
        assert "2024" in str(pm_2024.site_root)
        assert "2025" in str(pm_2025.site_root)

    def test_different_targets(self) -> None:
        """Test that paths change based on target configuration."""
        config_org = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "org-name"},
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
        config_user = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "user", "name": "user-name"},
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

        pm_org = PathManager(config_org)
        pm_user = PathManager(config_user)

        # Verify target is in raw paths
        assert "target=org-name" in str(pm_org.raw_root)
        assert "target=user-name" in str(pm_user.raw_root)
