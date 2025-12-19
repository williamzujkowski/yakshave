"""Tests for report export functionality."""

from pathlib import Path

import polars as pl
import pytest

from gh_year_end.config import Config
from gh_year_end.report.export import export_metrics
from gh_year_end.storage.parquet_writer import write_parquet
from gh_year_end.storage.paths import PathManager


@pytest.fixture
def config(tmp_path: Path) -> Config:
    """Create test configuration."""
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
            "storage": {"root": str(tmp_path / "data")},
            "report": {"output_dir": str(tmp_path / "site")},
        }
    )


@pytest.fixture
def paths(config: Config) -> PathManager:
    """Create path manager."""
    return PathManager(config)


@pytest.fixture
def setup_test_data(paths: PathManager) -> None:
    """Setup test metrics and dimension data."""
    # Create directories
    paths.metrics_root.mkdir(parents=True, exist_ok=True)
    paths.curated_root.mkdir(parents=True, exist_ok=True)

    # Create dim_user
    dim_user = pl.DataFrame(
        {
            "user_id": ["U_alice", "U_bob", "U_charlie"],
            "login": ["alice", "bob", "charlie"],
            "type": ["User", "User", "User"],
            "profile_url": [
                "https://github.com/alice",
                "https://github.com/bob",
                "https://github.com/charlie",
            ],
            "is_bot": [False, False, False],
            "bot_reason": [None, None, None],
            "display_name": ["Alice Smith", "Bob Jones", None],
        }
    )
    write_parquet(dim_user.to_arrow(), paths.dim_user_path)

    # Create dim_repo
    dim_repo = pl.DataFrame(
        {
            "repo_id": ["R_1", "R_2"],
            "full_name": ["test-org/repo1", "test-org/repo2"],
            "name": ["repo1", "repo2"],
            "description": ["Test repo 1", "Test repo 2"],
            "is_private": [False, False],
            "is_fork": [False, False],
            "is_archived": [False, False],
            "created_at": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
            "updated_at": ["2025-12-01T00:00:00Z", "2025-12-01T00:00:00Z"],
            "pushed_at": ["2025-12-01T00:00:00Z", "2025-12-01T00:00:00Z"],
            "language": ["Python", "JavaScript"],
            "topics": [[], []],
            "default_branch": ["main", "main"],
            "size_kb": [1000, 2000],
            "stargazers_count": [10, 20],
            "watchers_count": [5, 10],
            "forks_count": [2, 3],
            "open_issues_count": [1, 2],
            "visibility": ["public", "public"],
            "html_url": ["https://github.com/test-org/repo1", "https://github.com/test-org/repo2"],
        }
    )
    write_parquet(dim_repo.to_arrow(), paths.dim_repo_path)

    # Create metrics_leaderboard
    metrics_leaderboard = pl.DataFrame(
        {
            "year": [2025] * 6,
            "metric_key": [
                "prs_opened",
                "prs_opened",
                "prs_merged",
                "prs_merged",
                "issues_opened",
                "issues_opened",
            ],
            "scope": ["org", "org", "org", "org", "org", "org"],
            "repo_id": [None, None, None, None, None, None],
            "user_id": ["U_alice", "U_bob", "U_alice", "U_bob", "U_alice", "U_charlie"],
            "value": [10, 8, 9, 7, 5, 3],
            "rank": [1, 2, 1, 2, 1, 2],
        }
    )
    write_parquet(metrics_leaderboard.to_arrow(), paths.metrics_leaderboard_path)

    # Create metrics_time_series
    metrics_time_series = pl.DataFrame(
        {
            "year": [2025] * 4,
            "period_type": ["week", "week", "month", "month"],
            "period_start": ["2025-01-06", "2025-01-13", "2025-01-01", "2025-02-01"],
            "period_end": ["2025-01-12", "2025-01-19", "2025-01-31", "2025-02-28"],
            "scope": ["org", "org", "org", "org"],
            "repo_id": [None, None, None, None],
            "metric_key": ["prs_opened", "prs_opened", "prs_opened", "prs_opened"],
            "value": [5, 7, 20, 25],
        }
    ).with_columns(
        [
            pl.col("period_start").str.strptime(pl.Date, "%Y-%m-%d"),
            pl.col("period_end").str.strptime(pl.Date, "%Y-%m-%d"),
        ]
    )
    write_parquet(metrics_time_series.to_arrow(), paths.metrics_time_series_path)

    # Create metrics_repo_health
    metrics_repo_health = pl.DataFrame(
        {
            "repo_id": ["R_1", "R_2"],
            "repo_full_name": ["test-org/repo1", "test-org/repo2"],
            "year": [2025, 2025],
            "active_contributors_30d": [3, 2],
            "active_contributors_90d": [5, 4],
            "active_contributors_365d": [10, 8],
            "prs_opened": [20, 15],
            "prs_merged": [18, 12],
            "issues_opened": [10, 8],
            "issues_closed": [9, 7],
            "review_coverage": [85.0, 90.0],
            "median_time_to_first_review": [2.5, 1.8],
            "median_time_to_merge": [24.0, 18.5],
            "stale_pr_count": [1, 0],
            "stale_issue_count": [2, 1],
        }
    )
    write_parquet(metrics_repo_health.to_arrow(), paths.metrics_repo_health_path)

    # Create metrics_repo_hygiene_score
    metrics_hygiene_score = pl.DataFrame(
        {
            "repo_id": ["R_1", "R_2"],
            "repo_full_name": ["test-org/repo1", "test-org/repo2"],
            "year": [2025, 2025],
            "score": [85, 75],
            "has_readme": [True, True],
            "has_license": [True, True],
            "has_contributing": [True, False],
            "has_code_of_conduct": [True, False],
            "has_security_md": [True, True],
            "has_codeowners": [True, False],
            "has_ci_workflows": [True, True],
            "branch_protection_enabled": [True, True],
            "requires_reviews": [True, False],
            "dependabot_enabled": [True, True],
            "secret_scanning_enabled": [True, False],
            "notes": ["", "missing CONTRIBUTING, missing CODE_OF_CONDUCT"],
        }
    )
    write_parquet(metrics_hygiene_score.to_arrow(), paths.metrics_repo_hygiene_score_path)

    # Create metrics_awards
    metrics_awards = pl.DataFrame(
        {
            "award_key": ["top_pr_contributor", "healthiest_repo"],
            "title": ["Top PR Contributor", "Healthiest Repository"],
            "description": ["Most PRs opened in the year", "Best overall repository health"],
            "category": ["individual", "repository"],
            "winner_user_id": ["U_alice", None],
            "winner_repo_id": [None, "R_1"],
            "winner_name": ["alice", "test-org/repo1"],
            "supporting_stats": [
                '{"metric": "prs_opened", "value": 10}',
                '{"metric": "hygiene_score", "value": 85}',
            ],
        }
    )
    write_parquet(metrics_awards.to_arrow(), paths.metrics_awards_path)


class TestExportMetrics:
    """Tests for export_metrics function."""

    def test_export_metrics_raises_error_if_metrics_missing(
        self, config: Config, paths: PathManager
    ) -> None:
        """Test that export_metrics raises error if metrics directory doesn't exist."""
        with pytest.raises(ValueError, match="Metrics directory not found"):
            export_metrics(config, paths)

    def test_export_metrics_creates_json_files(
        self,
        config: Config,
        paths: PathManager,
        setup_test_data: None,  # noqa: ARG002
    ) -> None:
        """Test that export_metrics creates all expected JSON files."""
        stats = export_metrics(config, paths)

        # Check that files were written
        assert len(stats["files_written"]) > 0
        assert stats["total_size_bytes"] > 0
        assert len(stats["errors"]) == 0

        # Check that expected files exist
        assert (paths.site_data_path / "leaderboards.json").exists()
        assert (paths.site_data_path / "timeseries.json").exists()
        assert (paths.site_data_path / "repo_health.json").exists()
        assert (paths.site_data_path / "hygiene_scores.json").exists()
        assert (paths.site_data_path / "awards.json").exists()
        assert (paths.site_data_path / "summary.json").exists()

    def test_export_leaderboards_structure(
        self,
        config: Config,
        paths: PathManager,
        setup_test_data: None,  # noqa: ARG002
    ) -> None:
        """Test that leaderboards.json has correct structure."""
        export_metrics(config, paths)

        # Read leaderboards JSON
        import json

        with (paths.site_data_path / "leaderboards.json").open() as f:
            data = json.load(f)

        # Check structure
        assert "leaderboards" in data
        assert "metrics_available" in data
        assert "prs_opened" in data["leaderboards"]
        assert "prs_merged" in data["leaderboards"]

        # Check org leaderboard
        prs_opened = data["leaderboards"]["prs_opened"]
        assert "org" in prs_opened
        assert len(prs_opened["org"]) == 2  # alice and bob

        # Check enrichment with user data
        top_contributor = prs_opened["org"][0]
        assert top_contributor["rank"] == 1
        assert top_contributor["login"] == "alice"
        assert top_contributor["display_name"] == "Alice Smith"
        assert top_contributor["value"] == 10

    def test_export_timeseries_structure(
        self,
        config: Config,
        paths: PathManager,
        setup_test_data: None,  # noqa: ARG002
    ) -> None:
        """Test that timeseries.json has correct structure."""
        export_metrics(config, paths)

        # Read timeseries JSON
        import json

        with (paths.site_data_path / "timeseries.json").open() as f:
            data = json.load(f)

        # Check structure
        assert "timeseries" in data
        assert "period_types" in data
        assert "metrics_available" in data
        assert "week" in data["timeseries"]
        assert "month" in data["timeseries"]

        # Check weekly data
        weekly_prs = data["timeseries"]["week"]["prs_opened"]["org"]
        assert len(weekly_prs) == 2
        assert weekly_prs[0]["period_start"] == "2025-01-06"
        assert weekly_prs[0]["value"] == 5

    def test_export_repo_health_structure(
        self,
        config: Config,
        paths: PathManager,
        setup_test_data: None,  # noqa: ARG002
    ) -> None:
        """Test that repo_health.json has correct structure."""
        export_metrics(config, paths)

        # Read repo health JSON
        import json

        with (paths.site_data_path / "repo_health.json").open() as f:
            data = json.load(f)

        # Check structure
        assert "repos" in data
        assert "total_repos" in data
        assert data["total_repos"] == 2

        # Check repo data
        assert "R_1" in data["repos"]
        repo1 = data["repos"]["R_1"]
        assert repo1["repo_full_name"] == "test-org/repo1"
        assert repo1["prs_opened"] == 20
        assert repo1["prs_merged"] == 18
        assert repo1["review_coverage"] == 85.0

    def test_export_hygiene_scores_structure(
        self,
        config: Config,
        paths: PathManager,
        setup_test_data: None,  # noqa: ARG002
    ) -> None:
        """Test that hygiene_scores.json has correct structure."""
        export_metrics(config, paths)

        # Read hygiene scores JSON
        import json

        with (paths.site_data_path / "hygiene_scores.json").open() as f:
            data = json.load(f)

        # Check structure
        assert "repos" in data
        assert "summary" in data

        # Check summary
        summary = data["summary"]
        assert summary["total_repos"] == 2
        assert summary["average_score"] == 80.0  # (85 + 75) / 2
        assert summary["min_score"] == 75
        assert summary["max_score"] == 85

        # Check repo data
        repo1 = data["repos"]["R_1"]
        assert repo1["score"] == 85
        assert repo1["has_readme"] is True
        assert repo1["has_license"] is True

    def test_export_awards_structure(
        self,
        config: Config,
        paths: PathManager,
        setup_test_data: None,  # noqa: ARG002
    ) -> None:
        """Test that awards.json has correct structure."""
        export_metrics(config, paths)

        # Read awards JSON
        import json

        with (paths.site_data_path / "awards.json").open() as f:
            data = json.load(f)

        # Check structure
        assert "awards" in data
        assert "categories" in data
        assert "total_awards" in data
        assert data["total_awards"] == 2

        # Check individual award with enrichment
        individual_awards = data["awards"]["individual"]
        assert len(individual_awards) == 1
        assert individual_awards[0]["award_key"] == "top_pr_contributor"
        assert individual_awards[0]["winner_login"] == "alice"

        # Check repository award
        repo_awards = data["awards"]["repository"]
        assert len(repo_awards) == 1
        assert repo_awards[0]["award_key"] == "healthiest_repo"
        assert repo_awards[0]["winner_repo_name"] == "test-org/repo1"

    def test_export_summary_structure(
        self,
        config: Config,
        paths: PathManager,
        setup_test_data: None,  # noqa: ARG002
    ) -> None:
        """Test that summary.json has correct structure."""
        export_metrics(config, paths)

        # Read summary JSON
        import json

        with (paths.site_data_path / "summary.json").open() as f:
            data = json.load(f)

        # Check structure
        assert "year" in data
        assert data["year"] == 2025
        assert "target" in data
        assert data["target"]["mode"] == "org"
        assert data["target"]["name"] == "test-org"
        assert "generated_at" in data

        # Check aggregated metrics
        assert "total_repos" in data
        assert data["total_repos"] == 2
        assert "total_prs_opened" in data
        assert data["total_prs_opened"] == 35  # 20 + 15
        assert "total_prs_merged" in data
        assert data["total_prs_merged"] == 30  # 18 + 12

    def test_export_handles_missing_dimension_tables(
        self, config: Config, paths: PathManager
    ) -> None:
        """Test that export handles missing dimension tables gracefully."""
        # Create metrics directory without dimension tables
        paths.metrics_root.mkdir(parents=True, exist_ok=True)

        # Create minimal leaderboard
        metrics_leaderboard = pl.DataFrame(
            {
                "year": [2025],
                "metric_key": ["prs_opened"],
                "scope": ["org"],
                "repo_id": [None],
                "user_id": ["U_alice"],
                "value": [10],
                "rank": [1],
            }
        )
        write_parquet(metrics_leaderboard.to_arrow(), paths.metrics_leaderboard_path)

        # Should not raise error
        stats = export_metrics(config, paths)

        # Should have some errors for missing tables
        assert len(stats["errors"]) > 0
        # But should export what's available
        assert (paths.site_data_path / "leaderboards.json").exists()

    def test_export_stats_structure(
        self,
        config: Config,
        paths: PathManager,
        setup_test_data: None,  # noqa: ARG002
    ) -> None:
        """Test that export_metrics returns correct stats structure."""
        stats = export_metrics(config, paths)

        # Check required keys
        assert "files_written" in stats
        assert "total_size_bytes" in stats
        assert "record_counts" in stats
        assert "errors" in stats
        assert "duration_seconds" in stats

        # Check types
        assert isinstance(stats["files_written"], list)
        assert isinstance(stats["total_size_bytes"], int)
        assert isinstance(stats["record_counts"], dict)
        assert isinstance(stats["errors"], list)
        assert isinstance(stats["duration_seconds"], float)

        # Check record counts
        assert "leaderboards.json" in stats["record_counts"]
        assert stats["record_counts"]["leaderboards.json"] > 0
