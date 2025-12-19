"""Tests for metrics orchestrator."""

from pathlib import Path
from unittest.mock import patch

import pytest

from gh_year_end.config import Config
from gh_year_end.metrics.orchestrator import run_metrics
from gh_year_end.storage.paths import PathManager

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def config() -> Config:
    """Load test configuration."""
    from gh_year_end.config import load_config

    return load_config(FIXTURES_DIR / "valid_config.yaml")


@pytest.fixture
def paths(config: Config, tmp_path: Path) -> PathManager:
    """Create a PathManager with temporary directories."""
    # Override storage root to use tmp_path
    config.storage.root = str(tmp_path)
    return PathManager(config)


class TestRunMetrics:
    """Tests for run_metrics function."""

    def test_fails_if_curated_data_missing(self, config: Config, tmp_path: Path) -> None:
        """Test that run_metrics fails if curated data doesn't exist."""
        # Override storage root to use tmp_path (empty directory)
        config.storage.root = str(tmp_path)
        # Don't create curated_root, so it doesn't exist
        with pytest.raises(ValueError, match="Curated data not found"):
            run_metrics(config)

    def test_fails_if_no_curated_tables(self, config: Config, paths: PathManager) -> None:
        """Test that run_metrics fails if no curated tables exist."""
        # Create curated_root but no tables
        paths.curated_root.mkdir(parents=True, exist_ok=True)

        with pytest.raises(ValueError, match="No curated tables found"):
            run_metrics(config)

    def test_creates_metrics_directory(self, config: Config, paths: PathManager) -> None:
        """Test that run_metrics creates metrics directory."""
        # Create curated data
        paths.curated_root.mkdir(parents=True, exist_ok=True)

        # Create required tables
        dim_user_path = paths.curated_path("dim_user")
        dim_user_path.touch()

        dim_repo_path = paths.curated_path("dim_repo")
        dim_repo_path.touch()

        # Run metrics
        run_metrics(config)

        # Verify metrics directory was created
        assert paths.metrics_root.exists()
        assert paths.metrics_root.is_dir()

    def test_returns_statistics(self, config: Config, paths: PathManager) -> None:
        """Test that run_metrics returns proper statistics."""
        # Create curated data
        paths.curated_root.mkdir(parents=True, exist_ok=True)

        # Create required tables
        dim_user_path = paths.curated_path("dim_user")
        dim_user_path.touch()

        dim_repo_path = paths.curated_path("dim_repo")
        dim_repo_path.touch()

        # Run metrics
        stats = run_metrics(config)

        # Verify stats structure
        assert "start_time" in stats
        assert "end_time" in stats
        assert "duration_seconds" in stats
        assert "metrics_written" in stats
        assert "total_rows" in stats
        assert "errors" in stats

        # Verify types
        assert isinstance(stats["start_time"], str)
        assert isinstance(stats["end_time"], str)
        assert isinstance(stats["duration_seconds"], float)
        assert isinstance(stats["metrics_written"], list)
        assert isinstance(stats["total_rows"], int)
        assert isinstance(stats["errors"], list)

        # Verify duration is positive
        assert stats["duration_seconds"] >= 0

    def test_reports_missing_data_errors(self, config: Config, paths: PathManager) -> None:
        """Test that missing curated data is reported as errors."""
        # Create curated data with empty tables
        paths.curated_root.mkdir(parents=True, exist_ok=True)

        # Create required tables (empty files will cause read errors)
        dim_user_path = paths.curated_path("dim_user")
        dim_user_path.touch()

        dim_repo_path = paths.curated_path("dim_repo")
        dim_repo_path.touch()

        # Run metrics - calculators will fail due to invalid/empty parquet files
        stats = run_metrics(config)

        # Errors should be reported for each calculator
        assert len(stats["errors"]) >= 1  # At least some errors expected

        # No metrics should be written (files are invalid)
        assert stats["total_rows"] == 0

    def test_handles_calculator_exceptions(self, config: Config, paths: PathManager) -> None:
        """Test that exceptions in calculators are caught and reported."""
        # Create curated data
        paths.curated_root.mkdir(parents=True, exist_ok=True)

        # Create required tables
        dim_user_path = paths.curated_path("dim_user")
        dim_user_path.touch()

        dim_repo_path = paths.curated_path("dim_repo")
        dim_repo_path.touch()

        # Mock a calculator to raise an exception
        with patch("gh_year_end.metrics.orchestrator._run_leaderboards") as mock_leaderboards:
            mock_leaderboards.side_effect = RuntimeError("Test error")

            # Run metrics
            stats = run_metrics(config)

            # Verify error was captured
            assert any("leaderboards: Test error" in error for error in stats["errors"])

    def test_continues_after_calculator_failure(self, config: Config, paths: PathManager) -> None:
        """Test that orchestrator continues after a calculator fails."""
        # Create curated data
        paths.curated_root.mkdir(parents=True, exist_ok=True)

        # Create required tables
        dim_user_path = paths.curated_path("dim_user")
        dim_user_path.touch()

        dim_repo_path = paths.curated_path("dim_repo")
        dim_repo_path.touch()

        # Mock first calculator to fail, second to succeed
        with (
            patch("gh_year_end.metrics.orchestrator._run_leaderboards") as mock_leaderboards,
            patch("gh_year_end.metrics.orchestrator._run_time_series") as mock_time_series,
        ):
            mock_leaderboards.side_effect = RuntimeError("Leaderboards failed")
            mock_time_series.return_value = {
                "success": True,
                "table_name": "metrics_time_series",
                "row_count": 100,
            }

            # Run metrics
            stats = run_metrics(config)

            # Verify both calculators were called
            assert mock_leaderboards.called
            assert mock_time_series.called

            # Verify results
            assert "metrics_time_series" in stats["metrics_written"]
            assert stats["total_rows"] == 100
            assert any("leaderboards" in error.lower() for error in stats["errors"])

    def test_accumulates_row_counts(self, config: Config, paths: PathManager) -> None:
        """Test that row counts are accumulated across calculators."""
        # Create curated data
        paths.curated_root.mkdir(parents=True, exist_ok=True)

        # Create required tables
        dim_user_path = paths.curated_path("dim_user")
        dim_user_path.touch()

        dim_repo_path = paths.curated_path("dim_repo")
        dim_repo_path.touch()

        # Mock calculators to succeed with different row counts
        with (
            patch("gh_year_end.metrics.orchestrator._run_leaderboards") as mock_leaderboards,
            patch("gh_year_end.metrics.orchestrator._run_time_series") as mock_time_series,
            patch("gh_year_end.metrics.orchestrator._run_repo_health") as mock_repo_health,
        ):
            mock_leaderboards.return_value = {
                "success": True,
                "table_name": "metrics_leaderboard",
                "row_count": 50,
            }
            mock_time_series.return_value = {
                "success": True,
                "table_name": "metrics_time_series",
                "row_count": 100,
            }
            mock_repo_health.return_value = {
                "success": True,
                "table_name": "metrics_repo_health",
                "row_count": 25,
            }

            # Run metrics
            stats = run_metrics(config)

            # Verify totals
            assert stats["total_rows"] == 175
            assert len(stats["metrics_written"]) == 3


class TestCalculatorHelpers:
    """Tests for individual calculator helper functions."""

    def test_leaderboards_handles_missing_data(self, config: Config, paths: PathManager) -> None:
        """Test that _run_leaderboards handles missing curated data."""
        from gh_year_end.metrics.orchestrator import _run_leaderboards

        result = _run_leaderboards(config, paths)

        # Should fail gracefully when curated data is missing
        assert result["success"] is False
        assert result["table_name"] == "metrics_leaderboard"
        assert "error" in result or result.get("row_count", 0) == 0

    def test_time_series_handles_missing_data(self, config: Config, paths: PathManager) -> None:
        """Test that _run_time_series handles missing curated data."""
        from gh_year_end.metrics.orchestrator import _run_time_series

        result = _run_time_series(config, paths)

        assert result["table_name"] == "metrics_time_series"
        # May succeed with 0 rows or fail gracefully
        assert "row_count" in result or "error" in result

    def test_repo_health_handles_missing_data(self, config: Config, paths: PathManager) -> None:
        """Test that _run_repo_health handles missing curated data."""
        from gh_year_end.metrics.orchestrator import _run_repo_health

        result = _run_repo_health(config, paths)

        assert result["table_name"] == "metrics_repo_health"
        assert "row_count" in result or "error" in result

    def test_hygiene_scores_handles_missing_data(self, config: Config, paths: PathManager) -> None:
        """Test that _run_hygiene_scores handles missing curated data."""
        from gh_year_end.metrics.orchestrator import _run_hygiene_scores

        result = _run_hygiene_scores(config, paths)

        assert result["table_name"] == "metrics_repo_hygiene_score"
        assert "row_count" in result or "error" in result

    def test_awards_handles_missing_data(self, config: Config, paths: PathManager) -> None:
        """Test that _run_awards handles missing curated data."""
        from gh_year_end.metrics.orchestrator import _run_awards

        result = _run_awards(config, paths)

        assert result["table_name"] == "metrics_awards"
        assert "row_count" in result or "error" in result
