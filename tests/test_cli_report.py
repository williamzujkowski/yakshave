"""Tests for report CLI command."""

from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from gh_year_end.cli import main
from gh_year_end.config import Config
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
    config.report.output_dir = str(tmp_path / "site")
    return PathManager(config)


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    """Create a temporary config file."""
    config_content = """
github:
  target:
    mode: org
    name: test-org
  windows:
    year: 2025
    since: "2025-01-01T00:00:00Z"
    until: "2026-01-01T00:00:00Z"

storage:
  root: {storage_root}

report:
  output_dir: {site_root}
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        config_content.format(storage_root=str(tmp_path), site_root=str(tmp_path / "site"))
    )
    return config_path


class TestReportCommand:
    """Tests for report command."""

    def test_fails_if_metrics_missing(self, config_file: Path) -> None:
        """Test that report fails if metrics data doesn't exist."""
        runner = CliRunner()
        result = runner.invoke(main, ["report", "--config", str(config_file)])

        assert result.exit_code != 0
        assert "No metrics data found" in result.output

    def test_fails_if_no_metrics_tables(self, config_file: Path, tmp_path: Path) -> None:
        """Test that report fails if no metrics tables exist."""
        # Create metrics_root but no tables
        metrics_root = tmp_path / "metrics" / "year=2025"
        metrics_root.mkdir(parents=True, exist_ok=True)

        runner = CliRunner()
        result = runner.invoke(main, ["report", "--config", str(config_file)])

        assert result.exit_code != 0
        assert "No metrics tables found" in result.output

    def test_exports_and_builds_site(self, config_file: Path, tmp_path: Path) -> None:
        """Test that report exports metrics and builds site."""
        # Create metrics data
        metrics_root = tmp_path / "metrics" / "year=2025"
        metrics_root.mkdir(parents=True, exist_ok=True)

        # Create a dummy metrics file
        metrics_file = metrics_root / "metrics_leaderboard.parquet"
        metrics_file.touch()

        # Mock export_metrics and build_site
        with (
            patch("gh_year_end.report.export.export_metrics") as mock_export,
            patch("gh_year_end.report.build.build_site") as mock_build,
        ):
            mock_export.return_value = {
                "tables_exported": ["metrics_leaderboard"],
                "total_rows": 10,
                "errors": [],
            }
            mock_build.return_value = {
                "templates_rendered": ["index.html"],
                "data_files_written": 1,
                "assets_copied": 0,
                "errors": [],
            }

            runner = CliRunner()
            result = runner.invoke(main, ["report", "--config", str(config_file)])

            # Verify success
            assert result.exit_code == 0
            assert "Report generation complete!" in result.output
            assert "Exported 1 tables" in result.output
            assert "Rendered 1 templates" in result.output

            # Verify functions were called
            mock_export.assert_called_once()
            mock_build.assert_called_once()

    def test_shows_export_errors(self, config_file: Path, tmp_path: Path) -> None:
        """Test that export errors are displayed."""
        # Create metrics data
        metrics_root = tmp_path / "metrics" / "year=2025"
        metrics_root.mkdir(parents=True, exist_ok=True)

        metrics_file = metrics_root / "metrics_leaderboard.parquet"
        metrics_file.touch()

        # Mock export_metrics with errors
        with (
            patch("gh_year_end.report.export.export_metrics") as mock_export,
            patch("gh_year_end.report.build.build_site") as mock_build,
        ):
            mock_export.return_value = {
                "tables_exported": ["metrics_leaderboard"],
                "total_rows": 10,
                "errors": ["Error 1", "Error 2"],
            }
            mock_build.return_value = {
                "templates_rendered": ["index.html"],
                "data_files_written": 1,
                "assets_copied": 0,
                "errors": [],
            }

            runner = CliRunner()
            result = runner.invoke(main, ["report", "--config", str(config_file)])

            assert result.exit_code == 0
            assert "Export warnings: 2" in result.output
            assert "Error 1" in result.output
            assert "Completed with 2 warning(s)" in result.output

    def test_shows_build_errors(self, config_file: Path, tmp_path: Path) -> None:
        """Test that build errors are displayed."""
        # Create metrics data
        metrics_root = tmp_path / "metrics" / "year=2025"
        metrics_root.mkdir(parents=True, exist_ok=True)

        metrics_file = metrics_root / "metrics_leaderboard.parquet"
        metrics_file.touch()

        # Mock with build errors
        with (
            patch("gh_year_end.report.export.export_metrics") as mock_export,
            patch("gh_year_end.report.build.build_site") as mock_build,
        ):
            mock_export.return_value = {
                "tables_exported": ["metrics_leaderboard"],
                "total_rows": 10,
                "errors": [],
            }
            mock_build.return_value = {
                "templates_rendered": [],
                "data_files_written": 1,
                "assets_copied": 0,
                "errors": ["Build error"],
            }

            runner = CliRunner()
            result = runner.invoke(main, ["report", "--config", str(config_file)])

            assert result.exit_code == 0
            assert "Build warnings: 1" in result.output
            assert "Build error" in result.output

    def test_handles_export_exception(self, config_file: Path, tmp_path: Path) -> None:
        """Test that export exceptions are handled."""
        # Create metrics data
        metrics_root = tmp_path / "metrics" / "year=2025"
        metrics_root.mkdir(parents=True, exist_ok=True)

        metrics_file = metrics_root / "metrics_leaderboard.parquet"
        metrics_file.touch()

        # Mock export_metrics to raise exception
        with patch("gh_year_end.report.export.export_metrics") as mock_export:
            mock_export.side_effect = ValueError("Export failed")

            runner = CliRunner()
            result = runner.invoke(main, ["report", "--config", str(config_file)])

            assert result.exit_code != 0
            assert "Export failed" in result.output

    def test_handles_build_exception(self, config_file: Path, tmp_path: Path) -> None:
        """Test that build exceptions are handled."""
        # Create metrics data
        metrics_root = tmp_path / "metrics" / "year=2025"
        metrics_root.mkdir(parents=True, exist_ok=True)

        metrics_file = metrics_root / "metrics_leaderboard.parquet"
        metrics_file.touch()

        # Mock build_site to raise exception
        with (
            patch("gh_year_end.report.export.export_metrics") as mock_export,
            patch("gh_year_end.report.build.build_site") as mock_build,
        ):
            mock_export.return_value = {
                "tables_exported": ["metrics_leaderboard"],
                "total_rows": 10,
                "errors": [],
            }
            mock_build.side_effect = ValueError("Build failed")

            runner = CliRunner()
            result = runner.invoke(main, ["report", "--config", str(config_file)])

            assert result.exit_code != 0
            assert "Build failed" in result.output

    def test_shows_serve_command_hint(self, config_file: Path, tmp_path: Path) -> None:
        """Test that serve command hint is displayed."""
        # Create metrics data
        metrics_root = tmp_path / "metrics" / "year=2025"
        metrics_root.mkdir(parents=True, exist_ok=True)

        metrics_file = metrics_root / "metrics_leaderboard.parquet"
        metrics_file.touch()

        # Mock export and build
        with (
            patch("gh_year_end.report.export.export_metrics") as mock_export,
            patch("gh_year_end.report.build.build_site") as mock_build,
        ):
            mock_export.return_value = {
                "tables_exported": ["metrics_leaderboard"],
                "total_rows": 10,
                "errors": [],
            }
            mock_build.return_value = {
                "templates_rendered": ["index.html"],
                "data_files_written": 1,
                "assets_copied": 0,
                "errors": [],
            }

            runner = CliRunner()
            result = runner.invoke(main, ["report", "--config", str(config_file)])

            assert result.exit_code == 0
            assert "To view the report:" in result.output
            assert "python -m http.server" in result.output

    def test_force_flag_accepted(self, config_file: Path, tmp_path: Path) -> None:
        """Test that --force flag is accepted."""
        # Create metrics data
        metrics_root = tmp_path / "metrics" / "year=2025"
        metrics_root.mkdir(parents=True, exist_ok=True)

        metrics_file = metrics_root / "metrics_leaderboard.parquet"
        metrics_file.touch()

        # Mock export and build
        with (
            patch("gh_year_end.report.export.export_metrics") as mock_export,
            patch("gh_year_end.report.build.build_site") as mock_build,
        ):
            mock_export.return_value = {
                "tables_exported": ["metrics_leaderboard"],
                "total_rows": 10,
                "errors": [],
            }
            mock_build.return_value = {
                "templates_rendered": ["index.html"],
                "data_files_written": 1,
                "assets_copied": 0,
                "errors": [],
            }

            runner = CliRunner()
            result = runner.invoke(main, ["report", "--config", str(config_file), "--force"])

            assert result.exit_code == 0
            assert "Report generation complete!" in result.output

    def test_verbose_shows_traceback_on_error(self, config_file: Path, tmp_path: Path) -> None:
        """Test that --verbose shows traceback on error."""
        # Create metrics data
        metrics_root = tmp_path / "metrics" / "year=2025"
        metrics_root.mkdir(parents=True, exist_ok=True)

        metrics_file = metrics_root / "metrics_leaderboard.parquet"
        metrics_file.touch()

        # Mock to raise exception
        with patch("gh_year_end.report.export.export_metrics") as mock_export:
            mock_export.side_effect = RuntimeError("Test error")

            runner = CliRunner()
            result = runner.invoke(main, ["--verbose", "report", "--config", str(config_file)])

            assert result.exit_code != 0
            assert "Traceback:" in result.output
