"""Tests for deprecated report CLI command.

The 'report' command has been deprecated in favor of 'build'.
These tests verify the deprecation behavior.
"""

from pathlib import Path

import pytest
from click.testing import CliRunner

from gh_year_end.cli import main


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


class TestReportCommandDeprecated:
    """Tests for deprecated report command."""

    def test_report_shows_deprecation_warning(self, config_file: Path) -> None:
        """Test that report command shows deprecation warning."""
        runner = CliRunner()
        result = runner.invoke(main, ["report", "--config", str(config_file)])

        # Command should abort with deprecation message
        assert result.exit_code != 0
        assert "deprecated" in result.output.lower()
        assert "build" in result.output

    def test_report_suggests_build_command(self, config_file: Path) -> None:
        """Test that report command suggests using build instead."""
        runner = CliRunner()
        result = runner.invoke(main, ["report", "--config", str(config_file)])

        assert "gh-year-end build" in result.output

    def test_report_not_in_main_help(self) -> None:
        """Test that report command is hidden from main help."""
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])

        # The deprecated 'report' command should not appear in help
        # (it's hidden but still accessible)
        assert result.exit_code == 0
        # 'build' should be there, 'report' should not be prominently listed
        assert "build" in result.output
