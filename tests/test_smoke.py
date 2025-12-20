"""Smoke tests for simplified pipeline - fast regression protection.

These tests provide quick verification that basic functionality works
without deep testing. They are designed to run fast and catch obvious
breakage early.
"""

import pytest
from click.testing import CliRunner
from pathlib import Path

pytestmark = pytest.mark.smoke


class TestCLISmoke:
    """Verify CLI commands are accessible and display help correctly."""

    def test_main_help(self):
        """Main help displays without error."""
        from gh_year_end.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "collect" in result.output
        assert "build" in result.output
        assert "all" in result.output

    def test_main_version(self):
        """Version displays correctly."""
        from gh_year_end.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "gh-year-end" in result.output
        assert "0.1.0" in result.output

    def test_collect_help(self):
        """Collect command help displays."""
        from gh_year_end.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["collect", "--help"])
        assert result.exit_code == 0
        assert "Collect GitHub data" in result.output
        assert "--config" in result.output
        assert "--force" in result.output

    def test_build_help(self):
        """Build command help displays."""
        from gh_year_end.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["build", "--help"])
        assert result.exit_code == 0
        assert "Build static HTML site" in result.output
        assert "--config" in result.output

    def test_all_help(self):
        """All command help displays."""
        from gh_year_end.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["all", "--help"])
        assert result.exit_code == 0
        assert "complete pipeline" in result.output
        assert "--config" in result.output
        assert "--force" in result.output

    def test_collect_requires_config(self):
        """Collect command requires --config flag."""
        from gh_year_end.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["collect"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_build_requires_config(self):
        """Build command requires --config flag."""
        from gh_year_end.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["build"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()


class TestImportSmoke:
    """Verify key modules import without error."""

    def test_import_cli(self):
        """CLI module imports."""
        from gh_year_end import cli

        assert cli is not None
        assert hasattr(cli, "main")

    def test_import_config(self):
        """Config module imports."""
        from gh_year_end import config

        assert config is not None
        assert hasattr(config, "load_config")

    def test_import_aggregator(self):
        """MetricsAggregator imports."""
        from gh_year_end.collect.aggregator import MetricsAggregator

        assert MetricsAggregator is not None

    def test_import_orchestrator(self):
        """Orchestrator imports."""
        from gh_year_end.collect.orchestrator import collect_and_aggregate

        assert collect_and_aggregate is not None

    def test_import_build(self):
        """Build module imports."""
        from gh_year_end.report.build import build_site

        assert build_site is not None

    def test_import_path_manager(self):
        """PathManager imports."""
        from gh_year_end.storage.paths import PathManager

        assert PathManager is not None

    def test_import_logging(self):
        """Logging module imports."""
        from gh_year_end.logging import setup_logging

        assert setup_logging is not None

    def test_version_is_set(self):
        """Package version is defined."""
        from gh_year_end import __version__

        assert __version__ is not None
        assert isinstance(__version__, str)
        assert len(__version__) > 0


class TestAggregatorSmoke:
    """Basic aggregator functionality without data collection."""

    def test_aggregator_instantiation(self):
        """Aggregator can be instantiated."""
        from gh_year_end.collect.aggregator import MetricsAggregator

        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        assert agg.year == 2024
        assert agg.target_name == "test"
        assert agg.target_mode == "user"

    def test_aggregator_export_empty(self):
        """Aggregator can export with no data."""
        from gh_year_end.collect.aggregator import MetricsAggregator

        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        result = agg.export()

        # Verify expected top-level keys
        assert "summary" in result
        assert "leaderboards" in result
        assert "timeseries" in result
        assert "repo_health" in result
        assert "hygiene_scores" in result
        assert "awards" in result

    def test_aggregator_export_structure(self):
        """Exported data has correct structure."""
        from gh_year_end.collect.aggregator import MetricsAggregator

        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        result = agg.export()

        # Summary should be a dict
        assert isinstance(result["summary"], dict)

        # Leaderboards should be a dict with list values
        assert isinstance(result["leaderboards"], dict)

        # Timeseries should be a dict
        assert isinstance(result["timeseries"], dict)

        # Repo health should be a list
        assert isinstance(result["repo_health"], list)

        # Hygiene scores should be a dict (repo_id -> hygiene data)
        assert isinstance(result["hygiene_scores"], dict)

        # Awards should be a dict
        assert isinstance(result["awards"], dict)


class TestConfigSmoke:
    """Basic config loading functionality."""

    def test_config_module_exists(self):
        """Config module has expected functions."""
        from gh_year_end import config

        assert hasattr(config, "load_config")
        assert hasattr(config, "Config")

    def test_config_invalid_path_fails(self):
        """Loading non-existent config raises error."""
        from gh_year_end.config import load_config

        with pytest.raises(Exception):
            load_config(Path("/nonexistent/config.yaml"))


class TestPathManagerSmoke:
    """Basic PathManager functionality."""

    def test_path_manager_attributes(self):
        """PathManager has expected attributes after init."""
        from gh_year_end.storage.paths import PathManager
        from gh_year_end.config import Config, GitHubConfig, TargetConfig, WindowsConfig, StorageConfig

        # Create minimal config with proper year boundaries
        config = Config(
            github=GitHubConfig(
                target=TargetConfig(mode="user", name="test"),
                windows=WindowsConfig(year=2024, since="2024-01-01T00:00:00Z", until="2025-01-01T00:00:00Z"),
            ),
            storage=StorageConfig(root="data"),
        )

        paths = PathManager(config)

        # Check key attributes exist
        assert hasattr(paths, "site_root")
        assert hasattr(paths, "raw_root")
        assert hasattr(paths, "curated_root")
        assert hasattr(paths, "metrics_root")
        assert paths.site_root is not None
        assert paths.raw_root is not None
        assert paths.curated_root is not None
        assert paths.metrics_root is not None


class TestBuildSmoke:
    """Basic build module checks."""

    def test_build_site_callable(self):
        """build_site function is callable."""
        from gh_year_end.report.build import build_site

        assert callable(build_site)

    def test_build_module_exports(self):
        """Build module has expected exports."""
        from gh_year_end.report import build

        assert hasattr(build, "build_site")


class TestRemovedCommandsSmoke:
    """Verify removed commands are not available."""

    def test_removed_commands_not_available(self):
        """Removed commands return 'No such command' error."""
        from gh_year_end.cli import main

        runner = CliRunner()

        # These commands were fully removed (not just deprecated)
        removed_commands = ["plan", "normalize", "metrics", "report", "validate", "status"]

        for cmd in removed_commands:
            result = runner.invoke(main, [cmd, "--help"])
            assert result.exit_code == 2, f"Command '{cmd}' should not exist"
            assert "no such command" in result.output.lower(), f"Command '{cmd}' should show 'no such command'"

    def test_only_collect_build_all_available(self):
        """Only collect, build, and all commands are available."""
        from gh_year_end.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["--help"])

        assert result.exit_code == 0

        # Parse the Commands section to get actual command names
        lines = result.output.split("\n")
        in_commands = False
        command_names = []
        for line in lines:
            if line.strip() == "Commands:":
                in_commands = True
                continue
            if in_commands and line.strip():
                parts = line.split()
                if parts:
                    command_names.append(parts[0])

        # Only these commands should exist
        assert "collect" in command_names
        assert "build" in command_names
        assert "all" in command_names

        # Removed commands should NOT exist
        assert "normalize" not in command_names
        assert "metrics" not in command_names
        assert "report" not in command_names
        assert "validate" not in command_names
        assert "status" not in command_names
        assert "plan" not in command_names
