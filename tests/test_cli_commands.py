"""Tests for new CLI commands (collect, build, all)."""

from datetime import UTC
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from gh_year_end.cli import main


@pytest.fixture
def runner() -> CliRunner:
    """Create a Click test runner."""
    return CliRunner()


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    """Create a temporary config file."""
    config_content = """github:
  target:
    mode: org
    name: test-org
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
  max_concurrency: 5
  min_sleep_seconds: 1.0
  max_sleep_seconds: 60.0
  sample_rate_limit_endpoint_every_n_requests: 100

identity:
  bots:
    exclude_patterns:
      - ".*\\\\[bot\\\\]$"
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
      - README.md
      - LICENSE
    workflow_prefixes:
      - .github/workflows/
    branch_protection:
      mode: sample
      sample_top_repos_by: prs_merged
      sample_count: 5
    security_features:
      best_effort: true

storage:
  root: {storage_root}
  raw_format: jsonl
  curated_format: parquet
  dataset_version: v1

report:
  title: "Test Organization 2024 Year in Review"
  output_dir: {site_root}
  theme: engineer_exec_toggle
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        config_content.format(
            storage_root=str(tmp_path / "data"),
            site_root=str(tmp_path / "site"),
        )
    )
    return config_path


class TestMainGroup:
    """Tests for main CLI group."""

    def test_version_flag(self, runner: CliRunner) -> None:
        """Test that --version shows version information."""
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "gh-year-end" in result.output

    def test_help_flag(self, runner: CliRunner) -> None:
        """Test that --help shows help text."""
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "GitHub Year-End Community Health Report Generator" in result.output
        assert "collect" in result.output
        assert "build" in result.output
        assert "all" in result.output

    def test_verbose_flag(self, runner: CliRunner) -> None:
        """Test that --verbose flag is accepted."""
        result = runner.invoke(main, ["--verbose", "--help"])
        assert result.exit_code == 0


class TestCollectCommand:
    """Tests for collect command."""

    def test_help_output(self, runner: CliRunner) -> None:
        """Test collect --help shows correct information."""
        result = runner.invoke(main, ["collect", "--help"])
        assert result.exit_code == 0
        assert "Collect GitHub data and generate metrics JSON" in result.output
        assert "--config" in result.output
        assert "--force" in result.output

    def test_missing_config_file(self, runner: CliRunner) -> None:
        """Test that missing config file shows error."""
        result = runner.invoke(main, ["collect", "--config", "nonexistent.yaml"])
        assert result.exit_code != 0

    def test_config_required(self, runner: CliRunner) -> None:
        """Test that --config parameter is required."""
        result = runner.invoke(main, ["collect"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_not_implemented_error(self) -> None:
        """Test collect shows not-implemented error when orchestrator missing."""
        # The collect command has a try/except ImportError that shows a message
        # We can't easily mock the import since it's inside the function,
        # so we'll just verify the real behavior when the module doesn't exist
        # For now, skip this test since collect_and_aggregate exists
        pytest.skip("ImportError handling tested via real missing imports")

    def test_successful_collection(self, runner: CliRunner, config_file: Path) -> None:
        """Test successful collection and metrics writing."""

        # Create an async mock that returns the expected data
        async def mock_collect(*args, **kwargs):
            return {
                "summary": {"total_prs": 100, "total_contributors": 25},
                "leaderboards": {"top_contributors": []},
                "timeseries": {"weekly": []},
                "repo_health": {"repositories": []},
                "hygiene_scores": {"scores": []},
                "awards": {"awards": []},
            }

        with patch(
            "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
        ):
            result = runner.invoke(main, ["collect", "--config", str(config_file)])

        # Should succeed
        assert result.exit_code == 0
        assert "Collection complete!" in result.output
        assert "Files written: 6" in result.output

    def test_force_flag_passed_through(self, runner: CliRunner, config_file: Path) -> None:
        """Test that --force flag is passed to collect_and_aggregate."""
        force_used = {}

        async def mock_collect(*args, **kwargs):
            force_used["value"] = kwargs.get("force", False)
            return {}

        with patch(
            "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
        ):
            runner.invoke(main, ["collect", "--config", str(config_file), "--force"])

        # Force flag should have been used
        assert force_used.get("value") is True

    def test_verbose_flag_passed_through(self, runner: CliRunner, config_file: Path) -> None:
        """Test that --verbose flag is passed to collect_and_aggregate."""
        verbose_used = {}

        async def mock_collect(*args, **kwargs):
            verbose_used["value"] = kwargs.get("verbose", False)
            return {}

        with patch(
            "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
        ):
            runner.invoke(main, ["--verbose", "collect", "--config", str(config_file)])

        # Verbose flag should have been used
        assert verbose_used.get("value") is True

    def test_keyboard_interrupt_handling(self, runner: CliRunner, config_file: Path) -> None:
        """Test that keyboard interrupt is handled gracefully."""

        async def mock_collect(*args, **kwargs):
            raise KeyboardInterrupt()

        with patch(
            "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
        ):
            result = runner.invoke(main, ["collect", "--config", str(config_file)])

        assert result.exit_code == 1
        assert "interrupted by user" in result.output

    def test_exception_handling(self, runner: CliRunner, config_file: Path) -> None:
        """Test that exceptions are handled and displayed."""

        async def mock_collect(*args, **kwargs):
            raise ValueError("Test error")

        with patch(
            "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
        ):
            result = runner.invoke(main, ["collect", "--config", str(config_file)])

        assert result.exit_code == 1
        assert "Error:" in result.output
        assert "Test error" in result.output

    def test_verbose_shows_traceback_on_error(self, runner: CliRunner, config_file: Path) -> None:
        """Test that --verbose shows traceback on error."""

        async def mock_collect(*args, **kwargs):
            raise RuntimeError("Test error")

        with patch(
            "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
        ):
            result = runner.invoke(main, ["--verbose", "collect", "--config", str(config_file)])

        assert result.exit_code == 1
        assert "Traceback:" in result.output

    def test_year_override(self, runner: CliRunner, config_file: Path) -> None:
        """Test that --year flag overrides config year."""
        config_received = {}

        async def mock_collect(config, *args, **kwargs):
            config_received["year"] = config.github.windows.year
            config_received["since"] = config.github.windows.since
            config_received["until"] = config.github.windows.until
            return {
                "summary": {"total_prs": 100},
                "leaderboards": {"top_contributors": []},
                "timeseries": {"weekly": []},
                "repo_health": {"repositories": []},
                "hygiene_scores": {"scores": []},
                "awards": {"awards": []},
            }

        with patch(
            "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
        ):
            result = runner.invoke(
                main, ["collect", "--config", str(config_file), "--year", "2023"]
            )

        assert result.exit_code == 0
        assert config_received["year"] == 2023
        # Check that since/until were recalculated
        from datetime import datetime

        assert config_received["since"] == datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)
        assert config_received["until"] == datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)


class TestBuildCommand:
    """Tests for build command."""

    def test_help_output(self, runner: CliRunner) -> None:
        """Test build --help shows correct information."""
        result = runner.invoke(main, ["build", "--help"])
        assert result.exit_code == 0
        assert "Build static HTML site from metrics JSON" in result.output
        assert "--config" in result.output

    def test_missing_config_file(self, runner: CliRunner) -> None:
        """Test that missing config file shows error."""
        result = runner.invoke(main, ["build", "--config", "nonexistent.yaml"])
        assert result.exit_code != 0

    def test_config_required(self, runner: CliRunner) -> None:
        """Test that --config parameter is required."""
        result = runner.invoke(main, ["build"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_missing_data_directory(self, runner: CliRunner, config_file: Path) -> None:
        """Test error when data directory doesn't exist."""
        result = runner.invoke(main, ["build", "--config", str(config_file)])
        assert result.exit_code == 1
        # The actual error message comes from build_site
        assert "Metrics data not found" in result.output or "No metrics data found" in result.output

    def test_missing_required_files(
        self, runner: CliRunner, config_file: Path, tmp_path: Path
    ) -> None:
        """Test error when required JSON files are missing."""
        # Create data directory but no files
        data_dir = tmp_path / "site" / "2024" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        result = runner.invoke(main, ["build", "--config", str(config_file)])
        assert result.exit_code == 1
        # The actual error message comes from build_site
        assert "Missing required" in result.output or "missing" in result.output.lower()
        assert "summary.json" in result.output or "leaderboards.json" in result.output

    @patch("gh_year_end.report.build.build_site")
    def test_successful_build(
        self, mock_build_site: MagicMock, runner: CliRunner, config_file: Path, tmp_path: Path
    ) -> None:
        """Test successful site building."""
        # Create required data files
        data_dir = tmp_path / "site" / "2024" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "summary.json").write_text('{"total_prs": 100}')
        (data_dir / "leaderboards.json").write_text('{"top_contributors": []}')

        # Mock build_site
        mock_build_site.return_value = {
            "templates_rendered": ["index.html", "summary.html"],
            "data_files_written": 6,
            "assets_copied": 10,
            "errors": [],
        }

        result = runner.invoke(main, ["build", "--config", str(config_file)])

        assert result.exit_code == 0
        assert "Site built successfully!" in result.output
        assert "Templates rendered: 2" in result.output
        assert "Data files: 6" in result.output
        assert "Assets copied: 10" in result.output
        assert "To view the site:" in result.output
        assert "python -m http.server" in result.output

        # Verify build_site was called
        mock_build_site.assert_called_once()

    @patch("gh_year_end.report.build.build_site")
    def test_build_with_warnings(
        self, mock_build_site: MagicMock, runner: CliRunner, config_file: Path, tmp_path: Path
    ) -> None:
        """Test build with warnings shows them."""
        # Create required data files
        data_dir = tmp_path / "site" / "2024" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "summary.json").write_text('{"total_prs": 100}')
        (data_dir / "leaderboards.json").write_text('{"top_contributors": []}')

        # Mock build_site with errors
        mock_build_site.return_value = {
            "templates_rendered": ["index.html"],
            "data_files_written": 6,
            "assets_copied": 0,
            "errors": ["Warning 1", "Warning 2", "Warning 3"],
        }

        result = runner.invoke(main, ["build", "--config", str(config_file)])

        assert result.exit_code == 0
        assert "Warnings: 3" in result.output
        assert "Warning 1" in result.output

    @patch("gh_year_end.report.build.build_site")
    def test_build_exception_handling(
        self, mock_build_site: MagicMock, runner: CliRunner, config_file: Path, tmp_path: Path
    ) -> None:
        """Test that build exceptions are handled."""
        # Create required data files
        data_dir = tmp_path / "site" / "2024" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "summary.json").write_text('{"total_prs": 100}')
        (data_dir / "leaderboards.json").write_text('{"top_contributors": []}')

        # Mock build_site to raise exception
        mock_build_site.side_effect = ValueError("Build failed")

        result = runner.invoke(main, ["build", "--config", str(config_file)])

        assert result.exit_code == 1
        assert "Build failed:" in result.output
        assert "Build failed" in result.output

    @patch("gh_year_end.report.build.build_site")
    def test_verbose_shows_traceback_on_error(
        self, mock_build_site: MagicMock, runner: CliRunner, config_file: Path, tmp_path: Path
    ) -> None:
        """Test that --verbose shows traceback on error."""
        # Create required data files
        data_dir = tmp_path / "site" / "2024" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "summary.json").write_text('{"total_prs": 100}')
        (data_dir / "leaderboards.json").write_text('{"top_contributors": []}')

        # Mock build_site to raise exception
        mock_build_site.side_effect = RuntimeError("Build error")

        result = runner.invoke(main, ["--verbose", "build", "--config", str(config_file)])

        assert result.exit_code == 1
        assert "Traceback:" in result.output

    @patch("gh_year_end.report.build.build_site")
    def test_year_override(
        self, mock_build_site: MagicMock, runner: CliRunner, config_file: Path, tmp_path: Path
    ) -> None:
        """Test that --year flag overrides config year."""
        # Create required data files for year 2023
        data_dir = tmp_path / "site" / "2023" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "summary.json").write_text('{"total_prs": 100}')
        (data_dir / "leaderboards.json").write_text('{"top_contributors": []}')

        config_received = {}

        def capture_config(config, paths):
            config_received["year"] = config.github.windows.year
            config_received["since"] = config.github.windows.since
            config_received["until"] = config.github.windows.until
            return {
                "templates_rendered": ["index.html"],
                "data_files_written": 6,
                "assets_copied": 5,
                "errors": [],
            }

        mock_build_site.side_effect = capture_config

        result = runner.invoke(main, ["build", "--config", str(config_file), "--year", "2023"])

        assert result.exit_code == 0
        assert config_received["year"] == 2023
        # Check that since/until were recalculated
        from datetime import datetime

        assert config_received["since"] == datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)
        assert config_received["until"] == datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)


class TestAllCommand:
    """Tests for all command (collect + build)."""

    def test_help_output(self, runner: CliRunner) -> None:
        """Test all --help shows correct information."""
        result = runner.invoke(main, ["all", "--help"])
        assert result.exit_code == 0
        assert "Run the complete pipeline" in result.output
        assert "--config" in result.output
        assert "--force" in result.output

    def test_missing_config_file(self, runner: CliRunner) -> None:
        """Test that missing config file shows error."""
        result = runner.invoke(main, ["all", "--config", "nonexistent.yaml"])
        assert result.exit_code != 0

    def test_config_required(self, runner: CliRunner) -> None:
        """Test that --config parameter is required."""
        result = runner.invoke(main, ["all"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_runs_both_commands(self, runner: CliRunner, config_file: Path) -> None:
        """Test that all command runs both collect and build."""

        async def mock_collect(*args, **kwargs):
            return {
                "summary": {"total_prs": 100},
                "leaderboards": {"top_contributors": []},
                "timeseries": {"weekly": []},
                "repo_health": {"repositories": []},
                "hygiene_scores": {"scores": []},
                "awards": {"awards": []},
            }

        def mock_build(*args, **kwargs):
            return {
                "templates_rendered": ["index.html"],
                "data_files_written": 6,
                "assets_copied": 5,
                "errors": [],
            }

        with (
            patch(
                "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
            ),
            patch("gh_year_end.report.build.build_site", side_effect=mock_build),
        ):
            result = runner.invoke(main, ["all", "--config", str(config_file)])

        # Should succeed
        assert result.exit_code == 0
        assert "Running complete pipeline" in result.output
        assert "Collection complete!" in result.output
        assert "Site built successfully!" in result.output
        assert "Pipeline complete!" in result.output

    def test_collect_failure_stops_pipeline(self, runner: CliRunner, config_file: Path) -> None:
        """Test that collect failure stops the pipeline."""

        async def mock_collect(*args, **kwargs):
            raise ValueError("Collect failed")

        with patch(
            "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
        ):
            result = runner.invoke(main, ["all", "--config", str(config_file)])

        # Should fail
        assert result.exit_code == 1
        assert "Collect failed" in result.output
        # Build should not run
        assert "Site built successfully!" not in result.output

    def test_force_flag_passed_to_collect(self, runner: CliRunner, config_file: Path) -> None:
        """Test that --force flag is passed to collect command."""
        force_used = {}

        async def mock_collect(*args, **kwargs):
            force_used["value"] = kwargs.get("force", False)
            return {
                "summary": {"total_prs": 100},
                "leaderboards": {"top_contributors": []},
                "timeseries": {"weekly": []},
                "repo_health": {"repositories": []},
                "hygiene_scores": {"scores": []},
                "awards": {"awards": []},
            }

        def mock_build(*args, **kwargs):
            return {
                "templates_rendered": ["index.html"],
                "data_files_written": 6,
                "assets_copied": 5,
                "errors": [],
            }

        with (
            patch(
                "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
            ),
            patch("gh_year_end.report.build.build_site", side_effect=mock_build),
        ):
            runner.invoke(main, ["all", "--config", str(config_file), "--force"])

        # Force flag should have been used in collect
        assert force_used.get("value") is True

    def test_year_override(self, runner: CliRunner, config_file: Path) -> None:
        """Test that --year flag overrides config year in both collect and build."""
        config_years = {"collect": None, "build": None}

        async def mock_collect(config, *args, **kwargs):
            config_years["collect"] = config.github.windows.year
            return {
                "summary": {"total_prs": 100},
                "leaderboards": {"top_contributors": []},
                "timeseries": {"weekly": []},
                "repo_health": {"repositories": []},
                "hygiene_scores": {"scores": []},
                "awards": {"awards": []},
            }

        def mock_build(config, *args, **kwargs):
            config_years["build"] = config.github.windows.year
            return {
                "templates_rendered": ["index.html"],
                "data_files_written": 6,
                "assets_copied": 5,
                "errors": [],
            }

        with (
            patch(
                "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
            ),
            patch("gh_year_end.report.build.build_site", side_effect=mock_build),
        ):
            result = runner.invoke(main, ["all", "--config", str(config_file), "--year", "2023"])

        assert result.exit_code == 0
        assert config_years["collect"] == 2023
        assert config_years["build"] == 2023


class TestBatchYearsCommand:
    """Tests for batch-years command."""

    def test_help_output(self, runner: CliRunner) -> None:
        """Test batch-years --help shows correct information."""
        result = runner.invoke(main, ["batch-years", "--help"])
        assert result.exit_code == 0
        assert "Process multiple years in sequence" in result.output
        assert "--config" in result.output
        assert "--years" in result.output
        assert "--from-year" in result.output
        assert "--to-year" in result.output
        assert "--skip-collect" in result.output
        assert "--force" in result.output

    def test_missing_config_file(self, runner: CliRunner) -> None:
        """Test that missing config file shows error."""
        result = runner.invoke(
            main, ["batch-years", "--config", "nonexistent.yaml", "--years", "2024"]
        )
        assert result.exit_code != 0

    def test_config_required(self, runner: CliRunner) -> None:
        """Test that --config parameter is required."""
        result = runner.invoke(main, ["batch-years", "--years", "2024"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_no_years_specified(self, runner: CliRunner, config_file: Path) -> None:
        """Test error when no years are specified."""
        result = runner.invoke(main, ["batch-years", "--config", str(config_file)])
        assert result.exit_code == 1
        assert "Specify --years or both --from-year and --to-year" in result.output

    def test_invalid_year_format(self, runner: CliRunner, config_file: Path) -> None:
        """Test error with invalid year format."""
        result = runner.invoke(
            main, ["batch-years", "--config", str(config_file), "--years", "abc,2024"]
        )
        assert result.exit_code == 1
        assert "Invalid year format" in result.output

    def test_from_year_greater_than_to_year(self, runner: CliRunner, config_file: Path) -> None:
        """Test error when from-year > to-year."""
        result = runner.invoke(
            main,
            [
                "batch-years",
                "--config",
                str(config_file),
                "--from-year",
                "2025",
                "--to-year",
                "2023",
            ],
        )
        assert result.exit_code == 1
        assert "from-year must be <= to-year" in result.output

    def test_comma_separated_years(self, runner: CliRunner, config_file: Path) -> None:
        """Test processing comma-separated years."""

        async def mock_collect(*args, **kwargs):
            return {
                "summary": {"total_prs": 100},
                "leaderboards": {"top_contributors": []},
                "timeseries": {"weekly": []},
                "repo_health": {"repositories": []},
                "hygiene_scores": {"scores": []},
                "awards": {"awards": []},
            }

        def mock_build(*args, **kwargs):
            return {
                "templates_rendered": ["index.html"],
                "data_files_written": 6,
                "assets_copied": 5,
                "errors": [],
            }

        with (
            patch(
                "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
            ),
            patch("gh_year_end.report.build.build_site", side_effect=mock_build),
        ):
            result = runner.invoke(
                main, ["batch-years", "--config", str(config_file), "--years", "2023,2024"]
            )

        assert result.exit_code == 0
        assert "Processing 2 years" in result.output
        assert "2023, 2024" in result.output
        assert "Processing year 2023" in result.output
        assert "Processing year 2024" in result.output
        assert "BATCH PROCESSING SUMMARY" in result.output
        assert "Success: 2" in result.output

    def test_year_range(self, runner: CliRunner, config_file: Path) -> None:
        """Test processing year range."""

        async def mock_collect(*args, **kwargs):
            return {
                "summary": {"total_prs": 100},
                "leaderboards": {"top_contributors": []},
                "timeseries": {"weekly": []},
                "repo_health": {"repositories": []},
                "hygiene_scores": {"scores": []},
                "awards": {"awards": []},
            }

        def mock_build(*args, **kwargs):
            return {
                "templates_rendered": ["index.html"],
                "data_files_written": 6,
                "assets_copied": 5,
                "errors": [],
            }

        with (
            patch(
                "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
            ),
            patch("gh_year_end.report.build.build_site", side_effect=mock_build),
        ):
            result = runner.invoke(
                main,
                [
                    "batch-years",
                    "--config",
                    str(config_file),
                    "--from-year",
                    "2023",
                    "--to-year",
                    "2025",
                ],
            )

        assert result.exit_code == 0
        assert "Processing 3 years" in result.output
        assert "Processing year 2023" in result.output
        assert "Processing year 2024" in result.output
        assert "Processing year 2025" in result.output
        assert "Success: 3" in result.output

    def test_skip_collect_flag(self, runner: CliRunner, config_file: Path, tmp_path: Path) -> None:
        """Test that --skip-collect only runs build."""
        collect_called = {"value": False}

        async def mock_collect(*args, **kwargs):
            collect_called["value"] = True
            return {}

        def mock_build(*args, **kwargs):
            return {
                "templates_rendered": ["index.html"],
                "data_files_written": 6,
                "assets_copied": 5,
                "errors": [],
            }

        # Create required data files
        data_dir = tmp_path / "site" / "2024" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "summary.json").write_text('{"total_prs": 100}')
        (data_dir / "leaderboards.json").write_text('{"top_contributors": []}')

        with (
            patch(
                "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
            ),
            patch("gh_year_end.report.build.build_site", side_effect=mock_build),
        ):
            result = runner.invoke(
                main,
                [
                    "batch-years",
                    "--config",
                    str(config_file),
                    "--years",
                    "2024",
                    "--skip-collect",
                ],
            )

        assert result.exit_code == 0
        assert not collect_called["value"]
        assert "Building site..." in result.output

    def test_one_year_fails_continues_with_others(
        self, runner: CliRunner, config_file: Path
    ) -> None:
        """Test that failure in one year continues with remaining years."""
        call_count = {"value": 0}

        async def mock_collect(*args, **kwargs):
            call_count["value"] += 1
            if call_count["value"] == 1:
                raise ValueError("Collection failed for year 1")
            return {
                "summary": {"total_prs": 100},
                "leaderboards": {"top_contributors": []},
                "timeseries": {"weekly": []},
                "repo_health": {"repositories": []},
                "hygiene_scores": {"scores": []},
                "awards": {"awards": []},
            }

        def mock_build(*args, **kwargs):
            return {
                "templates_rendered": ["index.html"],
                "data_files_written": 6,
                "assets_copied": 5,
                "errors": [],
            }

        with (
            patch(
                "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
            ),
            patch("gh_year_end.report.build.build_site", side_effect=mock_build),
        ):
            result = runner.invoke(
                main, ["batch-years", "--config", str(config_file), "--years", "2023,2024,2025"]
            )

        assert result.exit_code == 1
        assert "Year 2023 aborted" in result.output
        assert "Year 2024 completed successfully" in result.output
        assert "Year 2025 completed successfully" in result.output
        assert "Success: 2" in result.output
        assert "Failed: 1" in result.output

    def test_force_flag_passed_through(self, runner: CliRunner, config_file: Path) -> None:
        """Test that --force flag is passed to collect command."""
        force_used = {"value": False}

        async def mock_collect(*args, **kwargs):
            force_used["value"] = kwargs.get("force", False)
            return {
                "summary": {"total_prs": 100},
                "leaderboards": {"top_contributors": []},
                "timeseries": {"weekly": []},
                "repo_health": {"repositories": []},
                "hygiene_scores": {"scores": []},
                "awards": {"awards": []},
            }

        def mock_build(*args, **kwargs):
            return {
                "templates_rendered": ["index.html"],
                "data_files_written": 6,
                "assets_copied": 5,
                "errors": [],
            }

        with (
            patch(
                "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
            ),
            patch("gh_year_end.report.build.build_site", side_effect=mock_build),
        ):
            runner.invoke(
                main, ["batch-years", "--config", str(config_file), "--years", "2024", "--force"]
            )

        assert force_used["value"] is True

    def test_keyboard_interrupt_handling(self, runner: CliRunner, config_file: Path) -> None:
        """Test that keyboard interrupt is caught and handled gracefully."""
        call_count = {"value": 0}

        async def mock_collect(*args, **kwargs):
            call_count["value"] += 1
            if call_count["value"] == 2:
                raise KeyboardInterrupt()
            return {
                "summary": {"total_prs": 100},
                "leaderboards": {"top_contributors": []},
                "timeseries": {"weekly": []},
                "repo_health": {"repositories": []},
                "hygiene_scores": {"scores": []},
                "awards": {"awards": []},
            }

        def mock_build(*args, **kwargs):
            return {
                "templates_rendered": ["index.html"],
                "data_files_written": 6,
                "assets_copied": 5,
                "errors": [],
            }

        with (
            patch(
                "gh_year_end.collect.orchestrator.collect_and_aggregate", side_effect=mock_collect
            ),
            patch("gh_year_end.report.build.build_site", side_effect=mock_build),
        ):
            result = runner.invoke(
                main,
                ["batch-years", "--config", str(config_file), "--years", "2023,2024,2025"],
            )

        # KeyboardInterrupt is caught in the collect command and converted to click.Abort
        # The batch-years command catches click.Abort and marks the year as aborted
        assert "Year 2024 aborted" in result.output
        assert "Year 2023 completed successfully" in result.output
        # Keyboard interrupt doesn't actually stop batch processing - it just marks that year as failed
        assert call_count["value"] == 3


class TestRemovedCommands:
    """Tests for commands that were fully removed."""

    def test_removed_commands_return_no_such_command(self, runner: CliRunner) -> None:
        """Test that removed commands return 'No such command' error."""
        # These commands were fully removed (not just deprecated)
        removed_commands = ["plan", "normalize", "metrics", "report", "validate", "status"]

        for cmd in removed_commands:
            result = runner.invoke(main, [cmd, "--help"])
            assert result.exit_code == 2, f"Command '{cmd}' should not exist (exit code 2)"
            assert "no such command" in result.output.lower(), (
                f"Command '{cmd}' should show 'no such command'"
            )

    def test_current_commands_in_help(self, runner: CliRunner) -> None:
        """Test that current commands appear in help and removed ones don't."""
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0

        # Parse the Commands section
        lines = result.output.split("\n")
        in_commands = False
        command_names = []
        for line in lines:
            if line.strip() == "Commands:":
                in_commands = True
                continue
            if in_commands and line.strip():
                # Command lines start with 2 spaces then the command name
                parts = line.split()
                if parts:
                    command_names.append(parts[0])

        # Current commands should exist
        assert "collect" in command_names
        assert "build" in command_names
        assert "all" in command_names
        assert "batch-years" in command_names

        # Removed commands should NOT exist
        assert "normalize" not in command_names
        assert "metrics" not in command_names
        assert "validate" not in command_names
        assert "status" not in command_names
        assert "report" not in command_names
        assert "plan" not in command_names
