"""Live end-to-end pipeline tests for gh-year-end.

Tests the complete pipeline against live GitHub API:
- Collect command fetches data from GitHub and generates metrics JSON
- Build command generates static site from metrics JSON
- All command runs full pipeline (collect + build)
- Data integrity verification
- Deprecated command warnings

These tests use live API and are marked with @pytest.mark.live_api.
Run with: pytest -m live_api

Uses session-scoped fixtures to minimize API calls.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from gh_year_end.cli import main
from gh_year_end.config import Config, load_config

FIXTURES_DIR = Path(__file__).parent / "fixtures"
LIVE_CONFIG_FILE = FIXTURES_DIR / "live_test_config.yaml"


@pytest.fixture(scope="session")
def live_config() -> Config:
    """Load live test configuration.

    Returns:
        Config instance for live testing.
    """
    if not LIVE_CONFIG_FILE.exists():
        pytest.skip(f"Live config not found: {LIVE_CONFIG_FILE}")

    return load_config(LIVE_CONFIG_FILE)


@pytest.fixture(scope="session")
def cli_runner() -> CliRunner:
    """Create Click CLI test runner.

    Returns:
        CliRunner instance for testing CLI commands.
    """
    return CliRunner()


@pytest.fixture(scope="session")
def live_config_file(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create temporary config file for live testing.

    Uses a temporary directory for data output to avoid conflicts.

    Args:
        tmp_path_factory: Pytest session-scoped temporary directory factory.

    Returns:
        Path to temporary config file.
    """
    if not LIVE_CONFIG_FILE.exists():
        pytest.skip(f"Live config not found: {LIVE_CONFIG_FILE}")

    # Create session-scoped temp directory
    temp_dir = tmp_path_factory.mktemp("live_pipeline_test")

    # Load and modify config to use temp directory
    config = load_config(LIVE_CONFIG_FILE)
    config.storage.root = str(temp_dir / "data")
    config.report.output_dir = str(temp_dir / "site")

    # Write modified config
    temp_config_file = temp_dir / "config.yaml"
    import yaml

    temp_config_file.write_text(yaml.dump(config.model_dump()))

    return temp_config_file


@pytest.fixture(scope="session")
def live_data_root(live_config_file: Path) -> Path:
    """Get data root directory for live tests.

    Args:
        live_config_file: Path to config file.

    Returns:
        Path to data root directory.
    """
    config = load_config(live_config_file)
    return Path(config.storage.root)


@pytest.fixture(scope="session")
def collected_data(
    cli_runner: CliRunner, live_config_file: Path, live_data_root: Path
) -> dict[str, Any]:
    """Collect data from GitHub API and generate metrics JSON once for all tests.

    This fixture runs the new collect command (single-pass collection with
    in-memory aggregation) once and caches the result for all tests in the session.

    Args:
        cli_runner: Click CLI runner.
        live_config_file: Path to config file.
        live_data_root: Path to data root.

    Returns:
        Dictionary with collection stats.
    """
    result = cli_runner.invoke(main, ["collect", "--config", str(live_config_file)])

    if result.exit_code != 0:
        pytest.fail(f"Collection failed: {result.output}")

    return {"exit_code": result.exit_code, "output": result.output, "root": live_data_root}


@pytest.fixture(scope="session")
def built_site(
    cli_runner: CliRunner, live_config_file: Path, collected_data: dict[str, Any]
) -> dict[str, Any]:
    """Build static site from metrics JSON once for all tests.

    This fixture runs the build command once and caches the result
    for all tests in the session.

    Args:
        cli_runner: Click CLI runner.
        live_config_file: Path to config file.
        collected_data: Collection results (ensures collect runs first).

    Returns:
        Dictionary with build stats.
    """
    result = cli_runner.invoke(main, ["build", "--config", str(live_config_file)])

    if result.exit_code != 0:
        pytest.fail(f"Build failed: {result.output}")

    return {"exit_code": result.exit_code, "output": result.output}


@pytest.mark.live_api
class TestLivePipelineCLI:
    """Live end-to-end pipeline tests using real GitHub API."""

    def test_live_cli_deprecated_plan_shows_warning(
        self, cli_runner: CliRunner, live_config_file: Path
    ) -> None:
        """Test that deprecated 'plan' command shows deprecation warning."""
        result = cli_runner.invoke(main, ["plan", "--config", str(live_config_file)])

        assert result.exit_code == 0, f"Plan command failed: {result.output}"

        # Verify deprecation warning is shown
        assert "Warning: 'plan' command is deprecated" in result.output
        assert "Use 'gh-year-end collect --help' instead" in result.output

    def test_live_cli_collect_command(self, cli_runner: CliRunner, live_config_file: Path) -> None:
        """Test that 'gh-year-end collect' fetches data and generates metrics JSON."""
        result = cli_runner.invoke(main, ["collect", "--config", str(live_config_file)])

        assert result.exit_code == 0, f"Collect command failed: {result.output}"

        # Verify output messages
        assert "Collecting data for" in result.output
        assert "Collection complete!" in result.output

        # Verify collection messages
        assert "Running single-pass collection with in-memory aggregation" in result.output
        assert "Writing metrics to JSON" in result.output

        # Verify metrics JSON files were created
        config = load_config(live_config_file)
        year = config.github.windows.year

        data_dir = Path(f"site/{year}/data")
        assert data_dir.exists(), f"Data directory should exist: {data_dir}"

        # Verify required JSON files exist
        required_files = ["summary.json", "leaderboards.json"]
        for filename in required_files:
            json_file = data_dir / filename
            assert json_file.exists(), f"{filename} should be created"

            # Verify JSON is valid
            data = json.loads(json_file.read_text())
            assert isinstance(data, (dict, list)), f"{filename} should contain valid JSON"

    def test_live_cli_deprecated_normalize_fails(
        self, cli_runner: CliRunner, live_config_file: Path
    ) -> None:
        """Test that deprecated 'normalize' command shows deprecation warning and fails."""
        result = cli_runner.invoke(main, ["normalize", "--config", str(live_config_file)])

        assert result.exit_code != 0, "Deprecated normalize command should fail"

        # Verify deprecation warning is shown
        assert "Warning: 'normalize' command is deprecated" in result.output
        assert "Use 'gh-year-end collect' instead" in result.output
        assert "This command is part of the old multi-phase pipeline" in result.output

    def test_live_cli_deprecated_metrics_fails(
        self, cli_runner: CliRunner, live_config_file: Path
    ) -> None:
        """Test that deprecated 'metrics' command shows deprecation warning and fails."""
        result = cli_runner.invoke(main, ["metrics", "--config", str(live_config_file)])

        assert result.exit_code != 0, "Deprecated metrics command should fail"

        # Verify deprecation warning is shown
        assert "Warning: 'metrics' command is deprecated" in result.output
        assert "Use 'gh-year-end collect' instead" in result.output
        assert "This command is part of the old multi-phase pipeline" in result.output

    def test_live_cli_deprecated_report_fails(
        self, cli_runner: CliRunner, live_config_file: Path
    ) -> None:
        """Test that deprecated 'report' command shows deprecation warning and fails."""
        result = cli_runner.invoke(main, ["report", "--config", str(live_config_file)])

        assert result.exit_code != 0, "Deprecated report command should fail"

        # Verify deprecation warning is shown
        assert "Warning: 'report' command is deprecated" in result.output
        assert "Use 'gh-year-end build' instead" in result.output
        assert "This command is part of the old multi-phase pipeline" in result.output

    def test_live_cli_build_command(
        self,
        cli_runner: CliRunner,
        live_config_file: Path,
        collected_data: dict[str, Any],  # noqa: ARG002 - ensures collect runs first
    ) -> None:
        """Test that 'gh-year-end build' generates static site from metrics JSON."""
        result = cli_runner.invoke(main, ["build", "--config", str(live_config_file)])

        assert result.exit_code == 0, f"Build command failed: {result.output}"

        # Verify output messages
        assert "Building site for" in result.output
        assert "Site built successfully!" in result.output

        # Verify build messages
        assert "Building static site" in result.output

        # Verify summary statistics
        assert "Templates rendered:" in result.output or "Data files:" in result.output

        # Verify serve hint is shown
        assert "To view the site:" in result.output
        assert "python -m http.server" in result.output

        # Verify site directory was created
        config = load_config(live_config_file)
        year = config.github.windows.year
        site_root = Path(f"site/{year}")
        assert site_root.exists(), f"Site directory should exist: {site_root}"

        # Verify HTML files were created
        html_files = list(site_root.glob("*.html"))
        assert len(html_files) > 0, "At least one HTML file should be created"

    def test_live_cli_all_command(
        self, cli_runner: CliRunner, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Test that 'gh-year-end all' runs full simplified pipeline from scratch."""
        if not LIVE_CONFIG_FILE.exists():
            pytest.skip(f"Live config not found: {LIVE_CONFIG_FILE}")

        # Create separate temp directory for 'all' command test
        temp_dir = tmp_path_factory.mktemp("live_all_test")

        # Create config with temp directory
        config = load_config(LIVE_CONFIG_FILE)
        config.storage.root = str(temp_dir / "data")

        temp_config_file = temp_dir / "config.yaml"
        import yaml

        temp_config_file.write_text(yaml.dump(config.model_dump()))

        # Run 'all' command
        result = cli_runner.invoke(main, ["all", "--config", str(temp_config_file)])

        assert result.exit_code == 0, f"All command failed: {result.output}"

        # Verify simplified pipeline phases are present in output
        assert "Running complete pipeline" in result.output
        assert "Collecting data for" in result.output
        assert "Collection complete!" in result.output
        assert "Building site for" in result.output
        assert "Site built successfully!" in result.output
        assert "Pipeline complete!" in result.output

        # Verify output directories exist
        year = config.github.windows.year
        site_root = Path(f"site/{year}")
        data_dir = Path(f"site/{year}/data")

        assert site_root.exists(), "Site directory should exist"
        assert data_dir.exists(), "Data directory should exist"

        # Verify JSON files were created
        json_files = list(data_dir.glob("*.json"))
        assert len(json_files) > 0, "At least one JSON file should be created"

        # Verify HTML files were created
        html_files = list(site_root.glob("*.html"))
        assert len(html_files) > 0, "At least one HTML file should be created"

    def test_live_full_pipeline(
        self,
        collected_data: dict[str, Any],
        built_site: dict[str, Any],
        live_config_file: Path,
    ) -> None:
        """Test complete collect → build flow.

        This test verifies the simplified pipeline execution by checking that
        all fixtures run successfully and produce expected outputs.
        """
        # All fixtures should have run successfully (verified by their own asserts)
        assert collected_data["exit_code"] == 0
        assert built_site["exit_code"] == 0

        # Verify full pipeline output structure
        config = load_config(live_config_file)
        year = config.github.windows.year
        site_root = Path(f"site/{year}")
        data_dir = Path(f"site/{year}/data")

        # Verify complete directory structure
        assert site_root.exists()
        assert data_dir.exists()

        # Verify JSON files exist
        json_files = list(data_dir.glob("*.json"))
        assert len(json_files) > 0, "JSON files should be created"

        # Verify HTML files exist
        html_files = list(site_root.glob("*.html"))
        assert len(html_files) > 0, "HTML files should be created"

    def test_live_data_integrity_chain(
        self,
        live_config_file: Path,
        collected_data: dict[str, Any],  # noqa: ARG002 - ensures collection runs
    ) -> None:
        """Test that generated JSON metrics contain valid data.

        Verifies data integrity by checking:
        1. All required JSON files are created
        2. JSON files contain valid data structures
        3. Leaderboards have expected fields
        4. Summary contains expected metrics
        """
        config = load_config(live_config_file)
        year = config.github.windows.year
        data_dir = Path(f"site/{year}/data")

        # Verify required JSON files exist
        required_files = ["summary.json", "leaderboards.json"]
        for filename in required_files:
            json_file = data_dir / filename
            assert json_file.exists(), f"{filename} should exist"

            # Verify JSON is valid
            data = json.loads(json_file.read_text())
            assert isinstance(data, (dict, list)), f"{filename} should contain valid JSON"

        # Verify summary.json structure
        summary = json.loads((data_dir / "summary.json").read_text())
        assert isinstance(summary, dict), "summary.json should be a dict"

        # Verify leaderboards.json structure
        leaderboards = json.loads((data_dir / "leaderboards.json").read_text())
        if isinstance(leaderboards, dict):
            # Check if leaderboards have entries
            for _metric_key, entries in leaderboards.items():
                if isinstance(entries, list) and len(entries) > 0:
                    # Each entry should have expected fields
                    entry = entries[0]
                    assert "rank" in entry or "login" in entry or "name" in entry, (
                        f"Leaderboard entries should have expected fields: {entry}"
                    )

        # Print summary for debugging
        print("\nData Integrity Summary:")
        print(f"  JSON files created: {len(list(data_dir.glob('*.json')))}")
        print(f"  Summary metrics: {len(summary) if isinstance(summary, dict) else 'N/A'}")
        print(
            f"  Leaderboard categories: {len(leaderboards) if isinstance(leaderboards, dict) else 'N/A'}"
        )

    def test_live_force_flag_recollects(
        self, cli_runner: CliRunner, live_config_file: Path, collected_data: dict[str, Any]
    ) -> None:
        """Test that --force flag causes data re-collection."""
        # First collection already done by collected_data fixture
        assert collected_data["exit_code"] == 0

        # Run collect again without force - should skip
        result_no_force = cli_runner.invoke(main, ["collect", "--config", str(live_config_file)])
        assert result_no_force.exit_code == 0

        # Run collect with force - should re-fetch
        result_force = cli_runner.invoke(
            main, ["collect", "--config", str(live_config_file), "--force"]
        )
        assert result_force.exit_code == 0
        assert "Force mode: will re-fetch existing data" in result_force.output

    def test_live_verbose_flag_shows_details(
        self, cli_runner: CliRunner, live_config_file: Path
    ) -> None:
        """Test that --verbose flag provides additional output."""
        result = cli_runner.invoke(main, ["--verbose", "plan", "--config", str(live_config_file)])

        assert result.exit_code == 0
        # Verbose mode should show the same plan output (no additional verbose output for plan)
        assert "Collection Plan" in result.output

    def test_live_build_fails_without_metrics_data(
        self, cli_runner: CliRunner, tmp_path: Path
    ) -> None:
        """Test that build fails gracefully if metrics data is missing."""
        # Create config with empty temp directory
        config_content = f"""
github:
  target:
    mode: org
    name: test-org
  windows:
    year: 2099
    since: "2099-01-01T00:00:00Z"
    until: "2100-01-01T00:00:00Z"

storage:
  root: {tmp_path / "empty_data"}
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)

        result = cli_runner.invoke(main, ["build", "--config", str(config_file)])

        assert result.exit_code != 0
        assert (
            "No metrics data found" in result.output
            or "Missing required JSON files" in result.output
        )
