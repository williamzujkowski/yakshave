"""Live end-to-end pipeline tests for gh-year-end.

Tests the complete pipeline against live GitHub API:
- Plan command shows collection plan
- Collect command fetches data from GitHub
- Normalize command creates Parquet tables
- Metrics command calculates leaderboards and scores
- Report command generates static site
- All command runs full pipeline
- Data integrity chain across phases

These tests use live API and are marked with @pytest.mark.live_api.
Run with: pytest -m live_api

Uses session-scoped fixtures to minimize API calls.
"""

import json
from pathlib import Path
from typing import Any

import polars as pl
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
    """Collect data from GitHub API once for all tests.

    This fixture runs the collect command once and caches the result
    for all tests in the session.

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
def normalized_data(
    cli_runner: CliRunner, live_config_file: Path, collected_data: dict[str, Any]
) -> dict[str, Any]:
    """Normalize collected data once for all tests.

    Args:
        cli_runner: Click CLI runner.
        live_config_file: Path to config file.
        collected_data: Collection results (ensures collect runs first).

    Returns:
        Dictionary with normalization stats.
    """
    result = cli_runner.invoke(main, ["normalize", "--config", str(live_config_file)])

    if result.exit_code != 0:
        pytest.fail(f"Normalization failed: {result.output}")

    return {"exit_code": result.exit_code, "output": result.output}


@pytest.fixture(scope="session")
def metrics_data(
    cli_runner: CliRunner, live_config_file: Path, normalized_data: dict[str, Any]
) -> dict[str, Any]:
    """Calculate metrics once for all tests.

    Args:
        cli_runner: Click CLI runner.
        live_config_file: Path to config file.
        normalized_data: Normalization results (ensures normalize runs first).

    Returns:
        Dictionary with metrics stats.
    """
    result = cli_runner.invoke(main, ["metrics", "--config", str(live_config_file)])

    if result.exit_code != 0:
        pytest.fail(f"Metrics calculation failed: {result.output}")

    return {"exit_code": result.exit_code, "output": result.output}


@pytest.mark.live_api
class TestLivePipelineCLI:
    """Live end-to-end pipeline tests using real GitHub API."""

    def test_live_cli_plan_command(self, cli_runner: CliRunner, live_config_file: Path) -> None:
        """Test that 'gh-year-end plan' shows collection plan without making API calls."""
        result = cli_runner.invoke(main, ["plan", "--config", str(live_config_file)])

        assert result.exit_code == 0, f"Plan command failed: {result.output}"

        # Verify plan output contains expected sections
        assert "Collection Plan" in result.output
        assert "Target:" in result.output
        assert "Year:" in result.output
        assert "Since:" in result.output
        assert "Until:" in result.output
        assert "Storage root:" in result.output
        assert "Enabled collectors:" in result.output

        # Verify individual collector flags
        assert "PRs:" in result.output
        assert "Issues:" in result.output
        assert "Reviews:" in result.output
        assert "Comments:" in result.output
        assert "Commits:" in result.output
        assert "Hygiene:" in result.output

    def test_live_cli_collect_command(
        self, cli_runner: CliRunner, live_config_file: Path, live_data_root: Path
    ) -> None:
        """Test that 'gh-year-end collect' creates raw data files."""
        result = cli_runner.invoke(main, ["collect", "--config", str(live_config_file)])

        assert result.exit_code == 0, f"Collect command failed: {result.output}"

        # Verify output messages
        assert "Collecting data for" in result.output
        assert "Collection complete!" in result.output

        # Verify summary statistics are shown
        assert "Duration:" in result.output
        assert "Repos discovered:" in result.output
        assert "Repos processed:" in result.output

        # Verify raw data directory was created
        config = load_config(live_config_file)
        year = config.github.windows.year
        target_name = config.github.target.name

        raw_root = (
            live_data_root / "raw" / f"year={year}" / "source=github" / f"target={target_name}"
        )
        assert raw_root.exists(), f"Raw data directory should exist: {raw_root}"

        # Verify repos.jsonl was created
        repos_file = raw_root / "repos.jsonl"
        assert repos_file.exists(), "repos.jsonl should be created"

        # Verify manifest.json was created
        manifest_file = live_data_root / "raw" / f"year={year}" / "manifest.json"
        assert manifest_file.exists(), "manifest.json should be created"

        # Verify manifest contains expected fields
        manifest = json.loads(manifest_file.read_text())
        assert "start_time" in manifest
        assert "end_time" in manifest
        assert "stats" in manifest

    def test_live_cli_normalize_command(
        self,
        cli_runner: CliRunner,
        live_config_file: Path,
        collected_data: dict[str, Any],  # noqa: ARG002 - ensures collect runs first
        live_data_root: Path,
    ) -> None:
        """Test that 'gh-year-end normalize' creates Parquet files."""
        result = cli_runner.invoke(main, ["normalize", "--config", str(live_config_file)])

        assert result.exit_code == 0, f"Normalize command failed: {result.output}"

        # Verify output messages
        assert "Normalizing data for year" in result.output
        assert "Normalization complete!" in result.output

        # Verify summary statistics
        assert "Duration:" in result.output
        assert "Tables written:" in result.output
        assert "Total rows:" in result.output

        # Verify curated directory was created
        config = load_config(live_config_file)
        year = config.github.windows.year
        target_name = config.github.target.name

        curated_root = (
            live_data_root / "curated" / f"year={year}" / "source=github" / f"target={target_name}"
        )
        assert curated_root.exists(), f"Curated directory should exist: {curated_root}"

        # Verify dimension tables exist
        assert (curated_root / "dim_user.parquet").exists(), "dim_user.parquet should exist"
        assert (curated_root / "dim_repo.parquet").exists(), "dim_repo.parquet should exist"
        assert (curated_root / "dim_identity_rule.parquet").exists(), (
            "dim_identity_rule.parquet should exist"
        )

        # Verify at least some fact tables exist (not all may be present depending on data)
        fact_tables = list(curated_root.glob("fact_*.parquet"))
        assert len(fact_tables) > 0, "At least one fact table should be created"

    def test_live_cli_metrics_command(
        self,
        cli_runner: CliRunner,
        live_config_file: Path,
        normalized_data: dict[str, Any],  # noqa: ARG002 - ensures normalize runs first
        live_data_root: Path,
    ) -> None:
        """Test that 'gh-year-end metrics' creates metrics files."""
        result = cli_runner.invoke(main, ["metrics", "--config", str(live_config_file)])

        assert result.exit_code == 0, f"Metrics command failed: {result.output}"

        # Verify output messages
        assert "Computing metrics for year" in result.output
        assert "Metrics calculation complete!" in result.output

        # Verify summary statistics
        assert "Duration:" in result.output
        assert "Metrics calculated:" in result.output
        assert "Total rows:" in result.output

        # Verify metrics directory was created
        config = load_config(live_config_file)
        year = config.github.windows.year

        metrics_root = live_data_root / "metrics" / f"year={year}"
        assert metrics_root.exists(), f"Metrics directory should exist: {metrics_root}"

        # Verify at least leaderboard metrics exist (other metrics may not be implemented yet)
        metrics_files = list(metrics_root.glob("*.parquet"))
        assert len(metrics_files) > 0, "At least one metrics file should be created"

        # Verify leaderboard file specifically
        leaderboard_file = metrics_root / "metrics_leaderboard.parquet"
        if leaderboard_file.exists():
            df = pl.read_parquet(leaderboard_file)
            assert len(df) > 0, "Leaderboard should have entries"
            assert "year" in df.columns
            assert "metric_key" in df.columns
            assert "user_id" in df.columns
            assert "value" in df.columns
            assert "rank" in df.columns

    def test_live_cli_report_command(
        self,
        cli_runner: CliRunner,
        live_config_file: Path,
        metrics_data: dict[str, Any],  # noqa: ARG002 - ensures metrics runs first
    ) -> None:
        """Test that 'gh-year-end report' generates static site."""
        result = cli_runner.invoke(main, ["report", "--config", str(live_config_file)])

        assert result.exit_code == 0, f"Report command failed: {result.output}"

        # Verify output messages
        assert "Generating report for year" in result.output
        assert "Report generation complete!" in result.output

        # Verify export and build sections
        assert "Exporting metrics to JSON..." in result.output
        assert "Building static site..." in result.output

        # Verify summary statistics
        assert "Duration:" in result.output
        assert "Tables exported:" in result.output

        # Verify serve hint is shown
        assert "To view the report:" in result.output
        assert "python -m http.server" in result.output

        # Verify site directory was created
        config = load_config(live_config_file)
        site_root = Path(config.report.output_dir)
        assert site_root.exists(), f"Site directory should exist: {site_root}"

        # Verify data directory exists with JSON files
        site_data_path = site_root / "data"
        assert site_data_path.exists(), "Site data directory should exist"

        json_files = list(site_data_path.glob("*.json"))
        assert len(json_files) > 0, "At least one JSON data file should be created"

        # Verify JSON files are valid
        for json_file in json_files:
            data = json.loads(json_file.read_text())
            assert isinstance(data, (dict, list)), f"{json_file.name} should contain valid JSON"

    def test_live_cli_all_command(
        self, cli_runner: CliRunner, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Test that 'gh-year-end all' runs full pipeline from scratch."""
        if not LIVE_CONFIG_FILE.exists():
            pytest.skip(f"Live config not found: {LIVE_CONFIG_FILE}")

        # Create separate temp directory for 'all' command test
        temp_dir = tmp_path_factory.mktemp("live_all_test")

        # Create config with temp directory
        config = load_config(LIVE_CONFIG_FILE)
        config.storage.root = str(temp_dir / "data")
        config.report.output_dir = str(temp_dir / "site")

        temp_config_file = temp_dir / "config.yaml"
        import yaml

        temp_config_file.write_text(yaml.dump(config.model_dump()))

        # Run 'all' command
        result = cli_runner.invoke(main, ["all", "--config", str(temp_config_file)])

        assert result.exit_code == 0, f"All command failed: {result.output}"

        # Verify all phases are present in output
        assert "Running complete pipeline" in result.output
        assert "Collecting data for" in result.output
        assert "Collection complete!" in result.output
        assert "Normalizing data for year" in result.output
        assert "Normalization complete!" in result.output
        assert "Computing metrics for year" in result.output
        assert "Metrics calculation complete!" in result.output
        assert "Generating report for year" in result.output
        assert "Report generation complete!" in result.output
        assert "Pipeline complete!" in result.output

        # Verify all output directories exist
        data_root = Path(config.storage.root)
        site_root = Path(config.report.output_dir)

        assert data_root.exists(), "Data directory should exist"
        assert (data_root / "raw").exists(), "Raw data directory should exist"
        assert (data_root / "curated").exists(), "Curated data directory should exist"
        assert (data_root / "metrics").exists(), "Metrics directory should exist"
        assert site_root.exists(), "Site directory should exist"

    def test_live_full_pipeline(
        self,
        collected_data: dict[str, Any],
        normalized_data: dict[str, Any],
        metrics_data: dict[str, Any],
        live_config_file: Path,
        cli_runner: CliRunner,
    ) -> None:
        """Test complete collect → normalize → metrics → report flow.

        This test verifies the full pipeline execution by checking that
        all fixtures run successfully and produce expected outputs.
        """
        # All fixtures should have run successfully (verified by their own asserts)
        assert collected_data["exit_code"] == 0
        assert normalized_data["exit_code"] == 0
        assert metrics_data["exit_code"] == 0

        # Run report command to complete the pipeline
        result = cli_runner.invoke(main, ["report", "--config", str(live_config_file)])
        assert result.exit_code == 0, f"Report generation failed: {result.output}"

        # Verify full pipeline output structure
        config = load_config(live_config_file)
        data_root = Path(config.storage.root)
        site_root = Path(config.report.output_dir)

        # Verify complete directory structure
        assert (data_root / "raw").exists()
        assert (data_root / "curated").exists()
        assert (data_root / "metrics").exists()
        assert site_root.exists()
        assert (site_root / "data").exists()

    def test_live_data_integrity_chain(
        self,
        live_config_file: Path,
        collected_data: dict[str, Any],  # noqa: ARG002 - ensures pipeline runs
        normalized_data: dict[str, Any],  # noqa: ARG002 - ensures pipeline runs
        metrics_data: dict[str, Any],  # noqa: ARG002 - ensures pipeline runs
    ) -> None:
        """Test that row counts are consistent across pipeline phases.

        Verifies data integrity by checking:
        1. Repos count matches between raw and normalized
        2. Users extracted from normalized data
        3. Fact tables reference valid dimension keys
        4. Metrics reference valid users
        5. No unexpected data loss between phases
        """
        config = load_config(live_config_file)
        data_root = Path(config.storage.root)
        year = config.github.windows.year
        target_name = config.github.target.name

        # Get paths
        raw_root = data_root / "raw" / f"year={year}" / "source=github" / f"target={target_name}"
        curated_root = (
            data_root / "curated" / f"year={year}" / "source=github" / f"target={target_name}"
        )
        metrics_root = data_root / "metrics" / f"year={year}"

        # Count raw repos
        repos_file = raw_root / "repos.jsonl"
        with repos_file.open() as f:
            raw_repos_count = sum(1 for _ in f)

        # Count normalized repos
        dim_repo = pl.read_parquet(curated_root / "dim_repo.parquet")
        normalized_repos_count = len(dim_repo)

        # Verify repo counts match
        assert raw_repos_count == normalized_repos_count, (
            f"Repo count mismatch: raw={raw_repos_count}, normalized={normalized_repos_count}"
        )

        # Verify users were extracted
        dim_user = pl.read_parquet(curated_root / "dim_user.parquet")
        assert len(dim_user) > 0, "Should have extracted users"

        # Verify bot detection worked
        assert "is_bot" in dim_user.columns
        bots = dim_user.filter(pl.col("is_bot") == True)  # noqa: E712
        humans = dim_user.filter(pl.col("is_bot") == False)  # noqa: E712
        assert len(humans) > 0, "Should have human users"

        # Verify fact tables if they exist
        fact_pr_path = curated_root / "fact_pull_request.parquet"
        if fact_pr_path.exists():
            fact_pr = pl.read_parquet(fact_pr_path)
            assert len(fact_pr) > 0, "Should have pull requests"

            # Verify foreign keys are valid
            if "repo_id" in fact_pr.columns:
                pr_repo_ids = set(fact_pr["repo_id"].unique().to_list())
                repo_ids = set(dim_repo["repo_id"].to_list())
                invalid_repo_ids = pr_repo_ids - repo_ids
                assert len(invalid_repo_ids) == 0, f"Invalid repo_ids in PRs: {invalid_repo_ids}"

            if "author_id" in fact_pr.columns:
                pr_author_ids = set(fact_pr["author_id"].unique().to_list())
                user_ids = set(dim_user["user_id"].to_list())
                invalid_author_ids = pr_author_ids - user_ids
                assert len(invalid_author_ids) == 0, (
                    f"Invalid author_ids in PRs: {invalid_author_ids}"
                )

        # Verify metrics reference valid users
        leaderboard_path = metrics_root / "metrics_leaderboard.parquet"
        if leaderboard_path.exists():
            leaderboard = pl.read_parquet(leaderboard_path)
            assert len(leaderboard) > 0, "Should have leaderboard entries"

            if "user_id" in leaderboard.columns:
                leaderboard_user_ids = set(leaderboard["user_id"].unique().to_list())
                user_ids = set(dim_user["user_id"].to_list())
                invalid_user_ids = leaderboard_user_ids - user_ids
                assert len(invalid_user_ids) == 0, (
                    f"Invalid user_ids in leaderboard: {invalid_user_ids}"
                )

                # Verify bots are filtered from leaderboard if humans_only is enabled
                if config.identity.humans_only:
                    bot_ids = set(bots["user_id"].to_list())
                    bots_in_leaderboard = leaderboard_user_ids & bot_ids
                    assert len(bots_in_leaderboard) == 0, (
                        f"Bots should not appear in leaderboard: {bots_in_leaderboard}"
                    )

        # Print summary for debugging
        print("\nData Integrity Summary:")
        print(f"  Repos (raw): {raw_repos_count}")
        print(f"  Repos (normalized): {normalized_repos_count}")
        print(f"  Users (total): {len(dim_user)}")
        print(f"  Users (humans): {len(humans)}")
        print(f"  Users (bots): {len(bots)}")
        if fact_pr_path.exists():
            print(f"  Pull Requests: {len(fact_pr)}")
        if leaderboard_path.exists():
            print(f"  Leaderboard Entries: {len(leaderboard)}")

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

    def test_live_normalize_fails_without_raw_data(
        self, cli_runner: CliRunner, tmp_path: Path
    ) -> None:
        """Test that normalize fails gracefully if raw data is missing."""
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

report:
  output_dir: {tmp_path / "site"}
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)

        result = cli_runner.invoke(main, ["normalize", "--config", str(config_file)])

        assert result.exit_code != 0
        assert "No raw data found" in result.output

    def test_live_metrics_fails_without_curated_data(
        self, cli_runner: CliRunner, tmp_path: Path
    ) -> None:
        """Test that metrics fails gracefully if curated data is missing."""
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

report:
  output_dir: {tmp_path / "site"}
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)

        result = cli_runner.invoke(main, ["metrics", "--config", str(config_file)])

        assert result.exit_code != 0
        assert "No curated data found" in result.output

    def test_live_report_fails_without_metrics_data(
        self, cli_runner: CliRunner, tmp_path: Path
    ) -> None:
        """Test that report fails gracefully if metrics data is missing."""
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

report:
  output_dir: {tmp_path / "site"}
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)

        result = cli_runner.invoke(main, ["report", "--config", str(config_file)])

        assert result.exit_code != 0
        assert "No metrics data found" in result.output
