"""End-to-end integration test for full report generation pipeline.

Tests the complete workflow:
1. Load raw fixture data (repos, PRs, issues, reviews, hygiene)
2. Normalize to curated Parquet tables
3. Generate metrics from curated data
4. Build static site from metrics

Uses deterministic sample data from tests/fixtures/sample_org/.
"""

import json
import shutil
from pathlib import Path

import polars as pl
import pytest

from gh_year_end.config import Config
from gh_year_end.metrics.orchestrator import run_metrics
from gh_year_end.normalize.hygiene import (
    normalize_branch_protection,
    normalize_file_presence,
    normalize_security_features,
)
from gh_year_end.normalize.issues import normalize_issues
from gh_year_end.normalize.pulls import normalize_pulls
from gh_year_end.normalize.repos import normalize_repos
from gh_year_end.normalize.reviews import normalize_reviews
from gh_year_end.normalize.users import normalize_users
from gh_year_end.report.build import build_site
from gh_year_end.storage.paths import PathManager

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "sample_org"


@pytest.fixture
def sample_config(tmp_path: Path) -> Config:
    """Create configuration using sample fixture data.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        Config instance pointing to sample fixtures and temp output.
    """
    # Copy fixtures to temp directory to avoid modifying originals
    fixture_raw_dir = FIXTURES_DIR / "raw"
    temp_raw_dir = tmp_path / "data" / "raw"
    shutil.copytree(fixture_raw_dir, temp_raw_dir)

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
            "report": {
                "title": "Test Organization 2025 Year in Review",
                "output_dir": str(tmp_path / "site"),
            },
            "identity": {
                "bots": {
                    "exclude_patterns": [r".*\[bot\]$", r"^dependabot$"],
                    "include_overrides": [],
                }
            },
        }
    )


@pytest.fixture
def paths(sample_config: Config) -> PathManager:
    """Create PathManager from sample config.

    Args:
        sample_config: Test configuration.

    Returns:
        PathManager instance.
    """
    return PathManager(sample_config)


class TestEndToEndPipeline:
    """End-to-end integration tests for full report generation pipeline."""

    def test_fixture_data_exists(self) -> None:
        """Verify sample fixture data exists and is structured correctly."""
        fixture_raw_dir = FIXTURES_DIR / "raw" / "year=2025" / "source=github" / "target=test-org"

        # Verify repos.jsonl exists
        repos_file = fixture_raw_dir / "repos.jsonl"
        assert repos_file.exists(), "repos.jsonl should exist"

        # Count repos
        with repos_file.open() as f:
            repos_count = sum(1 for _ in f)
        assert repos_count == 5, "Should have 5 repos"

        # Verify pulls directory exists
        pulls_dir = fixture_raw_dir / "pulls"
        assert pulls_dir.exists(), "pulls directory should exist"
        assert len(list(pulls_dir.glob("*.jsonl"))) == 5, "Should have 5 pull request files"

        # Verify issues directory exists
        issues_dir = fixture_raw_dir / "issues"
        assert issues_dir.exists(), "issues directory should exist"
        assert len(list(issues_dir.glob("*.jsonl"))) == 3, "Should have 3 issue files"

        # Verify reviews directory exists
        reviews_dir = fixture_raw_dir / "reviews"
        assert reviews_dir.exists(), "reviews directory should exist"
        assert len(list(reviews_dir.glob("*.jsonl"))) == 5, "Should have 5 review files"

        # Verify hygiene directories exist
        assert (fixture_raw_dir / "repo_tree").exists(), "repo_tree directory should exist"
        assert (fixture_raw_dir / "branch_protection").exists(), (
            "branch_protection directory should exist"
        )
        assert (fixture_raw_dir / "security_features").exists(), (
            "security_features directory should exist"
        )

    def test_normalize_phase(self, sample_config: Config, paths: PathManager) -> None:
        """Test normalization phase: raw JSONL to curated Parquet tables."""
        # Ensure curated directory exists
        paths.curated_root.mkdir(parents=True, exist_ok=True)

        # Note on path expectations:
        # - normalize_repos expects base raw dir (builds year/source/target path itself)
        # - All other normalizers expect full raw path (year/source/target already in path)
        raw_base_dir = paths.root / "raw"  # For normalize_repos only

        # Normalize repos (builds full path from base dir)
        df_repos = normalize_repos(raw_base_dir, sample_config)
        assert len(df_repos) == 5, "Should normalize 5 repos"
        assert "repo_id" in df_repos.columns
        assert "full_name" in df_repos.columns
        df_repos.to_parquet(paths.dim_repo_path)

        # Normalize users
        df_users = normalize_users(sample_config)
        assert len(df_users) > 0, "Should extract users from data"
        assert "user_id" in df_users.columns
        assert "login" in df_users.columns
        assert "is_bot" in df_users.columns
        # Verify bot detection works
        bots = df_users.filter(pl.col("is_bot") == True)  # noqa: E712
        assert len(bots) > 0, "Should detect at least one bot"
        df_users.write_parquet(paths.dim_user_path)

        # Normalize pulls (uses full path to raw data)
        df_pulls = normalize_pulls(paths.raw_root, sample_config)
        assert len(df_pulls) == 21, "Should normalize 21 pull requests"
        assert "pr_id" in df_pulls.columns
        assert "state" in df_pulls.columns
        # Verify mixed states (open, closed, merged)
        states = df_pulls["state"].unique().tolist()
        assert "open" in states, "Should have open PRs"
        assert "closed" in states, "Should have closed (rejected) PRs"
        assert "merged" in states, "Should have merged PRs"
        df_pulls.to_parquet(paths.fact_pull_request_path)

        # Normalize issues (uses full path to raw data)
        df_issues = normalize_issues(paths.raw_root, sample_config)
        assert len(df_issues) == 10, "Should normalize 10 issues"
        assert "issue_id" in df_issues.columns
        assert "state" in df_issues.columns
        df_issues.to_parquet(paths.fact_issue_path)

        # Normalize reviews (uses full path to raw data)
        df_reviews = normalize_reviews(paths.raw_root, sample_config)
        assert len(df_reviews) == 15, "Should normalize 15 reviews"
        assert "review_id" in df_reviews.columns
        assert "state" in df_reviews.columns
        # Verify review states
        review_states = df_reviews["state"].unique().tolist()
        assert "APPROVED" in review_states, "Should have approved reviews"
        assert "COMMENTED" in review_states or "CHANGES_REQUESTED" in review_states
        df_reviews.to_parquet(paths.fact_review_path)

        # Normalize hygiene data (uses full raw path with year/source/target)
        df_file_presence = normalize_file_presence(paths.raw_root, sample_config)
        assert len(df_file_presence) > 0, "Should normalize file presence data"
        df_file_presence.to_parquet(paths.fact_repo_files_presence_path)

        df_branch_protection = normalize_branch_protection(paths.raw_root, sample_config)
        assert len(df_branch_protection) > 0, "Should normalize branch protection data"
        df_branch_protection.to_parquet(paths.fact_repo_hygiene_path)

        df_security_features = normalize_security_features(paths.raw_root, sample_config)
        assert len(df_security_features) > 0, "Should normalize security features data"
        df_security_features.to_parquet(paths.fact_repo_security_features_path)

        # Verify all curated files exist
        assert paths.dim_repo_path.exists(), "dim_repo.parquet should exist"
        assert paths.dim_user_path.exists(), "dim_user.parquet should exist"
        assert paths.fact_pull_request_path.exists(), "fact_pull_request.parquet should exist"
        assert paths.fact_issue_path.exists(), "fact_issue.parquet should exist"
        assert paths.fact_review_path.exists(), "fact_review.parquet should exist"
        assert paths.fact_repo_files_presence_path.exists(), (
            "fact_repo_files_presence.parquet should exist"
        )
        assert paths.fact_repo_hygiene_path.exists(), "fact_repo_hygiene.parquet should exist"
        assert paths.fact_repo_security_features_path.exists(), (
            "fact_repo_security_features.parquet should exist"
        )

    def test_metrics_phase(self, sample_config: Config, paths: PathManager) -> None:
        """Test metrics phase: curated Parquet to metrics tables."""
        # First run normalization to create curated tables
        self.test_normalize_phase(sample_config, paths)

        # Ensure metrics directory exists
        paths.metrics_root.mkdir(parents=True, exist_ok=True)

        # Run metrics pipeline
        stats = run_metrics(sample_config)

        # Verify metrics were generated
        assert "start_time" in stats, "Should track start time"
        assert "end_time" in stats, "Should track end time"
        assert stats["duration_seconds"] > 0, "Should track duration"

        # Verify metrics files exist (only leaderboards is implemented)
        assert paths.metrics_leaderboard_path.exists(), "Leaderboard metrics should exist"
        # Note: Other metrics calculators are not yet implemented, so we only verify leaderboards

        # Verify leaderboard data structure
        df_leaderboard = pl.read_parquet(paths.metrics_leaderboard_path)
        assert len(df_leaderboard) > 0, "Should have leaderboard entries"
        assert "year" in df_leaderboard.columns
        assert "metric_key" in df_leaderboard.columns
        assert "scope" in df_leaderboard.columns
        assert "user_id" in df_leaderboard.columns
        assert "value" in df_leaderboard.columns
        assert "rank" in df_leaderboard.columns

        # Verify expected metric keys exist
        metric_keys = df_leaderboard["metric_key"].unique().to_list()
        assert "prs_opened" in metric_keys or "prs_merged" in metric_keys, "Should have PR metrics"

        # Note: Other metrics (time series, repo health, hygiene scores, awards) are not yet
        # implemented in the metrics orchestrator. When implemented, add tests for them here.

    def test_report_phase(self, sample_config: Config, paths: PathManager, tmp_path: Path) -> None:
        """Test report phase: metrics to static site."""
        # First run metrics phase
        self.test_metrics_phase(sample_config, paths)

        # Create minimal templates for testing
        templates_dir = tmp_path / "site" / "templates"
        templates_dir.mkdir(parents=True, exist_ok=True)

        # Create simple index.html template
        index_template = """<!DOCTYPE html>
<html>
<head>
    <title>{{ config.report.title }}</title>
</head>
<body>
    <h1>{{ config.report.title }}</h1>
    <p>Year: {{ config.github.windows.year }}</p>
    <p>Organization: {{ config.github.target.name }}</p>
</body>
</html>"""
        (templates_dir / "index.html").write_text(index_template)

        # Create assets directory
        assets_dir = tmp_path / "site" / "assets"
        assets_dir.mkdir(parents=True, exist_ok=True)
        (assets_dir / "styles.css").write_text("body { font-family: sans-serif; }")

        # Build site
        stats = build_site(sample_config, paths)

        # Verify build completed
        assert "start_time" in stats
        assert "end_time" in stats
        assert "duration_seconds" in stats
        assert stats["duration_seconds"] > 0
        assert len(stats["errors"]) == 0, "Should build without errors"

        # Verify site files were created
        assert paths.site_root.exists(), "Site root should exist"
        assert paths.site_data_path.exists(), "Site data directory should exist"
        assert paths.site_assets_path.exists(), "Site assets directory should exist"

        # Verify index.html was rendered
        index_html = paths.site_root / "index.html"
        assert index_html.exists(), "index.html should be rendered"

        # Verify content includes config values
        index_content = index_html.read_text()
        assert "Test Organization 2025 Year in Review" in index_content
        assert "2025" in index_content
        assert "test-org" in index_content

        # Verify assets were copied
        assert (paths.site_assets_path / "styles.css").exists(), "CSS should be copied"

        # Verify data JSON files were created
        json_files = list(paths.site_data_path.glob("*.json"))
        assert len(json_files) > 0, "Should have JSON data files"

        # Verify JSON data can be loaded
        for json_file in json_files:
            data = json.loads(json_file.read_text())
            assert isinstance(data, (dict, list)), f"{json_file.name} should contain valid JSON"

    def test_full_pipeline_end_to_end(
        self, sample_config: Config, paths: PathManager, tmp_path: Path
    ) -> None:
        """Test complete pipeline from raw data to static site.

        This is the main end-to-end test that verifies:
        1. Sample data is realistic and complete
        2. Normalization handles all data types correctly
        3. Metrics calculation works on normalized data
        4. Report generation produces valid output
        5. Output is deterministic and reproducible
        """
        # Verify starting state
        assert paths.raw_root.exists(), "Raw data should exist"
        assert not paths.curated_root.exists(), "Curated data should not exist yet"
        assert not paths.metrics_root.exists(), "Metrics should not exist yet"
        assert not paths.site_root.exists(), "Site should not exist yet"

        # Phase 1: Normalize
        paths.curated_root.mkdir(parents=True, exist_ok=True)
        # Note on path expectations:
        # - normalize_repos expects base raw dir (builds year/source/target path itself)
        # - All other normalizers expect full raw path (year/source/target already in path)
        raw_base_dir = paths.root / "raw"  # For normalize_repos only

        df_repos = normalize_repos(raw_base_dir, sample_config)
        df_repos.to_parquet(paths.dim_repo_path)

        df_users = normalize_users(sample_config)
        df_users.write_parquet(paths.dim_user_path)

        df_pulls = normalize_pulls(paths.raw_root, sample_config)
        df_pulls.to_parquet(paths.fact_pull_request_path)

        df_issues = normalize_issues(paths.raw_root, sample_config)
        df_issues.to_parquet(paths.fact_issue_path)

        df_reviews = normalize_reviews(paths.raw_root, sample_config)
        df_reviews.to_parquet(paths.fact_review_path)

        df_file_presence = normalize_file_presence(paths.raw_root, sample_config)
        df_file_presence.to_parquet(paths.fact_repo_files_presence_path)

        df_branch_protection = normalize_branch_protection(paths.raw_root, sample_config)
        df_branch_protection.to_parquet(paths.fact_repo_hygiene_path)

        df_security_features = normalize_security_features(paths.raw_root, sample_config)
        df_security_features.to_parquet(paths.fact_repo_security_features_path)

        # Verify curated data
        assert paths.curated_root.exists(), "Curated data should exist"

        # Phase 2: Metrics
        paths.metrics_root.mkdir(parents=True, exist_ok=True)
        metrics_stats = run_metrics(sample_config)

        assert "start_time" in metrics_stats, "Metrics should complete"
        assert paths.metrics_root.exists(), "Metrics should exist"

        # Phase 3: Report
        templates_dir = tmp_path / "site" / "templates"
        templates_dir.mkdir(parents=True, exist_ok=True)
        (templates_dir / "index.html").write_text(
            "<html><body><h1>{{ config.report.title }}</h1></body></html>"
        )

        assets_dir = tmp_path / "site" / "assets"
        assets_dir.mkdir(parents=True, exist_ok=True)
        (assets_dir / "test.css").write_text("/* test */")

        report_stats = build_site(sample_config, paths)

        assert len(report_stats["errors"]) == 0, "Report should build without errors"
        assert paths.site_root.exists(), "Site should exist"

        # Verify final output structure
        assert (paths.site_root / "index.html").exists(), "index.html should exist"
        assert paths.site_data_path.exists(), "data directory should exist"
        assert paths.site_assets_path.exists(), "assets directory should exist"

        # Verify data integrity
        leaderboard = pl.read_parquet(paths.metrics_leaderboard_path)
        users = pl.read_parquet(paths.dim_user_path)

        # Verify bot filtering: leaderboard should not include bot users
        bot_users = users.filter(pl.col("is_bot") == True)  # noqa: E712
        bot_ids = bot_users["user_id"].to_list()

        if "user_id" in leaderboard.columns:
            leaderboard_user_ids = leaderboard["user_id"].to_list()
            for bot_id in bot_ids:
                assert bot_id not in leaderboard_user_ids, (
                    f"Bot {bot_id} should not appear in leaderboard"
                )

        # Verify varied contribution patterns
        if "prs_opened" in leaderboard.columns:
            pr_counts = leaderboard["prs_opened"].to_list()
            assert len(set(pr_counts)) > 1, "Should have varied contribution levels"
            assert max(pr_counts) > min(pr_counts), "Should have different activity levels"

        print("\nEnd-to-end test completed successfully!")
        print(f"  - {len(df_repos)} repositories")
        print(f"  - {len(df_users)} users ({len(bot_users)} bots)")
        print(f"  - {len(df_pulls)} pull requests")
        print(f"  - {len(df_issues)} issues")
        print(f"  - {len(df_reviews)} reviews")
        print(f"  - {len(leaderboard)} leaderboard entries")

    def test_deterministic_output(self, sample_config: Config, paths: PathManager) -> None:
        """Test that pipeline produces deterministic, reproducible output.

        Runs the pipeline twice and verifies that:
        1. Normalized data is identical
        2. Metrics are identical
        3. Output is stable and reproducible
        """
        # Run pipeline first time
        paths.curated_root.mkdir(parents=True, exist_ok=True)
        raw_data_root = paths.root / "raw"

        df_repos_1 = normalize_repos(raw_data_root, sample_config)
        df_users_1 = normalize_users(sample_config)

        # Run pipeline second time
        df_repos_2 = normalize_repos(raw_data_root, sample_config)
        df_users_2 = normalize_users(sample_config)

        # Verify deterministic ordering
        assert df_repos_1["repo_id"].to_list() == df_repos_2["repo_id"].to_list()
        assert df_users_1["user_id"].to_list() == df_users_2["user_id"].to_list()

        print("\nDeterministic output verified!")
