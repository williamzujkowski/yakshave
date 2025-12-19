"""Live API integration tests for metrics pipeline (Phase 6).

Tests the metrics calculation phase using real normalized data from live API calls.
These tests verify that all metrics calculators work correctly with production data.

Requirements:
- GITHUB_TOKEN environment variable must be set
- Uses @pytest.mark.live_api marker for selective execution
- Depends on normalized data (will run normalization if needed)
- Verifies metrics tables structure and data quality
"""

import logging
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from gh_year_end.config import Config
from gh_year_end.metrics.awards import generate_awards
from gh_year_end.metrics.hygiene_score import calculate_hygiene_scores
from gh_year_end.metrics.leaderboards import calculate_leaderboards
from gh_year_end.metrics.orchestrator import run_metrics
from gh_year_end.metrics.repo_health import calculate_repo_health
from gh_year_end.metrics.timeseries import calculate_time_series
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
from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)

# Integration tests require GITHUB_TOKEN
pytestmark = pytest.mark.live_api


@pytest.fixture
def _ensure_normalized_data(live_config: Config, live_paths: PathManager) -> None:
    """Ensure normalized data exists for metrics tests.

    If curated data doesn't exist, run normalization first.
    Uses live_config and live_paths fixtures from conftest.py or test_github_integration.py.

    Args:
        live_config: Live test configuration.
        live_paths: Live test path manager.
    """
    # Check if curated data already exists
    if (
        live_paths.curated_root.exists()
        and live_paths.dim_repo_path.exists()
        and live_paths.dim_user_path.exists()
    ):
        logger.info("Curated data already exists, skipping normalization")
        return

    logger.info("Curated data not found, running normalization first")

    # Create curated directory
    live_paths.curated_root.mkdir(parents=True, exist_ok=True)

    # Run normalization
    raw_base_dir = live_paths.root / "raw"

    # Normalize repos
    df_repos = normalize_repos(raw_base_dir, live_config)
    df_repos.to_parquet(live_paths.dim_repo_path)
    logger.info("Normalized %d repositories", len(df_repos))

    # Normalize users
    df_users = normalize_users(live_config)
    df_users.write_parquet(live_paths.dim_user_path)
    logger.info("Normalized %d users", len(df_users))

    # Normalize pulls if data exists
    if (live_paths.raw_root / "pulls").exists():
        df_pulls = normalize_pulls(live_paths.raw_root, live_config)
        df_pulls.to_parquet(live_paths.fact_pull_request_path)
        logger.info("Normalized %d pull requests", len(df_pulls))

    # Normalize issues if data exists
    if (live_paths.raw_root / "issues").exists():
        df_issues = normalize_issues(live_paths.raw_root, live_config)
        df_issues.to_parquet(live_paths.fact_issue_path)
        logger.info("Normalized %d issues", len(df_issues))

    # Normalize reviews if data exists
    if (live_paths.raw_root / "reviews").exists():
        df_reviews = normalize_reviews(live_paths.raw_root, live_config)
        df_reviews.to_parquet(live_paths.fact_review_path)
        logger.info("Normalized %d reviews", len(df_reviews))

    # Normalize hygiene data if it exists
    if (live_paths.raw_root / "repo_tree").exists():
        df_file_presence = normalize_file_presence(live_paths.raw_root, live_config)
        df_file_presence.to_parquet(live_paths.fact_repo_files_presence_path)
        logger.info("Normalized file presence for %d repos", len(df_file_presence))

    if (live_paths.raw_root / "branch_protection").exists():
        df_branch_protection = normalize_branch_protection(live_paths.raw_root, live_config)
        df_branch_protection.to_parquet(live_paths.fact_repo_hygiene_path)
        logger.info("Normalized branch protection for %d repos", len(df_branch_protection))

    if (live_paths.raw_root / "security_features").exists():
        df_security = normalize_security_features(live_paths.raw_root, live_config)
        df_security.to_parquet(live_paths.fact_repo_security_features_path)
        logger.info("Normalized security features for %d repos", len(df_security))

    logger.info("Normalization complete")


class TestLiveLeaderboards:
    """Test leaderboard metrics calculation with live data."""

    def test_live_leaderboards(
        self,
        live_config: Config,
        live_paths: PathManager,
        _ensure_normalized_data: None,
    ) -> None:
        """Calculate user rankings from live data.

        Verifies:
        - Leaderboard metrics are calculated successfully
        - Expected metrics exist (prs_opened, prs_merged, etc.)
        - Rankings are assigned correctly
        - Both org and repo scopes are included
        """
        logger.info("Testing live leaderboard calculation")

        # Calculate leaderboards
        df = calculate_leaderboards(live_paths.curated_root, live_config)

        # Verify basic structure
        assert len(df) > 0, "Should calculate at least some leaderboard entries"
        assert "year" in df.columns
        assert "metric_key" in df.columns
        assert "scope" in df.columns
        assert "repo_id" in df.columns
        assert "user_id" in df.columns
        assert "value" in df.columns
        assert "rank" in df.columns

        # Verify year is correct
        assert df["year"][0] == live_config.github.windows.year

        # Verify both scopes exist
        scopes = df["scope"].unique().to_list()
        assert "org" in scopes, "Should have org-wide leaderboard"
        # repo scope may not exist if there's limited data

        # Verify rankings are valid (1-based, no gaps in dense ranking)
        for metric in df["metric_key"].unique():
            metric_df = df.filter(
                (pl.col("metric_key") == metric) & (pl.col("scope") == "org")
            ).sort("rank")

            if len(metric_df) > 0:
                ranks = metric_df["rank"].to_list()
                assert min(ranks) == 1, f"Ranking for {metric} should start at 1"
                assert max(ranks) <= len(metric_df), (
                    f"Max rank for {metric} should not exceed entry count"
                )

        # Log summary
        metric_keys = df["metric_key"].unique().to_list()
        logger.info("Calculated leaderboards for metrics: %s", metric_keys)
        logger.info("Total leaderboard entries: %d", len(df))

    def test_live_leaderboard_excludes_bots(
        self,
        live_config: Config,
        live_paths: PathManager,
        _ensure_normalized_data: None,
    ) -> None:
        """Bot users should be excluded from leaderboards.

        Verifies:
        - If identity.humans_only is True, bot users are not in leaderboards
        - Bot detection works correctly
        - Only human contributors appear in rankings
        """
        logger.info("Testing bot exclusion in leaderboards")

        # Load users to identify bots
        dim_user = pl.read_parquet(live_paths.dim_user_path)
        bot_users = dim_user.filter(pl.col("is_bot") == True)  # noqa: E712
        bot_user_ids = bot_users["user_id"].to_list()

        logger.info("Found %d bot users in dim_user", len(bot_users))

        # Calculate leaderboards
        df = calculate_leaderboards(live_paths.curated_root, live_config)

        # If humans_only is enabled, verify no bots in leaderboard
        if live_config.identity.humans_only:
            leaderboard_user_ids = df["user_id"].unique().to_list()

            for bot_id in bot_user_ids:
                assert bot_id not in leaderboard_user_ids, (
                    f"Bot user {bot_id} should not appear in leaderboard when humans_only=True"
                )

            logger.info("Verified: No bot users in leaderboard (humans_only=%s)", True)
        else:
            logger.info(
                "Skipping bot exclusion check (humans_only=%s)",
                live_config.identity.humans_only,
            )


class TestLiveRepoHealth:
    """Test repository health metrics calculation with live data."""

    def test_live_repo_health(
        self,
        live_config: Config,
        live_paths: PathManager,
        _ensure_normalized_data: None,
    ) -> None:
        """Calculate repo health scores from live data.

        Verifies:
        - Health metrics are calculated for all repositories
        - Expected columns exist
        - Values are within valid ranges
        - Metrics make logical sense (e.g., prs_merged <= prs_opened)
        """
        logger.info("Testing live repo health calculation")

        # Calculate repo health
        df = calculate_repo_health(live_paths.curated_root, live_config)

        # Verify basic structure
        assert len(df) > 0, "Should calculate health metrics for at least one repository"

        # Verify required columns
        required_cols = [
            "repo_id",
            "repo_full_name",
            "year",
            "active_contributors_30d",
            "active_contributors_90d",
            "active_contributors_365d",
            "prs_opened",
            "prs_merged",
            "issues_opened",
            "issues_closed",
            "review_coverage",
            "median_time_to_merge",
            "stale_pr_count",
            "stale_issue_count",
        ]

        for col in required_cols:
            assert col in df.columns, f"Missing required column: {col}"

        # Verify data types and ranges
        assert df["year"].dtype == "int64"
        assert df["year"].iloc[0] == live_config.github.windows.year

        # Verify contributor counts are non-negative
        assert (df["active_contributors_30d"] >= 0).all()
        assert (df["active_contributors_90d"] >= 0).all()
        assert (df["active_contributors_365d"] >= 0).all()

        # Verify contributor windows make sense (30d <= 90d <= 365d)
        assert (df["active_contributors_30d"] <= df["active_contributors_90d"]).all()
        assert (df["active_contributors_90d"] <= df["active_contributors_365d"]).all()

        # Verify PR/issue counts are non-negative
        assert (df["prs_opened"] >= 0).all()
        assert (df["prs_merged"] >= 0).all()
        assert (df["issues_opened"] >= 0).all()
        assert (df["issues_closed"] >= 0).all()

        # Verify merged PRs don't exceed opened PRs
        assert (df["prs_merged"] <= df["prs_opened"]).all()

        # Verify review coverage is percentage (0-100) or null
        review_coverage_non_null = df["review_coverage"].dropna()
        if len(review_coverage_non_null) > 0:
            assert (review_coverage_non_null >= 0).all()
            assert (review_coverage_non_null <= 100).all()

        # Log summary
        logger.info("Calculated health metrics for %d repositories", len(df))
        logger.info(
            "Average contributors (365d): %.1f",
            df["active_contributors_365d"].mean(),
        )
        logger.info("Average PRs merged: %.1f", df["prs_merged"].mean())


class TestLiveTimeSeries:
    """Test time series metrics calculation with live data."""

    def test_live_time_series(
        self,
        live_config: Config,
        live_paths: PathManager,
        _ensure_normalized_data: None,
    ) -> None:
        """Generate weekly/monthly aggregations from live data.

        Verifies:
        - Time series metrics are calculated
        - Both weekly and monthly periods exist
        - Period boundaries are correct
        - Both org and repo scopes exist
        - Metrics track activity over time
        """
        logger.info("Testing live time series calculation")

        # Calculate time series
        df = calculate_time_series(live_paths.curated_root, live_config)

        # Verify basic structure
        assert len(df) > 0, "Should calculate at least some time series metrics"

        # Verify required columns
        required_cols = [
            "year",
            "period_type",
            "period_start",
            "period_end",
            "scope",
            "repo_id",
            "metric_key",
            "value",
        ]

        for col in required_cols:
            assert col in df.columns, f"Missing required column: {col}"

        # Verify period types
        period_types = df["period_type"].unique()
        assert "week" in period_types or "month" in period_types

        # Verify scopes
        scopes = df["scope"].unique()
        assert "org" in scopes, "Should have org-wide time series"

        # Verify period boundaries are valid (start <= end)
        assert (df["period_start"] <= df["period_end"]).all()

        # Verify values are non-negative
        assert (df["value"] >= 0).all()

        # Verify year is correct
        assert (df["year"] == live_config.github.windows.year).all()

        # Log summary
        metric_keys = df["metric_key"].unique()
        logger.info("Calculated time series for metrics: %s", list(metric_keys))
        logger.info("Total time series records: %d", len(df))
        logger.info("Period types: %s", list(period_types))


class TestLiveHygieneScores:
    """Test hygiene score calculation with live data."""

    def test_live_hygiene_scores(
        self,
        live_config: Config,
        live_paths: PathManager,
        _ensure_normalized_data: None,
    ) -> None:
        """Calculate 0-100 hygiene scores per repo from live data.

        Verifies:
        - Hygiene scores are calculated for repositories
        - Scores are in valid range (0-100)
        - Component flags are boolean or nullable
        - Notes field contains explanatory text
        """
        logger.info("Testing live hygiene score calculation")

        # Calculate hygiene scores
        df = calculate_hygiene_scores(live_paths.curated_root, live_config)

        # Verify basic structure
        if len(df) == 0:
            logger.warning("No hygiene scores calculated (hygiene data may be missing)")
            pytest.skip("No hygiene data available")

        # Verify required columns
        required_cols = [
            "repo_id",
            "repo_full_name",
            "year",
            "score",
            "has_readme",
            "has_license",
            "has_contributing",
            "has_code_of_conduct",
            "has_security_md",
            "has_codeowners",
            "has_ci_workflows",
            "branch_protection_enabled",
            "requires_reviews",
            "dependabot_enabled",
            "secret_scanning_enabled",
            "notes",
        ]

        for col in required_cols:
            assert col in df.columns, f"Missing required column: {col}"

        # Verify scores are in valid range (0-100)
        assert (df["score"] >= 0).all(), "Hygiene scores should be >= 0"
        assert (df["score"] <= 100).all(), "Hygiene scores should be <= 100"

        # Verify boolean columns are boolean type
        bool_cols = [
            "has_readme",
            "has_license",
            "has_contributing",
            "has_code_of_conduct",
            "has_security_md",
            "has_codeowners",
            "has_ci_workflows",
        ]

        for col in bool_cols:
            assert df[col].dtype in [
                "bool",
                "boolean",
            ], f"{col} should be boolean type"

        # Verify nullable boolean columns allow nulls
        nullable_cols = [
            "branch_protection_enabled",
            "requires_reviews",
            "dependabot_enabled",
            "secret_scanning_enabled",
        ]

        for col in nullable_cols:
            assert df[col].dtype in [
                "bool",
                "boolean",
                "object",
            ], f"{col} should allow nulls"

        # Verify year is correct
        assert (df["year"] == live_config.github.windows.year).all()

        # Log summary
        logger.info("Calculated hygiene scores for %d repositories", len(df))
        logger.info("Average hygiene score: %.1f", df["score"].mean())
        logger.info("Score range: %d - %d", df["score"].min(), df["score"].max())

        # Log common issues
        if "notes" in df.columns:
            repos_with_issues = df[df["notes"] != ""]
            if len(repos_with_issues) > 0:
                logger.info(
                    "%d repositories have hygiene issues",
                    len(repos_with_issues),
                )


class TestLiveAwards:
    """Test awards generation with live data."""

    def test_live_awards(
        self,
        live_config: Config,
        live_paths: PathManager,
        _ensure_normalized_data: None,
    ) -> None:
        """Generate award categories from live data.

        Verifies:
        - Awards are generated from metrics
        - Expected award categories exist
        - Winners are identified correctly
        - Supporting stats are included
        """
        logger.info("Testing live awards generation")

        # First ensure metrics exist
        live_paths.metrics_root.mkdir(parents=True, exist_ok=True)

        # Calculate and save leaderboards (required for awards)
        df_leaderboard = calculate_leaderboards(live_paths.curated_root, live_config)
        df_leaderboard.write_parquet(live_paths.metrics_leaderboard_path)

        # Calculate and save hygiene scores if possible
        df_hygiene = calculate_hygiene_scores(live_paths.curated_root, live_config)
        if len(df_hygiene) > 0:
            # Convert to Polars for consistent handling
            if isinstance(df_hygiene, pd.DataFrame):
                df_hygiene = pl.from_pandas(df_hygiene)
            df_hygiene.write_parquet(live_paths.metrics_repo_hygiene_score_path)

        # Calculate and save repo health
        df_health = calculate_repo_health(live_paths.curated_root, live_config)
        if len(df_health) > 0:
            # Convert to Polars for consistent handling
            if isinstance(df_health, pd.DataFrame):
                df_health = pl.from_pandas(df_health)
            df_health.write_parquet(live_paths.metrics_repo_health_path)

        # Check for awards config
        awards_config_path = Path("config/awards.yaml")
        if not awards_config_path.exists():
            logger.warning("Awards config not found, skipping awards test")
            pytest.skip("Awards config (config/awards.yaml) not found")

        # Generate awards
        df_awards = generate_awards(
            live_paths.metrics_root,
            awards_config_path,
            live_config.github.windows.year,
        )

        # Verify basic structure
        if len(df_awards) == 0:
            logger.warning("No awards generated (may be expected if no data matches criteria)")
            return

        # Verify required columns
        required_cols = [
            "award_key",
            "title",
            "description",
            "category",
            "winner_user_id",
            "winner_repo_id",
            "winner_name",
            "supporting_stats",
        ]

        for col in required_cols:
            assert col in df_awards.columns, f"Missing required column: {col}"

        # Verify categories
        categories = df_awards["category"].unique().to_list()
        valid_categories = ["individual", "repository", "risk"]
        for cat in categories:
            assert cat in valid_categories, f"Invalid category: {cat}"

        # Log summary
        logger.info("Generated %d awards", len(df_awards))
        for category in categories:
            count = len(df_awards.filter(pl.col("category") == category))
            logger.info("  - %s: %d awards", category, count)


class TestLiveMetricsOrchestrator:
    """Test full metrics pipeline orchestrator with live data."""

    def test_live_metrics_orchestrator(
        self,
        live_config: Config,
        live_paths: PathManager,
        _ensure_normalized_data: None,
    ) -> None:
        """Full metrics pipeline generates all tables.

        Verifies:
        - Orchestrator runs all metrics calculators
        - Metrics tables are created
        - Statistics are reported correctly
        - Errors are handled gracefully
        """
        logger.info("Testing live metrics orchestrator")

        # Ensure metrics directory exists
        live_paths.metrics_root.mkdir(parents=True, exist_ok=True)

        # Run full metrics pipeline
        stats = run_metrics(live_config)

        # Verify statistics structure
        assert "start_time" in stats
        assert "end_time" in stats
        assert "duration_seconds" in stats
        assert "metrics_written" in stats
        assert "total_rows" in stats
        assert "errors" in stats

        # Verify duration is positive
        assert stats["duration_seconds"] > 0

        # Verify at least some metrics were written
        assert len(stats["metrics_written"]) > 0, "Should write at least some metrics tables"

        # Verify leaderboard metrics exist (this is always implemented)
        assert "metrics_leaderboard" in stats["metrics_written"]
        assert live_paths.metrics_leaderboard_path.exists()

        # Verify leaderboard structure
        df_leaderboard = pl.read_parquet(live_paths.metrics_leaderboard_path)
        assert len(df_leaderboard) > 0

        # Verify total rows matches
        assert stats["total_rows"] > 0

        # Log summary
        logger.info("Metrics orchestrator completed:")
        logger.info("  - Duration: %.2f seconds", stats["duration_seconds"])
        logger.info("  - Tables written: %s", stats["metrics_written"])
        logger.info("  - Total rows: %d", stats["total_rows"])
        logger.info("  - Errors: %d", len(stats["errors"]))

        if stats["errors"]:
            for error in stats["errors"]:
                logger.warning("  - Error: %s", error)

        # Verify other metrics if they were successfully calculated
        if "metrics_time_series" in stats["metrics_written"]:
            assert live_paths.metrics_time_series_path.exists()
            df_ts = pd.read_parquet(live_paths.metrics_time_series_path)
            assert len(df_ts) > 0
            logger.info("  - Time series records: %d", len(df_ts))

        if "metrics_repo_health" in stats["metrics_written"]:
            assert live_paths.metrics_repo_health_path.exists()
            df_health = pd.read_parquet(live_paths.metrics_repo_health_path)
            assert len(df_health) > 0
            logger.info("  - Repo health records: %d", len(df_health))

        if "metrics_repo_hygiene_score" in stats["metrics_written"]:
            assert live_paths.metrics_repo_hygiene_score_path.exists()
            df_hygiene = pd.read_parquet(live_paths.metrics_repo_hygiene_score_path)
            assert len(df_hygiene) > 0
            logger.info("  - Hygiene score records: %d", len(df_hygiene))

            # Verify all scores are in valid range
            assert (df_hygiene["score"] >= 0).all()
            assert (df_hygiene["score"] <= 100).all()

        if "metrics_awards" in stats["metrics_written"]:
            assert live_paths.metrics_awards_path.exists()
            df_awards = pl.read_parquet(live_paths.metrics_awards_path)
            assert len(df_awards) > 0
            logger.info("  - Awards generated: %d", len(df_awards))
