"""Tests for sample metrics fixtures and report building."""

from pathlib import Path

import polars as pl
import pytest

from gh_year_end.config import Config
from gh_year_end.report.build import build_site
from gh_year_end.report.export import export_metrics
from gh_year_end.storage.paths import PathManager


def test_sample_metrics_dir_exists(sample_metrics_dir: Path) -> None:
    """Verify sample metrics fixtures directory exists and contains expected files."""
    assert sample_metrics_dir.exists()
    assert sample_metrics_dir.is_dir()

    # Check for required Parquet files
    expected_files = [
        "metrics_leaderboard.parquet",
        "metrics_time_series.parquet",
        "metrics_repo_health.parquet",
        "metrics_repo_hygiene_score.parquet",
        "metrics_awards.parquet",
    ]

    for filename in expected_files:
        file_path = sample_metrics_dir / filename
        assert file_path.exists(), f"Missing fixture file: {filename}"
        assert file_path.stat().st_size > 0, f"Empty fixture file: {filename}"


def test_sample_metrics_leaderboard_structure(sample_metrics_dir: Path) -> None:
    """Verify leaderboard metrics have expected schema and data."""
    leaderboard_path = sample_metrics_dir / "metrics_leaderboard.parquet"
    df = pl.read_parquet(leaderboard_path)

    # Check schema
    expected_columns = {
        "year",
        "metric_key",
        "scope",
        "repo_id",
        "user_id",
        "value",
        "rank",
    }
    assert set(df.columns) == expected_columns

    # Check data exists
    assert len(df) > 0

    # Check org-wide data exists
    org_data = df.filter(pl.col("scope") == "org")
    assert len(org_data) > 0

    # Check expected metrics exist
    expected_metrics = {
        "prs_opened",
        "prs_merged",
        "reviews_submitted",
        "approvals",
        "issues_opened",
    }
    actual_metrics = set(df["metric_key"].unique().to_list())
    assert expected_metrics.issubset(actual_metrics)


def test_sample_metrics_time_series_structure(sample_metrics_dir: Path) -> None:
    """Verify time series metrics have expected schema and data."""
    timeseries_path = sample_metrics_dir / "metrics_time_series.parquet"
    df = pl.read_parquet(timeseries_path)

    # Check schema
    expected_columns = {
        "year",
        "period_type",
        "period_start",
        "period_end",
        "scope",
        "repo_id",
        "metric_key",
        "value",
    }
    assert set(df.columns) == expected_columns

    # Check data exists
    assert len(df) > 0

    # Check weekly and monthly data
    period_types = set(df["period_type"].unique().to_list())
    assert "week" in period_types
    assert "month" in period_types

    # Verify we have full year coverage (52 weeks)
    weekly_org = df.filter(
        (pl.col("period_type") == "week") & (pl.col("scope") == "org")
    )
    # Each metric should have ~52 weeks
    metrics_count = len(weekly_org["metric_key"].unique())
    expected_weekly_records = metrics_count * 52
    assert len(weekly_org) >= expected_weekly_records * 0.9  # Allow 10% variance


def test_sample_metrics_repo_health_structure(sample_metrics_dir: Path) -> None:
    """Verify repo health metrics have expected schema and data."""
    repo_health_path = sample_metrics_dir / "metrics_repo_health.parquet"
    df = pl.read_parquet(repo_health_path)

    # Check schema
    expected_columns = {
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
        "median_time_to_first_review",
        "median_time_to_merge",
        "stale_pr_count",
        "stale_issue_count",
    }
    assert set(df.columns) == expected_columns

    # Check data exists
    assert len(df) > 0

    # Verify repo_full_name format
    for repo_name in df["repo_full_name"].to_list():
        assert "/" in repo_name, f"Invalid repo name format: {repo_name}"


def test_sample_metrics_hygiene_score_structure(sample_metrics_dir: Path) -> None:
    """Verify hygiene score metrics have expected schema and data."""
    hygiene_path = sample_metrics_dir / "metrics_repo_hygiene_score.parquet"
    df = pl.read_parquet(hygiene_path)

    # Check schema - note: some columns may be nullable
    required_columns = {
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
    }
    assert required_columns.issubset(set(df.columns))

    # Check data exists
    assert len(df) > 0

    # Verify score range (0-100)
    scores = df["score"].to_list()
    assert all(0 <= score <= 100 for score in scores)


def test_sample_metrics_awards_structure(sample_metrics_dir: Path) -> None:
    """Verify awards metrics have expected schema and data."""
    awards_path = sample_metrics_dir / "metrics_awards.parquet"
    df = pl.read_parquet(awards_path)

    # Check schema
    expected_columns = {
        "award_key",
        "title",
        "description",
        "category",
        "winner_user_id",
        "winner_repo_id",
        "winner_name",
        "supporting_stats",
    }
    assert set(df.columns) == expected_columns

    # Check data exists
    assert len(df) > 0

    # Check categories
    categories = set(df["category"].unique().to_list())
    expected_categories = {"individual", "repository", "risk"}
    assert categories.issubset(expected_categories)


def test_export_metrics_with_sample_data(
    sample_metrics_config: Config, sample_metrics_paths: PathManager
) -> None:
    """Test exporting sample metrics to JSON."""
    # Ensure site data directory exists
    sample_metrics_paths.site_data_path.mkdir(parents=True, exist_ok=True)

    # Export metrics
    stats = export_metrics(sample_metrics_config, sample_metrics_paths)

    # Verify export succeeded
    assert "files_written" in stats
    assert len(stats["files_written"]) > 0
    assert stats["total_size_bytes"] > 0

    # Verify JSON files were created
    expected_files = [
        "leaderboards.json",
        "timeseries.json",
        "repo_health.json",
        "hygiene_scores.json",
        "awards.json",
        "summary.json",
    ]

    for filename in expected_files:
        json_path = sample_metrics_paths.site_data_path / filename
        assert json_path.exists(), f"Missing JSON export: {filename}"
        assert json_path.stat().st_size > 0, f"Empty JSON export: {filename}"


def test_build_site_with_sample_data(
    sample_metrics_config: Config, sample_metrics_paths: PathManager
) -> None:
    """Test building static site with sample metrics data."""
    # Build site
    stats = build_site(sample_metrics_config, sample_metrics_paths)

    # Verify build succeeded
    assert "templates_rendered" in stats
    assert "data_files_written" in stats
    assert len(stats["errors"]) == 0, f"Build errors: {stats['errors']}"

    # Verify site structure
    assert sample_metrics_paths.site_root.exists()
    assert sample_metrics_paths.site_data_path.exists()

    # Verify manifest was created
    manifest_path = sample_metrics_paths.site_root / "manifest.json"
    assert manifest_path.exists()


@pytest.mark.parametrize(
    "metric_key,expected_min_value",
    [
        ("prs_merged", 5),
        ("reviews_submitted", 5),
        ("issues_opened", 2),
    ],
)
def test_sample_metrics_leaderboard_values(
    sample_metrics_dir: Path, metric_key: str, expected_min_value: int
) -> None:
    """Verify leaderboard metrics have realistic values."""
    leaderboard_path = sample_metrics_dir / "metrics_leaderboard.parquet"
    df = pl.read_parquet(leaderboard_path)

    # Get org-wide data for this metric
    metric_data = df.filter(
        (pl.col("metric_key") == metric_key) & (pl.col("scope") == "org")
    )

    assert len(metric_data) > 0, f"No data for metric: {metric_key}"

    # Check top contributor has reasonable value
    top_value = metric_data.filter(pl.col("rank") == 1)["value"].to_list()[0]
    assert top_value >= expected_min_value, (
        f"Top value for {metric_key} ({top_value}) below minimum ({expected_min_value})"
    )
