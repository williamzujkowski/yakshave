"""Tests for executive summary view generator."""

from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from gh_year_end.report.views.exec_summary import (
    generate_exec_summary,
)
from gh_year_end.storage.parquet_writer import write_parquet


@pytest.fixture
def mock_leaderboard_data() -> pl.DataFrame:
    """Create mock leaderboard data."""
    return pl.DataFrame(
        {
            "year": [2025] * 15,
            "metric_key": [
                "prs_merged",
                "prs_merged",
                "prs_merged",
                "prs_opened",
                "prs_opened",
                "prs_opened",
                "reviews_submitted",
                "reviews_submitted",
                "reviews_submitted",
                "issues_opened",
                "issues_opened",
                "issues_closed",
                "issues_closed",
                "comments_total",
                "comments_total",
            ],
            "scope": ["org"] * 15,
            "repo_id": [None] * 15,
            "user_id": [
                "user1",
                "user2",
                "user3",
                "user1",
                "user2",
                "user4",
                "user5",
                "user6",
                "user7",
                "user1",
                "user2",
                "user1",
                "user2",
                "user1",
                "user2",
            ],
            "value": [150, 120, 80, 200, 150, 100, 300, 250, 200, 50, 40, 45, 35, 100, 80],
            "rank": [1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 1, 2, 1, 2],
        }
    )


@pytest.fixture
def mock_repo_health_data() -> pd.DataFrame:
    """Create mock repository health data."""
    return pd.DataFrame(
        {
            "repo_id": ["repo1", "repo2", "repo3", "repo4", "repo5"],
            "repo_full_name": [
                "org/repo1",
                "org/repo2",
                "org/repo3",
                "org/repo4",
                "org/repo5",
            ],
            "year": [2025] * 5,
            "active_contributors_30d": [10, 8, 5, 3, 2],
            "active_contributors_90d": [15, 12, 8, 5, 3],
            "active_contributors_365d": [25, 20, 15, 10, 5],
            "prs_opened": [100, 80, 50, 30, 20],
            "prs_merged": [90, 75, 45, 25, 15],
            "issues_opened": [50, 40, 30, 20, 10],
            "issues_closed": [40, 35, 25, 15, 8],
            "review_coverage": [85.0, 70.0, 60.0, 45.0, 30.0],
            "median_time_to_first_review": [2.5, 4.0, 6.0, 8.0, 12.0],
            "median_time_to_merge": [24.0, 36.0, 48.0, 72.0, 96.0],
            "stale_pr_count": [2, 5, 8, 10, 15],
            "stale_issue_count": [5, 8, 12, 15, 20],
        }
    )


@pytest.fixture
def mock_hygiene_scores_data() -> pd.DataFrame:
    """Create mock hygiene scores data."""
    return pd.DataFrame(
        {
            "repo_id": ["repo1", "repo2", "repo3", "repo4", "repo5"],
            "repo_full_name": [
                "org/repo1",
                "org/repo2",
                "org/repo3",
                "org/repo4",
                "org/repo5",
            ],
            "year": [2025] * 5,
            "score": [85, 70, 55, 40, 30],
            "has_readme": [True, True, True, True, False],
            "has_license": [True, True, True, False, False],
            "has_contributing": [True, True, False, False, False],
            "has_code_of_conduct": [True, False, False, False, False],
            "has_security_md": [True, True, False, False, False],
            "has_codeowners": [True, False, False, False, False],
            "has_ci_workflows": [True, True, True, True, False],
            "branch_protection_enabled": [True, True, True, False, False],
            "requires_reviews": [True, True, False, False, False],
            "dependabot_enabled": [True, True, True, False, False],
            "secret_scanning_enabled": [True, False, False, False, False],
            "notes": [
                "",
                "missing CODEOWNERS",
                "missing LICENSE, missing SECURITY.md",
                "missing README, no branch protection",
                "missing README, missing LICENSE, no CI workflows",
            ],
        }
    )


@pytest.fixture
def mock_awards_data() -> pl.DataFrame:
    """Create mock awards data."""
    return pl.DataFrame(
        {
            "award_key": [
                "merge_machine",
                "review_paladin",
                "best_hygiene",
                "no_branch_protection",
                "stale_prs",
            ],
            "title": [
                "Merge Machine",
                "Review Paladin",
                "Best Hygiene",
                "No Branch Protection",
                "Stale PRs",
            ],
            "description": [
                "Most PRs merged",
                "Most reviews submitted",
                "Highest hygiene score",
                "Repos without branch protection",
                "PRs open > 30 days",
            ],
            "category": ["individual", "individual", "repository", "risk", "risk"],
            "winner_user_id": ["user1", "user5", None, None, None],
            "winner_repo_id": [None, None, "repo1", None, None],
            "winner_name": ["user1", "user5", "org/repo1", "2 repositories", "40 PRs"],
            "supporting_stats": [
                "{'metric': 'prs_merged', 'value': 150}",
                "{'metric': 'reviews_submitted', 'value': 300}",
                "{'metric': 'hygiene_score', 'value': 85}",
                "{'count': 2, 'filter': 'no_branch_protection'}",
                "{'count': 40, 'metric': 'stale_pr_count'}",
            ],
        }
    )


@pytest.fixture
def metrics_dir(
    tmp_path: Path,
    mock_leaderboard_data: pl.DataFrame,
    mock_repo_health_data: pd.DataFrame,
    mock_hygiene_scores_data: pd.DataFrame,
    mock_awards_data: pl.DataFrame,
) -> Path:
    """Create a temporary metrics directory with test data."""
    metrics_path = tmp_path / "metrics" / "year=2025"
    metrics_path.mkdir(parents=True)

    # Write leaderboard
    leaderboard_path = metrics_path / "metrics_leaderboard.parquet"
    write_parquet(mock_leaderboard_data.to_arrow(), leaderboard_path)

    # Write repo health
    repo_health_path = metrics_path / "metrics_repo_health.parquet"
    mock_repo_health_data.to_parquet(repo_health_path, index=False)

    # Write hygiene scores
    hygiene_path = metrics_path / "metrics_repo_hygiene_score.parquet"
    mock_hygiene_scores_data.to_parquet(hygiene_path, index=False)

    # Write awards
    awards_path = metrics_path / "metrics_awards.parquet"
    write_parquet(mock_awards_data.to_arrow(), awards_path)

    return metrics_path


def test_generate_exec_summary_structure(metrics_dir: Path):
    """Test that exec summary has correct structure."""
    summary = generate_exec_summary(metrics_dir, 2025)

    # Check top-level keys
    assert "year" in summary
    assert "health_signals" in summary
    assert "key_metrics" in summary
    assert "top_highlights" in summary
    assert "risk_alerts" in summary
    assert "awards_summary" in summary

    assert summary["year"] == 2025


def test_health_signals_structure(metrics_dir: Path):
    """Test health signals have correct structure."""
    summary = generate_exec_summary(metrics_dir, 2025)
    signals = summary["health_signals"]

    # Check required signals exist
    assert "activity_level" in signals
    assert "review_coverage" in signals
    assert "issue_resolution" in signals
    assert "hygiene_score" in signals

    # Check each signal has required fields
    for _signal_name, signal in signals.items():
        assert "value" in signal
        assert "label" in signal
        assert "status" in signal
        assert "description" in signal
        assert signal["status"] in ["green", "yellow", "red", "gray"]


def test_health_signals_calculations(metrics_dir: Path):
    """Test health signal calculations."""
    summary = generate_exec_summary(metrics_dir, 2025)
    signals = summary["health_signals"]

    # Activity level: 150 + 120 + 80 = 350 PRs merged
    assert signals["activity_level"]["value"] == 350
    assert signals["activity_level"]["status"] == "green"  # >= 100

    # Review coverage: 3 out of 5 repos have >= 50% coverage = 60%
    assert signals["review_coverage"]["value"] == 60.0
    assert signals["review_coverage"]["status"] == "yellow"  # >= 50, < 80

    # Issue resolution: (40 + 35 + 25 + 15 + 8) / (50 + 40 + 30 + 20 + 10) = 123/150 = 82%
    assert abs(signals["issue_resolution"]["value"] - 82.0) < 0.1
    assert signals["issue_resolution"]["status"] == "green"  # >= 70

    # Hygiene score: average of 85, 70, 55, 40, 30 = 56
    assert abs(signals["hygiene_score"]["value"] - 56.0) < 0.1
    assert signals["hygiene_score"]["status"] == "yellow"  # >= 50, < 75


def test_health_signals_custom_thresholds(metrics_dir: Path):
    """Test health signals with custom thresholds."""
    custom_thresholds = {
        "activity_level": {"green": 500, "yellow": 200},
        "review_coverage": {"green": 90.0, "yellow": 70.0},
        "issue_resolution": {"green": 90.0, "yellow": 80.0},
        "hygiene_score": {"green": 80.0, "yellow": 60.0},
    }

    summary = generate_exec_summary(metrics_dir, 2025, thresholds=custom_thresholds)
    signals = summary["health_signals"]

    # Activity level: 350 < 500 but >= 200
    assert signals["activity_level"]["status"] == "yellow"

    # Review coverage: 60% < 70%
    assert signals["review_coverage"]["status"] == "red"

    # Issue resolution: 82% < 90% but >= 80%
    assert signals["issue_resolution"]["status"] == "yellow"

    # Hygiene score: 56% < 60%
    assert signals["hygiene_score"]["status"] == "red"


def test_key_metrics_structure(metrics_dir: Path):
    """Test key metrics have correct structure."""
    summary = generate_exec_summary(metrics_dir, 2025)
    metrics = summary["key_metrics"]

    # Check required metrics exist
    assert "prs_merged" in metrics
    assert "contributors" in metrics
    assert "reviews" in metrics
    assert "avg_time_to_merge" in metrics

    # Check each metric has required fields
    for _metric_name, metric in metrics.items():
        assert "value" in metric
        assert "label" in metric
        assert "description" in metric


def test_key_metrics_calculations(metrics_dir: Path):
    """Test key metrics calculations."""
    summary = generate_exec_summary(metrics_dir, 2025)
    metrics = summary["key_metrics"]

    # PRs merged: sum from repo health
    assert metrics["prs_merged"]["value"] == 350

    # Contributors: unique users with prs_opened
    assert metrics["contributors"]["value"] == 3  # user1, user2, user4

    # Reviews: sum of reviews_submitted
    assert metrics["reviews"]["value"] == 750  # 300 + 250 + 200

    # Average time to merge: mean of medians
    expected_avg = (24.0 + 36.0 + 48.0 + 72.0 + 96.0) / 5
    assert abs(metrics["avg_time_to_merge"]["value"] - expected_avg) < 0.1
    assert "formatted" in metrics["avg_time_to_merge"]


def test_top_highlights_structure(metrics_dir: Path):
    """Test top highlights have correct structure."""
    summary = generate_exec_summary(metrics_dir, 2025)
    highlights = summary["top_highlights"]

    # Check required highlight lists exist
    assert "top_contributors" in highlights
    assert "top_reviewers" in highlights
    assert "top_repos" in highlights

    # Check lists are not empty
    assert len(highlights["top_contributors"]) > 0
    assert len(highlights["top_reviewers"]) > 0
    assert len(highlights["top_repos"]) > 0


def test_top_highlights_content(metrics_dir: Path):
    """Test top highlights content."""
    summary = generate_exec_summary(metrics_dir, 2025)
    highlights = summary["top_highlights"]

    # Top contributors (by PRs merged, max 5)
    top_contributors = highlights["top_contributors"]
    assert len(top_contributors) <= 5
    assert top_contributors[0]["rank"] == 1
    assert top_contributors[0]["value"] == 150  # user1
    assert top_contributors[1]["rank"] == 2
    assert top_contributors[1]["value"] == 120  # user2
    assert top_contributors[2]["rank"] == 3
    assert top_contributors[2]["value"] == 80  # user3

    # Top reviewers (by reviews submitted, max 5)
    top_reviewers = highlights["top_reviewers"]
    assert len(top_reviewers) <= 5
    assert top_reviewers[0]["rank"] == 1
    assert top_reviewers[0]["value"] == 300  # user5
    assert top_reviewers[1]["rank"] == 2
    assert top_reviewers[1]["value"] == 250  # user6

    # Top repos (by PRs merged, max 5)
    top_repos = highlights["top_repos"]
    assert len(top_repos) == 5
    assert top_repos[0]["rank"] == 1
    assert top_repos[0]["repo_name"] == "org/repo1"
    assert top_repos[0]["value"] == 90


def test_risk_alerts_structure(metrics_dir: Path):
    """Test risk alerts have correct structure."""
    summary = generate_exec_summary(metrics_dir, 2025)
    alerts = summary["risk_alerts"]

    # Check required alerts exist
    assert "low_hygiene" in alerts
    assert "stale_prs" in alerts
    assert "no_branch_protection" in alerts

    # Check structure
    assert "count" in alerts["low_hygiene"]
    assert "repos" in alerts["low_hygiene"]
    assert "description" in alerts["low_hygiene"]

    assert "count" in alerts["stale_prs"]
    assert "description" in alerts["stale_prs"]

    assert "count" in alerts["no_branch_protection"]
    assert "description" in alerts["no_branch_protection"]


def test_risk_alerts_content(metrics_dir: Path):
    """Test risk alerts content."""
    summary = generate_exec_summary(metrics_dir, 2025)
    alerts = summary["risk_alerts"]

    # Low hygiene repos (score < 50): repo4 (40), repo5 (30)
    assert alerts["low_hygiene"]["count"] == 2
    assert len(alerts["low_hygiene"]["repos"]) == 2
    assert alerts["low_hygiene"]["repos"][0]["score"] == 30  # Sorted ascending

    # Stale PRs: sum across all repos
    assert alerts["stale_prs"]["count"] == 40  # 2 + 5 + 8 + 10 + 15

    # No branch protection: from awards risk signal
    assert alerts["no_branch_protection"]["count"] == 2


def test_awards_summary_structure(metrics_dir: Path):
    """Test awards summary has correct structure."""
    summary = generate_exec_summary(metrics_dir, 2025)
    awards = summary["awards_summary"]

    # Check it's a list
    assert isinstance(awards, list)

    # Check structure of each award
    for award in awards:
        assert "title" in award
        assert "winner" in award
        assert "description" in award
        assert "category" in award


def test_awards_summary_content(metrics_dir: Path):
    """Test awards summary content."""
    summary = generate_exec_summary(metrics_dir, 2025)
    awards = summary["awards_summary"]

    # Should have top 5 non-risk awards
    assert len(awards) <= 5

    # Should exclude risk signals
    for award in awards:
        assert award["category"] in ["individual", "repository"]

    # Check first award
    assert awards[0]["title"] == "Merge Machine"
    assert awards[0]["winner"] == "user1"
    assert awards[0]["category"] == "individual"


def test_missing_metrics_files(tmp_path: Path):
    """Test behavior when metrics files are missing."""
    metrics_path = tmp_path / "metrics" / "year=2025"
    metrics_path.mkdir(parents=True)

    # Create only leaderboard
    leaderboard = pl.DataFrame(
        {
            "year": [2025] * 3,
            "metric_key": ["prs_merged"] * 3,
            "scope": ["org"] * 3,
            "repo_id": [None] * 3,
            "user_id": ["user1", "user2", "user3"],
            "value": [100, 80, 60],
            "rank": [1, 2, 3],
        }
    )
    write_parquet(leaderboard.to_arrow(), metrics_path / "metrics_leaderboard.parquet")

    summary = generate_exec_summary(metrics_path, 2025)

    # Should still return valid structure with some N/A values
    assert summary["year"] == 2025
    assert "health_signals" in summary

    # Review coverage should be gray (no data)
    assert summary["health_signals"]["review_coverage"]["status"] == "gray"

    # Top repos should be empty
    assert summary["top_highlights"]["top_repos"] == []


def test_empty_metrics_data(tmp_path: Path):
    """Test behavior with empty metrics files."""
    metrics_path = tmp_path / "metrics" / "year=2025"
    metrics_path.mkdir(parents=True)

    # Create empty leaderboard
    leaderboard = pl.DataFrame(
        schema={
            "year": pl.Int32,
            "metric_key": pl.Utf8,
            "scope": pl.Utf8,
            "repo_id": pl.Utf8,
            "user_id": pl.Utf8,
            "value": pl.Int64,
            "rank": pl.Int32,
        }
    )
    write_parquet(leaderboard.to_arrow(), metrics_path / "metrics_leaderboard.parquet")

    summary = generate_exec_summary(metrics_path, 2025)

    # Should handle empty data gracefully
    assert summary["year"] == 2025
    assert summary["health_signals"]["activity_level"]["value"] == 0
    assert summary["key_metrics"]["prs_merged"]["value"] == 0
    assert summary["top_highlights"]["top_contributors"] == []


def test_health_signal_thresholds_boundary_cases(metrics_dir: Path):
    """Test health signal calculations at threshold boundaries."""
    # Test with thresholds that should hit exact boundaries
    thresholds = {
        "activity_level": {"green": 350, "yellow": 100},  # Exact match on green
        "review_coverage": {"green": 60.0, "yellow": 50.0},  # Exact match on green
        "issue_resolution": {"green": 82.0, "yellow": 50.0},  # Close to green
        "hygiene_score": {"green": 100.0, "yellow": 56.0},  # Exact match on yellow
    }

    summary = generate_exec_summary(metrics_dir, 2025, thresholds=thresholds)
    signals = summary["health_signals"]

    # Activity level: exactly at green threshold
    assert signals["activity_level"]["status"] == "green"

    # Review coverage: exactly at green threshold
    assert signals["review_coverage"]["status"] == "green"

    # Hygiene score: at yellow threshold
    assert signals["hygiene_score"]["status"] == "yellow"


def test_time_formatting():
    """Test time formatting helper."""
    from gh_year_end.report.views.exec_summary import _format_hours

    # Minutes
    assert _format_hours(0.5) == "30 minutes"
    assert _format_hours(0.25) == "15 minutes"

    # Hours
    assert _format_hours(1.5) == "1.5 hours"
    assert _format_hours(12.0) == "12.0 hours"

    # Days
    assert _format_hours(24.0) == "1.0 days"
    assert _format_hours(48.0) == "2.0 days"

    # Weeks
    assert _format_hours(168.0) == "1.0 weeks"  # 7 days
    assert _format_hours(336.0) == "2.0 weeks"  # 14 days
