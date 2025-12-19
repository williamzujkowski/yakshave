"""Tests for engineer drilldown view generation."""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest

from gh_year_end.report.views.engineer_view import (
    filter_by_metric,
    filter_by_repo,
    filter_by_user,
    generate_engineer_view,
)


@pytest.fixture
def metrics_dir(tmp_path: Path) -> Path:
    """Create temporary metrics directory with test data.

    Args:
        tmp_path: Pytest temporary path fixture.

    Returns:
        Path to metrics directory.
    """
    metrics_path = tmp_path / "metrics" / "year=2025"
    metrics_path.mkdir(parents=True)
    return metrics_path


@pytest.fixture
def curated_dir(tmp_path: Path) -> Path:
    """Create temporary curated directory.

    Args:
        tmp_path: Pytest temporary path fixture.

    Returns:
        Path to curated directory.
    """
    curated_path = tmp_path / "curated" / "year=2025"
    curated_path.mkdir(parents=True)
    return curated_path


@pytest.fixture
def sample_leaderboard(metrics_dir: Path) -> pl.DataFrame:
    """Create sample leaderboard data.

    Args:
        metrics_dir: Path to metrics directory.

    Returns:
        Sample leaderboard DataFrame.
    """
    data = {
        "year": [2025, 2025, 2025, 2025, 2025, 2025, 2025, 2025],
        "metric_key": [
            "prs_opened",
            "prs_opened",
            "prs_merged",
            "prs_merged",
            "prs_opened",
            "prs_opened",
            "prs_merged",
            "prs_merged",
        ],
        "scope": ["org", "org", "org", "org", "repo", "repo", "repo", "repo"],
        "repo_id": [None, None, None, None, "R1", "R1", "R1", "R1"],
        "user_id": ["U1", "U2", "U1", "U2", "U1", "U2", "U1", "U2"],
        "value": [10, 8, 9, 7, 5, 3, 4, 2],
        "rank": [1, 2, 1, 2, 1, 2, 1, 2],
    }
    df = pl.DataFrame(data)

    # Write to parquet
    table = df.to_arrow()
    pq.write_table(table, metrics_dir / "metrics_leaderboard.parquet")

    return df


@pytest.fixture
def sample_repo_health(metrics_dir: Path) -> pl.DataFrame:
    """Create sample repo health data.

    Args:
        metrics_dir: Path to metrics directory.

    Returns:
        Sample repo health DataFrame.
    """
    data = {
        "repo_id": ["R1", "R2"],
        "repo_full_name": ["org/repo1", "org/repo2"],
        "year": [2025, 2025],
        "active_contributors_30d": [5, 3],
        "active_contributors_90d": [8, 4],
        "active_contributors_365d": [12, 6],
        "prs_opened": [20, 10],
        "prs_merged": [18, 9],
        "issues_opened": [15, 8],
        "issues_closed": [12, 6],
        "review_coverage": [85.5, 70.2],
        "median_time_to_first_review": [2.5, 4.8],
        "median_time_to_merge": [12.3, 24.6],
        "stale_pr_count": [2, 5],
        "stale_issue_count": [3, 8],
    }
    df = pl.DataFrame(data)

    # Write to parquet
    table = df.to_arrow()
    pq.write_table(table, metrics_dir / "metrics_repo_health.parquet")

    return df


@pytest.fixture
def sample_hygiene_scores(metrics_dir: Path) -> pl.DataFrame:
    """Create sample hygiene scores data.

    Args:
        metrics_dir: Path to metrics directory.

    Returns:
        Sample hygiene scores DataFrame.
    """
    data = {
        "repo_id": ["R1", "R2"],
        "repo_full_name": ["org/repo1", "org/repo2"],
        "hygiene_score": [85, 65],
        "has_security_md": [True, False],
        "has_readme": [True, True],
        "has_license": [True, True],
        "has_contributing": [True, False],
        "has_code_of_conduct": [True, False],
        "has_codeowners": [True, False],
        "has_workflows": [True, True],
        "branch_protection_enabled": [True, False],
    }
    df = pl.DataFrame(data)

    # Write to parquet
    table = df.to_arrow()
    pq.write_table(table, metrics_dir / "metrics_repo_hygiene_score.parquet")

    return df


@pytest.fixture
def sample_awards(metrics_dir: Path) -> pl.DataFrame:
    """Create sample awards data.

    Args:
        metrics_dir: Path to metrics directory.

    Returns:
        Sample awards DataFrame.
    """
    data = {
        "award_key": ["top_pr_opener", "best_repo"],
        "title": ["Top PR Opener", "Best Repository"],
        "description": ["Most PRs opened", "Highest hygiene score"],
        "category": ["individual", "repository"],
        "winner_user_id": ["U1", None],
        "winner_repo_id": [None, "R1"],
        "winner_name": ["user1", "org/repo1"],
        "supporting_stats": [
            "{'metric': 'prs_opened', 'value': 10, 'rank': 1}",
            "{'metric': 'hygiene_score', 'value': 85}",
        ],
    }
    df = pl.DataFrame(data)

    # Write to parquet
    table = df.to_arrow()
    pq.write_table(table, metrics_dir / "metrics_awards.parquet")

    return df


@pytest.fixture
def sample_dim_user(curated_dir: Path) -> pl.DataFrame:
    """Create sample user dimension data.

    Args:
        curated_dir: Path to curated directory.

    Returns:
        Sample user dimension DataFrame.
    """
    data = {
        "user_id": ["U1", "U2"],
        "login": ["user1", "user2"],
        "is_bot": [False, False],
    }
    df = pl.DataFrame(data)

    # Write to parquet
    pq.write_table(df.to_arrow(), curated_dir / "dim_user.parquet")

    return df


@pytest.fixture
def sample_dim_repo(curated_dir: Path) -> pl.DataFrame:
    """Create sample repo dimension data.

    Args:
        curated_dir: Path to curated directory.

    Returns:
        Sample repo dimension DataFrame.
    """
    data = {
        "repo_id": ["R1", "R2"],
        "full_name": ["org/repo1", "org/repo2"],
    }
    df = pl.DataFrame(data)

    # Write to parquet
    pq.write_table(df.to_arrow(), curated_dir / "dim_repo.parquet")

    return df


@pytest.fixture
def sample_fact_pr(curated_dir: Path) -> pl.DataFrame:
    """Create sample PR fact data.

    Args:
        curated_dir: Path to curated directory.

    Returns:
        Sample PR fact DataFrame.
    """
    data = {
        "pr_id": ["PR1", "PR2", "PR3"],
        "repo_id": ["R1", "R1", "R2"],
        "author_user_id": ["U1", "U2", "U1"],
        "created_at": [
            datetime(2025, 1, 15, tzinfo=UTC),
            datetime(2025, 2, 20, tzinfo=UTC),
            datetime(2025, 3, 10, tzinfo=UTC),
        ],
        "state": ["merged", "merged", "open"],
    }
    df = pl.DataFrame(data)

    # Write to parquet
    pq.write_table(df.to_arrow(), curated_dir / "fact_pull_request.parquet")

    return df


@pytest.fixture
def sample_fact_issue(curated_dir: Path) -> pl.DataFrame:
    """Create sample issue fact data.

    Args:
        curated_dir: Path to curated directory.

    Returns:
        Sample issue fact DataFrame.
    """
    data = {
        "issue_id": ["I1", "I2"],
        "repo_id": ["R1", "R2"],
        "author_user_id": ["U1", "U2"],
        "created_at": [
            datetime(2025, 1, 10, tzinfo=UTC),
            datetime(2025, 2, 15, tzinfo=UTC),
        ],
        "state": ["closed", "open"],
    }
    df = pl.DataFrame(data)

    # Write to parquet
    pq.write_table(df.to_arrow(), curated_dir / "fact_issue.parquet")

    return df


def test_generate_engineer_view_complete(
    metrics_dir: Path,
    curated_dir: Path,
    sample_leaderboard: pl.DataFrame,
    sample_repo_health: pl.DataFrame,
    sample_hygiene_scores: pl.DataFrame,
    sample_awards: pl.DataFrame,
    sample_dim_user: pl.DataFrame,
    sample_dim_repo: pl.DataFrame,
    sample_fact_pr: pl.DataFrame,
    sample_fact_issue: pl.DataFrame,
) -> None:
    """Test generating complete engineer view.

    Args:
        metrics_dir: Path to metrics directory.
        curated_dir: Path to curated directory.
        sample_leaderboard: Sample leaderboard data.
        sample_repo_health: Sample repo health data.
        sample_hygiene_scores: Sample hygiene scores data.
        sample_awards: Sample awards data.
        sample_dim_user: Sample user dimension data.
        sample_dim_repo: Sample repo dimension data.
        sample_fact_pr: Sample PR fact data.
        sample_fact_issue: Sample issue fact data.
    """
    view = generate_engineer_view(metrics_dir, 2025)

    # Verify structure
    assert "year" in view
    assert view["year"] == 2025
    assert "generated_at" in view
    assert "leaderboards" in view
    assert "repo_breakdown" in view
    assert "time_series" in view
    assert "awards" in view
    assert "contributor_profiles" in view
    assert "filters" in view


def test_leaderboards_org_and_repo(
    metrics_dir: Path,
    curated_dir: Path,
    sample_leaderboard: pl.DataFrame,
    sample_dim_user: pl.DataFrame,
    sample_dim_repo: pl.DataFrame,
) -> None:
    """Test leaderboard generation with org and repo scopes.

    Args:
        metrics_dir: Path to metrics directory.
        curated_dir: Path to curated directory.
        sample_leaderboard: Sample leaderboard data.
        sample_dim_user: Sample user dimension data.
        sample_dim_repo: Sample repo dimension data.
    """
    view = generate_engineer_view(metrics_dir, 2025)

    leaderboards = view["leaderboards"]

    # Check org-wide leaderboards
    assert "org_wide" in leaderboards
    assert "prs_opened" in leaderboards["org_wide"]
    assert "prs_merged" in leaderboards["org_wide"]

    # Verify prs_opened rankings
    prs_opened = leaderboards["org_wide"]["prs_opened"]
    assert prs_opened["metric"] == "prs_opened"
    assert prs_opened["scope"] == "org"
    assert prs_opened["total_entries"] == 2
    assert len(prs_opened["rankings"]) == 2

    # Check rank order
    assert prs_opened["rankings"][0]["rank"] == 1
    assert prs_opened["rankings"][0]["user_id"] == "U1"
    assert prs_opened["rankings"][0]["value"] == 10
    assert prs_opened["rankings"][0]["login"] == "user1"

    # Check percentiles are included
    assert "percentile" in prs_opened["rankings"][0]

    # Check per-repo leaderboards
    assert "per_repo" in leaderboards
    assert "R1" in leaderboards["per_repo"]

    repo1_lb = leaderboards["per_repo"]["R1"]
    assert repo1_lb["repo_id"] == "R1"
    assert repo1_lb["repo_name"] == "org/repo1"
    assert "prs_opened" in repo1_lb["leaderboards"]


def test_repo_breakdown(
    metrics_dir: Path,
    curated_dir: Path,
    sample_leaderboard: pl.DataFrame,
    sample_repo_health: pl.DataFrame,
    sample_hygiene_scores: pl.DataFrame,
    sample_dim_user: pl.DataFrame,
    sample_dim_repo: pl.DataFrame,
) -> None:
    """Test repo breakdown generation.

    Args:
        metrics_dir: Path to metrics directory.
        curated_dir: Path to curated directory.
        sample_leaderboard: Sample leaderboard data.
        sample_repo_health: Sample repo health data.
        sample_hygiene_scores: Sample hygiene scores data.
        sample_dim_user: Sample user dimension data.
        sample_dim_repo: Sample repo dimension data.
    """
    view = generate_engineer_view(metrics_dir, 2025)

    breakdown = view["repo_breakdown"]

    # Check R1 exists
    assert "R1" in breakdown
    repo1 = breakdown["R1"]

    # Check structure
    assert repo1["repo_id"] == "R1"
    assert repo1["repo_name"] == "org/repo1"
    assert "contributors" in repo1
    assert "health_metrics" in repo1
    assert "hygiene_metrics" in repo1

    # Check contributors
    assert len(repo1["contributors"]) > 0
    contributor = repo1["contributors"][0]
    assert "user_id" in contributor
    assert "login" in contributor
    assert "metrics" in contributor

    # Check health metrics
    health = repo1["health_metrics"]
    assert health["active_contributors_30d"] == 5
    assert health["prs_opened"] == 20
    assert health["prs_merged"] == 18

    # Check hygiene metrics
    hygiene = repo1["hygiene_metrics"]
    assert hygiene["hygiene_score"] == 85
    assert hygiene["has_security_md"] is True


def test_time_series(
    metrics_dir: Path,
    curated_dir: Path,
    sample_dim_user: pl.DataFrame,
    sample_dim_repo: pl.DataFrame,
    sample_fact_pr: pl.DataFrame,
    sample_fact_issue: pl.DataFrame,
) -> None:
    """Test time series generation.

    Args:
        metrics_dir: Path to metrics directory.
        curated_dir: Path to curated directory.
        sample_dim_user: Sample user dimension data.
        sample_dim_repo: Sample repo dimension data.
        sample_fact_pr: Sample PR fact data.
        sample_fact_issue: Sample issue fact data.
    """
    # Create minimal leaderboard to avoid missing file errors
    minimal_lb = pl.DataFrame(
        {
            "year": [2025],
            "metric_key": ["prs_opened"],
            "scope": ["org"],
            "repo_id": [None],
            "user_id": ["U1"],
            "value": [1],
            "rank": [1],
        }
    )
    pq.write_table(minimal_lb.to_arrow(), metrics_dir / "metrics_leaderboard.parquet")

    view = generate_engineer_view(metrics_dir, 2025)

    time_series = view["time_series"]

    # Check structure
    assert "weekly" in time_series
    assert "monthly" in time_series

    # Check weekly data
    assert len(time_series["weekly"]) > 0
    week_entry = time_series["weekly"][0]
    assert "period" in week_entry
    assert "prs" in week_entry
    assert "issues" in week_entry

    # Check monthly data
    assert len(time_series["monthly"]) > 0
    month_entry = time_series["monthly"][0]
    assert "period" in month_entry
    assert "prs" in month_entry
    assert "issues" in month_entry


def test_detailed_awards(
    metrics_dir: Path,
    curated_dir: Path,
    sample_leaderboard: pl.DataFrame,
    sample_awards: pl.DataFrame,
    sample_dim_user: pl.DataFrame,
    sample_dim_repo: pl.DataFrame,
) -> None:
    """Test detailed awards generation.

    Args:
        metrics_dir: Path to metrics directory.
        curated_dir: Path to curated directory.
        sample_leaderboard: Sample leaderboard data.
        sample_awards: Sample awards data.
        sample_dim_user: Sample user dimension data.
        sample_dim_repo: Sample repo dimension data.
    """
    view = generate_engineer_view(metrics_dir, 2025)

    awards = view["awards"]

    # Check awards exist
    assert len(awards) == 2

    # Check individual award
    individual_award = next(a for a in awards if a["category"] == "individual")
    assert individual_award["key"] == "top_pr_opener"
    assert individual_award["title"] == "Top PR Opener"
    assert individual_award["winner"]["user_id"] == "U1"
    assert individual_award["winner"]["name"] == "user1"

    # Check supporting stats parsed
    assert individual_award["supporting_stats"] is not None
    assert "metric" in individual_award["supporting_stats"]

    # Check honorable mentions (should have U2 since they're rank 2)
    assert "honorable_mentions" in individual_award
    assert len(individual_award["honorable_mentions"]) > 0


def test_contributor_profiles(
    metrics_dir: Path,
    curated_dir: Path,
    sample_leaderboard: pl.DataFrame,
    sample_dim_user: pl.DataFrame,
    sample_dim_repo: pl.DataFrame,
) -> None:
    """Test contributor profiles generation.

    Args:
        metrics_dir: Path to metrics directory.
        curated_dir: Path to curated directory.
        sample_leaderboard: Sample leaderboard data.
        sample_dim_user: Sample user dimension data.
        sample_dim_repo: Sample repo dimension data.
    """
    view = generate_engineer_view(metrics_dir, 2025)

    profiles = view["contributor_profiles"]

    # Check U1 profile
    assert "U1" in profiles
    u1_profile = profiles["U1"]

    assert u1_profile["user_id"] == "U1"
    assert u1_profile["login"] == "user1"
    assert "metrics" in u1_profile
    assert "repos_contributed" in u1_profile

    # Check metrics
    assert "prs_opened" in u1_profile["metrics"]
    assert u1_profile["metrics"]["prs_opened"]["value"] == 10
    assert u1_profile["metrics"]["prs_opened"]["rank"] == 1

    # Check repos contributed
    assert len(u1_profile["repos_contributed"]) > 0
    assert u1_profile["repos_contributed"][0]["repo_id"] == "R1"


def test_filter_options(
    metrics_dir: Path,
    curated_dir: Path,
    sample_leaderboard: pl.DataFrame,
    sample_dim_user: pl.DataFrame,
    sample_dim_repo: pl.DataFrame,
) -> None:
    """Test filter options generation.

    Args:
        metrics_dir: Path to metrics directory.
        curated_dir: Path to curated directory.
        sample_leaderboard: Sample leaderboard data.
        sample_dim_user: Sample user dimension data.
        sample_dim_repo: Sample repo dimension data.
    """
    view = generate_engineer_view(metrics_dir, 2025)

    filters = view["filters"]

    # Check metrics
    assert "metrics" in filters
    assert "prs_opened" in filters["metrics"]
    assert "prs_merged" in filters["metrics"]

    # Check repos
    assert "repos" in filters
    assert len(filters["repos"]) == 2
    assert any(r["repo_id"] == "R1" for r in filters["repos"])

    # Check users
    assert "users" in filters
    assert "U1" in filters["users"]
    assert "U2" in filters["users"]


def test_filter_by_repo(
    metrics_dir: Path,
    curated_dir: Path,
    sample_leaderboard: pl.DataFrame,
    sample_repo_health: pl.DataFrame,
    sample_dim_user: pl.DataFrame,
    sample_dim_repo: pl.DataFrame,
) -> None:
    """Test filtering view by repository.

    Args:
        metrics_dir: Path to metrics directory.
        curated_dir: Path to curated directory.
        sample_leaderboard: Sample leaderboard data.
        sample_repo_health: Sample repo health data.
        sample_dim_user: Sample user dimension data.
        sample_dim_repo: Sample repo dimension data.
    """
    view = generate_engineer_view(metrics_dir, 2025)
    filtered = filter_by_repo(view, "R1")

    # Check per-repo leaderboards filtered
    assert "R1" in filtered["leaderboards"]["per_repo"]
    assert "R2" not in filtered["leaderboards"]["per_repo"]

    # Check repo breakdown filtered
    assert "R1" in filtered["repo_breakdown"]
    assert "R2" not in filtered["repo_breakdown"]


def test_filter_by_user(
    metrics_dir: Path,
    curated_dir: Path,
    sample_leaderboard: pl.DataFrame,
    sample_dim_user: pl.DataFrame,
    sample_dim_repo: pl.DataFrame,
) -> None:
    """Test filtering view by user.

    Args:
        metrics_dir: Path to metrics directory.
        curated_dir: Path to curated directory.
        sample_leaderboard: Sample leaderboard data.
        sample_dim_user: Sample user dimension data.
        sample_dim_repo: Sample repo dimension data.
    """
    view = generate_engineer_view(metrics_dir, 2025)
    filtered = filter_by_user(view, "U1")

    # Check org-wide leaderboards filtered
    prs_opened = filtered["leaderboards"]["org_wide"]["prs_opened"]
    assert len(prs_opened["rankings"]) == 1
    assert prs_opened["rankings"][0]["user_id"] == "U1"

    # Check contributor profiles filtered
    assert "U1" in filtered["contributor_profiles"]
    assert "U2" not in filtered["contributor_profiles"]


def test_filter_by_metric(
    metrics_dir: Path,
    curated_dir: Path,
    sample_leaderboard: pl.DataFrame,
    sample_dim_user: pl.DataFrame,
    sample_dim_repo: pl.DataFrame,
) -> None:
    """Test filtering view by metric.

    Args:
        metrics_dir: Path to metrics directory.
        curated_dir: Path to curated directory.
        sample_leaderboard: Sample leaderboard data.
        sample_dim_user: Sample user dimension data.
        sample_dim_repo: Sample repo dimension data.
    """
    view = generate_engineer_view(metrics_dir, 2025)
    filtered = filter_by_metric(view, "prs_opened")

    # Check org-wide leaderboards filtered
    assert "prs_opened" in filtered["leaderboards"]["org_wide"]
    assert "prs_merged" not in filtered["leaderboards"]["org_wide"]

    # Check metrics list updated
    assert filtered["leaderboards"]["metrics"] == ["prs_opened"]


def test_empty_metrics(
    metrics_dir: Path,
    curated_dir: Path,
) -> None:
    """Test engineer view with no metrics data.

    Args:
        metrics_dir: Path to metrics directory.
        curated_dir: Path to curated directory.
    """
    view = generate_engineer_view(metrics_dir, 2025)

    # Should return valid structure with empty data
    assert view["year"] == 2025
    assert view["leaderboards"]["org_wide"] == {}
    assert view["leaderboards"]["per_repo"] == {}
    assert view["repo_breakdown"] == {}
    assert view["awards"] == []
    assert view["contributor_profiles"] == {}


def test_percentile_calculation(
    metrics_dir: Path,
    curated_dir: Path,
    sample_dim_user: pl.DataFrame,
    sample_dim_repo: pl.DataFrame,
) -> None:
    """Test percentile calculation in rankings.

    Args:
        metrics_dir: Path to metrics directory.
        curated_dir: Path to curated directory.
        sample_dim_user: Sample user dimension data.
        sample_dim_repo: Sample repo dimension data.
    """
    # Create leaderboard with 5 entries
    data = {
        "year": [2025] * 5,
        "metric_key": ["prs_opened"] * 5,
        "scope": ["org"] * 5,
        "repo_id": [None] * 5,
        "user_id": ["U1", "U2", "U3", "U4", "U5"],
        "value": [100, 80, 60, 40, 20],
        "rank": [1, 2, 3, 4, 5],
    }
    df = pl.DataFrame(data)
    pq.write_table(df.to_arrow(), metrics_dir / "metrics_leaderboard.parquet")

    view = generate_engineer_view(metrics_dir, 2025)

    prs_opened = view["leaderboards"]["org_wide"]["prs_opened"]
    rankings = prs_opened["rankings"]

    # Check percentiles
    assert rankings[0]["percentile"] == 20.0  # Rank 1 of 5 = 20%
    assert rankings[1]["percentile"] == 40.0  # Rank 2 of 5 = 40%
    assert rankings[4]["percentile"] == 100.0  # Rank 5 of 5 = 100%


def test_missing_dim_tables(
    metrics_dir: Path,
    curated_dir: Path,
    sample_leaderboard: pl.DataFrame,
) -> None:
    """Test engineer view handles missing dimension tables gracefully.

    Args:
        metrics_dir: Path to metrics directory.
        curated_dir: Path to curated directory.
        sample_leaderboard: Sample leaderboard data.
    """
    # Generate view without dim_user and dim_repo
    view = generate_engineer_view(metrics_dir, 2025)

    # Should still work, but without login/repo names
    assert "leaderboards" in view
    assert "prs_opened" in view["leaderboards"]["org_wide"]

    # Rankings should have None for login
    rankings = view["leaderboards"]["org_wide"]["prs_opened"]["rankings"]
    assert rankings[0]["login"] is None
