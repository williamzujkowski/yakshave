"""Tests for awards generator."""

from pathlib import Path
from textwrap import dedent

import polars as pl
import pyarrow.parquet as pq
import pytest

from gh_year_end.metrics.awards import AwardsConfig, generate_awards


@pytest.fixture
def awards_config_yaml(tmp_path: Path) -> Path:
    """Create a test awards configuration file."""
    config_path = tmp_path / "awards.yaml"
    config_content = dedent("""
        user_awards:
          - key: merge_machine
            title: "Merge Machine"
            description: "Most PRs merged"
            metric: prs_merged
            humans_only: true
            tie_breaker: prs_opened

          - key: review_paladin
            title: "Review Paladin"
            description: "Most code reviews submitted"
            metric: reviews_submitted
            humans_only: true

          - key: early_bird
            title: "Early Bird"
            description: "Fastest average time to first review"
            metric: avg_time_to_first_review
            direction: asc

        repo_awards:
          - key: bus_factor_alarm
            title: "Bus Factor Alarm"
            description: "High activity but concentrated among few contributors"
            metric: bus_factor_risk_score
            direction: desc

          - key: best_hygiene
            title: "Best Hygiene"
            description: "Highest hygiene score"
            metric: hygiene_score
            direction: desc

          - key: merge_velocity_champion
            title: "Merge Velocity Champion"
            description: "Fastest median time to merge"
            metric: median_time_to_merge
            direction: asc
            min_prs: 10

        risk_signals:
          - key: no_security_policy
            title: "Missing Security Policy"
            description: "Repositories without SECURITY.md"
            filter: missing_security_md

          - key: stale_issues
            title: "Stale Issues"
            description: "Repositories with many old open issues"
            metric: stale_issue_count
            threshold: 20
    """)
    config_path.write_text(config_content)
    return config_path


@pytest.fixture
def metrics_dir(tmp_path: Path) -> Path:
    """Create a test metrics directory with sample data."""
    metrics_path = tmp_path / "metrics" / "year=2025"
    metrics_path.mkdir(parents=True)

    # Create leaderboard data
    leaderboard_df = pl.DataFrame(
        {
            "year": pl.Series([2025] * 6, dtype=pl.Int64),
            "metric_key": pl.Series(
                [
                    "prs_merged",
                    "prs_merged",
                    "reviews_submitted",
                    "reviews_submitted",
                    "avg_time_to_first_review",
                    "avg_time_to_first_review",
                ],
                dtype=pl.String,
            ),
            "scope": pl.Series(["org"] * 6, dtype=pl.String),
            "repo_id": pl.Series([None] * 6, dtype=pl.String),
            "user_id": pl.Series(
                ["user1", "user2", "user1", "user2", "user1", "user2"], dtype=pl.String
            ),
            "user_login": pl.Series(
                ["alice", "bob", "alice", "bob", "alice", "bob"], dtype=pl.String
            ),
            "value": pl.Series([100.0, 75.0, 50.0, 45.0, 2.5, 5.0], dtype=pl.Float64),
            "rank": pl.Series([1, 2, 1, 2, 1, 2], dtype=pl.Int64),
        }
    )
    # Write without dictionary encoding to avoid type conflicts
    pq.write_table(
        leaderboard_df.to_arrow(),
        metrics_path / "metrics_leaderboard.parquet",
        use_dictionary=False,
        compression="snappy",
    )

    # Create repo health data
    repo_health_df = pl.DataFrame(
        {
            "repo_id": pl.Series(["repo1", "repo2", "repo3"], dtype=pl.String),
            "repo_full_name": pl.Series(["org/repo1", "org/repo2", "org/repo3"], dtype=pl.String),
            "prs_merged": pl.Series([150, 75, 5], dtype=pl.Int64),
            "active_contributors_365d": pl.Series([10, 3, 1], dtype=pl.Int64),
            "bus_factor_risk_score": pl.Series([0.2, 0.8, 0.9], dtype=pl.Float64),
            "median_time_to_merge": pl.Series([24.0, 12.0, 48.0], dtype=pl.Float64),
        }
    )
    pq.write_table(
        repo_health_df.to_arrow(),
        metrics_path / "metrics_repo_health.parquet",
        use_dictionary=False,
        compression="snappy",
    )

    # Create hygiene scores data
    hygiene_df = pl.DataFrame(
        {
            "repo_id": pl.Series(["repo1", "repo2", "repo3"], dtype=pl.String),
            "repo_full_name": pl.Series(["org/repo1", "org/repo2", "org/repo3"], dtype=pl.String),
            "hygiene_score": pl.Series([95, 60, 40], dtype=pl.Int64),
            "missing_security_md": pl.Series([False, True, True], dtype=pl.Boolean),
            "missing_codeowners": pl.Series([False, False, True], dtype=pl.Boolean),
            "missing_ci": pl.Series([False, True, False], dtype=pl.Boolean),
            "stale_issue_count": pl.Series([5, 25, 50], dtype=pl.Int64),
        }
    )
    pq.write_table(
        hygiene_df.to_arrow(),
        metrics_path / "metrics_repo_hygiene_score.parquet",
        use_dictionary=False,
        compression="snappy",
    )

    return metrics_path


def test_awards_config_loading(awards_config_yaml: Path) -> None:
    """Test loading and validating awards configuration."""
    config = AwardsConfig(awards_config_yaml)

    assert len(config.user_awards) == 3
    assert len(config.repo_awards) == 3
    assert len(config.risk_signals) == 2

    # Check user award
    merge_machine = config.user_awards[0]
    assert merge_machine["key"] == "merge_machine"
    assert merge_machine["title"] == "Merge Machine"
    assert merge_machine["metric"] == "prs_merged"

    # Check repo award
    bus_factor = config.repo_awards[0]
    assert bus_factor["key"] == "bus_factor_alarm"
    assert bus_factor["direction"] == "desc"

    # Check risk signal
    no_security = config.risk_signals[0]
    assert no_security["key"] == "no_security_policy"
    assert no_security["filter"] == "missing_security_md"


def test_awards_config_missing_file(tmp_path: Path) -> None:
    """Test that loading missing config raises error."""
    missing_path = tmp_path / "nonexistent.yaml"
    with pytest.raises(FileNotFoundError, match="Awards config not found"):
        AwardsConfig(missing_path)


def test_awards_config_invalid_yaml(tmp_path: Path) -> None:
    """Test that invalid config raises error."""
    config_path = tmp_path / "invalid.yaml"
    config_path.write_text(
        dedent("""
        user_awards:
          - key: missing_required_fields
    """)
    )

    with pytest.raises(ValueError, match="missing required fields"):
        AwardsConfig(config_path)


def test_generate_user_awards(metrics_dir: Path, awards_config_yaml: Path) -> None:
    """Test generating user awards from leaderboard."""
    awards = generate_awards(metrics_dir, awards_config_yaml, year=2025)

    # Filter to user awards
    user_awards = awards.filter(pl.col("category") == "individual")

    assert len(user_awards) == 3

    # Check Merge Machine award
    merge_machine = user_awards.filter(pl.col("award_key") == "merge_machine")
    assert len(merge_machine) == 1
    row = merge_machine.row(0, named=True)
    assert row["title"] == "Merge Machine"
    assert row["winner_user_id"] == "user1"
    assert row["winner_name"] == "alice"
    assert "prs_merged" in row["supporting_stats"]
    assert "100" in row["supporting_stats"]

    # Check Review Paladin award
    review_paladin = user_awards.filter(pl.col("award_key") == "review_paladin")
    assert len(review_paladin) == 1
    row = review_paladin.row(0, named=True)
    assert row["winner_user_id"] == "user1"
    assert row["winner_name"] == "alice"

    # Check Early Bird award (direction: asc, so lower is better)
    early_bird = user_awards.filter(pl.col("award_key") == "early_bird")
    assert len(early_bird) == 1
    row = early_bird.row(0, named=True)
    assert row["winner_user_id"] == "user1"  # alice has 2.5, bob has 5.0
    assert row["winner_name"] == "alice"


def test_generate_repo_awards(metrics_dir: Path, awards_config_yaml: Path) -> None:
    """Test generating repository awards."""
    awards = generate_awards(metrics_dir, awards_config_yaml, year=2025)

    # Filter to repo awards
    repo_awards = awards.filter(pl.col("category") == "repository")

    assert len(repo_awards) == 3

    # Check Bus Factor Alarm (highest risk score)
    bus_factor = repo_awards.filter(pl.col("award_key") == "bus_factor_alarm")
    assert len(bus_factor) == 1
    row = bus_factor.row(0, named=True)
    assert row["winner_repo_id"] == "repo3"  # 0.9 risk score
    assert row["winner_name"] == "org/repo3"

    # Check Best Hygiene (highest hygiene score)
    best_hygiene = repo_awards.filter(pl.col("award_key") == "best_hygiene")
    assert len(best_hygiene) == 1
    row = best_hygiene.row(0, named=True)
    assert row["winner_repo_id"] == "repo1"  # 95 score
    assert row["winner_name"] == "org/repo1"

    # Check Merge Velocity Champion (fastest merge, min 10 PRs)
    merge_velocity = repo_awards.filter(pl.col("award_key") == "merge_velocity_champion")
    assert len(merge_velocity) == 1
    row = merge_velocity.row(0, named=True)
    # repo2 has 12.0 median time, repo1 has 24.0, repo3 has 48.0 but only 5 PRs
    assert row["winner_repo_id"] == "repo2"


def test_generate_risk_signals(metrics_dir: Path, awards_config_yaml: Path) -> None:
    """Test generating risk signals."""
    awards = generate_awards(metrics_dir, awards_config_yaml, year=2025)

    # Filter to risk signals
    risk_signals = awards.filter(pl.col("category") == "risk")

    assert len(risk_signals) == 2

    # Check Missing Security Policy signal
    no_security = risk_signals.filter(pl.col("award_key") == "no_security_policy")
    assert len(no_security) == 1
    row = no_security.row(0, named=True)
    assert "2 repositories" in row["winner_name"]  # repo2 and repo3

    # Check Stale Issues signal (threshold > 20)
    stale_issues = risk_signals.filter(pl.col("award_key") == "stale_issues")
    assert len(stale_issues) == 1
    row = stale_issues.row(0, named=True)
    assert "2 repositories" in row["winner_name"]  # repo2 (25) and repo3 (50)


def test_generate_awards_empty_metrics(tmp_path: Path, awards_config_yaml: Path) -> None:
    """Test generating awards with empty metrics."""
    # Create empty metrics directory
    metrics_path = tmp_path / "metrics" / "year=2025"
    metrics_path.mkdir(parents=True)

    # Create empty Parquet files
    empty_df = pl.DataFrame(
        {
            "year": pl.Series([], dtype=pl.Int64),
            "metric_key": pl.Series([], dtype=pl.String),
            "scope": pl.Series([], dtype=pl.String),
            "repo_id": pl.Series([], dtype=pl.String),
            "user_id": pl.Series([], dtype=pl.String),
            "user_login": pl.Series([], dtype=pl.String),
            "value": pl.Series([], dtype=pl.Float64),
            "rank": pl.Series([], dtype=pl.Int64),
        }
    )
    pq.write_table(
        empty_df.to_arrow(),
        metrics_path / "metrics_leaderboard.parquet",
        use_dictionary=False,
        compression="snappy",
    )

    awards = generate_awards(metrics_path, awards_config_yaml, year=2025)

    # Should return empty DataFrame with correct schema
    assert len(awards) == 0
    assert "award_key" in awards.columns
    assert "category" in awards.columns
    assert "winner_user_id" in awards.columns


def test_generate_awards_missing_metrics(tmp_path: Path, awards_config_yaml: Path) -> None:
    """Test generating awards when some metrics files are missing."""
    # Create metrics directory but don't create all files
    metrics_path = tmp_path / "metrics" / "year=2025"
    metrics_path.mkdir(parents=True)

    # Only create leaderboard
    leaderboard_df = pl.DataFrame(
        {
            "year": pl.Series([2025], dtype=pl.Int64),
            "metric_key": pl.Series(["prs_merged"], dtype=pl.String),
            "scope": pl.Series(["org"], dtype=pl.String),
            "repo_id": pl.Series([None], dtype=pl.String),
            "user_id": pl.Series(["user1"], dtype=pl.String),
            "user_login": pl.Series(["alice"], dtype=pl.String),
            "value": pl.Series([100.0], dtype=pl.Float64),
            "rank": pl.Series([1], dtype=pl.Int64),
        }
    )
    pq.write_table(
        leaderboard_df.to_arrow(),
        metrics_path / "metrics_leaderboard.parquet",
        use_dictionary=False,
        compression="snappy",
    )

    awards = generate_awards(metrics_path, awards_config_yaml, year=2025)

    # Should only generate user awards
    assert len(awards) > 0
    assert all(awards["category"] == "individual")


def test_awards_schema(metrics_dir: Path, awards_config_yaml: Path) -> None:
    """Test that generated awards have correct schema."""
    awards = generate_awards(metrics_dir, awards_config_yaml, year=2025)

    # Check schema
    expected_columns = [
        "award_key",
        "title",
        "description",
        "category",
        "winner_user_id",
        "winner_repo_id",
        "winner_name",
        "supporting_stats",
    ]

    for col in expected_columns:
        assert col in awards.columns

    # Check types
    assert awards["award_key"].dtype == pl.String
    assert awards["category"].dtype == pl.String

    # Check that user awards have user_id and repo awards have repo_id
    user_awards = awards.filter(pl.col("category") == "individual")
    if len(user_awards) > 0:
        assert all(user_awards["winner_user_id"].is_not_null())
        assert all(user_awards["winner_repo_id"].is_null())

    repo_awards = awards.filter(pl.col("category") == "repository")
    if len(repo_awards) > 0:
        assert all(repo_awards["winner_repo_id"].is_not_null())
        assert all(repo_awards["winner_user_id"].is_null())


def test_supporting_stats_format(metrics_dir: Path, awards_config_yaml: Path) -> None:
    """Test that supporting stats are properly formatted."""
    awards = generate_awards(metrics_dir, awards_config_yaml, year=2025)

    # Check that all awards have supporting_stats
    assert all(awards["supporting_stats"].is_not_null())

    # Check that supporting_stats contain expected keys
    user_award = awards.filter(pl.col("award_key") == "merge_machine").row(0, named=True)
    stats = user_award["supporting_stats"]
    assert "metric" in stats
    assert "value" in stats
    assert "rank" in stats
