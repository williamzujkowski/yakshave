#!/usr/bin/env python3
"""Generate a sample site with mock data for testing and validation."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_ROOT = PROJECT_ROOT / "data"
SITE_ROOT = PROJECT_ROOT / "site"

YEAR = 2025
METRICS_PATH = DATA_ROOT / f"metrics/year={YEAR}"
CURATED_PATH = DATA_ROOT / f"curated/year={YEAR}"
SITE_OUTPUT = SITE_ROOT / f"year={YEAR}"


def create_directories() -> None:
    """Create required directories."""
    METRICS_PATH.mkdir(parents=True, exist_ok=True)
    CURATED_PATH.mkdir(parents=True, exist_ok=True)
    SITE_OUTPUT.mkdir(parents=True, exist_ok=True)
    (SITE_OUTPUT / "data").mkdir(exist_ok=True)


def create_sample_users() -> pl.DataFrame:
    """Create sample user dimension table."""
    return pl.DataFrame(
        {
            "user_id": ["u1", "u2", "u3", "u4", "u5", "bot1"],
            "login": ["alice", "bob", "charlie", "diana", "eve", "dependabot[bot]"],
            "display_name": [
                "Alice Smith",
                "Bob Jones",
                "Charlie Brown",
                "Diana Lee",
                "Eve Wilson",
                None,
            ],
            "type": ["User", "User", "User", "User", "User", "Bot"],
            "is_bot": [False, False, False, False, False, True],
            "bot_reason": [None, None, None, None, None, "matched pattern .*\\[bot\\]$"],
        }
    )


def create_sample_repos() -> pl.DataFrame:
    """Create sample repository dimension table."""
    return pl.DataFrame(
        {
            "repo_id": ["r1", "r2", "r3"],
            "full_name": ["org/api-service", "org/web-frontend", "org/docs"],
            "name": ["api-service", "web-frontend", "docs"],
            "owner": ["org", "org", "org"],
            "is_fork": [False, False, False],
            "is_archived": [False, False, False],
            "is_private": [False, False, True],
            "default_branch": ["main", "main", "main"],
            "stars": [150, 85, 20],
            "forks": [45, 12, 3],
        }
    )


def create_sample_leaderboard() -> pl.DataFrame:
    """Create sample leaderboard metrics."""
    metrics = []

    # PRs opened - org scope
    for i, (user_id, value) in enumerate(
        [("u1", 45), ("u2", 38), ("u3", 25), ("u4", 18), ("u5", 12)], 1
    ):
        metrics.append(
            {
                "year": YEAR,
                "metric_key": "prs_opened",
                "scope": "org",
                "repo_id": None,
                "user_id": user_id,
                "value": value,
                "rank": i,
            }
        )

    # PRs merged - org scope
    for i, (user_id, value) in enumerate(
        [("u1", 42), ("u2", 35), ("u3", 22), ("u4", 15), ("u5", 10)], 1
    ):
        metrics.append(
            {
                "year": YEAR,
                "metric_key": "prs_merged",
                "scope": "org",
                "repo_id": None,
                "user_id": user_id,
                "value": value,
                "rank": i,
            }
        )

    # Reviews submitted - org scope
    for i, (user_id, value) in enumerate(
        [("u2", 78), ("u1", 65), ("u3", 45), ("u4", 30), ("u5", 15)], 1
    ):
        metrics.append(
            {
                "year": YEAR,
                "metric_key": "reviews_submitted",
                "scope": "org",
                "repo_id": None,
                "user_id": user_id,
                "value": value,
                "rank": i,
            }
        )

    # Comments - org scope
    for i, (user_id, value) in enumerate(
        [("u2", 120), ("u1", 95), ("u3", 60), ("u4", 40), ("u5", 25)], 1
    ):
        metrics.append(
            {
                "year": YEAR,
                "metric_key": "comments_total",
                "scope": "org",
                "repo_id": None,
                "user_id": user_id,
                "value": value,
                "rank": i,
            }
        )

    # Per-repo metrics for r1
    for i, (user_id, value) in enumerate([("u1", 20), ("u2", 15), ("u3", 10)], 1):
        metrics.append(
            {
                "year": YEAR,
                "metric_key": "prs_opened",
                "scope": "repo",
                "repo_id": "r1",
                "user_id": user_id,
                "value": value,
                "rank": i,
            }
        )

    return pl.DataFrame(metrics)


def create_sample_repo_health() -> pl.DataFrame:
    """Create sample repo health metrics."""
    return pl.DataFrame(
        {
            "year": [YEAR, YEAR, YEAR],
            "repo_id": ["r1", "r2", "r3"],
            "repo_full_name": ["org/api-service", "org/web-frontend", "org/docs"],
            "active_contributors_30d": [8, 5, 2],
            "active_contributors_90d": [12, 8, 4],
            "active_contributors_365d": [15, 10, 5],
            "prs_opened": [85, 45, 15],
            "prs_merged": [78, 40, 12],
            "issues_opened": [35, 20, 8],
            "issues_closed": [30, 18, 6],
            "review_coverage": [92.0, 85.0, 70.0],
            "median_time_to_first_review": [4.5, 8.2, 12.0],
            "median_time_to_merge": [24.0, 48.0, 72.0],
            "stale_pr_count": [2, 5, 1],
            "stale_issue_count": [3, 2, 1],
        }
    )


def create_sample_hygiene_scores() -> pl.DataFrame:
    """Create sample hygiene scores."""
    return pl.DataFrame(
        {
            "year": [YEAR, YEAR, YEAR],
            "repo_id": ["r1", "r2", "r3"],
            "repo_full_name": ["org/api-service", "org/web-frontend", "org/docs"],
            "score": [85, 72, 55],
            "has_security_md": [True, True, False],
            "has_readme": [True, True, True],
            "has_license": [True, True, False],
            "has_contributing": [True, False, False],
            "has_code_of_conduct": [True, False, False],
            "has_codeowners": [True, True, False],
            "has_workflows": [True, True, False],
            "has_ci_workflows": [True, True, False],
            "branch_protection_enabled": [True, True, False],
            "requires_reviews": [True, False, None],
            "notes": [
                "All hygiene checks passed",
                "Missing CONTRIBUTING.md and CODE_OF_CONDUCT.md",
                "Missing SECURITY.md, LICENSE, and branch protection",
            ],
        }
    )


def create_sample_awards() -> pl.DataFrame:
    """Create sample awards."""
    return pl.DataFrame(
        {
            "award_key": [
                "merge_machine",
                "review_paladin",
                "docs_champion",
                "bus_factor_alarm",
                "stale_queue_dragon",
            ],
            "title": [
                "Merge Machine",
                "Review Paladin",
                "Docs Champion",
                "Bus Factor Alarm",
                "Stale Queue Dragon",
            ],
            "description": [
                "Most pull requests merged this year",
                "Most thorough code reviewer",
                "Most documentation contributions",
                "Repository with high activity but few contributors",
                "Repository with most long-lived open PRs",
            ],
            "category": ["individual", "individual", "individual", "risk", "risk"],
            "winner_user_id": ["u1", "u2", "u3", None, None],
            "winner_repo_id": [None, None, None, "r1", "r2"],
            "winner_name": ["alice", "bob", "charlie", "org/api-service", "org/web-frontend"],
            "supporting_stats": [
                '{"metric": "prs_merged", "value": 42}',
                '{"metric": "reviews_submitted", "value": 78}',
                '{"metric": "docs_commits", "value": 25}',
                '{"contributors": 3, "prs": 85}',
                '{"stale_prs": 5}',
            ],
        }
    )


def create_sample_time_series() -> pl.DataFrame:
    """Create sample time series data with normalized structure."""
    rows = []
    base_date = datetime(YEAR, 1, 1, tzinfo=UTC)

    metrics = [
        ("prs_opened", lambda w: max(5, 15 + (w % 10) - 5)),
        ("prs_merged", lambda w: max(3, 12 + (w % 8) - 4)),
        ("issues_opened", lambda w: max(2, 8 + (w % 6) - 3)),
        ("issues_closed", lambda w: max(1, 6 + (w % 5) - 2)),
        ("reviews_submitted", lambda w: max(8, 20 + (w % 12) - 6)),
        ("comments_total", lambda w: max(15, 35 + (w % 15) - 7)),
    ]

    for week_num in range(1, 53):
        week_start = base_date + timedelta(weeks=week_num - 1)
        week_end = week_start + timedelta(days=6)

        for metric_key, value_fn in metrics:
            rows.append(
                {
                    "year": YEAR,
                    "period_type": "week",
                    "period": f"{YEAR}-W{week_num:02d}",
                    "period_start": week_start.isoformat(),
                    "period_end": week_end.isoformat(),
                    "scope": "org",
                    "repo_id": None,
                    "metric_key": metric_key,
                    "value": value_fn(week_num),
                }
            )

    return pl.DataFrame(rows)


def save_metrics_tables() -> None:
    """Save all metrics tables as Parquet files."""
    print("Creating metrics tables...")

    leaderboard = create_sample_leaderboard()
    leaderboard.write_parquet(METRICS_PATH / "metrics_leaderboard.parquet")
    print(f"  - metrics_leaderboard.parquet ({len(leaderboard)} rows)")

    repo_health = create_sample_repo_health()
    repo_health.write_parquet(METRICS_PATH / "metrics_repo_health.parquet")
    print(f"  - metrics_repo_health.parquet ({len(repo_health)} rows)")

    hygiene_scores = create_sample_hygiene_scores()
    hygiene_scores.write_parquet(METRICS_PATH / "metrics_repo_hygiene_score.parquet")
    print(f"  - metrics_repo_hygiene_score.parquet ({len(hygiene_scores)} rows)")

    awards = create_sample_awards()
    awards.write_parquet(METRICS_PATH / "metrics_awards.parquet")
    print(f"  - metrics_awards.parquet ({len(awards)} rows)")

    time_series = create_sample_time_series()
    time_series.write_parquet(METRICS_PATH / "metrics_time_series.parquet")
    print(f"  - metrics_time_series.parquet ({len(time_series)} rows)")


def save_curated_tables() -> None:
    """Save curated dimension tables."""
    print("Creating curated tables...")

    dim_user = create_sample_users()
    dim_user.write_parquet(CURATED_PATH / "dim_user.parquet")
    print(f"  - dim_user.parquet ({len(dim_user)} rows)")

    dim_repo = create_sample_repos()
    dim_repo.write_parquet(CURATED_PATH / "dim_repo.parquet")
    print(f"  - dim_repo.parquet ({len(dim_repo)} rows)")


def main() -> None:
    """Generate sample data and site."""
    print("=" * 60)
    print("Generating sample data for gh-year-end testing")
    print("=" * 60)

    create_directories()
    save_curated_tables()
    save_metrics_tables()

    print("\nSample data generated successfully!")
    print(f"  Metrics: {METRICS_PATH}")
    print(f"  Curated: {CURATED_PATH}")
    print("\nRun the report command to generate the site:")
    print("  uv run gh-year-end report --config config/config.example.yaml")


if __name__ == "__main__":
    main()
