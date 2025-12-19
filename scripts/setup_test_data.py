#!/usr/bin/env python3
"""Generate minimal test data for gh-year-end website testing.

Creates realistic sample data in Parquet format for rapid local development
and testing without requiring full data collection.

Usage:
    python scripts/setup_test_data.py --year 2024 --output data/metrics/year=2024

Default output: data/metrics/year=2024/
"""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Sample users for test data
SAMPLE_USERS = [
    {"user_id": "U_001", "login": "alice", "display_name": "Alice Smith", "is_bot": False},
    {"user_id": "U_002", "login": "bob", "display_name": "Bob Johnson", "is_bot": False},
    {"user_id": "U_003", "login": "charlie", "display_name": "Charlie Davis", "is_bot": False},
    {"user_id": "U_004", "login": "diana", "display_name": "Diana Miller", "is_bot": False},
    {"user_id": "U_005", "login": "eve", "display_name": "Eve Wilson", "is_bot": False},
    {"user_id": "U_006", "login": "frank", "display_name": "Frank Moore", "is_bot": False},
    {"user_id": "U_007", "login": "grace", "display_name": "Grace Taylor", "is_bot": False},
    {"user_id": "U_008", "login": "henry", "display_name": "Henry Anderson", "is_bot": False},
    {"user_id": "U_009", "login": "iris", "display_name": "Iris Thomas", "is_bot": False},
    {"user_id": "U_010", "login": "jack", "display_name": "Jack Jackson", "is_bot": False},
]

# Sample repositories
SAMPLE_REPOS = [
    {"repo_id": "R_001", "full_name": "test-org/backend-api", "description": "Backend API service"},
    {"repo_id": "R_002", "full_name": "test-org/frontend-web", "description": "Frontend web app"},
    {"repo_id": "R_003", "full_name": "test-org/mobile-app", "description": "Mobile application"},
    {"repo_id": "R_004", "full_name": "test-org/data-pipeline", "description": "Data pipeline"},
    {"repo_id": "R_005", "full_name": "test-org/docs-site", "description": "Documentation site"},
]


def generate_leaderboard_metrics(year: int) -> pd.DataFrame:
    """Generate metrics_leaderboard.parquet.

    Schema:
        - year (int32): Year
        - metric_key (string): Metric identifier
        - scope (string): "org" or "repo"
        - repo_id (string, nullable): Repository ID for repo scope
        - user_id (string): User ID
        - value (int64): Metric value
        - rank (int32): Rank within scope
    """
    records = []

    # Metrics to generate: prs_opened, prs_merged, reviews_submitted, approvals,
    # changes_requested, issues_opened, issues_closed, comments_total, review_comments_total
    metrics = [
        ("prs_opened", [45, 38, 32, 28, 22, 18, 15, 12, 10, 8]),
        ("prs_merged", [42, 35, 30, 25, 20, 16, 14, 11, 9, 7]),
        ("reviews_submitted", [55, 48, 40, 35, 28, 22, 18, 14, 10, 6]),
        ("approvals", [50, 43, 38, 32, 25, 20, 16, 12, 9, 5]),
        ("changes_requested", [15, 12, 10, 8, 6, 5, 4, 3, 2, 1]),
        ("issues_opened", [25, 20, 18, 15, 12, 10, 8, 6, 4, 2]),
        ("issues_closed", [22, 18, 16, 13, 11, 9, 7, 5, 3, 2]),
        ("comments_total", [120, 98, 85, 72, 60, 48, 38, 28, 20, 12]),
        ("review_comments_total", [80, 65, 55, 45, 38, 30, 24, 18, 12, 8]),
    ]

    for metric_key, values in metrics:
        # Org-wide leaderboard
        for rank, (user, value) in enumerate(zip(SAMPLE_USERS, values, strict=False), start=1):
            records.append(
                {
                    "year": year,
                    "metric_key": metric_key,
                    "scope": "org",
                    "repo_id": None,
                    "user_id": user["user_id"],
                    "value": value,
                    "rank": rank,
                }
            )

        # Per-repo leaderboards (top 3 repos, top 5 users each)
        for repo in SAMPLE_REPOS[:3]:
            for rank, (user, base_value) in enumerate(
                zip(SAMPLE_USERS[:5], values[:5], strict=False), start=1
            ):
                # Vary per-repo values slightly
                repo_value = int(base_value * 0.3)
                records.append(
                    {
                        "year": year,
                        "metric_key": metric_key,
                        "scope": "repo",
                        "repo_id": repo["repo_id"],
                        "user_id": user["user_id"],
                        "value": repo_value,
                        "rank": rank,
                    }
                )

    df = pd.DataFrame(records)
    return df


def generate_time_series_metrics(year: int) -> pd.DataFrame:
    """Generate metrics_time_series.parquet.

    Schema:
        - year (int32): Year
        - period_type (string): "week" or "month"
        - period_start (date): Start of period
        - period_end (date): End of period
        - scope (string): "org" or "repo"
        - repo_id (string, nullable): Repository ID for repo scope
        - metric_key (string): Metric identifier
        - value (int64): Count for period
    """
    records = []

    # Generate weekly data for full year
    start_date = datetime(year, 1, 1, tzinfo=UTC)
    end_date = datetime(year + 1, 1, 1, tzinfo=UTC)

    metrics = ["prs_opened", "prs_merged", "issues_opened", "issues_closed", "reviews_submitted"]

    # Weekly org-wide metrics
    current = start_date
    while current < end_date:
        week_end = min(current + timedelta(days=7), end_date)

        for metric_key in metrics:
            # Simulate realistic weekly variation
            base_value = 8
            variation = hash((current.isocalendar()[1], metric_key)) % 6
            value = base_value + variation

            records.append(
                {
                    "year": year,
                    "period_type": "week",
                    "period_start": current.date(),
                    "period_end": week_end.date(),
                    "scope": "org",
                    "repo_id": None,
                    "metric_key": metric_key,
                    "value": value,
                }
            )

        current = week_end

    # Monthly org-wide metrics
    for month in range(1, 13):
        month_start = datetime(year, month, 1, tzinfo=UTC)
        if month == 12:
            month_end = datetime(year + 1, 1, 1, tzinfo=UTC)
        else:
            month_end = datetime(year, month + 1, 1, tzinfo=UTC)

        for metric_key in metrics:
            # Monthly values are ~4x weekly
            value = 35 + (hash((month, metric_key)) % 15)

            records.append(
                {
                    "year": year,
                    "period_type": "month",
                    "period_start": month_start.date(),
                    "period_end": month_end.date(),
                    "scope": "org",
                    "repo_id": None,
                    "metric_key": metric_key,
                    "value": value,
                }
            )

    # Per-repo weekly metrics (just top 2 repos for brevity)
    current = start_date
    while current < end_date:
        week_end = min(current + timedelta(days=7), end_date)

        for repo in SAMPLE_REPOS[:2]:
            for metric_key in metrics:
                value = 2 + (hash((current.isocalendar()[1], repo["repo_id"], metric_key)) % 4)

                records.append(
                    {
                        "year": year,
                        "period_type": "week",
                        "period_start": current.date(),
                        "period_end": week_end.date(),
                        "scope": "repo",
                        "repo_id": repo["repo_id"],
                        "metric_key": metric_key,
                        "value": value,
                    }
                )

        current = week_end

    df = pd.DataFrame(records)
    return df


def generate_repo_health_metrics(year: int) -> pd.DataFrame:
    """Generate metrics_repo_health.parquet.

    Schema:
        - repo_id (string): Repository ID
        - repo_full_name (string): Repository full name
        - year (int32): Year
        - active_contributors_30d (int32): Contributors in last 30 days
        - active_contributors_90d (int32): Contributors in last 90 days
        - active_contributors_365d (int32): Contributors in last 365 days
        - prs_opened (int32): PRs opened in year
        - prs_merged (int32): PRs merged in year
        - issues_opened (int32): Issues opened in year
        - issues_closed (int32): Issues closed in year
        - review_coverage (float32): % of PRs with reviews
        - median_time_to_first_review (float32, nullable): Hours to first review
        - median_time_to_merge (float32, nullable): Hours to merge
        - stale_pr_count (int32): Open PRs older than 30 days
        - stale_issue_count (int32): Open issues older than 30 days
    """
    records = []

    for idx, repo in enumerate(SAMPLE_REPOS):
        # Vary metrics by repo
        base = 100 - (idx * 15)

        records.append(
            {
                "repo_id": repo["repo_id"],
                "repo_full_name": repo["full_name"],
                "year": year,
                "active_contributors_30d": 3 + idx,
                "active_contributors_90d": 5 + idx,
                "active_contributors_365d": 8 + idx,
                "prs_opened": base + 20,
                "prs_merged": base + 15,
                "issues_opened": base // 2,
                "issues_closed": (base // 2) - 5,
                "review_coverage": 85.0 - (idx * 5.0),
                "median_time_to_first_review": 2.5 + (idx * 0.5),  # hours
                "median_time_to_merge": 24.0 + (idx * 6.0),  # hours
                "stale_pr_count": idx * 2,
                "stale_issue_count": idx * 3,
            }
        )

    df = pd.DataFrame(records)
    return df


def generate_hygiene_score_metrics(year: int) -> pd.DataFrame:
    """Generate metrics_repo_hygiene_score.parquet.

    Schema:
        - repo_id (string): Repository ID
        - repo_full_name (string): Repository full name
        - year (int32): Year
        - score (int32): Hygiene score 0-100
        - has_readme (bool): README exists
        - has_license (bool): LICENSE exists
        - has_contributing (bool): CONTRIBUTING exists
        - has_code_of_conduct (bool): CODE_OF_CONDUCT exists
        - has_security_md (bool): SECURITY.md exists
        - has_codeowners (bool): CODEOWNERS exists
        - has_ci_workflows (bool): CI workflows exist
        - branch_protection_enabled (bool, nullable): Branch protection enabled
        - requires_reviews (bool, nullable): Requires PR reviews
        - dependabot_enabled (bool, nullable): Dependabot alerts enabled
        - secret_scanning_enabled (bool, nullable): Secret scanning enabled
        - notes (string): Issues/warnings
    """
    records = []

    for idx, repo in enumerate(SAMPLE_REPOS):
        # Vary hygiene by repo (first repos are better)
        has_all = idx < 2

        score = 85 - (idx * 10)

        records.append(
            {
                "repo_id": repo["repo_id"],
                "repo_full_name": repo["full_name"],
                "year": year,
                "score": score,
                "has_readme": True,
                "has_license": has_all or idx < 3,
                "has_contributing": has_all or idx < 3,
                "has_code_of_conduct": has_all,
                "has_security_md": has_all or idx < 4,
                "has_codeowners": has_all or idx < 3,
                "has_ci_workflows": True,
                "branch_protection_enabled": has_all,
                "requires_reviews": has_all or idx < 3,
                "dependabot_enabled": has_all or idx < 4,
                "secret_scanning_enabled": has_all,
                "notes": "" if has_all else "Missing some documentation files",
            }
        )

    df = pd.DataFrame(records)
    return df


def generate_awards_metrics(year: int) -> pd.DataFrame:
    """Generate metrics_awards.parquet.

    Schema:
        - award_key (string): Unique identifier
        - title (string): Display title
        - description (string): Award description
        - category (string): "individual", "repository", or "risk"
        - winner_user_id (string, nullable): User ID for individual awards
        - winner_repo_id (string, nullable): Repository ID for repo awards
        - winner_name (string): Display name
        - supporting_stats (string): JSON stats
    """
    records = [
        {
            "award_key": "top_contributor",
            "title": "Top Contributor",
            "description": "Most PRs merged in the year",
            "category": "individual",
            "winner_user_id": SAMPLE_USERS[0]["user_id"],
            "winner_repo_id": None,
            "winner_name": SAMPLE_USERS[0]["login"],
            "supporting_stats": json.dumps({"prs_merged": 42, "reviews_submitted": 55}),
        },
        {
            "award_key": "review_champion",
            "title": "Review Champion",
            "description": "Most code reviews submitted",
            "category": "individual",
            "winner_user_id": SAMPLE_USERS[0]["user_id"],
            "winner_repo_id": None,
            "winner_name": SAMPLE_USERS[0]["login"],
            "supporting_stats": json.dumps({"reviews_submitted": 55, "approvals": 50}),
        },
        {
            "award_key": "most_active_repo",
            "title": "Most Active Repository",
            "description": "Repository with most PR activity",
            "category": "repository",
            "winner_user_id": None,
            "winner_repo_id": SAMPLE_REPOS[0]["repo_id"],
            "winner_name": SAMPLE_REPOS[0]["full_name"],
            "supporting_stats": json.dumps({"prs_merged": 120, "prs_opened": 135}),
        },
        {
            "award_key": "best_hygiene",
            "title": "Best Repository Hygiene",
            "description": "Highest hygiene score",
            "category": "repository",
            "winner_user_id": None,
            "winner_repo_id": SAMPLE_REPOS[0]["repo_id"],
            "winner_name": SAMPLE_REPOS[0]["full_name"],
            "supporting_stats": json.dumps({"hygiene_score": 85}),
        },
        {
            "award_key": "stale_prs",
            "title": "Stale Pull Requests",
            "description": "Repository with most stale PRs",
            "category": "risk",
            "winner_user_id": None,
            "winner_repo_id": SAMPLE_REPOS[4]["repo_id"],
            "winner_name": SAMPLE_REPOS[4]["full_name"],
            "supporting_stats": json.dumps({"stale_pr_count": 8, "stale_issue_count": 12}),
        },
    ]

    df = pd.DataFrame(records)
    return df


def generate_dim_user() -> pd.DataFrame:
    """Generate dim_user.parquet for enriching leaderboards with user info.

    Schema:
        - user_id (string): Unique user ID
        - login (string): GitHub login
        - display_name (string): Display name
        - avatar_url (string): GitHub avatar URL
    """
    records = []
    for user in SAMPLE_USERS:
        records.append(
            {
                "user_id": user["user_id"],
                "login": user["login"],
                "display_name": user["display_name"],
                "avatar_url": f"https://github.com/{user['login']}.png",
            }
        )
    return pd.DataFrame(records)


def generate_dim_repo() -> pd.DataFrame:
    """Generate dim_repo.parquet for enriching data with repo info.

    Schema:
        - repo_id (string): Unique repo ID
        - full_name (string): Full repo name (owner/repo)
        - name (string): Short repo name
        - description (string): Repository description
    """
    records = []
    for repo in SAMPLE_REPOS:
        name = repo["full_name"].split("/")[-1] if "/" in repo["full_name"] else repo["full_name"]
        records.append(
            {
                "repo_id": repo["repo_id"],
                "full_name": repo["full_name"],
                "name": name,
                "description": repo["description"],
            }
        )
    return pd.DataFrame(records)


def write_parquet(df: pd.DataFrame, path: Path, schema: pa.Schema | None = None) -> None:
    """Write DataFrame to Parquet with optional schema enforcement.

    Args:
        df: Pandas DataFrame to write.
        path: Output file path.
        schema: Optional PyArrow schema for validation.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(df, schema=schema)
    pq.write_table(table, path, compression="snappy")

    print(f"Written {len(df)} records to {path}")


def main() -> None:
    """Generate test data files."""
    parser = argparse.ArgumentParser(description="Generate test data for gh-year-end")
    parser.add_argument("--year", type=int, default=2024, help="Year for test data")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/metrics/year=2024"),
        help="Output directory for Parquet files",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Root data directory (for dimension tables)",
    )

    args = parser.parse_args()

    output_dir = args.output
    year = args.year
    data_root = args.data_root

    print(f"Generating test data for year {year}")
    print(f"Output directory: {output_dir}")

    # Generate and write each metrics table
    datasets: dict[str, tuple[pd.DataFrame, str]] = {
        "metrics_leaderboard.parquet": (generate_leaderboard_metrics(year), "leaderboard"),
        "metrics_time_series.parquet": (generate_time_series_metrics(year), "time series"),
        "metrics_repo_health.parquet": (generate_repo_health_metrics(year), "repo health"),
        "metrics_repo_hygiene_score.parquet": (
            generate_hygiene_score_metrics(year),
            "hygiene scores",
        ),
        "metrics_awards.parquet": (generate_awards_metrics(year), "awards"),
    }

    for filename, (df, description) in datasets.items():
        output_path = output_dir / filename
        write_parquet(df, output_path)
        print(f"  {description}: {len(df)} records")

    # Generate dimension tables in curated directory
    curated_dir = data_root / f"curated/year={year}"
    print(f"\nGenerating dimension tables in {curated_dir}")

    dim_user_df = generate_dim_user()
    write_parquet(dim_user_df, curated_dir / "dim_user.parquet")
    print(f"  dim_user: {len(dim_user_df)} records")

    dim_repo_df = generate_dim_repo()
    write_parquet(dim_repo_df, curated_dir / "dim_repo.parquet")
    print(f"  dim_repo: {len(dim_repo_df)} records")

    print("\nTest data generation complete!")
    print(f"Total files: {len(datasets) + 2}")
    print("\nTo use this data, run:")
    print("  gh-year-end report --config config/config.yaml")


if __name__ == "__main__":
    main()
