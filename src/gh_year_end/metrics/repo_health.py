"""Repository health metrics calculator.

Computes repository health metrics from curated Parquet tables.

Metrics include:
- Active contributor counts (30d, 90d, 365d windows)
- PR and issue statistics
- Review coverage
- Time-to-review and time-to-merge medians
- Stale PR and issue counts
"""

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from gh_year_end.config import Config

logger = logging.getLogger(__name__)


def calculate_repo_health(curated_path: Path, config: Config) -> pd.DataFrame:
    """Calculate repository health metrics.

    Reads curated Parquet tables and computes health metrics per repository.

    Args:
        curated_path: Path to curated data directory (data/curated/year=YYYY/).
        config: Application configuration.

    Returns:
        DataFrame with metrics_repo_health schema:
            - repo_id (string): Repository node ID
            - repo_full_name (string): Repository full name
            - year (int): Year of metrics
            - active_contributors_30d (int): Contributors in last 30 days
            - active_contributors_90d (int): Contributors in last 90 days
            - active_contributors_365d (int): Contributors in last 365 days
            - prs_opened (int): Total PRs opened in year
            - prs_merged (int): Total PRs merged in year
            - issues_opened (int): Total issues opened in year
            - issues_closed (int): Total issues closed in year
            - review_coverage (float): % of PRs with at least 1 review
            - median_time_to_first_review (float, nullable): Hours to first review
            - median_time_to_merge (float, nullable): Hours to merge
            - stale_pr_count (int): Open PRs older than 30 days
            - stale_issue_count (int): Open issues older than 30 days
    """
    logger.info("Calculating repository health metrics from %s", curated_path)

    # Read curated tables
    dim_repo = _read_parquet_safe(curated_path / "dim_repo.parquet")
    fact_pr = _read_parquet_safe(curated_path / "fact_pull_request.parquet")
    fact_issue = _read_parquet_safe(curated_path / "fact_issue.parquet")
    fact_review = _read_parquet_safe(curated_path / "fact_review.parquet")
    fact_issue_comment = _read_parquet_safe(curated_path / "fact_issue_comment.parquet")
    fact_review_comment = _read_parquet_safe(curated_path / "fact_review_comment.parquet")

    if dim_repo.empty:
        logger.warning("No repository data found, returning empty metrics")
        return _empty_metrics_dataframe(config.github.windows.year)

    # Calculate end of year for time windows
    year = config.github.windows.year
    year_end = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=UTC)
    window_30d = year_end - timedelta(days=30)
    window_90d = year_end - timedelta(days=90)
    window_365d = year_end - timedelta(days=365)

    # Calculate metrics per repository
    metrics_records = []

    for _, repo in dim_repo.iterrows():
        repo_id = repo["repo_id"]
        repo_full_name = repo["full_name"]

        logger.debug("Calculating metrics for %s", repo_full_name)

        metrics = {
            "repo_id": repo_id,
            "repo_full_name": repo_full_name,
            "year": year,
        }

        # Active contributors (unique authors in time windows)
        metrics.update(
            _calculate_active_contributors(
                repo_id,
                window_30d,
                window_90d,
                window_365d,
                fact_pr,
                fact_issue,
                fact_review,
                fact_issue_comment,
                fact_review_comment,
            )
        )

        # PR statistics
        metrics.update(_calculate_pr_stats(repo_id, fact_pr))

        # Issue statistics
        metrics.update(_calculate_issue_stats(repo_id, fact_issue))

        # Review metrics
        metrics.update(_calculate_review_metrics(repo_id, fact_pr, fact_review))

        # Stale counts
        metrics.update(_calculate_stale_counts(repo_id, fact_pr, fact_issue, year_end))

        metrics_records.append(metrics)

    # Create DataFrame
    df = pd.DataFrame(metrics_records)

    # Sort by repo_id for deterministic output
    df = df.sort_values("repo_id").reset_index(drop=True)

    logger.info("Calculated health metrics for %d repositories", len(df))

    return df


def _read_parquet_safe(path: Path) -> pd.DataFrame:
    """Read Parquet file, return empty DataFrame if not found.

    Args:
        path: Path to Parquet file.

    Returns:
        DataFrame or empty DataFrame if file doesn't exist.
    """
    if not path.exists():
        logger.debug("File not found: %s, returning empty DataFrame", path)
        return pd.DataFrame()

    try:
        table = pq.read_table(path)
        df = table.to_pandas()
        logger.debug("Read %d rows from %s", len(df), path.name)
        return df
    except Exception as e:
        logger.warning("Failed to read %s: %s", path, e)
        return pd.DataFrame()


def _calculate_active_contributors(
    repo_id: str,
    window_30d: datetime,
    window_90d: datetime,
    window_365d: datetime,
    fact_pr: pd.DataFrame,
    fact_issue: pd.DataFrame,
    fact_review: pd.DataFrame,
    fact_issue_comment: pd.DataFrame,
    fact_review_comment: pd.DataFrame,
) -> dict[str, int]:
    """Calculate active contributor counts for time windows.

    Args:
        repo_id: Repository ID.
        window_30d: Start of 30-day window.
        window_90d: Start of 90-day window.
        window_365d: Start of 365-day window.
        fact_pr: PR fact table.
        fact_issue: Issue fact table.
        fact_review: Review fact table.
        fact_issue_comment: Issue comment fact table.
        fact_review_comment: Review comment fact table.

    Returns:
        Dictionary with active_contributors_30d, 90d, 365d counts.
    """
    # Collect all contributor user IDs with their timestamps
    contributors: list[tuple[str | None, datetime | None]] = []

    # PR authors
    repo_prs = fact_pr[fact_pr["repo_id"] == repo_id] if not fact_pr.empty else pd.DataFrame()
    if not repo_prs.empty:
        for _, pr in repo_prs.iterrows():
            if pd.notna(pr["author_user_id"]) and pd.notna(pr["created_at"]):
                contributors.append((pr["author_user_id"], pr["created_at"]))

    # Issue authors
    repo_issues = (
        fact_issue[fact_issue["repo_id"] == repo_id] if not fact_issue.empty else pd.DataFrame()
    )
    if not repo_issues.empty:
        for _, issue in repo_issues.iterrows():
            if pd.notna(issue["author_user_id"]) and pd.notna(issue["created_at"]):
                contributors.append((issue["author_user_id"], issue["created_at"]))

    # Reviewers
    repo_reviews = (
        fact_review[fact_review["repo_id"] == repo_id] if not fact_review.empty else pd.DataFrame()
    )
    if not repo_reviews.empty:
        for _, review in repo_reviews.iterrows():
            if pd.notna(review["reviewer_user_id"]) and pd.notna(review["submitted_at"]):
                contributors.append((review["reviewer_user_id"], review["submitted_at"]))

    # Issue comment authors
    repo_issue_comments = (
        fact_issue_comment[fact_issue_comment["repo_id"] == repo_id]
        if not fact_issue_comment.empty
        else pd.DataFrame()
    )
    if not repo_issue_comments.empty:
        for _, comment in repo_issue_comments.iterrows():
            if pd.notna(comment["author_user_id"]) and pd.notna(comment["created_at"]):
                contributors.append((comment["author_user_id"], comment["created_at"]))

    # Review comment authors
    repo_review_comments = (
        fact_review_comment[fact_review_comment["repo_id"] == repo_id]
        if not fact_review_comment.empty
        else pd.DataFrame()
    )
    if not repo_review_comments.empty:
        for _, comment in repo_review_comments.iterrows():
            if pd.notna(comment["author_user_id"]) and pd.notna(comment["created_at"]):
                contributors.append((comment["author_user_id"], comment["created_at"]))

    # Convert timestamps to timezone-aware if needed
    contributors_with_tz = []
    for user_id, ts in contributors:
        if user_id and ts:
            # Ensure timestamp is timezone-aware
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            elif ts.tzinfo != UTC:
                ts = ts.astimezone(UTC)
            contributors_with_tz.append((user_id, ts))

    # Count unique contributors in each window
    contributors_30d = {user_id for user_id, ts in contributors_with_tz if ts >= window_30d}
    contributors_90d = {user_id for user_id, ts in contributors_with_tz if ts >= window_90d}
    contributors_365d = {user_id for user_id, ts in contributors_with_tz if ts >= window_365d}

    return {
        "active_contributors_30d": len(contributors_30d),
        "active_contributors_90d": len(contributors_90d),
        "active_contributors_365d": len(contributors_365d),
    }


def _calculate_pr_stats(repo_id: str, fact_pr: pd.DataFrame) -> dict[str, int]:
    """Calculate PR statistics.

    Args:
        repo_id: Repository ID.
        fact_pr: PR fact table.

    Returns:
        Dictionary with prs_opened and prs_merged counts.
    """
    if fact_pr.empty:
        return {"prs_opened": 0, "prs_merged": 0}

    repo_prs = fact_pr[fact_pr["repo_id"] == repo_id]

    prs_opened = len(repo_prs)
    prs_merged = len(repo_prs[repo_prs["state"] == "merged"])

    return {
        "prs_opened": prs_opened,
        "prs_merged": prs_merged,
    }


def _calculate_issue_stats(repo_id: str, fact_issue: pd.DataFrame) -> dict[str, int]:
    """Calculate issue statistics.

    Args:
        repo_id: Repository ID.
        fact_issue: Issue fact table.

    Returns:
        Dictionary with issues_opened and issues_closed counts.
    """
    if fact_issue.empty:
        return {"issues_opened": 0, "issues_closed": 0}

    repo_issues = fact_issue[fact_issue["repo_id"] == repo_id]

    issues_opened = len(repo_issues)
    issues_closed = len(repo_issues[repo_issues["state"] == "closed"])

    return {
        "issues_opened": issues_opened,
        "issues_closed": issues_closed,
    }


def _calculate_review_metrics(
    repo_id: str, fact_pr: pd.DataFrame, fact_review: pd.DataFrame
) -> dict[str, Any]:
    """Calculate review-related metrics.

    Args:
        repo_id: Repository ID.
        fact_pr: PR fact table.
        fact_review: Review fact table.

    Returns:
        Dictionary with review_coverage, median_time_to_first_review,
        and median_time_to_merge.
    """
    if fact_pr.empty:
        return {
            "review_coverage": 0.0,
            "median_time_to_first_review": None,
            "median_time_to_merge": None,
        }

    repo_prs = fact_pr[fact_pr["repo_id"] == repo_id].copy()

    # Review coverage: % of PRs with at least one review
    if len(repo_prs) == 0:
        review_coverage = 0.0
    else:
        if not fact_review.empty:
            repo_reviews = fact_review[fact_review["repo_id"] == repo_id]
            # Note: reviews don't have pr_id linked, need to match via repo
            # For now, use simplified logic
            prs_with_reviews = (
                len(repo_reviews["pr_id"].dropna().unique())
                if "pr_id" in repo_reviews.columns
                else 0
            )
            review_coverage = (prs_with_reviews / len(repo_prs)) * 100.0
        else:
            review_coverage = 0.0

    # Time to first review (requires joining reviews to PRs)
    # Since reviews may not have pr_id, skip this for now
    median_time_to_first_review = None

    # Time to merge: merged_at - created_at for merged PRs
    merged_prs = repo_prs[
        (repo_prs["state"] == "merged")
        & pd.notna(repo_prs["merged_at"])
        & pd.notna(repo_prs["created_at"])
    ]

    if not merged_prs.empty:
        # Calculate hours to merge
        time_to_merge_hours = []
        for _, pr in merged_prs.iterrows():
            created = pr["created_at"]
            merged = pr["merged_at"]

            # Ensure timezone-aware
            if created.tzinfo is None:
                created = created.replace(tzinfo=UTC)
            if merged.tzinfo is None:
                merged = merged.replace(tzinfo=UTC)

            delta = merged - created
            hours = delta.total_seconds() / 3600.0
            if hours >= 0:  # Sanity check
                time_to_merge_hours.append(hours)

        if time_to_merge_hours:
            median_time_to_merge = float(pd.Series(time_to_merge_hours).median())
        else:
            median_time_to_merge = None
    else:
        median_time_to_merge = None

    return {
        "review_coverage": review_coverage,
        "median_time_to_first_review": median_time_to_first_review,
        "median_time_to_merge": median_time_to_merge,
    }


def _calculate_stale_counts(
    repo_id: str,
    fact_pr: pd.DataFrame,
    fact_issue: pd.DataFrame,
    year_end: datetime,
) -> dict[str, int]:
    """Calculate stale PR and issue counts.

    Stale = open and created more than 30 days ago.

    Args:
        repo_id: Repository ID.
        fact_pr: PR fact table.
        fact_issue: Issue fact table.
        year_end: End of year timestamp.

    Returns:
        Dictionary with stale_pr_count and stale_issue_count.
    """
    stale_threshold = year_end - timedelta(days=30)

    # Stale PRs: state=open and created_at < threshold
    stale_prs = 0
    if not fact_pr.empty:
        repo_prs = fact_pr[
            (fact_pr["repo_id"] == repo_id)
            & (fact_pr["state"] == "open")
            & pd.notna(fact_pr["created_at"])
        ]

        for _, pr in repo_prs.iterrows():
            created = pr["created_at"]
            # Ensure timezone-aware
            if created.tzinfo is None:
                created = created.replace(tzinfo=UTC)
            elif created.tzinfo != UTC:
                created = created.astimezone(UTC)

            if created < stale_threshold:
                stale_prs += 1

    # Stale issues: state=open and created_at < threshold
    stale_issues = 0
    if not fact_issue.empty:
        repo_issues = fact_issue[
            (fact_issue["repo_id"] == repo_id)
            & (fact_issue["state"] == "open")
            & pd.notna(fact_issue["created_at"])
        ]

        for _, issue in repo_issues.iterrows():
            created = issue["created_at"]
            # Ensure timezone-aware
            if created.tzinfo is None:
                created = created.replace(tzinfo=UTC)
            elif created.tzinfo != UTC:
                created = created.astimezone(UTC)

            if created < stale_threshold:
                stale_issues += 1

    return {
        "stale_pr_count": stale_prs,
        "stale_issue_count": stale_issues,
    }


def _empty_metrics_dataframe(year: int) -> pd.DataFrame:
    """Create empty DataFrame with correct schema.

    Args:
        year: Year for metrics.

    Returns:
        Empty DataFrame with metrics_repo_health schema.
    """
    return pd.DataFrame(
        columns=[
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
        ]
    )
