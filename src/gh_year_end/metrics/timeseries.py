"""Time series metrics calculator.

Generates time series metrics from curated Parquet tables, tracking activity
over weekly and monthly periods for both organization-wide and per-repo scopes.

Schema (metrics_time_series):
    - year (int32): Year for partitioning
    - period_type (string): "week" or "month"
    - period_start (date): Start of the period
    - period_end (date): End of the period
    - scope (string): "org" or "repo"
    - repo_id (string, nullable): Repository node ID (null for org scope)
    - metric_key (string): Metric identifier
    - value (int64): Count for that period

Metrics tracked:
    - prs_opened: PRs created in period
    - prs_merged: PRs merged in period
    - prs_closed: PRs closed (not merged) in period
    - issues_opened: Issues opened in period
    - issues_closed: Issues closed in period
    - reviews_submitted: Reviews submitted in period
    - comments_total: All comments (issue + review) in period
    - commits_count: Commits authored in period
"""

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from gh_year_end.config import Config

logger = logging.getLogger(__name__)


def calculate_time_series(curated_path: Path, config: Config) -> pd.DataFrame:
    """Calculate time series metrics from curated tables.

    Reads fact tables with timestamps, groups by week and month periods,
    and calculates activity counts for both org-wide and per-repo scopes.

    Args:
        curated_path: Path to curated data directory (data/curated/year=YYYY/).
        config: Application configuration.

    Returns:
        DataFrame with metrics_time_series schema, sorted deterministically.

    Raises:
        FileNotFoundError: If required curated tables don't exist.
    """
    logger.info("Calculating time series metrics from %s", curated_path)

    year = config.github.windows.year
    records: list[dict[str, Any]] = []

    # Process PRs (opened, merged, closed)
    pr_path = curated_path / "fact_pull_request.parquet"
    if pr_path.exists():
        logger.debug("Processing PR time series")
        pr_df = pq.read_table(pr_path).to_pandas()
        records.extend(_process_prs(pr_df, year))
    else:
        logger.warning("fact_pull_request.parquet not found, skipping PR metrics")

    # Process Issues (opened, closed)
    issue_path = curated_path / "fact_issue.parquet"
    if issue_path.exists():
        logger.debug("Processing issue time series")
        issue_df = pq.read_table(issue_path).to_pandas()
        records.extend(_process_issues(issue_df, year))
    else:
        logger.warning("fact_issue.parquet not found, skipping issue metrics")

    # Process Reviews (submitted)
    review_path = curated_path / "fact_review.parquet"
    if review_path.exists():
        logger.debug("Processing review time series")
        review_df = pq.read_table(review_path).to_pandas()
        records.extend(_process_reviews(review_df, year))
    else:
        logger.warning("fact_review.parquet not found, skipping review metrics")

    # Process Comments (issue + review comments)
    issue_comment_path = curated_path / "fact_issue_comment.parquet"
    review_comment_path = curated_path / "fact_review_comment.parquet"

    comment_records = []
    if issue_comment_path.exists():
        logger.debug("Processing issue comment time series")
        issue_comment_df = pq.read_table(issue_comment_path).to_pandas()
        comment_records.extend(_process_comments(issue_comment_df, year, "issue"))
    else:
        logger.warning("fact_issue_comment.parquet not found")

    if review_comment_path.exists():
        logger.debug("Processing review comment time series")
        review_comment_df = pq.read_table(review_comment_path).to_pandas()
        comment_records.extend(_process_comments(review_comment_df, year, "review"))
    else:
        logger.warning("fact_review_comment.parquet not found")

    records.extend(comment_records)

    # Process Commits (authored)
    commit_path = curated_path / "fact_commit.parquet"
    if commit_path.exists():
        logger.debug("Processing commit time series")
        commit_df = pq.read_table(commit_path).to_pandas()
        records.extend(_process_commits(commit_df, year))
    else:
        logger.warning("fact_commit.parquet not found, skipping commit metrics")

    if not records:
        logger.warning("No time series metrics calculated, returning empty DataFrame")
        return _empty_time_series_df()

    logger.info("Calculated %d time series metric records", len(records))

    # Convert to DataFrame
    df = pd.DataFrame(records)

    # Ensure deterministic ordering: year, period_type, period_start, scope, repo_id, metric_key
    df = df.sort_values(
        ["year", "period_type", "period_start", "scope", "repo_id", "metric_key"]
    ).reset_index(drop=True)

    return df


def _process_prs(pr_df: pd.DataFrame, year: int) -> list[dict[str, Any]]:
    """Process PR fact table to generate time series metrics.

    Args:
        pr_df: DataFrame from fact_pull_request.
        year: Year for partitioning.

    Returns:
        List of time series records for PRs.
    """
    records: list[dict[str, Any]] = []

    # PRs opened (use created_at)
    if "created_at" in pr_df.columns:
        records.extend(
            _aggregate_by_timestamp(
                pr_df,
                timestamp_col="created_at",
                metric_key="prs_opened",
                year=year,
            )
        )

    # PRs merged (use merged_at, filter non-null)
    if "merged_at" in pr_df.columns:
        merged_df = pr_df[pr_df["merged_at"].notna()].copy()
        if not merged_df.empty:
            records.extend(
                _aggregate_by_timestamp(
                    merged_df,
                    timestamp_col="merged_at",
                    metric_key="prs_merged",
                    year=year,
                )
            )

    # PRs closed (not merged) - use closed_at, filter out merged PRs
    if "closed_at" in pr_df.columns and "state" in pr_df.columns:
        closed_not_merged_df = pr_df[
            (pr_df["closed_at"].notna()) & (pr_df["state"] == "closed")
        ].copy()
        if not closed_not_merged_df.empty:
            records.extend(
                _aggregate_by_timestamp(
                    closed_not_merged_df,
                    timestamp_col="closed_at",
                    metric_key="prs_closed",
                    year=year,
                )
            )

    return records


def _process_issues(issue_df: pd.DataFrame, year: int) -> list[dict[str, Any]]:
    """Process issue fact table to generate time series metrics.

    Args:
        issue_df: DataFrame from fact_issue.
        year: Year for partitioning.

    Returns:
        List of time series records for issues.
    """
    records: list[dict[str, Any]] = []

    # Issues opened (use created_at)
    if "created_at" in issue_df.columns:
        records.extend(
            _aggregate_by_timestamp(
                issue_df,
                timestamp_col="created_at",
                metric_key="issues_opened",
                year=year,
            )
        )

    # Issues closed (use closed_at, filter non-null)
    if "closed_at" in issue_df.columns:
        closed_df = issue_df[issue_df["closed_at"].notna()].copy()
        if not closed_df.empty:
            records.extend(
                _aggregate_by_timestamp(
                    closed_df,
                    timestamp_col="closed_at",
                    metric_key="issues_closed",
                    year=year,
                )
            )

    return records


def _process_reviews(review_df: pd.DataFrame, year: int) -> list[dict[str, Any]]:
    """Process review fact table to generate time series metrics.

    Args:
        review_df: DataFrame from fact_review.
        year: Year for partitioning.

    Returns:
        List of time series records for reviews.
    """
    records: list[dict[str, Any]] = []

    # Reviews submitted (use submitted_at)
    if "submitted_at" in review_df.columns:
        records.extend(
            _aggregate_by_timestamp(
                review_df,
                timestamp_col="submitted_at",
                metric_key="reviews_submitted",
                year=year,
            )
        )

    return records


def _process_comments(
    comment_df: pd.DataFrame, year: int, comment_type: str
) -> list[dict[str, Any]]:
    """Process comment fact table to generate time series metrics.

    Args:
        comment_df: DataFrame from fact_issue_comment or fact_review_comment.
        year: Year for partitioning.
        comment_type: "issue" or "review" for logging.

    Returns:
        List of time series records for comments.
    """
    records: list[dict[str, Any]] = []

    # Comments total (use created_at)
    if "created_at" in comment_df.columns:
        records.extend(
            _aggregate_by_timestamp(
                comment_df,
                timestamp_col="created_at",
                metric_key="comments_total",
                year=year,
            )
        )

    return records


def _process_commits(commit_df: pd.DataFrame, year: int) -> list[dict[str, Any]]:
    """Process commit fact table to generate time series metrics.

    Args:
        commit_df: DataFrame from fact_commit.
        year: Year for partitioning.

    Returns:
        List of time series records for commits.
    """
    records: list[dict[str, Any]] = []

    # Commits count (use authored_at)
    if "authored_at" in commit_df.columns:
        records.extend(
            _aggregate_by_timestamp(
                commit_df,
                timestamp_col="authored_at",
                metric_key="commits_count",
                year=year,
            )
        )

    return records


def _aggregate_by_timestamp(
    df: pd.DataFrame,
    timestamp_col: str,
    metric_key: str,
    year: int,
) -> list[dict[str, Any]]:
    """Aggregate DataFrame by timestamp into weekly and monthly periods.

    Generates both org-wide and per-repo time series.

    Args:
        df: DataFrame with timestamp and repo_id columns.
        timestamp_col: Name of the timestamp column to group by.
        metric_key: Metric identifier (e.g., "prs_opened").
        year: Year for partitioning.

    Returns:
        List of time series records for both weekly and monthly periods.
    """
    records: list[dict[str, Any]] = []

    # Filter out null timestamps
    df = df[df[timestamp_col].notna()].copy()
    if df.empty:
        return records

    # Convert timestamp to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])

    # Generate weekly time series
    records.extend(_aggregate_by_period(df, timestamp_col, metric_key, year, period_type="week"))

    # Generate monthly time series
    records.extend(_aggregate_by_period(df, timestamp_col, metric_key, year, period_type="month"))

    return records


def _aggregate_by_period(
    df: pd.DataFrame,
    timestamp_col: str,
    metric_key: str,
    year: int,
    period_type: Literal["week", "month"],
) -> list[dict[str, Any]]:
    """Aggregate DataFrame by period (week or month).

    Args:
        df: DataFrame with timestamp and repo_id columns.
        timestamp_col: Name of the timestamp column.
        metric_key: Metric identifier.
        year: Year for partitioning.
        period_type: "week" or "month".

    Returns:
        List of time series records for the specified period type.
    """
    records: list[dict[str, Any]] = []

    # Extract date from timestamp
    df = df.copy()
    df["_date"] = df[timestamp_col].dt.date

    # Calculate period bounds for each row (as strings for grouping stability)
    if period_type == "week":
        df["_period_start_str"] = df["_date"].apply(lambda d: _get_week_start(d).isoformat())
        df["_period_end_str"] = df["_date"].apply(lambda d: _get_week_end(d).isoformat())
        df["_period_start"] = df["_date"].apply(_get_week_start)
        df["_period_end"] = df["_date"].apply(_get_week_end)
    else:  # month
        df["_period_start_str"] = df["_date"].apply(lambda d: _get_month_start(d).isoformat())
        df["_period_end_str"] = df["_date"].apply(lambda d: _get_month_end(d).isoformat())
        df["_period_start"] = df["_date"].apply(_get_month_start)
        df["_period_end"] = df["_date"].apply(_get_month_end)

    # Org-wide aggregation (scope="org", repo_id=null)
    # Group by string representations to avoid pandas grouping issues
    org_agg = (
        df.groupby(["_period_start_str", "_period_end_str", "_period_start", "_period_end"])
        .size()
        .reset_index(name="value")
    )

    for _, row in org_agg.iterrows():
        records.append(
            {
                "year": year,
                "period_type": period_type,
                "period_start": row["_period_start"],
                "period_end": row["_period_end"],
                "scope": "org",
                "repo_id": None,
                "metric_key": metric_key,
                "value": int(row["value"]),
            }
        )

    # Per-repo aggregation (scope="repo")
    if "repo_id" in df.columns:
        # Filter out null repo_ids
        repo_df = df[df["repo_id"].notna()].copy()
        if not repo_df.empty:
            repo_agg = (
                repo_df.groupby(
                    [
                        "repo_id",
                        "_period_start_str",
                        "_period_end_str",
                        "_period_start",
                        "_period_end",
                    ]
                )
                .size()
                .reset_index(name="value")
            )

            for _, row in repo_agg.iterrows():
                records.append(
                    {
                        "year": year,
                        "period_type": period_type,
                        "period_start": row["_period_start"],
                        "period_end": row["_period_end"],
                        "scope": "repo",
                        "repo_id": row["repo_id"],
                        "metric_key": metric_key,
                        "value": int(row["value"]),
                    }
                )

    return records


def _get_week_start(d: date) -> date:
    """Get the start date of the ISO week for a given date.

    ISO week starts on Monday.

    Args:
        d: Input date.

    Returns:
        Start date of the week (Monday).
    """
    # ISO weekday: Monday=1, Sunday=7
    weekday = d.isoweekday()
    # Calculate days to subtract to get to Monday
    days_to_monday = weekday - 1
    return d - timedelta(days=days_to_monday)


def _get_week_end(d: date) -> date:
    """Get the end date of the ISO week for a given date.

    ISO week ends on Sunday.

    Args:
        d: Input date.

    Returns:
        End date of the week (Sunday).
    """
    week_start = _get_week_start(d)
    return week_start + timedelta(days=6)


def _get_month_start(d: date) -> date:
    """Get the start date of the month for a given date.

    Args:
        d: Input date.

    Returns:
        First day of the month.
    """
    return date(d.year, d.month, 1)


def _get_month_end(d: date) -> date:
    """Get the end date of the month for a given date.

    Args:
        d: Input date.

    Returns:
        Last day of the month.
    """
    # Get first day of next month, then subtract one day
    next_month_start = date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)
    return next_month_start - timedelta(days=1)


def _empty_time_series_df() -> pd.DataFrame:
    """Create empty DataFrame with correct time series schema.

    Returns:
        Empty DataFrame with metrics_time_series columns.
    """
    return pd.DataFrame(
        columns=[
            "year",
            "period_type",
            "period_start",
            "period_end",
            "scope",
            "repo_id",
            "metric_key",
            "value",
        ]
    )


def save_time_series_metrics(df: pd.DataFrame, output_path: Path) -> None:
    """Save time series metrics DataFrame to Parquet.

    Args:
        df: Time series metrics DataFrame.
        output_path: Path to write Parquet file.
    """
    # Define PyArrow schema
    schema = pa.schema(
        [
            pa.field("year", pa.int32()),
            pa.field("period_type", pa.string()),
            pa.field("period_start", pa.date32()),
            pa.field("period_end", pa.date32()),
            pa.field("scope", pa.string()),
            pa.field("repo_id", pa.string()),  # Nullable
            pa.field("metric_key", pa.string()),
            pa.field("value", pa.int64()),
        ]
    )

    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df, schema=schema)

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write to Parquet
    pq.write_table(table, output_path, compression="snappy", use_dictionary=False)

    logger.info("Wrote %d time series records to %s", len(df), output_path)
