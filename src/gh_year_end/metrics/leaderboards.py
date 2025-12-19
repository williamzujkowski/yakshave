"""Leaderboard metrics calculator.

Computes leaderboards for various activity metrics across users.
Produces metrics_leaderboard table with rankings at org and repo scopes.

Schema:
    - year (int): Year for partitioning
    - metric_key (string): e.g., "prs_opened", "prs_merged", "reviews_submitted"
    - scope (string): "org" or "repo"
    - repo_id (string, nullable): null for org-wide, repo_id for per-repo
    - user_id (string): User's node ID
    - value (int): Count/metric value
    - rank (int): 1-based rank within scope (dense ranking)
"""

import logging
from pathlib import Path

import polars as pl

from gh_year_end.config import Config

logger = logging.getLogger(__name__)


def calculate_leaderboards(curated_path: Path, config: Config) -> pl.DataFrame:
    """Calculate leaderboards from curated Parquet tables.

    Computes leaderboards for:
    - PRs: opened, closed, merged
    - Issues: opened, closed
    - Reviews: submitted, approvals, changes_requested
    - Comments: total (issue + review comments), review_comments_total

    Args:
        curated_path: Path to curated data directory (e.g., data/curated/year=2025/)
        config: Application configuration.

    Returns:
        DataFrame with metrics_leaderboard schema.

    Raises:
        FileNotFoundError: If curated tables don't exist.
    """
    logger.info("Calculating leaderboards from %s", curated_path)

    year = config.github.windows.year
    humans_only = config.identity.humans_only

    # Load dim_user for bot filtering
    dim_user_path = curated_path / "dim_user.parquet"
    if not dim_user_path.exists():
        msg = f"dim_user table not found: {dim_user_path}"
        raise FileNotFoundError(msg)

    dim_user = pl.read_parquet(dim_user_path)

    # Filter to humans only if configured
    if humans_only:
        logger.info("Filtering to humans only")
        human_users = dim_user.filter(pl.col("is_bot") == False).select("user_id")  # noqa: E712
    else:
        human_users = dim_user.select("user_id")

    # Calculate each metric
    leaderboards: list[pl.DataFrame] = []

    # PR metrics
    pr_path = curated_path / "fact_pull_request.parquet"
    if pr_path.exists():
        fact_pr = pl.read_parquet(pr_path)
        leaderboards.extend(_calculate_pr_metrics(fact_pr, human_users, year))
    else:
        logger.warning("fact_pull_request not found, skipping PR metrics")

    # Issue metrics
    issue_path = curated_path / "fact_issue.parquet"
    if issue_path.exists():
        fact_issue = pl.read_parquet(issue_path)
        leaderboards.extend(_calculate_issue_metrics(fact_issue, human_users, year))
    else:
        logger.warning("fact_issue not found, skipping issue metrics")

    # Review metrics
    review_path = curated_path / "fact_review.parquet"
    if review_path.exists():
        fact_review = pl.read_parquet(review_path)
        leaderboards.extend(_calculate_review_metrics(fact_review, human_users, year))
    else:
        logger.warning("fact_review not found, skipping review metrics")

    # Comment metrics
    issue_comment_path = curated_path / "fact_issue_comment.parquet"
    review_comment_path = curated_path / "fact_review_comment.parquet"
    leaderboards.extend(
        _calculate_comment_metrics(
            curated_path,
            issue_comment_path,
            review_comment_path,
            human_users,
            year,
        )
    )

    # Combine all leaderboards
    if not leaderboards:
        logger.warning("No leaderboards calculated, returning empty DataFrame")
        return _empty_leaderboard_schema()

    combined = pl.concat(leaderboards, how="vertical")

    # Sort for determinism: year, metric_key, scope, repo_id, rank
    combined = combined.sort(["year", "metric_key", "scope", "repo_id", "rank"])

    logger.info("Calculated %d leaderboard entries", len(combined))
    return combined


def _calculate_pr_metrics(
    fact_pr: pl.DataFrame,
    human_users: pl.DataFrame,
    year: int,
) -> list[pl.DataFrame]:
    """Calculate PR-related leaderboard metrics.

    Args:
        fact_pr: fact_pull_request DataFrame.
        human_users: DataFrame with user_id column (filtered to humans if configured).
        year: Year for partitioning.

    Returns:
        List of leaderboard DataFrames for PR metrics.
    """
    # Early return for empty DataFrame to avoid schema mismatch on join
    if len(fact_pr) == 0:
        logger.debug("No PRs to calculate metrics from")
        return []

    results: list[pl.DataFrame] = []

    # PRs opened (all non-null created_at), filtered to target users
    prs_opened = fact_pr.filter(pl.col("created_at").is_not_null()).join(
        human_users.rename({"user_id": "author_user_id"}), on="author_user_id", how="inner"
    )
    results.append(_calculate_metric(prs_opened, "prs_opened", "author_user_id", year))

    # PRs closed (state = closed or merged), filtered to target users
    prs_closed = fact_pr.filter(
        pl.col("state").is_in(["closed", "merged"]) & pl.col("closed_at").is_not_null()
    ).join(human_users.rename({"user_id": "author_user_id"}), on="author_user_id", how="inner")
    results.append(_calculate_metric(prs_closed, "prs_closed", "author_user_id", year))

    # PRs merged (state = merged), filtered to target users
    prs_merged = fact_pr.filter(
        (pl.col("state") == "merged") & pl.col("merged_at").is_not_null()
    ).join(human_users.rename({"user_id": "author_user_id"}), on="author_user_id", how="inner")
    results.append(_calculate_metric(prs_merged, "prs_merged", "author_user_id", year))

    return results


def _calculate_issue_metrics(
    fact_issue: pl.DataFrame,
    human_users: pl.DataFrame,
    year: int,
) -> list[pl.DataFrame]:
    """Calculate issue-related leaderboard metrics.

    Args:
        fact_issue: fact_issue DataFrame.
        human_users: DataFrame with user_id column (filtered to humans if configured).
        year: Year for partitioning.

    Returns:
        List of leaderboard DataFrames for issue metrics.
    """
    results: list[pl.DataFrame] = []

    # Issues opened, filtered to target users
    issues_opened = fact_issue.filter(pl.col("created_at").is_not_null()).join(
        human_users.rename({"user_id": "author_user_id"}), on="author_user_id", how="inner"
    )
    results.append(_calculate_metric(issues_opened, "issues_opened", "author_user_id", year))

    # Issues closed, filtered to target users
    issues_closed = fact_issue.filter(
        (pl.col("state") == "closed") & pl.col("closed_at").is_not_null()
    ).join(human_users.rename({"user_id": "author_user_id"}), on="author_user_id", how="inner")
    results.append(_calculate_metric(issues_closed, "issues_closed", "author_user_id", year))

    return results


def _calculate_review_metrics(
    fact_review: pl.DataFrame,
    human_users: pl.DataFrame,
    year: int,
) -> list[pl.DataFrame]:
    """Calculate review-related leaderboard metrics.

    Args:
        fact_review: fact_review DataFrame.
        human_users: DataFrame with user_id column (filtered to humans if configured).
        year: Year for partitioning.

    Returns:
        List of leaderboard DataFrames for review metrics.
    """
    # Early return for empty DataFrame to avoid schema mismatch on join
    if len(fact_review) == 0:
        logger.debug("No reviews to calculate metrics from")
        return []

    results: list[pl.DataFrame] = []

    # Reviews submitted (all reviews), filtered to target users
    reviews_submitted = fact_review.filter(pl.col("submitted_at").is_not_null()).join(
        human_users.rename({"user_id": "reviewer_user_id"}), on="reviewer_user_id", how="inner"
    )
    results.append(
        _calculate_metric(reviews_submitted, "reviews_submitted", "reviewer_user_id", year)
    )

    # Approvals, filtered to target users
    approvals = fact_review.filter(pl.col("state") == "APPROVED").join(
        human_users.rename({"user_id": "reviewer_user_id"}), on="reviewer_user_id", how="inner"
    )
    results.append(_calculate_metric(approvals, "approvals", "reviewer_user_id", year))

    # Changes requested, filtered to target users
    changes_requested = fact_review.filter(pl.col("state") == "CHANGES_REQUESTED").join(
        human_users.rename({"user_id": "reviewer_user_id"}), on="reviewer_user_id", how="inner"
    )
    results.append(
        _calculate_metric(changes_requested, "changes_requested", "reviewer_user_id", year)
    )

    return results


def _calculate_comment_metrics(
    curated_path: Path,
    issue_comment_path: Path,
    review_comment_path: Path,
    human_users: pl.DataFrame,
    year: int,
) -> list[pl.DataFrame]:
    """Calculate comment-related leaderboard metrics.

    Args:
        curated_path: Path to curated data directory.
        issue_comment_path: Path to fact_issue_comment Parquet.
        review_comment_path: Path to fact_review_comment Parquet.
        human_users: DataFrame with user_id column (filtered to humans if configured).
        year: Year for partitioning.

    Returns:
        List of leaderboard DataFrames for comment metrics.
    """
    results: list[pl.DataFrame] = []

    # Review comments total
    if review_comment_path.exists():
        fact_review_comment = pl.read_parquet(review_comment_path)
        if len(fact_review_comment) > 0:
            fact_review_comment = fact_review_comment.join(
                human_users.rename({"user_id": "author_user_id"}),
                on="author_user_id",
                how="inner",
            )
            results.append(
                _calculate_metric(
                    fact_review_comment,
                    "review_comments_total",
                    "author_user_id",
                    year,
                )
            )
        else:
            logger.debug("No review comments to calculate metrics from")
    else:
        logger.warning("fact_review_comment not found, skipping review comment metrics")

    # Comments total (issue + review comments)
    comment_dfs: list[pl.DataFrame] = []

    if issue_comment_path.exists():
        fact_issue_comment = pl.read_parquet(issue_comment_path)
        if len(fact_issue_comment) > 0:
            comment_dfs.append(
                fact_issue_comment.select(
                    [
                        "author_user_id",
                        "repo_id",
                    ]
                )
            )

    if review_comment_path.exists():
        fact_review_comment = pl.read_parquet(review_comment_path)
        if len(fact_review_comment) > 0:
            comment_dfs.append(
                fact_review_comment.select(
                    [
                        "author_user_id",
                        "repo_id",
                    ]
                )
            )

    if comment_dfs:
        all_comments = pl.concat(comment_dfs, how="vertical")
        all_comments = all_comments.join(
            human_users.rename({"user_id": "author_user_id"}),
            on="author_user_id",
            how="inner",
        )
        results.append(
            _calculate_metric(
                all_comments,
                "comments_total",
                "author_user_id",
                year,
            )
        )
    else:
        logger.debug("No comment data found, skipping total comment metrics")

    return results


def _calculate_metric(
    df: pl.DataFrame,
    metric_key: str,
    user_column: str,
    year: int,
) -> pl.DataFrame:
    """Calculate a single metric with org and repo scope rankings.

    Args:
        df: DataFrame with records to count.
        user_column: Column name containing user_id (e.g., "author_user_id").
        metric_key: Metric key identifier.
        year: Year for partitioning.

    Returns:
        DataFrame with leaderboard entries for both org and repo scopes.
    """
    if len(df) == 0:
        return _empty_leaderboard_schema()

    # Ensure user_column is renamed to user_id for consistency
    df = df.rename({user_column: "user_id"})

    # Org-wide leaderboard
    org_leaderboard = (
        df.group_by("user_id")
        .agg(pl.len().cast(pl.Int64).alias("value"))
        .with_columns(
            [
                pl.lit(year).alias("year"),
                pl.lit(metric_key).alias("metric_key"),
                pl.lit("org").alias("scope"),
                pl.lit(None).cast(pl.Utf8).alias("repo_id"),
            ]
        )
    )

    org_leaderboard = _assign_ranks(org_leaderboard, [])

    # Per-repo leaderboard
    repo_leaderboard = (
        df.filter(pl.col("repo_id").is_not_null())
        .group_by(["repo_id", "user_id"])
        .agg(pl.len().cast(pl.Int64).alias("value"))
        .with_columns(
            [
                pl.lit(year).alias("year"),
                pl.lit(metric_key).alias("metric_key"),
                pl.lit("repo").alias("scope"),
            ]
        )
    )

    repo_leaderboard = _assign_ranks(repo_leaderboard, ["repo_id"])

    # Combine - both need same column order
    combined = pl.concat(
        [
            org_leaderboard.select(
                ["year", "metric_key", "scope", "repo_id", "user_id", "value", "rank"]
            ),
            repo_leaderboard.select(
                ["year", "metric_key", "scope", "repo_id", "user_id", "value", "rank"]
            ),
        ],
        how="vertical",
    )

    # Ensure consistent column order
    return combined.select(
        [
            "year",
            "metric_key",
            "scope",
            "repo_id",
            "user_id",
            "value",
            "rank",
        ]
    )


def _assign_ranks(df: pl.DataFrame, partition_cols: list[str]) -> pl.DataFrame:
    """Assign dense ranks within partitions.

    Args:
        df: DataFrame with "value" column to rank by.
        partition_cols: Columns to partition by (e.g., ["repo_id"] for per-repo ranking).

    Returns:
        DataFrame with "rank" column added.
    """
    if len(df) == 0:
        return df.with_columns(pl.lit(None).cast(pl.Int32).alias("rank"))

    # Dense ranking: same values get same rank, next rank is +1
    if partition_cols:
        # Partition by repo_id (or other columns)
        df = df.with_columns(
            pl.col("value")
            .rank(method="dense", descending=True)
            .over(partition_cols)
            .cast(pl.Int32)
            .alias("rank")
        )
    else:
        # Global ranking (org-wide)
        df = df.with_columns(
            pl.col("value").rank(method="dense", descending=True).cast(pl.Int32).alias("rank")
        )

    return df


def _empty_leaderboard_schema() -> pl.DataFrame:
    """Create empty DataFrame with leaderboard schema.

    Returns:
        Empty DataFrame with correct schema.
    """
    return pl.DataFrame(
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
