"""Export metrics to JSON for D3.js frontend.

Reads Parquet metrics tables and exports to JSON format suitable for
visualization in the static site.

Output structure:
    site/YYYY/data/
        leaderboards.json - leaderboard rankings with user info
        timeseries.json - time series data for charts
        repo_health.json - repository health metrics
        hygiene_scores.json - hygiene scores for gauge displays
        awards.json - award data for display
        summary.json - overall summary statistics
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from gh_year_end.config import Config
from gh_year_end.storage.parquet_writer import read_parquet
from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


def export_metrics(config: Config, paths: PathManager) -> dict[str, Any]:
    """Export all metrics from Parquet to JSON files.

    Reads metrics Parquet files, enriches with dimension data, and writes
    JSON files to site/YYYY/data/ directory.

    Args:
        config: Application configuration.
        paths: Path manager for locating files.

    Returns:
        Dictionary with export statistics:
            - files_written: List of output file paths
            - total_size_bytes: Total size of exported files
            - record_counts: Dict mapping filename to record count
            - errors: List of error messages for missing/failed files

    Raises:
        ValueError: If metrics directory doesn't exist.
    """
    start_time = datetime.now()
    logger.info("Starting metrics export for year %d", config.github.windows.year)

    # Verify metrics directory exists
    if not paths.metrics_root.exists():
        msg = f"Metrics directory not found: {paths.metrics_root}. Run 'metrics' command first."
        raise ValueError(msg)

    # Ensure output directory exists
    paths.site_data_path.mkdir(parents=True, exist_ok=True)

    stats: dict[str, Any] = {
        "files_written": [],
        "total_size_bytes": 0,
        "record_counts": {},
        "errors": [],
    }

    # Load dimension tables for enrichment
    dim_user = _load_dim_user_safe(paths)
    dim_repo = _load_dim_repo_safe(paths)

    # Export each metrics table
    def export_leaderboards() -> dict[str, Any]:
        return _export_leaderboards(paths, dim_user, dim_repo)

    def export_timeseries() -> dict[str, Any]:
        return _export_timeseries(paths)

    def export_repo_health() -> dict[str, Any]:
        return _export_repo_health(paths)

    def export_hygiene_scores() -> dict[str, Any]:
        return _export_hygiene_scores(paths)

    def export_awards() -> dict[str, Any]:
        return _export_awards(paths, dim_user, dim_repo)

    def export_summary() -> dict[str, Any]:
        return _export_summary(paths, config)

    exporters: list[tuple[str, Any]] = [
        ("leaderboards.json", export_leaderboards),
        ("timeseries.json", export_timeseries),
        ("repo_health.json", export_repo_health),
        ("hygiene_scores.json", export_hygiene_scores),
        ("awards.json", export_awards),
        ("summary.json", export_summary),
    ]

    for filename, exporter_fn in exporters:
        try:
            data = exporter_fn()
            output_path = paths.site_data_path / filename

            # Write JSON
            _write_json(data, output_path)

            # Update stats
            file_size = output_path.stat().st_size
            stats["files_written"].append(str(output_path))
            stats["total_size_bytes"] += file_size
            stats["record_counts"][filename] = _count_records(data)

            logger.info("Exported %s (%d bytes)", filename, file_size)

        except FileNotFoundError as e:
            error_msg = f"{filename}: {e}"
            stats["errors"].append(error_msg)
            logger.warning("Skipped %s: file not found", filename)
        except Exception as e:
            error_msg = f"{filename}: {e}"
            stats["errors"].append(error_msg)
            logger.exception("Failed to export %s", filename)

    # Add timing
    end_time = datetime.now()
    stats["duration_seconds"] = (end_time - start_time).total_seconds()

    logger.info(
        "Export complete: %d files, %d bytes, %d errors",
        len(stats["files_written"]),
        stats["total_size_bytes"],
        len(stats["errors"]),
    )

    return stats


def _load_dim_user_safe(paths: PathManager) -> pl.DataFrame | None:
    """Load dim_user table if it exists.

    Args:
        paths: Path manager.

    Returns:
        Polars DataFrame or None if file doesn't exist.
    """
    try:
        if paths.dim_user_path.exists():
            table = read_parquet(paths.dim_user_path)
            result = pl.from_arrow(table)
            assert isinstance(result, pl.DataFrame)
            return result
    except Exception as e:
        logger.warning("Failed to load dim_user: %s", e)
    return None


def _load_dim_repo_safe(paths: PathManager) -> pl.DataFrame | None:
    """Load dim_repo table if it exists.

    Args:
        paths: Path manager.

    Returns:
        Polars DataFrame or None if file doesn't exist.
    """
    try:
        if paths.dim_repo_path.exists():
            table = read_parquet(paths.dim_repo_path)
            result = pl.from_arrow(table)
            assert isinstance(result, pl.DataFrame)
            return result
    except Exception as e:
        logger.warning("Failed to load dim_repo: %s", e)
    return None


def _compute_leaderboards_from_facts(paths: PathManager) -> pl.DataFrame | None:
    """Compute leaderboard metrics from curated fact tables.

    This is a fallback when metrics_leaderboard.parquet doesn't exist or is empty.
    Scans curated fact tables and aggregates contributor metrics.

    Args:
        paths: Path manager.

    Returns:
        DataFrame with leaderboard schema or None if no fact tables exist.
    """
    leaderboards: list[pl.DataFrame] = []

    # Helper to create leaderboard entry
    def make_leaderboard(
        df: pl.DataFrame,
        metric_key: str,
        user_col: str,
        year: int,
    ) -> pl.DataFrame:
        if len(df) == 0:
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

        # Rename user column to user_id for consistency
        df = df.rename({user_col: "user_id"})

        # Org-wide aggregation
        org_agg = (
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
            .with_columns(
                pl.col("value").rank(method="dense", descending=True).cast(pl.Int32).alias("rank")
            )
        )

        # Per-repo aggregation
        repo_agg = (
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
            .with_columns(
                pl.col("value")
                .rank(method="dense", descending=True)
                .over("repo_id")
                .cast(pl.Int32)
                .alias("rank")
            )
        )

        return pl.concat(
            [
                org_agg.select(
                    ["year", "metric_key", "scope", "repo_id", "user_id", "value", "rank"]
                ),
                repo_agg.select(
                    ["year", "metric_key", "scope", "repo_id", "user_id", "value", "rank"]
                ),
            ],
            how="vertical",
        )

    # Determine year from path (extract from curated_root: data/curated/year=YYYY)
    year = paths.year

    # Load and process fact_pull_request
    if paths.fact_pull_request_path.exists():
        try:
            fact_pr = pl.read_parquet(paths.fact_pull_request_path)

            # PRs opened
            prs_opened = fact_pr.filter(pl.col("created_at").is_not_null())
            leaderboards.append(make_leaderboard(prs_opened, "prs_opened", "author_user_id", year))

            # PRs merged
            prs_merged = fact_pr.filter(
                (pl.col("state") == "merged") & pl.col("merged_at").is_not_null()
            )
            leaderboards.append(make_leaderboard(prs_merged, "prs_merged", "author_user_id", year))

            logger.info("Computed PR metrics from fact_pull_request")
        except Exception as e:
            logger.warning("Failed to compute PR metrics: %s", e)

    # Load and process fact_issue
    if paths.fact_issue_path.exists():
        try:
            fact_issue = pl.read_parquet(paths.fact_issue_path)

            # Issues opened
            issues_opened = fact_issue.filter(pl.col("created_at").is_not_null())
            leaderboards.append(
                make_leaderboard(issues_opened, "issues_opened", "author_user_id", year)
            )

            # Issues closed
            issues_closed = fact_issue.filter(
                (pl.col("state") == "closed") & pl.col("closed_at").is_not_null()
            )
            leaderboards.append(
                make_leaderboard(issues_closed, "issues_closed", "author_user_id", year)
            )

            logger.info("Computed issue metrics from fact_issue")
        except Exception as e:
            logger.warning("Failed to compute issue metrics: %s", e)

    # Load and process fact_review
    if paths.fact_review_path.exists():
        try:
            fact_review = pl.read_parquet(paths.fact_review_path)

            # Reviews submitted
            reviews_submitted = fact_review.filter(pl.col("submitted_at").is_not_null())
            leaderboards.append(
                make_leaderboard(reviews_submitted, "reviews_submitted", "reviewer_user_id", year)
            )

            # Approvals
            approvals = fact_review.filter(pl.col("state") == "APPROVED")
            leaderboards.append(make_leaderboard(approvals, "approvals", "reviewer_user_id", year))

            # Changes requested
            changes_requested = fact_review.filter(pl.col("state") == "CHANGES_REQUESTED")
            leaderboards.append(
                make_leaderboard(changes_requested, "changes_requested", "reviewer_user_id", year)
            )

            logger.info("Computed review metrics from fact_review")
        except Exception as e:
            logger.warning("Failed to compute review metrics: %s", e)

    # Load and process comment tables
    comment_dfs: list[pl.DataFrame] = []

    if paths.fact_issue_comment_path.exists():
        try:
            fact_issue_comment = pl.read_parquet(paths.fact_issue_comment_path)
            comment_dfs.append(fact_issue_comment.select(["author_user_id", "repo_id"]))
        except Exception as e:
            logger.warning("Failed to load fact_issue_comment: %s", e)

    if paths.fact_review_comment_path.exists():
        try:
            fact_review_comment = pl.read_parquet(paths.fact_review_comment_path)
            comment_dfs.append(fact_review_comment.select(["author_user_id", "repo_id"]))
            # Also compute review_comments_total
            leaderboards.append(
                make_leaderboard(
                    fact_review_comment, "review_comments_total", "author_user_id", year
                )
            )
        except Exception as e:
            logger.warning("Failed to load fact_review_comment: %s", e)

    if comment_dfs:
        try:
            all_comments = pl.concat(comment_dfs, how="vertical")
            leaderboards.append(
                make_leaderboard(all_comments, "comments_total", "author_user_id", year)
            )
            logger.info("Computed comment metrics from fact tables")
        except Exception as e:
            logger.warning("Failed to compute comment metrics: %s", e)

    # Combine all leaderboards
    if not leaderboards:
        logger.warning("No fact tables available to compute leaderboards")
        return None

    combined = pl.concat(leaderboards, how="vertical")

    # Filter out bot users
    dim_user_path = paths.dim_user_path
    if dim_user_path.exists():
        try:
            dim_user = pl.read_parquet(dim_user_path)
            human_users = dim_user.filter(pl.col("is_bot") == False).select("user_id")
            before_count = len(combined)
            combined = combined.join(human_users, on="user_id", how="inner")
            logger.info(
                "Filtered bot users: %d -> %d entries", before_count, len(combined)
            )
        except Exception as e:
            logger.warning("Failed to filter bot users: %s", e)

    # Re-rank after bot filtering
    if len(combined) > 0:
        combined = combined.with_columns(
            pl.col("value")
            .rank(method="dense", descending=True)
            .over(["metric_key", "scope", "repo_id"])
            .cast(pl.Int32)
            .alias("rank")
        )

    # Sort for determinism
    combined = combined.sort(["year", "metric_key", "scope", "repo_id", "rank"])

    logger.info("Computed %d leaderboard entries from fact tables", len(combined))
    return combined


def _export_leaderboards(
    paths: PathManager,
    dim_user: pl.DataFrame | None,
    dim_repo: pl.DataFrame | None,
) -> dict[str, Any]:
    """Export leaderboard metrics to JSON.

    Enriches leaderboard data with user login and display names.
    If metrics_leaderboard.parquet doesn't exist or is empty, attempts to
    compute leaderboards from curated fact tables as a fallback.

    Args:
        paths: Path manager.
        dim_user: User dimension table for enrichment.
        dim_repo: Repository dimension table for enrichment.

    Returns:
        Dictionary with leaderboard data structure for D3.js.

    Raises:
        FileNotFoundError: If neither metrics nor curated tables exist.
    """
    df = None

    # Try loading pre-computed metrics first
    if paths.metrics_leaderboard_path.exists():
        try:
            table = read_parquet(paths.metrics_leaderboard_path)
            df_result = pl.from_arrow(table)
            assert isinstance(df_result, pl.DataFrame)
            df = df_result

            if len(df) == 0:
                logger.warning("metrics_leaderboard.parquet is empty, computing from fact tables")
                df = None
        except Exception as e:
            logger.warning(
                "Failed to load metrics_leaderboard.parquet: %s, computing from fact tables", e
            )
            df = None

    # Fallback: compute from curated fact tables if metrics don't exist or are empty
    if df is None:
        logger.info("Computing leaderboards from curated fact tables")
        df = _compute_leaderboards_from_facts(paths)

        if df is None or len(df) == 0:
            logger.warning("No leaderboard data available from metrics or fact tables")
            return {
                "leaderboards": {},
                "metrics_available": [],
            }

    # Enrich with user info
    if dim_user is not None:
        df = df.join(
            dim_user.select(["user_id", "login", "display_name"]),
            on="user_id",
            how="left",
        )
    else:
        # Add empty columns if dim_user not available
        df = df.with_columns([pl.lit(None).alias("login"), pl.lit(None).alias("display_name")])

    # Enrich with repo info (for per-repo leaderboards)
    # Need to handle null repo_id for org-level leaderboards
    if dim_repo is not None:
        # Cast repo_id to string to match dim_repo
        df = df.with_columns(pl.col("repo_id").cast(pl.Utf8))
        df = df.join(
            dim_repo.select(["repo_id", "full_name"]).rename({"full_name": "repo_full_name"}),
            on="repo_id",
            how="left",
        )
    else:
        df = df.with_columns(pl.lit(None).alias("repo_full_name"))

    # Group by metric_key and scope
    leaderboards = {}

    for metric_key in df["metric_key"].unique().to_list():
        metric_data = df.filter(pl.col("metric_key") == metric_key)

        leaderboards[metric_key] = {
            "org": _leaderboard_to_json(
                metric_data.filter(pl.col("scope") == "org"),
                scope="org",
            ),
            "repos": _leaderboard_by_repo_to_json(
                metric_data.filter(pl.col("scope") == "repo"),
            ),
        }

    return {
        "leaderboards": leaderboards,
        "metrics_available": df["metric_key"].unique().to_list(),
    }


def _leaderboard_to_json(df: pl.DataFrame, scope: str) -> list[dict[str, Any]]:
    """Convert leaderboard DataFrame to JSON-serializable list.

    Args:
        df: Leaderboard DataFrame.
        scope: Scope identifier (org or repo).

    Returns:
        List of leaderboard entries.
    """
    if df.is_empty():
        return []

    # Sort by rank
    df = df.sort("rank")

    records = []
    for row in df.iter_rows(named=True):
        records.append(
            {
                "rank": row["rank"],
                "user_id": row["user_id"],
                "login": row.get("login"),
                "display_name": row.get("display_name"),
                "value": row["value"],
            }
        )

    return records


def _leaderboard_by_repo_to_json(df: pl.DataFrame) -> dict[str, list[dict[str, Any]]]:
    """Convert per-repo leaderboard DataFrame to JSON structure.

    Args:
        df: Per-repo leaderboard DataFrame.

    Returns:
        Dictionary mapping repo_id to leaderboard entries.
    """
    if df.is_empty():
        return {}

    repos = {}
    for repo_id in df["repo_id"].unique().to_list():
        repo_data = df.filter(pl.col("repo_id") == repo_id).sort("rank")

        records = []
        for row in repo_data.iter_rows(named=True):
            records.append(
                {
                    "rank": row["rank"],
                    "user_id": row["user_id"],
                    "login": row.get("login"),
                    "display_name": row.get("display_name"),
                    "value": row["value"],
                    "repo_id": row["repo_id"],
                    "repo_full_name": row.get("repo_full_name"),
                }
            )

        repos[repo_id] = records

    return repos


def _export_timeseries(paths: PathManager) -> dict[str, Any]:
    """Export time series metrics to JSON.

    Args:
        paths: Path manager.

    Returns:
        Dictionary with time series data for D3.js charts.

    Raises:
        FileNotFoundError: If time series parquet doesn't exist.
    """
    if not paths.metrics_time_series_path.exists():
        msg = f"Time series metrics not found: {paths.metrics_time_series_path}"
        raise FileNotFoundError(msg)

    table = read_parquet(paths.metrics_time_series_path)
    df_result = pl.from_arrow(table)
    assert isinstance(df_result, pl.DataFrame)
    df = df_result

    # Convert dates to ISO format strings
    df = df.with_columns(
        [
            pl.col("period_start").cast(pl.Utf8).alias("period_start"),
            pl.col("period_end").cast(pl.Utf8).alias("period_end"),
        ]
    )

    # Group by period_type and metric_key
    timeseries: dict[str, Any] = {}

    for period_type in df["period_type"].unique().to_list():
        period_data = df.filter(pl.col("period_type") == period_type)

        timeseries[period_type] = {}

        for metric_key in period_data["metric_key"].unique().to_list():
            metric_data = period_data.filter(pl.col("metric_key") == metric_key)

            timeseries[period_type][metric_key] = {
                "org": _timeseries_to_json(metric_data.filter(pl.col("scope") == "org")),
                "repos": _timeseries_by_repo_to_json(metric_data.filter(pl.col("scope") == "repo")),
            }

    return {
        "timeseries": timeseries,
        "period_types": df["period_type"].unique().to_list(),
        "metrics_available": df["metric_key"].unique().to_list(),
    }


def _timeseries_to_json(df: pl.DataFrame) -> list[dict[str, Any]]:
    """Convert time series DataFrame to JSON-serializable list.

    Args:
        df: Time series DataFrame.

    Returns:
        List of time series data points.
    """
    if df.is_empty():
        return []

    # Sort by period_start
    df = df.sort("period_start")

    records = []
    for row in df.iter_rows(named=True):
        records.append(
            {
                "period_start": row["period_start"],
                "period_end": row["period_end"],
                "value": row["value"],
            }
        )

    return records


def _timeseries_by_repo_to_json(df: pl.DataFrame) -> dict[str, list[dict[str, Any]]]:
    """Convert per-repo time series DataFrame to JSON structure.

    Args:
        df: Per-repo time series DataFrame.

    Returns:
        Dictionary mapping repo_id to time series data points.
    """
    if df.is_empty():
        return {}

    repos = {}
    for repo_id in df["repo_id"].unique().to_list():
        repo_data = df.filter(pl.col("repo_id") == repo_id).sort("period_start")

        records = []
        for row in repo_data.iter_rows(named=True):
            records.append(
                {
                    "period_start": row["period_start"],
                    "period_end": row["period_end"],
                    "value": row["value"],
                }
            )

        repos[repo_id] = records

    return repos


def _export_repo_health(paths: PathManager) -> dict[str, Any]:
    """Export repository health metrics to JSON.

    Args:
        paths: Path manager.

    Returns:
        Dictionary with repo health data indexed by repo_id.

    Raises:
        FileNotFoundError: If repo health parquet doesn't exist.
    """
    if not paths.metrics_repo_health_path.exists():
        msg = f"Repo health metrics not found: {paths.metrics_repo_health_path}"
        raise FileNotFoundError(msg)

    table = read_parquet(paths.metrics_repo_health_path)
    df_result = pl.from_arrow(table)
    assert isinstance(df_result, pl.DataFrame)
    df = df_result

    # Convert to dictionary indexed by repo_id
    repos = {}
    for row in df.iter_rows(named=True):
        repos[row["repo_id"]] = {
            "repo_full_name": row["repo_full_name"],
            "year": row["year"],
            "active_contributors_30d": row["active_contributors_30d"],
            "active_contributors_90d": row["active_contributors_90d"],
            "active_contributors_365d": row["active_contributors_365d"],
            "prs_opened": row["prs_opened"],
            "prs_merged": row["prs_merged"],
            "issues_opened": row["issues_opened"],
            "issues_closed": row["issues_closed"],
            "review_coverage": row["review_coverage"],
            "median_time_to_first_review": row.get("median_time_to_first_review"),
            "median_time_to_merge": row.get("median_time_to_merge"),
            "stale_pr_count": row["stale_pr_count"],
            "stale_issue_count": row["stale_issue_count"],
        }

    return {"repos": repos, "total_repos": len(repos)}


def _export_hygiene_scores(paths: PathManager) -> dict[str, Any]:
    """Export hygiene scores to JSON.

    Args:
        paths: Path manager.

    Returns:
        Dictionary with hygiene score data indexed by repo_id.

    Raises:
        FileNotFoundError: If hygiene scores parquet doesn't exist.
    """
    if not paths.metrics_repo_hygiene_score_path.exists():
        msg = f"Hygiene scores not found: {paths.metrics_repo_hygiene_score_path}"
        raise FileNotFoundError(msg)

    table = read_parquet(paths.metrics_repo_hygiene_score_path)
    df_result = pl.from_arrow(table)
    assert isinstance(df_result, pl.DataFrame)
    df = df_result

    # Convert to dictionary indexed by repo_id
    repos = {}
    for row in df.iter_rows(named=True):
        repos[row["repo_id"]] = {
            "repo_full_name": row["repo_full_name"],
            "year": row["year"],
            "score": row["score"],
            "has_readme": row["has_readme"],
            "has_license": row["has_license"],
            "has_contributing": row["has_contributing"],
            "has_code_of_conduct": row["has_code_of_conduct"],
            "has_security_md": row["has_security_md"],
            "has_codeowners": row["has_codeowners"],
            "has_ci_workflows": row["has_ci_workflows"],
            "branch_protection_enabled": row.get("branch_protection_enabled"),
            "requires_reviews": row.get("requires_reviews"),
            "dependabot_enabled": row.get("dependabot_enabled"),
            "secret_scanning_enabled": row.get("secret_scanning_enabled"),
            "notes": row.get("notes", ""),
        }

    # Calculate summary statistics
    scores = [r["score"] for r in repos.values()]
    summary = {
        "total_repos": len(repos),
        "average_score": sum(scores) / len(scores) if scores else 0,
        "min_score": min(scores) if scores else 0,
        "max_score": max(scores) if scores else 0,
    }

    return {"repos": repos, "summary": summary}


def _export_awards(
    paths: PathManager,
    dim_user: pl.DataFrame | None,
    dim_repo: pl.DataFrame | None,
) -> dict[str, Any]:
    """Export awards to JSON.

    Args:
        paths: Path manager.
        dim_user: User dimension table for enrichment.
        dim_repo: Repository dimension table for enrichment.

    Returns:
        Dictionary with award data by category.

    Raises:
        FileNotFoundError: If awards parquet doesn't exist.
    """
    if not paths.metrics_awards_path.exists():
        msg = f"Awards not found: {paths.metrics_awards_path}"
        raise FileNotFoundError(msg)

    table = read_parquet(paths.metrics_awards_path)
    df_result = pl.from_arrow(table)
    assert isinstance(df_result, pl.DataFrame)
    df = df_result

    # Enrich with user login if available
    if dim_user is not None and "winner_user_id" in df.columns:
        df = df.join(
            dim_user.select(["user_id", "login"]).rename({"login": "winner_login"}),
            left_on="winner_user_id",
            right_on="user_id",
            how="left",
        )
    else:
        df = df.with_columns(pl.lit(None).alias("winner_login"))

    # Enrich with repo name if available
    if dim_repo is not None and "winner_repo_id" in df.columns:
        df = df.join(
            dim_repo.select(["repo_id", "full_name"]).rename({"full_name": "winner_repo_name"}),
            left_on="winner_repo_id",
            right_on="repo_id",
            how="left",
        )
    else:
        df = df.with_columns(pl.lit(None).alias("winner_repo_name"))

    # Group by category
    awards_by_category = {}

    for category in df["category"].unique().to_list():
        category_data = df.filter(pl.col("category") == category)

        awards = []
        for row in category_data.iter_rows(named=True):
            awards.append(
                {
                    "award_key": row["award_key"],
                    "title": row["title"],
                    "description": row["description"],
                    "category": row["category"],
                    "winner_user_id": row.get("winner_user_id"),
                    "winner_repo_id": row.get("winner_repo_id"),
                    "winner_name": row.get("winner_name"),
                    "winner_login": row.get("winner_login"),
                    "winner_repo_name": row.get("winner_repo_name"),
                    "supporting_stats": row.get("supporting_stats"),
                }
            )

        awards_by_category[category] = awards

    return {
        "awards": awards_by_category,
        "categories": df["category"].unique().to_list(),
        "total_awards": len(df),
    }


def _export_summary(paths: PathManager, config: Config) -> dict[str, Any]:
    """Export overall summary statistics.

    Aggregates metrics across all tables to provide high-level summary.

    Args:
        paths: Path manager.
        config: Application configuration.

    Returns:
        Dictionary with summary statistics.
    """
    summary: dict[str, Any] = {
        "year": config.github.windows.year,
        "target": {
            "mode": config.github.target.mode,
            "name": config.github.target.name,
        },
        "generated_at": datetime.now().isoformat(),
    }

    # Load metrics and calculate summaries
    try:
        if paths.metrics_leaderboard_path.exists():
            table = read_parquet(paths.metrics_leaderboard_path)
            df_result = pl.from_arrow(table)
            assert isinstance(df_result, pl.DataFrame)
            df = df_result

            # Count unique contributors
            org_wide = df.filter(pl.col("scope") == "org")
            if not org_wide.is_empty():
                summary["total_contributors"] = len(org_wide["user_id"].unique())

                # Get top metrics
                for metric_key in ["prs_merged", "issues_opened", "reviews_submitted"]:
                    metric_data = org_wide.filter(pl.col("metric_key") == metric_key)
                    if not metric_data.is_empty():
                        summary[f"total_{metric_key}"] = int(metric_data["value"].sum())

    except Exception as e:
        logger.warning("Failed to load leaderboard summary: %s", e)

    try:
        if paths.metrics_repo_health_path.exists():
            table = read_parquet(paths.metrics_repo_health_path)
            df_result = pl.from_arrow(table)
            assert isinstance(df_result, pl.DataFrame)
            df = df_result

            summary["total_repos"] = len(df)
            summary["total_prs_opened"] = int(df["prs_opened"].sum())
            summary["total_prs_merged"] = int(df["prs_merged"].sum())
            summary["total_issues_opened"] = int(df["issues_opened"].sum())
            summary["total_issues_closed"] = int(df["issues_closed"].sum())

    except Exception as e:
        logger.warning("Failed to load repo health summary: %s", e)

    try:
        if paths.metrics_repo_hygiene_score_path.exists():
            table = read_parquet(paths.metrics_repo_hygiene_score_path)
            df_result = pl.from_arrow(table)
            assert isinstance(df_result, pl.DataFrame)
            df = df_result

            scores = df["score"].to_list()
            if scores:
                summary["hygiene"] = {
                    "average_score": sum(scores) / len(scores),
                    "min_score": min(scores),
                    "max_score": max(scores),
                }

    except Exception as e:
        logger.warning("Failed to load hygiene score summary: %s", e)

    return summary


def _write_json(data: dict[str, Any], path: Path) -> None:
    """Write data to JSON file with proper formatting.

    Args:
        data: Data to serialize.
        path: Output file path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    # Custom JSON encoder for handling special types
    class CustomEncoder(json.JSONEncoder):
        def default(self, obj: Any) -> Any:
            if isinstance(obj, datetime | date):
                return obj.isoformat()
            if isinstance(obj, Path):
                return str(obj)
            # Handle pandas/numpy types
            if hasattr(obj, "item"):
                return obj.item()
            return super().default(obj)

    with path.open("w") as f:
        json.dump(data, f, cls=CustomEncoder, indent=2, ensure_ascii=False)


def _count_records(data: dict[str, Any]) -> int:
    """Count total records in exported data structure.

    Args:
        data: Exported data dictionary.

    Returns:
        Approximate count of records.
    """
    count = 0

    # Count based on structure
    if "leaderboards" in data:
        for metric_data in data["leaderboards"].values():
            if "org" in metric_data:
                count += len(metric_data["org"])
            if "repos" in metric_data:
                for repo_data in metric_data["repos"].values():
                    count += len(repo_data)

    if "timeseries" in data:
        for period_data in data["timeseries"].values():
            for metric_data in period_data.values():
                if "org" in metric_data:
                    count += len(metric_data["org"])
                if "repos" in metric_data:
                    for repo_data in metric_data["repos"].values():
                        count += len(repo_data)

    if "repos" in data:
        count += len(data["repos"])

    if "awards" in data:
        for awards_list in data["awards"].values():
            count += len(awards_list)

    return count
