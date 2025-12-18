"""Engineer drilldown view generator.

Generates detailed technical view with full leaderboards, per-repo breakdowns,
time series data, and individual contributor profiles.

This view provides comprehensive access to all metrics for technical users
who want to drill down into detailed statistics.
"""

import json
import logging
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from gh_year_end.storage.parquet_writer import read_parquet

logger = logging.getLogger(__name__)


def generate_engineer_view(metrics_path: Path, year: int) -> dict[str, Any]:
    """Generate engineer drilldown view from metrics data.

    Args:
        metrics_path: Path to metrics directory (e.g., data/metrics/year=2025/).
        year: Year for the report.

    Returns:
        Dictionary with engineer view data:
            - leaderboards: Full rankings (all 10 metrics, org + per-repo)
            - repo_breakdown: Per-repo contributor statistics
            - time_series: Weekly/monthly activity trends
            - awards: Detailed award information with criteria
            - contributor_profiles: Individual contributor details
            - filters: Available filter options

    Raises:
        FileNotFoundError: If required metrics files don't exist.
    """
    logger.info("Generating engineer view from %s", metrics_path)

    # Load all metrics tables
    leaderboard = _load_leaderboard(metrics_path)
    repo_health = _load_repo_health(metrics_path)
    hygiene_scores = _load_hygiene_scores(metrics_path)
    awards = _load_awards(metrics_path)

    # Load curated tables for additional details
    curated_path = metrics_path.parent.parent / "curated" / f"year={year}"
    dim_user = _load_dim_user(curated_path)
    dim_repo = _load_dim_repo(curated_path)

    # Build comprehensive view
    view = {
        "year": year,
        "generated_at": datetime.now(UTC).isoformat(),
        "leaderboards": _build_full_leaderboards(leaderboard, dim_user, dim_repo),
        "repo_breakdown": _build_repo_breakdown(
            leaderboard, repo_health, hygiene_scores, dim_user, dim_repo
        ),
        "time_series": _build_time_series(curated_path, year),
        "awards": _build_detailed_awards(awards, leaderboard, dim_user, dim_repo),
        "contributor_profiles": _build_contributor_profiles(leaderboard, dim_user, dim_repo),
        "filters": _build_filter_options(leaderboard, dim_repo),
    }

    logger.info("Engineer view generated successfully")
    return view


def _load_leaderboard(metrics_path: Path) -> pl.DataFrame | None:
    """Load leaderboard metrics table.

    Args:
        metrics_path: Path to metrics directory.

    Returns:
        Leaderboard DataFrame or None if not found.
    """
    leaderboard_path = metrics_path / "metrics_leaderboard.parquet"
    if not leaderboard_path.exists():
        logger.warning("Leaderboard metrics not found: %s", leaderboard_path)
        return None

    table = read_parquet(leaderboard_path)
    df = pl.from_arrow(table)
    if not isinstance(df, pl.DataFrame):
        msg = f"Expected DataFrame from leaderboard, got {type(df)}"
        raise TypeError(msg)
    return df


def _load_repo_health(metrics_path: Path) -> pl.DataFrame | None:
    """Load repo health metrics table.

    Args:
        metrics_path: Path to metrics directory.

    Returns:
        Repo health DataFrame or None if not found.
    """
    repo_health_path = metrics_path / "metrics_repo_health.parquet"
    if not repo_health_path.exists():
        logger.warning("Repo health metrics not found: %s", repo_health_path)
        return None

    table = read_parquet(repo_health_path)
    df = pl.from_arrow(table)
    if not isinstance(df, pl.DataFrame):
        msg = f"Expected DataFrame from repo health, got {type(df)}"
        raise TypeError(msg)
    return df


def _load_hygiene_scores(metrics_path: Path) -> pl.DataFrame | None:
    """Load hygiene scores table.

    Args:
        metrics_path: Path to metrics directory.

    Returns:
        Hygiene scores DataFrame or None if not found.
    """
    hygiene_path = metrics_path / "metrics_repo_hygiene_score.parquet"
    if not hygiene_path.exists():
        logger.warning("Hygiene scores not found: %s", hygiene_path)
        return None

    table = read_parquet(hygiene_path)
    df = pl.from_arrow(table)
    if not isinstance(df, pl.DataFrame):
        msg = f"Expected DataFrame from hygiene scores, got {type(df)}"
        raise TypeError(msg)
    return df


def _load_awards(metrics_path: Path) -> pl.DataFrame | None:
    """Load awards table.

    Args:
        metrics_path: Path to metrics directory.

    Returns:
        Awards DataFrame or None if not found.
    """
    awards_path = metrics_path / "metrics_awards.parquet"
    if not awards_path.exists():
        logger.warning("Awards not found: %s", awards_path)
        return None

    table = read_parquet(awards_path)
    df = pl.from_arrow(table)
    if not isinstance(df, pl.DataFrame):
        msg = f"Expected DataFrame from awards, got {type(df)}"
        raise TypeError(msg)
    return df


def _load_dim_user(curated_path: Path) -> pl.DataFrame | None:
    """Load user dimension table.

    Args:
        curated_path: Path to curated directory.

    Returns:
        User dimension DataFrame or None if not found.
    """
    dim_user_path = curated_path / "dim_user.parquet"
    if not dim_user_path.exists():
        logger.warning("User dimension not found: %s", dim_user_path)
        return None

    df = pl.read_parquet(dim_user_path)
    return df


def _load_dim_repo(curated_path: Path) -> pl.DataFrame | None:
    """Load repository dimension table.

    Args:
        curated_path: Path to curated directory.

    Returns:
        Repository dimension DataFrame or None if not found.
    """
    dim_repo_path = curated_path / "dim_repo.parquet"
    if not dim_repo_path.exists():
        logger.warning("Repository dimension not found: %s", dim_repo_path)
        return None

    df = pl.read_parquet(dim_repo_path)
    return df


def _build_full_leaderboards(
    leaderboard: pl.DataFrame | None,
    dim_user: pl.DataFrame | None,
    dim_repo: pl.DataFrame | None,
) -> dict[str, Any]:
    """Build complete leaderboards with all metrics and scopes.

    Args:
        leaderboard: Leaderboard DataFrame.
        dim_user: User dimension DataFrame.
        dim_repo: Repository dimension DataFrame.

    Returns:
        Dictionary with full leaderboard data:
            - org_wide: All metrics at organization scope
            - per_repo: All metrics at repository scope
            - metrics: List of available metrics
    """
    if leaderboard is None or leaderboard.is_empty():
        return {"org_wide": {}, "per_repo": {}, "metrics": []}

    # Join with dimension tables for display names
    if dim_user is not None:
        leaderboard = leaderboard.join(
            dim_user.select(["user_id", "login"]), on="user_id", how="left"
        )
    else:
        leaderboard = leaderboard.with_columns(pl.lit(None).alias("login"))

    # Get unique metrics
    metrics = sorted(leaderboard["metric_key"].unique().to_list())

    # Build org-wide leaderboards
    org_wide = {}
    org_data = leaderboard.filter(pl.col("scope") == "org")

    for metric in metrics:
        metric_data = org_data.filter(pl.col("metric_key") == metric).sort("rank")

        # Add percentile information
        total_count = len(metric_data)
        metric_data = metric_data.with_columns(
            (pl.col("rank") / total_count * 100).alias("percentile")
        )

        org_wide[metric] = {
            "metric": metric,
            "scope": "org",
            "total_entries": total_count,
            "rankings": [
                {
                    "rank": row["rank"],
                    "user_id": row["user_id"],
                    "login": row.get("login"),
                    "value": row["value"],
                    "percentile": round(row["percentile"], 2),
                }
                for row in metric_data.iter_rows(named=True)
            ],
        }

    # Build per-repo leaderboards
    per_repo = {}
    repo_data = leaderboard.filter(pl.col("scope") == "repo")

    if not repo_data.is_empty():
        # Group by repo_id
        for repo_id in sorted(repo_data["repo_id"].unique().drop_nulls().to_list()):
            repo_leaderboards = {}
            repo_subset = repo_data.filter(pl.col("repo_id") == repo_id)

            # Get repo name
            repo_name = None
            if dim_repo is not None:
                repo_info = dim_repo.filter(pl.col("repo_id") == repo_id)
                if not repo_info.is_empty():
                    repo_name = repo_info.row(0, named=True).get("full_name")

            for metric in metrics:
                metric_data = repo_subset.filter(pl.col("metric_key") == metric).sort("rank")

                if metric_data.is_empty():
                    continue

                total_count = len(metric_data)
                metric_data = metric_data.with_columns(
                    (pl.col("rank") / total_count * 100).alias("percentile")
                )

                repo_leaderboards[metric] = {
                    "metric": metric,
                    "scope": "repo",
                    "total_entries": total_count,
                    "rankings": [
                        {
                            "rank": row["rank"],
                            "user_id": row["user_id"],
                            "login": row.get("login"),
                            "value": row["value"],
                            "percentile": round(row["percentile"], 2),
                        }
                        for row in metric_data.iter_rows(named=True)
                    ],
                }

            per_repo[repo_id] = {
                "repo_id": repo_id,
                "repo_name": repo_name,
                "leaderboards": repo_leaderboards,
            }

    return {"org_wide": org_wide, "per_repo": per_repo, "metrics": metrics}


def _build_repo_breakdown(
    leaderboard: pl.DataFrame | None,
    repo_health: pl.DataFrame | None,
    hygiene_scores: pl.DataFrame | None,
    dim_user: pl.DataFrame | None,
    dim_repo: pl.DataFrame | None,
) -> dict[str, Any]:
    """Build per-repository breakdown of contributors and stats.

    Args:
        leaderboard: Leaderboard DataFrame.
        repo_health: Repo health DataFrame.
        hygiene_scores: Hygiene scores DataFrame.
        dim_user: User dimension DataFrame.
        dim_repo: Repository dimension DataFrame.

    Returns:
        Dictionary with per-repo breakdown.
    """
    if dim_repo is None or dim_repo.is_empty():
        return {}

    breakdown = {}

    for repo_row in dim_repo.iter_rows(named=True):
        repo_id = repo_row["repo_id"]
        repo_name = repo_row["full_name"]

        repo_data: dict[str, Any] = {
            "repo_id": repo_id,
            "repo_name": repo_name,
            "contributors": [],
            "health_metrics": {},
            "hygiene_metrics": {},
        }

        # Get contributors from leaderboard
        if leaderboard is not None:
            repo_contributors = (
                leaderboard.filter((pl.col("scope") == "repo") & (pl.col("repo_id") == repo_id))
                .select(["user_id", "metric_key", "value", "rank"])
                .group_by("user_id")
                .agg(
                    [
                        pl.struct(["metric_key", "value", "rank"]).alias("metrics"),
                    ]
                )
            )

            # Join with user info
            if dim_user is not None:
                repo_contributors = repo_contributors.join(
                    dim_user.select(["user_id", "login"]), on="user_id", how="left"
                )

            # Convert to list
            for contributor in repo_contributors.iter_rows(named=True):
                metrics_dict = {}
                for metric_struct in contributor["metrics"]:
                    metrics_dict[metric_struct["metric_key"]] = {
                        "value": metric_struct["value"],
                        "rank": metric_struct["rank"],
                    }

                repo_data["contributors"].append(
                    {
                        "user_id": contributor["user_id"],
                        "login": contributor.get("login"),
                        "metrics": metrics_dict,
                    }
                )

        # Get health metrics
        if repo_health is not None:
            health_data = repo_health.filter(pl.col("repo_id") == repo_id)
            if not health_data.is_empty():
                health_row = health_data.row(0, named=True)
                repo_data["health_metrics"] = {
                    "active_contributors_30d": health_row.get("active_contributors_30d"),
                    "active_contributors_90d": health_row.get("active_contributors_90d"),
                    "active_contributors_365d": health_row.get("active_contributors_365d"),
                    "prs_opened": health_row.get("prs_opened"),
                    "prs_merged": health_row.get("prs_merged"),
                    "issues_opened": health_row.get("issues_opened"),
                    "issues_closed": health_row.get("issues_closed"),
                    "review_coverage": health_row.get("review_coverage"),
                    "median_time_to_first_review": health_row.get("median_time_to_first_review"),
                    "median_time_to_merge": health_row.get("median_time_to_merge"),
                    "stale_pr_count": health_row.get("stale_pr_count"),
                    "stale_issue_count": health_row.get("stale_issue_count"),
                }

        # Get hygiene metrics
        if hygiene_scores is not None:
            hygiene_data = hygiene_scores.filter(pl.col("repo_id") == repo_id)
            if not hygiene_data.is_empty():
                hygiene_row = hygiene_data.row(0, named=True)
                repo_data["hygiene_metrics"] = {
                    "hygiene_score": hygiene_row.get("hygiene_score"),
                    "has_security_md": hygiene_row.get("has_security_md"),
                    "has_readme": hygiene_row.get("has_readme"),
                    "has_license": hygiene_row.get("has_license"),
                    "has_contributing": hygiene_row.get("has_contributing"),
                    "has_code_of_conduct": hygiene_row.get("has_code_of_conduct"),
                    "has_codeowners": hygiene_row.get("has_codeowners"),
                    "has_workflows": hygiene_row.get("has_workflows"),
                    "branch_protection_enabled": hygiene_row.get("branch_protection_enabled"),
                }

        breakdown[repo_id] = repo_data

    return breakdown


def _build_time_series(curated_path: Path, year: int) -> dict[str, Any]:
    """Build time series data for activity trends.

    Args:
        curated_path: Path to curated directory.
        year: Year for the report.

    Returns:
        Dictionary with time series data by week and month.
    """
    # Load fact tables
    fact_pr_path = curated_path / "fact_pull_request.parquet"
    fact_issue_path = curated_path / "fact_issue.parquet"

    time_series: dict[str, Any] = {"weekly": [], "monthly": []}

    if not fact_pr_path.exists() and not fact_issue_path.exists():
        logger.warning("No fact tables found for time series")
        return time_series

    # Load PRs if available
    weekly_stats: dict[str, dict[str, int]] = defaultdict(lambda: {"prs": 0, "issues": 0})
    monthly_stats: dict[str, dict[str, int]] = defaultdict(lambda: {"prs": 0, "issues": 0})

    if fact_pr_path.exists():
        fact_pr = pl.read_parquet(fact_pr_path)
        pr_by_week = (
            fact_pr.with_columns(
                pl.col("created_at").dt.truncate("1w").alias("week"),
            )
            .group_by("week")
            .agg(pl.len().alias("count"))
            .sort("week")
        )

        for row in pr_by_week.iter_rows(named=True):
            week_key = row["week"].strftime("%Y-W%U") if row["week"] else "unknown"
            weekly_stats[week_key]["prs"] = row["count"]

        pr_by_month = (
            fact_pr.with_columns(
                pl.col("created_at").dt.truncate("1mo").alias("month"),
            )
            .group_by("month")
            .agg(pl.len().alias("count"))
            .sort("month")
        )

        for row in pr_by_month.iter_rows(named=True):
            month_key = row["month"].strftime("%Y-%m") if row["month"] else "unknown"
            monthly_stats[month_key]["prs"] = row["count"]

    # Load issues if available
    if fact_issue_path.exists():
        fact_issue = pl.read_parquet(fact_issue_path)
        issue_by_week = (
            fact_issue.with_columns(
                pl.col("created_at").dt.truncate("1w").alias("week"),
            )
            .group_by("week")
            .agg(pl.len().alias("count"))
            .sort("week")
        )

        for row in issue_by_week.iter_rows(named=True):
            week_key = row["week"].strftime("%Y-W%U") if row["week"] else "unknown"
            weekly_stats[week_key]["issues"] = row["count"]

        issue_by_month = (
            fact_issue.with_columns(
                pl.col("created_at").dt.truncate("1mo").alias("month"),
            )
            .group_by("month")
            .agg(pl.len().alias("count"))
            .sort("month")
        )

        for row in issue_by_month.iter_rows(named=True):
            month_key = row["month"].strftime("%Y-%m") if row["month"] else "unknown"
            monthly_stats[month_key]["issues"] = row["count"]

    # Convert to lists
    time_series["weekly"] = [
        {"period": k, "prs": v["prs"], "issues": v["issues"]}
        for k, v in sorted(weekly_stats.items())
    ]

    time_series["monthly"] = [
        {"period": k, "prs": v["prs"], "issues": v["issues"]}
        for k, v in sorted(monthly_stats.items())
    ]

    return time_series


def _build_detailed_awards(
    awards: pl.DataFrame | None,
    leaderboard: pl.DataFrame | None,
    dim_user: pl.DataFrame | None,
    dim_repo: pl.DataFrame | None,
) -> list[dict[str, Any]]:
    """Build detailed award information with criteria and stats.

    Args:
        awards: Awards DataFrame.
        leaderboard: Leaderboard DataFrame.
        dim_user: User dimension DataFrame.
        dim_repo: Repository dimension DataFrame.

    Returns:
        List of detailed award dictionaries.
    """
    if awards is None or awards.is_empty():
        return []

    detailed_awards = []

    for award_row in awards.iter_rows(named=True):
        award_data = {
            "key": award_row["award_key"],
            "title": award_row["title"],
            "description": award_row["description"],
            "category": award_row["category"],
            "winner": {
                "user_id": award_row.get("winner_user_id"),
                "repo_id": award_row.get("winner_repo_id"),
                "name": award_row["winner_name"],
            },
            "supporting_stats": _parse_supporting_stats(award_row.get("supporting_stats")),
            "honorable_mentions": [],
        }

        # Get honorable mentions from leaderboard (top 5)
        if award_row["category"] == "individual" and leaderboard is not None:
            stats = award_data["supporting_stats"]
            if stats and "metric" in stats:
                metric_key = stats["metric"]
                top_5 = (
                    leaderboard.filter(
                        (pl.col("metric_key") == metric_key) & (pl.col("scope") == "org")
                    )
                    .sort("rank")
                    .head(5)
                )

                if dim_user is not None:
                    top_5 = top_5.join(
                        dim_user.select(["user_id", "login"]), on="user_id", how="left"
                    )

                for mention in top_5.iter_rows(named=True):
                    if mention["user_id"] != award_row.get("winner_user_id"):
                        award_data["honorable_mentions"].append(
                            {
                                "rank": mention["rank"],
                                "user_id": mention["user_id"],
                                "login": mention.get("login"),
                                "value": mention["value"],
                            }
                        )

        detailed_awards.append(award_data)

    return detailed_awards


def _parse_supporting_stats(stats_str: str | None) -> dict[str, Any] | None:
    """Parse supporting stats JSON string.

    Args:
        stats_str: JSON string of supporting stats.

    Returns:
        Parsed dictionary or None if invalid.
    """
    if not stats_str:
        return None

    try:
        # Handle string representation of dict
        if stats_str.startswith("{") and stats_str.endswith("}"):
            # Try to parse as JSON
            parsed: dict[str, Any] = json.loads(stats_str.replace("'", '"'))
            return parsed
        # Handle Python dict string representation
        result: dict[str, Any] = eval(stats_str)
        return result
    except (json.JSONDecodeError, SyntaxError, NameError) as e:
        logger.warning("Failed to parse supporting stats: %s", e)
        return None


def _build_contributor_profiles(
    leaderboard: pl.DataFrame | None,
    dim_user: pl.DataFrame | None,
    dim_repo: pl.DataFrame | None,
) -> dict[str, Any]:
    """Build individual contributor profiles.

    Args:
        leaderboard: Leaderboard DataFrame.
        dim_user: User dimension DataFrame.
        dim_repo: Repository dimension DataFrame.

    Returns:
        Dictionary mapping user_id to contributor profile.
    """
    if leaderboard is None or leaderboard.is_empty():
        return {}

    profiles = {}

    # Get unique users
    users = leaderboard.select("user_id").unique()

    if dim_user is not None:
        users = users.join(dim_user.select(["user_id", "login"]), on="user_id", how="left")

    for user_row in users.iter_rows(named=True):
        user_id = user_row["user_id"]
        login = user_row.get("login")

        # Get all metrics for this user (org-wide)
        user_metrics = leaderboard.filter(
            (pl.col("user_id") == user_id) & (pl.col("scope") == "org")
        )

        metrics_summary = {}
        for metric_row in user_metrics.iter_rows(named=True):
            metrics_summary[metric_row["metric_key"]] = {
                "value": metric_row["value"],
                "rank": metric_row["rank"],
            }

        # Get repos contributed to
        repos_contributed = (
            leaderboard.filter(
                (pl.col("user_id") == user_id)
                & (pl.col("scope") == "repo")
                & pl.col("repo_id").is_not_null()
            )
            .select("repo_id")
            .unique()
        )

        repos_list = []
        if not repos_contributed.is_empty() and dim_repo is not None:
            repos_contributed = repos_contributed.join(
                dim_repo.select(["repo_id", "full_name"]), on="repo_id", how="left"
            )

        if not repos_contributed.is_empty():
            for repo_row in repos_contributed.iter_rows(named=True):
                repos_list.append(
                    {
                        "repo_id": repo_row["repo_id"],
                        "repo_name": repo_row.get("full_name"),
                    }
                )

        # Get review activity (from reviews_submitted metric)
        review_stats = None
        if "reviews_submitted" in metrics_summary:
            review_stats = metrics_summary["reviews_submitted"]

        profiles[user_id] = {
            "user_id": user_id,
            "login": login,
            "metrics": metrics_summary,
            "repos_contributed": repos_list,
            "review_activity": review_stats,
        }

    return profiles


def _build_filter_options(
    leaderboard: pl.DataFrame | None, dim_repo: pl.DataFrame | None
) -> dict[str, Any]:
    """Build available filter options.

    Args:
        leaderboard: Leaderboard DataFrame.
        dim_repo: Repository dimension DataFrame.

    Returns:
        Dictionary with filter options.
    """
    filters: dict[str, Any] = {"repos": [], "metrics": [], "users": []}

    if leaderboard is None or leaderboard.is_empty():
        return filters

    # Get unique metrics
    filters["metrics"] = sorted(leaderboard["metric_key"].unique().to_list())

    # Get unique repos
    if dim_repo is not None:
        filters["repos"] = [
            {"repo_id": row["repo_id"], "repo_name": row["full_name"]}
            for row in dim_repo.iter_rows(named=True)
        ]

    # Get unique users (from org-wide leaderboard)
    unique_users = leaderboard.filter(pl.col("scope") == "org").select("user_id").unique()
    filters["users"] = unique_users["user_id"].to_list()

    return filters


def filter_by_repo(view_data: dict[str, Any], repo_id: str) -> dict[str, Any]:
    """Filter engineer view data by repository.

    Args:
        view_data: Full engineer view data.
        repo_id: Repository ID to filter by.

    Returns:
        Filtered view data containing only the specified repository.
    """
    filtered = view_data.copy()

    # Filter per-repo leaderboards
    if "leaderboards" in filtered and "per_repo" in filtered["leaderboards"]:
        per_repo = filtered["leaderboards"]["per_repo"]
        if repo_id in per_repo:
            filtered["leaderboards"]["per_repo"] = {repo_id: per_repo[repo_id]}
        else:
            filtered["leaderboards"]["per_repo"] = {}

    # Filter repo breakdown
    if "repo_breakdown" in filtered:
        if repo_id in filtered["repo_breakdown"]:
            filtered["repo_breakdown"] = {repo_id: filtered["repo_breakdown"][repo_id]}
        else:
            filtered["repo_breakdown"] = {}

    return filtered


def filter_by_user(view_data: dict[str, Any], user_id: str) -> dict[str, Any]:
    """Filter engineer view data by user.

    Args:
        view_data: Full engineer view data.
        user_id: User ID to filter by.

    Returns:
        Filtered view data containing only the specified user.
    """
    filtered = view_data.copy()

    # Filter leaderboards
    if "leaderboards" in filtered:
        # Filter org-wide
        if "org_wide" in filtered["leaderboards"]:
            for _metric, data in filtered["leaderboards"]["org_wide"].items():
                data["rankings"] = [r for r in data["rankings"] if r["user_id"] == user_id]

        # Filter per-repo
        if "per_repo" in filtered["leaderboards"]:
            for _repo_id, repo_data in filtered["leaderboards"]["per_repo"].items():
                for _metric, data in repo_data["leaderboards"].items():
                    data["rankings"] = [r for r in data["rankings"] if r["user_id"] == user_id]

    # Filter contributor profiles
    if "contributor_profiles" in filtered:
        if user_id in filtered["contributor_profiles"]:
            filtered["contributor_profiles"] = {user_id: filtered["contributor_profiles"][user_id]}
        else:
            filtered["contributor_profiles"] = {}

    return filtered


def filter_by_metric(view_data: dict[str, Any], metric_key: str) -> dict[str, Any]:
    """Filter engineer view data by metric.

    Args:
        view_data: Full engineer view data.
        metric_key: Metric key to filter by.

    Returns:
        Filtered view data containing only the specified metric.
    """
    filtered = view_data.copy()

    # Filter leaderboards
    if "leaderboards" in filtered:
        # Filter org-wide
        if "org_wide" in filtered["leaderboards"]:
            org_wide = filtered["leaderboards"]["org_wide"]
            if metric_key in org_wide:
                filtered["leaderboards"]["org_wide"] = {metric_key: org_wide[metric_key]}
            else:
                filtered["leaderboards"]["org_wide"] = {}

        # Filter per-repo
        if "per_repo" in filtered["leaderboards"]:
            for _repo_id, repo_data in filtered["leaderboards"]["per_repo"].items():
                leaderboards = repo_data["leaderboards"]
                if metric_key in leaderboards:
                    repo_data["leaderboards"] = {metric_key: leaderboards[metric_key]}
                else:
                    repo_data["leaderboards"] = {}

        # Update metrics list
        filtered["leaderboards"]["metrics"] = [metric_key]

    return filtered
