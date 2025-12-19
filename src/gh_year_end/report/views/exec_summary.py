"""Executive summary view generator.

Produces a high-level dashboard for non-technical stakeholders with:
- Health signals (green/yellow/red indicators)
- Key metrics (big numbers)
- Top 5 highlights
- Risk alerts
- Awards summary
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

from gh_year_end.storage.parquet_writer import read_parquet

logger = logging.getLogger(__name__)

# Default health signal thresholds
DEFAULT_THRESHOLDS: dict[str, dict[str, float]] = {
    "activity_level": {
        "green": 100.0,  # >= 100 PRs merged
        "yellow": 25.0,  # >= 25 PRs merged
    },
    "review_coverage": {
        "green": 80.0,  # >= 80% of PRs reviewed
        "yellow": 50.0,  # >= 50% of PRs reviewed
    },
    "issue_resolution": {
        "green": 70.0,  # >= 70% of issues closed
        "yellow": 40.0,  # >= 40% of issues closed
    },
    "hygiene_score": {
        "green": 75.0,  # Average >= 75
        "yellow": 50.0,  # Average >= 50
    },
}


def generate_exec_summary(
    metrics_path: Path,
    year: int,
    thresholds: dict[str, dict[str, float]] | None = None,
) -> dict[str, Any]:
    """Generate executive summary from metrics data.

    Args:
        metrics_path: Path to metrics directory (e.g., data/metrics/year=2025/).
        year: Year for the report.
        thresholds: Optional custom thresholds for health signals.
                   If not provided, uses DEFAULT_THRESHOLDS.

    Returns:
        Dictionary with executive summary structure:
        - health_signals: dict with green/yellow/red indicators
        - key_metrics: dict with big numbers
        - top_highlights: dict with top 5 lists
        - risk_alerts: dict with risk indicators
        - awards_summary: list of top awards

    Raises:
        FileNotFoundError: If required metrics files don't exist.
    """
    if thresholds is None:
        thresholds = DEFAULT_THRESHOLDS

    logger.info("Generating executive summary for year %d from %s", year, metrics_path)

    # Load metrics tables
    leaderboard = _load_metrics_table(metrics_path / "metrics_leaderboard.parquet", use_polars=True)
    repo_health = _load_metrics_table(
        metrics_path / "metrics_repo_health.parquet", use_polars=False
    )
    hygiene_scores = _load_metrics_table(
        metrics_path / "metrics_repo_hygiene_score.parquet", use_polars=False
    )
    awards = _load_metrics_table(metrics_path / "metrics_awards.parquet", use_polars=True)

    # Calculate health signals
    health_signals = _calculate_health_signals(leaderboard, repo_health, hygiene_scores, thresholds)

    # Calculate key metrics
    key_metrics = _calculate_key_metrics(leaderboard, repo_health)

    # Generate top highlights
    top_highlights = _generate_top_highlights(leaderboard, repo_health)

    # Generate risk alerts
    risk_alerts = _generate_risk_alerts(repo_health, hygiene_scores, awards)

    # Generate awards summary
    awards_summary = _generate_awards_summary(awards)

    summary = {
        "year": year,
        "health_signals": health_signals,
        "key_metrics": key_metrics,
        "top_highlights": top_highlights,
        "risk_alerts": risk_alerts,
        "awards_summary": awards_summary,
    }

    logger.info("Executive summary generated successfully")
    return summary


def _load_metrics_table(path: Path, use_polars: bool = False) -> pd.DataFrame | pl.DataFrame | None:
    """Load metrics table if it exists.

    Args:
        path: Path to Parquet file.
        use_polars: If True, load as polars DataFrame. Otherwise, load as pandas.

    Returns:
        DataFrame if file exists, None otherwise.
    """
    if not path.exists():
        logger.debug("Metrics table not found: %s", path)
        return None

    try:
        if use_polars:
            # Use polars (for leaderboard and awards)
            table = read_parquet(path)
            df = pl.from_arrow(table)
            if isinstance(df, pl.DataFrame):
                return df
            msg = f"Expected polars DataFrame from {path}"
            raise TypeError(msg)
        # Use pandas (for repo_health and hygiene_scores)
        return pd.read_parquet(path)
    except Exception as e:
        logger.warning("Failed to load %s: %s", path, e)
        return None


def _calculate_health_signals(
    leaderboard: pl.DataFrame | None,
    repo_health: pd.DataFrame | None,
    hygiene_scores: pd.DataFrame | None,
    thresholds: dict[str, dict[str, float]],
) -> dict[str, Any]:
    """Calculate health signals with color indicators.

    Args:
        leaderboard: Leaderboard metrics.
        repo_health: Repository health metrics.
        hygiene_scores: Repository hygiene scores.
        thresholds: Threshold configuration.

    Returns:
        Dictionary with health signals and their status (green/yellow/red).
    """
    signals = {}

    # Activity level (based on total PRs merged)
    activity_value = 0
    if leaderboard is not None:
        prs_merged = leaderboard.filter(
            (pl.col("metric_key") == "prs_merged") & (pl.col("scope") == "org")
        )
        if len(prs_merged) > 0:
            activity_value = int(prs_merged.select(pl.col("value").sum()).item())

    signals["activity_level"] = {
        "value": activity_value,
        "label": f"{activity_value} PRs merged",
        "status": _calculate_health_signal("activity_level", activity_value, thresholds),
        "description": "Total pull requests merged across all repositories",
    }

    # Review coverage (% of repos with >50% PR review coverage)
    review_coverage_value = None
    if repo_health is not None and len(repo_health) > 0:
        repos_with_reviews = repo_health[repo_health["review_coverage"] >= 50.0]
        total_repos = len(repo_health)
        if total_repos > 0:
            review_coverage_value = (len(repos_with_reviews) / total_repos) * 100.0

    signals["review_coverage"] = {
        "value": review_coverage_value,
        "label": (
            f"{review_coverage_value:.1f}% of repos" if review_coverage_value is not None else "N/A"
        ),
        "status": (
            _calculate_health_signal("review_coverage", review_coverage_value, thresholds)
            if review_coverage_value is not None
            else "gray"
        ),
        "description": "Percentage of repositories with good PR review practices",
    }

    # Issue resolution rate (closed / opened across all repos)
    issue_resolution_value = None
    if repo_health is not None and len(repo_health) > 0:
        total_opened = repo_health["issues_opened"].sum()
        total_closed = repo_health["issues_closed"].sum()
        if total_opened > 0:
            issue_resolution_value = (total_closed / total_opened) * 100.0

    signals["issue_resolution"] = {
        "value": issue_resolution_value,
        "label": (
            f"{issue_resolution_value:.1f}%" if issue_resolution_value is not None else "N/A"
        ),
        "status": (
            _calculate_health_signal("issue_resolution", issue_resolution_value, thresholds)
            if issue_resolution_value is not None
            else "gray"
        ),
        "description": "Rate at which reported issues are being resolved",
    }

    # Hygiene score average
    hygiene_value = None
    if hygiene_scores is not None and len(hygiene_scores) > 0:
        hygiene_value = float(hygiene_scores["score"].mean())

    signals["hygiene_score"] = {
        "value": hygiene_value,
        "label": f"{hygiene_value:.0f}/100" if hygiene_value is not None else "N/A",
        "status": (
            _calculate_health_signal("hygiene_score", hygiene_value, thresholds)
            if hygiene_value is not None
            else "gray"
        ),
        "description": "Code quality and best practices score across repositories",
    }

    return signals


def _calculate_health_signal(
    metric: str, value: float | None, thresholds: dict[str, dict[str, float]]
) -> str:
    """Calculate health signal color for a metric.

    Args:
        metric: Metric name (e.g., "activity_level").
        value: Metric value.
        thresholds: Threshold configuration.

    Returns:
        Signal color: "green", "yellow", "red", or "gray" (no data).
    """
    if value is None:
        return "gray"

    if metric not in thresholds:
        return "gray"

    metric_thresholds = thresholds[metric]
    green_threshold = metric_thresholds.get("green", 100)
    yellow_threshold = metric_thresholds.get("yellow", 50)

    if value >= green_threshold:
        return "green"
    if value >= yellow_threshold:
        return "yellow"
    return "red"


def _calculate_key_metrics(
    leaderboard: pl.DataFrame | None, repo_health: pd.DataFrame | None
) -> dict[str, Any]:
    """Calculate key metrics (big numbers).

    Args:
        leaderboard: Leaderboard metrics.
        repo_health: Repository health metrics.

    Returns:
        Dictionary with key metric values and labels.
    """
    metrics = {}

    # Total PRs merged
    prs_merged = 0
    if leaderboard is not None:
        prs_merged_df = leaderboard.filter(
            (pl.col("metric_key") == "prs_merged") & (pl.col("scope") == "org")
        )
        if len(prs_merged_df) > 0:
            prs_merged = int(prs_merged_df.select(pl.col("value").sum()).item())

    metrics["prs_merged"] = {
        "value": prs_merged,
        "label": "Pull Requests Merged",
        "description": "Successfully merged contributions",
    }

    # Total contributors
    contributors = 0
    if leaderboard is not None:
        # Count unique users who opened PRs
        prs_opened_df = leaderboard.filter(
            (pl.col("metric_key") == "prs_opened") & (pl.col("scope") == "org")
        )
        if len(prs_opened_df) > 0:
            contributors = len(prs_opened_df.select("user_id").unique())

    metrics["contributors"] = {
        "value": contributors,
        "label": "Active Contributors",
        "description": "People who contributed code",
    }

    # Total reviews
    reviews = 0
    if leaderboard is not None:
        reviews_df = leaderboard.filter(
            (pl.col("metric_key") == "reviews_submitted") & (pl.col("scope") == "org")
        )
        if len(reviews_df) > 0:
            reviews = int(reviews_df.select(pl.col("value").sum()).item())

    metrics["reviews"] = {
        "value": reviews,
        "label": "Code Reviews",
        "description": "Reviews submitted on pull requests",
    }

    # Average time to merge
    avg_time_to_merge = None
    if repo_health is not None and len(repo_health) > 0:
        valid_times = repo_health["median_time_to_merge"].dropna()
        if len(valid_times) > 0:
            avg_time_to_merge = float(valid_times.mean())

    metrics["avg_time_to_merge"] = {
        "value": avg_time_to_merge,
        "label": "Average Time to Merge",
        "description": "Typical time from PR creation to merge",
        "formatted": (_format_hours(avg_time_to_merge) if avg_time_to_merge is not None else "N/A"),
    }

    return metrics


def _format_hours(hours: float) -> str:
    """Format hours into human-readable string.

    Args:
        hours: Number of hours.

    Returns:
        Formatted string (e.g., "2.5 hours", "3 days").
    """
    if hours < 1:
        minutes = int(hours * 60)
        return f"{minutes} minutes"
    if hours < 24:
        return f"{hours:.1f} hours"
    days = hours / 24
    if days < 7:
        return f"{days:.1f} days"
    weeks = days / 7
    return f"{weeks:.1f} weeks"


def _generate_top_highlights(
    leaderboard: pl.DataFrame | None, repo_health: pd.DataFrame | None
) -> dict[str, list[dict[str, Any]]]:
    """Generate top 5 highlights.

    Args:
        leaderboard: Leaderboard metrics.
        repo_health: Repository health metrics.

    Returns:
        Dictionary with top 5 lists.
    """
    highlights = {}

    if leaderboard is not None:
        # Top 5 contributors (by PRs merged)
        top_contributors = leaderboard.filter(
            (pl.col("metric_key") == "prs_merged")
            & (pl.col("scope") == "org")
            & (pl.col("rank") <= 5)
        ).sort("rank")

        highlights["top_contributors"] = [
            {
                "rank": int(row["rank"]),
                "user_id": row["user_id"],
                "value": int(row["value"]),
                "label": f"{row['value']} PRs merged",
            }
            for row in top_contributors.iter_rows(named=True)
        ]

        # Top 5 reviewers
        top_reviewers = leaderboard.filter(
            (pl.col("metric_key") == "reviews_submitted")
            & (pl.col("scope") == "org")
            & (pl.col("rank") <= 5)
        ).sort("rank")

        highlights["top_reviewers"] = [
            {
                "rank": int(row["rank"]),
                "user_id": row["user_id"],
                "value": int(row["value"]),
                "label": f"{row['value']} reviews",
            }
            for row in top_reviewers.iter_rows(named=True)
        ]
    else:
        highlights["top_contributors"] = []
        highlights["top_reviewers"] = []

    if repo_health is not None and len(repo_health) > 0:
        # Top 5 most active repos (by PRs merged)
        top_repos = (
            repo_health.nlargest(5, "prs_merged")[["repo_full_name", "prs_merged"]]
            .reset_index(drop=True)
            .to_dict("records")
        )

        highlights["top_repos"] = [
            {
                "rank": idx + 1,
                "repo_name": row["repo_full_name"],
                "value": int(row["prs_merged"]),
                "label": f"{row['prs_merged']} PRs merged",
            }
            for idx, row in enumerate(top_repos)
        ]
    else:
        highlights["top_repos"] = []

    return highlights


def _generate_risk_alerts(
    repo_health: pd.DataFrame | None,
    hygiene_scores: pd.DataFrame | None,
    awards: pl.DataFrame | None,
) -> dict[str, Any]:
    """Generate risk alerts.

    Args:
        repo_health: Repository health metrics.
        hygiene_scores: Repository hygiene scores.
        awards: Awards (including risk signals).

    Returns:
        Dictionary with risk alerts.
    """
    alerts = {}

    # Low hygiene scores (< 50)
    low_hygiene_repos = []
    if hygiene_scores is not None and len(hygiene_scores) > 0:
        low_hygiene = hygiene_scores[hygiene_scores["score"] < 50].sort_values("score")
        low_hygiene_repos = [
            {
                "repo_name": row["repo_full_name"],
                "score": int(row["score"]),
                "issues": row["notes"] if pd.notna(row["notes"]) else "",
            }
            for _, row in low_hygiene.head(5).iterrows()
        ]

    alerts["low_hygiene"] = {
        "count": len(low_hygiene_repos),
        "repos": low_hygiene_repos,
        "description": "Repositories with code quality scores below 50",
    }

    # Stale PRs
    stale_pr_count = 0
    if repo_health is not None and len(repo_health) > 0:
        stale_pr_count = int(repo_health["stale_pr_count"].sum())

    alerts["stale_prs"] = {
        "count": stale_pr_count,
        "description": "Pull requests open for more than 30 days",
    }

    # Repos without branch protection (from risk signals in awards)
    no_protection_count = 0
    if awards is not None and len(awards) > 0:
        no_protection = awards.filter(
            (pl.col("category") == "risk") & (pl.col("award_key") == "no_branch_protection")
        )
        if len(no_protection) > 0:
            # Parse count from supporting_stats or winner_name
            winner_name = no_protection.row(0, named=True).get("winner_name", "0 repositories")
            if "repositories" in winner_name:
                no_protection_count = int(winner_name.split()[0])

    alerts["no_branch_protection"] = {
        "count": no_protection_count,
        "description": "Repositories without branch protection enabled",
    }

    return alerts


def _generate_awards_summary(awards: pl.DataFrame | None) -> list[dict[str, str]]:
    """Generate awards summary (top 5 awards).

    Args:
        awards: Awards table.

    Returns:
        List of top 5 awards with user-friendly descriptions.
    """
    if awards is None or len(awards) == 0:
        return []

    # Filter to individual and repository awards (exclude risk signals)
    filtered_awards = awards.filter(pl.col("category").is_in(["individual", "repository"]))

    if len(filtered_awards) == 0:
        return []

    # Take first 5 awards
    top_awards = filtered_awards.head(5)

    return [
        {
            "title": row["title"],
            "winner": row["winner_name"],
            "description": row["description"],
            "category": row["category"],
        }
        for row in top_awards.iter_rows(named=True)
    ]
