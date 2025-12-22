"""Chart data generation functions for D3.js visualization."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

__all__ = ["generate_chart_data"]


def generate_chart_data(
    timeseries_data: dict[str, Any],
    summary_data: dict[str, Any],
    leaderboards_data: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    """Generate chart-ready data from metrics for D3.js visualization.

    Args:
        timeseries_data: Time series data from timeseries.json.
        summary_data: Summary statistics from summary.json.
        leaderboards_data: Leaderboard data from leaderboards.json.

    Returns:
        Dictionary with chart data arrays:
        - collaboration_data: Weekly reviews, comments, cross-team activity
        - velocity_data: Weekly PRs opened/merged, time to merge
        - quality_data: Weekly review coverage, CI pass rate
        - community_data: Weekly active contributors, new contributors
    """
    chart_data: dict[str, list[dict[str, Any]]] = {
        "collaboration_data": [],
        "velocity_data": [],
        "quality_data": [],
        "community_data": [],
    }

    weekly_data = timeseries_data.get("weekly", {})

    # Build collaboration_data: reviews, comments, cross_team activity
    collaboration_data = _generate_collaboration_data(weekly_data)
    chart_data["collaboration_data"] = collaboration_data

    # Build velocity_data: prs_opened, prs_merged, time_to_merge
    velocity_data = _generate_velocity_data(weekly_data)
    chart_data["velocity_data"] = velocity_data

    # Build quality_data: review_coverage, ci_pass_rate (placeholder)
    quality_data = _generate_quality_data(weekly_data)
    chart_data["quality_data"] = quality_data

    # Build community_data: active_contributors, new_contributors
    community_data = _generate_community_data(weekly_data)
    chart_data["community_data"] = community_data

    return chart_data


def _generate_collaboration_data(weekly_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Generate collaboration chart data from weekly timeseries.

    Args:
        weekly_data: Weekly metrics from timeseries data.

    Returns:
        List of {date, reviews, comments, cross_team} dicts sorted chronologically.
    """
    collaboration_by_week: dict[str, dict[str, int]] = defaultdict(
        lambda: {"reviews": 0, "comments": 0, "cross_team": 0}
    )

    # Aggregate reviews submitted
    reviews_data = weekly_data.get("reviews_submitted", [])
    for entry in reviews_data:
        period = entry.get("period", "")
        count = entry.get("count", 0)
        if period:
            collaboration_by_week[period]["reviews"] += count

    # Aggregate comments (review comments + issue comments)
    review_comments = weekly_data.get("review_comments", [])
    for entry in review_comments:
        period = entry.get("period", "")
        count = entry.get("count", 0)
        if period:
            collaboration_by_week[period]["comments"] += count

    issue_comments = weekly_data.get("issue_comments", [])
    for entry in issue_comments:
        period = entry.get("period", "")
        count = entry.get("count", 0)
        if period:
            collaboration_by_week[period]["comments"] += count

    # Cross-team reviews - not directly available, use 0 for now
    # TODO: Implement cross-team detection based on repo ownership

    # Convert to chart format with ISO dates
    result = []
    for period in sorted(collaboration_by_week.keys()):
        iso_date = _period_to_iso_date(period)
        if iso_date:
            data = collaboration_by_week[period]
            result.append(
                {
                    "date": iso_date,
                    "reviews": data["reviews"],
                    "comments": data["comments"],
                    "cross_team": data["cross_team"],
                }
            )

    return result


def _generate_velocity_data(weekly_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Generate velocity chart data from weekly timeseries.

    Args:
        weekly_data: Weekly metrics from timeseries data.

    Returns:
        List of {date, prs_opened, prs_merged, time_to_merge} dicts sorted chronologically.
    """
    velocity_by_week: dict[str, dict[str, int]] = defaultdict(
        lambda: {"prs_opened": 0, "prs_merged": 0, "time_to_merge": 0}
    )

    # Aggregate PRs opened
    prs_opened = weekly_data.get("prs_opened", [])
    for entry in prs_opened:
        period = entry.get("period", "")
        count = entry.get("count", 0)
        if period:
            velocity_by_week[period]["prs_opened"] += count

    # Aggregate PRs merged
    prs_merged = weekly_data.get("prs_merged", [])
    for entry in prs_merged:
        period = entry.get("period", "")
        count = entry.get("count", 0)
        if period:
            velocity_by_week[period]["prs_merged"] += count

    # Time to merge - not directly available in weekly aggregates
    # TODO: Calculate from PR-level data when available

    # Convert to chart format with ISO dates
    result = []
    for period in sorted(velocity_by_week.keys()):
        iso_date = _period_to_iso_date(period)
        if iso_date:
            data = velocity_by_week[period]
            result.append(
                {
                    "date": iso_date,
                    "prs_opened": data["prs_opened"],
                    "prs_merged": data["prs_merged"],
                    "time_to_merge": data["time_to_merge"],
                }
            )

    return result


def _generate_quality_data(weekly_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Generate quality chart data from weekly timeseries.

    Args:
        weekly_data: Weekly metrics from timeseries data.

    Returns:
        List of {date, review_coverage, ci_pass_rate} dicts sorted chronologically.
    """
    # Quality metrics are not available in weekly aggregates
    # These would require PR-level data or repo health snapshots over time
    # Return empty list for now
    # TODO: Implement when repo health time series data is available

    return []


def _generate_community_data(weekly_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Generate community chart data from weekly timeseries.

    Args:
        weekly_data: Weekly metrics from timeseries data.

    Returns:
        List of {date, active_contributors, new_contributors} dicts sorted chronologically.
    """
    community_by_week: dict[str, set[str]] = defaultdict(set)

    # Track unique contributors per week across all activity types
    metric_types = [
        "prs_opened",
        "prs_merged",
        "reviews_submitted",
        "issues_opened",
        "issues_closed",
        "review_comments",
        "issue_comments",
    ]

    for metric_name in metric_types:
        metric_data = weekly_data.get(metric_name, [])
        for entry in metric_data:
            period = entry.get("period", "")
            user = entry.get("user", "")
            if period and user:
                community_by_week[period].add(user)

    # Convert to chart format with ISO dates
    result = []
    for period in sorted(community_by_week.keys()):
        iso_date = _period_to_iso_date(period)
        if iso_date:
            active_count = len(community_by_week[period])
            result.append(
                {
                    "date": iso_date,
                    "active_contributors": active_count,
                    "new_contributors": 0,  # TODO: Track first-time contributors
                }
            )

    return result


def _period_to_iso_date(period: str) -> str | None:
    """Convert period string (YYYY-WXX) to ISO date string (YYYY-MM-DD).

    Args:
        period: Period string in format "YYYY-WXX" (e.g., "2025-W07").

    Returns:
        ISO date string for the Monday of that week, or None if parsing fails.
    """
    try:
        year, week = period.split("-W")
        year_int = int(year)
        week_int = int(week)

        # Calculate ISO date for Monday of this week
        jan4 = datetime(year_int, 1, 4)
        week1_monday = jan4 - timedelta(days=jan4.weekday())
        target_monday = week1_monday + timedelta(weeks=week_int - 1)

        return target_monday.strftime("%Y-%m-%d")
    except (ValueError, AttributeError) as e:
        logger.warning("Failed to parse period %s: %s", period, e)
        return None
