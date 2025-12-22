"""Chart data generation functions for D3.js visualization."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

__all__ = [
    "generate_chart_data",
    "generate_engineer_charts",
]


def generate_chart_data(
    timeseries_data: dict[str, Any],
    summary_data: dict[str, Any],
    leaderboards_data: dict[str, Any],
    repo_health_list: list[dict[str, Any]] | None = None,
    hygiene_scores_list: list[dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Generate chart-ready data from metrics for D3.js visualization.

    Args:
        timeseries_data: Time series data from timeseries.json.
        summary_data: Summary statistics from summary.json.
        leaderboards_data: Leaderboard data from leaderboards.json.
        repo_health_list: Optional list of repo health metrics.
        hygiene_scores_list: Optional list of hygiene scores for quality chart.

    Returns:
        Dictionary with chart data arrays:
        - collaboration_data: Weekly reviews, comments, cross-team activity
        - velocity_data: Weekly PRs opened/merged, time to merge
        - quality_data: Bar chart of hygiene adoption rates
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
    velocity_data = _generate_velocity_data(weekly_data, repo_health_list or [])
    chart_data["velocity_data"] = velocity_data

    # Build quality_data: hygiene adoption rates as bar chart
    quality_data = _generate_quality_data_from_hygiene(hygiene_scores_list or [])
    chart_data["quality_data"] = quality_data

    # Build community_data: active_contributors, new_contributors
    community_data = _generate_community_data(weekly_data, summary_data)
    chart_data["community_data"] = community_data

    return chart_data


def generate_engineer_charts(
    timeseries_data: dict[str, Any],
    summary_data: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    """Generate chart data for Engineers page.

    Args:
        timeseries_data: Time series data from timeseries.json.
        summary_data: Summary statistics from summary.json.

    Returns:
        Dictionary with engineer chart data:
        - contribution_timeline: Weekly total contributions over time
        - contribution_types: Distribution by activity type (PRs, Reviews, Issues, Comments)
        - contribution_by_repo: Top 10 repos by total contributions
    """
    weekly_data = timeseries_data.get("weekly", {})

    return {
        "contribution_timeline": _generate_contribution_timeline(weekly_data),
        "contribution_types": _generate_contribution_types(weekly_data, summary_data),
        "contribution_by_repo": _generate_contribution_by_repo(weekly_data),
    }


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

    # Cross-team reviews - requires team/org ownership data not currently tracked.
    # Future enhancement: would need repo ownership mapping and contributor team assignments
    # to detect when a contributor from one team reviews PRs in another team's repo.

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


def _generate_velocity_data(
    weekly_data: dict[str, Any], repo_health_list: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Generate velocity chart data from weekly timeseries.

    Args:
        weekly_data: Weekly metrics from timeseries data.
        repo_health_list: Repository health metrics containing median_time_to_merge.

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

    # Calculate overall median time to merge from repo health data
    # Note: This is a global average, not per-week. Weekly time-to-merge would require
    # storing merge timestamps with PRs and aggregating by week.
    merge_times = [
        repo.get("median_time_to_merge", 0)
        for repo in repo_health_list
        if repo.get("median_time_to_merge") is not None
    ]
    overall_median_time_to_merge = int(sum(merge_times) / len(merge_times)) if merge_times else 0

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
                    "time_to_merge": overall_median_time_to_merge,
                }
            )

    return result


def _generate_quality_data_from_hygiene(
    hygiene_scores_list: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Generate quality chart data from current hygiene metrics.

    Since we don't have time-series hygiene data, this creates a bar chart showing
    current adoption rates of key quality practices across all repositories.

    Args:
        hygiene_scores_list: List of repository hygiene scores with has_* fields.

    Returns:
        List of {category, value} dicts for bar chart visualization.
        Value represents percentage of repos that have adopted each practice.
    """
    if not hygiene_scores_list:
        return []

    total_repos = len(hygiene_scores_list)
    if total_repos == 0:
        return []

    # Count repos with each quality practice
    ci_count = sum(1 for h in hygiene_scores_list if h.get("has_ci_workflows", False))
    security_count = sum(1 for h in hygiene_scores_list if h.get("has_security_md", False))
    codeowners_count = sum(1 for h in hygiene_scores_list if h.get("has_codeowners", False))
    protection_count = sum(
        1 for h in hygiene_scores_list if h.get("branch_protection_enabled", False)
    )

    # Calculate percentages and create bar chart data
    quality_data = [
        {
            "category": "CI Workflows",
            "value": round((ci_count / total_repos) * 100, 1),
        },
        {
            "category": "Security Policy",
            "value": round((security_count / total_repos) * 100, 1),
        },
        {
            "category": "Code Owners",
            "value": round((codeowners_count / total_repos) * 100, 1),
        },
        {
            "category": "Branch Protection",
            "value": round((protection_count / total_repos) * 100, 1),
        },
    ]

    logger.info(
        "Generated quality chart data: CI=%.1f%%, Security=%.1f%%, "
        "CodeOwners=%.1f%%, Protection=%.1f%%",
        quality_data[0]["value"],
        quality_data[1]["value"],
        quality_data[2]["value"],
        quality_data[3]["value"],
    )

    return quality_data


def _generate_community_data(
    weekly_data: dict[str, Any], summary_data: dict[str, Any]
) -> list[dict[str, Any]]:
    """Generate community chart data from weekly timeseries.

    Args:
        weekly_data: Weekly metrics from timeseries data.
        summary_data: Summary statistics containing total new_contributors.

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

    # Note: summary_data contains total new_contributors for the year, but tracking
    # first-time contributors per week would require storing the week each contributor
    # first appeared during collection. For now, use 0 for per-week values.

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
                    # Use 0 for per-week new contributors since we only track the year total
                    # Future: track first appearance week to show new contributors per week
                    "new_contributors": 0,
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


def _generate_contribution_timeline(weekly_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Generate contribution timeline chart data for Engineers page.

    Aggregates all contribution types (PRs, reviews, issues, comments) per week.

    Args:
        weekly_data: Weekly metrics from timeseries data.

    Returns:
        List of {date, count} dicts sorted chronologically.
    """
    contribution_by_week: dict[str, int] = defaultdict(int)

    # Aggregate all contribution types per week
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
            count = entry.get("count", 0)
            if period:
                contribution_by_week[period] += count

    # Convert to chart format with ISO dates
    result = []
    for period in sorted(contribution_by_week.keys()):
        iso_date = _period_to_iso_date(period)
        if iso_date:
            result.append(
                {
                    "date": iso_date,
                    "count": contribution_by_week[period],
                }
            )

    logger.info("Generated contribution timeline with %d weeks of data", len(result))
    return result


def _generate_contribution_types(
    weekly_data: dict[str, Any], summary_data: dict[str, Any]
) -> list[dict[str, Any]]:
    """Generate contribution types distribution for Engineers page.

    Shows breakdown by activity type: PRs, Reviews, Issues, Comments.

    Args:
        weekly_data: Weekly metrics from timeseries data.
        summary_data: Summary statistics for totals.

    Returns:
        List of {type, count} dicts for donut chart.
    """
    # Aggregate totals across all weeks
    totals = {
        "PRs": 0,
        "Reviews": 0,
        "Issues": 0,
        "Comments": 0,
    }

    # Count PRs (opened + merged, avoiding double-count)
    prs_opened = weekly_data.get("prs_opened", [])
    totals["PRs"] = sum(entry.get("count", 0) for entry in prs_opened)

    # Count Reviews
    reviews = weekly_data.get("reviews_submitted", [])
    totals["Reviews"] = sum(entry.get("count", 0) for entry in reviews)

    # Count Issues (opened + closed, avoiding double-count)
    issues_opened = weekly_data.get("issues_opened", [])
    totals["Issues"] = sum(entry.get("count", 0) for entry in issues_opened)

    # Count Comments (review + issue)
    review_comments = weekly_data.get("review_comments", [])
    issue_comments = weekly_data.get("issue_comments", [])
    totals["Comments"] = sum(entry.get("count", 0) for entry in review_comments) + sum(
        entry.get("count", 0) for entry in issue_comments
    )

    # Convert to chart format
    result = [{"type": activity_type, "count": count} for activity_type, count in totals.items()]

    logger.info(
        "Generated contribution types: PRs=%d, Reviews=%d, Issues=%d, Comments=%d",
        totals["PRs"],
        totals["Reviews"],
        totals["Issues"],
        totals["Comments"],
    )

    return result


def _generate_contribution_by_repo(weekly_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Generate contribution by repository chart data for Engineers page.

    Shows top 10 repositories by total contributions.

    Args:
        weekly_data: Weekly metrics from timeseries data.

    Returns:
        List of {repo, count} dicts sorted by count descending (top 10).
    """
    contribution_by_repo: dict[str, int] = defaultdict(int)

    # Aggregate contributions per repo across all activity types
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
            repo = entry.get("repo", "")
            count = entry.get("count", 0)
            if repo:
                contribution_by_repo[repo] += count

    # Sort by count descending and take top 10
    sorted_repos = sorted(contribution_by_repo.items(), key=lambda x: x[1], reverse=True)[:10]

    result = [{"repo": repo, "count": count} for repo, count in sorted_repos]

    logger.info(
        "Generated contribution by repo chart with %d repos (top 10 of %d total)",
        len(result),
        len(contribution_by_repo),
    )

    return result
