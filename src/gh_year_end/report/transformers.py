"""Data transformation functions for report generation."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

__all__ = [
    "calculate_fun_facts",
    "calculate_highlights",
    "transform_activity_timeline",
    "transform_awards_data",
    "transform_leaderboards",
]


def transform_awards_data(awards_data: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Transform awards from simple key-value format to categorized format.

    Transforms from:
        {"top_pr_author": {"user": "...", "count": 10, "avatar_url": "..."}}
    To:
        {"individual": [{"award_key": "top_pr_author", "title": "...", ...}]}
    """
    awards_by_category: dict[str, list[Any]] = {
        "individual": [],
        "repository": [],
        "risk": [],
    }

    # Map award keys to display info
    award_definitions = {
        "top_pr_author": {
            "category": "individual",
            "title": "Top PR Author",
            "description": "Most pull requests opened",
            "stat_label": "PRs opened",
        },
        "top_reviewer": {
            "category": "individual",
            "title": "Top Reviewer",
            "description": "Most reviews submitted",
            "stat_label": "reviews",
        },
        "top_issue_opener": {
            "category": "individual",
            "title": "Top Issue Opener",
            "description": "Most issues opened",
            "stat_label": "issues",
        },
    }

    # Transform each award
    for award_key, award_data in awards_data.items():
        if award_key not in award_definitions:
            continue

        definition = award_definitions[award_key]
        category = definition["category"]

        transformed_award = {
            "award_key": award_key,
            "title": definition["title"],
            "description": definition["description"],
            "winner_name": award_data.get("user", ""),
            "winner_avatar_url": award_data.get("avatar_url", ""),
            "supporting_stats": f"{award_data.get('count', 0)} {definition['stat_label']}",
        }

        awards_by_category[category].append(transformed_award)

    return awards_by_category


def transform_leaderboards(leaderboards_data: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Transform leaderboards data to flat format expected by templates.

    Templates expect: leaderboards.prs_merged, leaderboards.reviews_submitted, etc.
    as direct lists of {login, avatar_url, value} dicts.

    Transforms from export.py format: {"user": "...", "count": 123, "avatar_url": "..."}
    To template format: {"login": "...", "value": 123, "avatar_url": "..."}
    """
    result: dict[str, list[dict[str, Any]]] = {
        "prs_merged": [],
        "prs_opened": [],
        "reviews_submitted": [],
        "approvals": [],
        "changes_requested": [],
        "issues_opened": [],
        "issues_closed": [],
        "comments_total": [],
        "review_comments_total": [],
        "overall": [],
    }

    # Handle both nested format (leaderboards: {metrics}) and flat format (metrics at top level)
    nested = leaderboards_data.get("leaderboards", leaderboards_data)

    for metric_name in result:
        metric_data = nested.get(metric_name, [])

        # Data may be nested under "org" key or direct list
        if isinstance(metric_data, dict):
            raw_list = metric_data.get("org", [])
        elif isinstance(metric_data, list):
            raw_list = metric_data
        else:
            raw_list = []

        # Transform each entry to match template expectations
        transformed_list = []
        for entry in raw_list:
            # Handle different field name formats
            # Export format: {"user": "...", "count": 123}
            # Template expects: {"login": "...", "value": 123}
            transformed_entry = {
                "login": entry.get("login") or entry.get("user", ""),
                "avatar_url": entry.get("avatar_url", ""),
                "value": entry.get("value") or entry.get("count", 0),
            }

            # For overall leaderboard, include additional metrics
            if metric_name == "overall":
                transformed_entry.update(
                    {
                        "prs_merged": entry.get("prs_merged", 0),
                        "reviews_submitted": entry.get("reviews_submitted", 0),
                        "issues_closed": entry.get("issues_closed", 0),
                        "comments_total": entry.get("comments_total", 0),
                        "overall_score": entry.get("overall_score")
                        or entry.get("value")
                        or entry.get("count", 0),
                    }
                )

            transformed_list.append(transformed_entry)

        result[metric_name] = transformed_list

    return result


def transform_activity_timeline(timeseries_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Transform timeseries data to activity timeline format for D3.js charts.

    Args:
        timeseries_data: Time series data from timeseries.json.

    Returns:
        List of {date: str, value: int} dicts for the activity chart.
        Uses prs_merged metric as the primary activity indicator.
    """
    activity_timeline = []

    try:
        # Timeseries data structure: {"weekly": {"prs_merged": [{period, user, count}]}}
        weekly_data = timeseries_data.get("weekly", {})
        prs_merged = weekly_data.get("prs_merged", [])

        if prs_merged:
            # Group by period and sum counts across all users
            period_totals: dict[str, int] = defaultdict(int)

            for entry in prs_merged:
                period = entry.get("period", "")
                count = entry.get("count", 0)

                if period:
                    period_totals[period] += count

            # Convert to D3.js format: {date: ISO string, value: number}
            # Period format is "YYYY-WXX", convert to ISO date (first day of week)
            for period, total_count in sorted(period_totals.items()):
                try:
                    # Parse week format: "2025-W07" -> ISO date of Monday of that week
                    year, week = period.split("-W")
                    year_int = int(year)
                    week_int = int(week)

                    # Calculate ISO date for Monday of this week
                    # ISO week 1 is the first week with a Thursday in the new year
                    jan4 = datetime(year_int, 1, 4)
                    week1_monday = jan4 - timedelta(days=jan4.weekday())
                    target_monday = week1_monday + timedelta(weeks=week_int - 1)

                    activity_timeline.append(
                        {
                            "date": target_monday.strftime("%Y-%m-%d"),
                            "value": total_count,
                        }
                    )
                except (ValueError, AttributeError) as e:
                    logger.warning("Failed to parse period %s: %s", period, e)
                    continue

            logger.info("Transformed %d activity timeline entries", len(activity_timeline))
        else:
            logger.warning("No weekly prs_merged data found in timeseries")

    except Exception as e:
        logger.warning("Failed to transform activity timeline: %s", e)

    return activity_timeline


def calculate_highlights(
    summary_data: dict[str, Any],
    timeseries_data: dict[str, Any],
    repo_health_list: list[dict[str, Any]],
) -> dict[str, Any]:
    """Calculate highlights section from metrics data.

    Args:
        summary_data: Summary statistics from summary.json.
        timeseries_data: Time series data from timeseries.json.
        repo_health_list: Repository health metrics.

    Returns:
        Dictionary with highlight values.
    """
    highlights: dict[str, Any] = {
        "most_active_month": "N/A",
        "most_active_month_prs": 0,
        "avg_review_time": "N/A",
        "review_coverage": 0,
        "new_contributors": 0,
    }

    # Calculate most active month from timeseries data
    try:
        # Timeseries data structure: {"monthly": {"prs_merged": [{period, user, count}]}}
        monthly_data = timeseries_data.get("monthly", {})
        prs_merged = monthly_data.get("prs_merged", [])

        if prs_merged:
            # Group by month and sum PRs across all users
            monthly_prs: dict[str, int] = defaultdict(int)

            for entry in prs_merged:
                period = entry.get("period", "")  # Format: "2025-11"
                count = entry.get("count", 0)

                if period:
                    try:
                        # Parse period format: "2025-11" -> "November 2025"
                        dt = datetime.strptime(period, "%Y-%m")
                        month_key = dt.strftime("%B %Y")
                        monthly_prs[month_key] += count
                    except (ValueError, AttributeError):
                        continue

            if monthly_prs:
                most_active = max(monthly_prs.items(), key=lambda x: x[1])
                highlights["most_active_month"] = most_active[0]
                highlights["most_active_month_prs"] = most_active[1]

    except Exception as e:
        logger.warning("Failed to calculate most active month: %s", e)

    # Calculate average review coverage from repo health data
    try:
        if repo_health_list:
            review_coverages = [
                r.get("review_coverage", 0)
                for r in repo_health_list
                if r.get("review_coverage") is not None
            ]
            if review_coverages:
                avg_coverage = sum(review_coverages) / len(review_coverages)
                highlights["review_coverage"] = round(avg_coverage, 1)

    except Exception as e:
        logger.warning("Failed to calculate review coverage: %s", e)

    # Calculate average review time from repo health data
    try:
        if repo_health_list:
            review_times: list[float] = [
                float(r.get("median_time_to_first_review", 0))
                for r in repo_health_list
                if r.get("median_time_to_first_review") is not None
            ]
            if review_times:
                avg_review_time_seconds = sum(review_times) / len(review_times)
                # Convert to hours
                hours = avg_review_time_seconds / 3600
                if hours < 1:
                    minutes = avg_review_time_seconds / 60
                    highlights["avg_review_time"] = f"{minutes:.0f} minutes"
                elif hours < 24:
                    highlights["avg_review_time"] = f"{hours:.1f} hours"
                else:
                    days = hours / 24
                    highlights["avg_review_time"] = f"{days:.1f} days"

    except Exception as e:
        logger.warning("Failed to calculate average review time: %s", e)

    # New contributors - would need contributor data from metrics
    # For now, keep as 0 since we don't have first-contribution tracking
    highlights["new_contributors"] = 0

    return highlights


def calculate_fun_facts(
    summary_data: dict[str, Any],
    timeseries_data: dict[str, Any],
    leaderboards_data: dict[str, Any],
) -> dict[str, Any]:
    """Calculate fun facts from available metrics data.

    Args:
        summary_data: Summary statistics from summary.json.
        timeseries_data: Time series data from timeseries.json.
        leaderboards_data: Leaderboard data from leaderboards.json.

    Returns:
        Dictionary with fun fact values or None for unavailable metrics.
    """
    fun_facts: dict[str, Any] = {}

    # Total comments from summary data (actual data available)
    fun_facts["total_comments"] = summary_data.get("total_comments", 0)

    # Calculate busiest day from timeseries weekly data
    busiest_day = None
    most_active_day_count = 0

    try:
        weekly_data = timeseries_data.get("weekly", {})
        # Use prs_opened as activity indicator
        prs_opened = weekly_data.get("prs_opened", [])

        if prs_opened:
            # Group by period and sum counts
            period_totals: dict[str, int] = defaultdict(int)

            for entry in prs_opened:
                period = entry.get("period", "")
                count = entry.get("count", 0)
                if period:
                    period_totals[period] += count

            if period_totals:
                # Find busiest week
                busiest_period, max_count = max(period_totals.items(), key=lambda x: x[1])
                most_active_day_count = max_count

                # Convert period to readable date
                try:
                    year, week = busiest_period.split("-W")
                    year_int = int(year)
                    week_int = int(week)

                    # Calculate ISO date for Monday of this week
                    jan4 = datetime(year_int, 1, 4)
                    week1_monday = jan4 - timedelta(days=jan4.weekday())
                    target_monday = week1_monday + timedelta(weeks=week_int - 1)

                    busiest_day = target_monday.strftime("%B %d, %Y")
                except (ValueError, AttributeError) as e:
                    logger.warning("Failed to parse busiest period %s: %s", busiest_period, e)
                    busiest_day = None

    except Exception as e:
        logger.warning("Failed to calculate busiest day: %s", e)

    fun_facts["busiest_day"] = busiest_day
    fun_facts["busiest_day_count"] = most_active_day_count if busiest_day else None

    # Most active hour - not available from current data
    fun_facts["most_active_hour"] = None

    # Total lines changed and average PR size - not available from current data
    # Would require PR details with additions/deletions
    fun_facts["total_lines_changed"] = None
    fun_facts["avg_pr_size"] = None

    # Most used emoji - not available (requires comment text analysis)
    fun_facts["most_used_emoji"] = None

    return fun_facts
