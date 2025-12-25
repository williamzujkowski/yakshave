"""Highlights and fun facts calculation functions."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

__all__ = ["calculate_fun_facts", "calculate_highlights"]


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
        "reviews_per_pr": 0.0,
        "new_contributors": 0,
    }

    # Calculate most active month from timeseries data
    try:
        monthly_prs: dict[str, int] = defaultdict(int)

        # Handle flat list format: [{"period_start": "2025-01-01", "metric_key": "prs_merged", ...}]
        if isinstance(timeseries_data, list):
            for entry in timeseries_data:
                if entry.get("metric_key") != "prs_merged":
                    continue
                if entry.get("scope") != "org":
                    continue
                period_start = entry.get("period_start", "")
                value = entry.get("value", 0)
                if period_start and value:
                    try:
                        dt = datetime.strptime(period_start, "%Y-%m-%d")
                        month_key = dt.strftime("%B %Y")
                        monthly_prs[month_key] += value
                    except (ValueError, AttributeError):
                        continue
        elif isinstance(timeseries_data, dict):
            # Transformed format: {weekly: {prs_merged: [{period, count}]}, monthly: {...}}
            # Try weekly data first (aggregate into months), then monthly
            weekly_data = timeseries_data.get("weekly", {})
            prs_merged = weekly_data.get("prs_merged", [])

            if prs_merged:
                # Aggregate weekly data into months
                for entry in prs_merged:
                    period = entry.get("period", "")
                    count = entry.get("count", 0)
                    if period and count:
                        try:
                            dt = datetime.strptime(period, "%Y-%m-%d")
                            month_key = dt.strftime("%B %Y")
                            monthly_prs[month_key] += count
                        except (ValueError, AttributeError):
                            continue
            else:
                # Fallback to monthly data if available
                monthly_data = timeseries_data.get("monthly", {})
                prs_merged = monthly_data.get("prs_merged", [])
                for entry in prs_merged:
                    period = entry.get("period", "")
                    count = entry.get("count", 0)
                    if period:
                        try:
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

    # Calculate review metrics from summary data
    # reviews_per_pr: Average reviews per PR (ratio, e.g., 1.0 means 1 review per PR)
    # review_coverage: Capped at 100% for display purposes
    try:
        total_prs = summary_data.get("total_prs", 0)
        total_reviews = summary_data.get("total_reviews", 0)

        if total_prs > 0:
            reviews_per_pr = total_reviews / total_prs
            highlights["reviews_per_pr"] = round(reviews_per_pr, 1)
            # Cap at 100% for display - shows "at least 100% have reviews"
            highlights["review_coverage"] = min(round(reviews_per_pr * 100, 1), 100.0)

    except Exception as e:
        logger.warning("Failed to calculate review metrics: %s", e)

    # Calculate average merge time from repo health data
    # median_time_to_merge is stored in hours
    try:
        if repo_health_list:
            merge_times: list[float] = []
            for r in repo_health_list:
                merge_val = r.get("median_time_to_merge")
                if merge_val is not None and float(merge_val) > 0:
                    merge_times.append(float(merge_val))
            if merge_times:
                avg_merge_time_hours = sum(merge_times) / len(merge_times)
                # Format based on magnitude
                if avg_merge_time_hours < 1:
                    minutes = avg_merge_time_hours * 60
                    highlights["avg_review_time"] = f"{minutes:.0f} minutes"
                elif avg_merge_time_hours < 24:
                    highlights["avg_review_time"] = f"{avg_merge_time_hours:.1f} hours"
                else:
                    days = avg_merge_time_hours / 24
                    highlights["avg_review_time"] = f"{days:.1f} days"

    except Exception as e:
        logger.warning("Failed to calculate average merge time: %s", e)

    # New contributors - get from summary data
    highlights["new_contributors"] = summary_data.get("new_contributors", 0)

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
        # Handle case where weekly_data is a list instead of dict (malformed data)
        if not isinstance(weekly_data, dict):
            weekly_data = {}
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
                # Handle both formats: "YYYY-WXX" and "YYYY-MM-DD"
                try:
                    if "-W" in busiest_period:
                        # ISO week format: "2025-W07"
                        year, week = busiest_period.split("-W")
                        year_int = int(year)
                        week_int = int(week)

                        # Calculate ISO date for Monday of this week
                        jan4 = datetime(year_int, 1, 4)
                        week1_monday = jan4 - timedelta(days=jan4.weekday())
                        target_monday = week1_monday + timedelta(weeks=week_int - 1)

                        busiest_day = target_monday.strftime("%B %d, %Y")
                    else:
                        # Date format: "2025-01-15" - parse and format
                        dt = datetime.strptime(busiest_period, "%Y-%m-%d")
                        busiest_day = dt.strftime("%B %d, %Y")
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
