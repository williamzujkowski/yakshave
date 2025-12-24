"""Time series data transformation functions."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

__all__ = ["transform_activity_timeline"]


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
        # Handle case where weekly_data is a list instead of dict (malformed data)
        if not isinstance(weekly_data, dict):
            weekly_data = {}
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
            # Handle both period formats:
            # - "YYYY-WXX": ISO week format (original aggregator output)
            # - "YYYY-MM-DD": Date format (from metrics_time_series.json)
            for period, total_count in sorted(period_totals.items()):
                try:
                    if "-W" in period:
                        # Parse week format: "2025-W07" -> ISO date of Monday of that week
                        year, week = period.split("-W")
                        year_int = int(year)
                        week_int = int(week)

                        # Calculate ISO date for Monday of this week
                        # ISO week 1 is the first week with a Thursday in the new year
                        jan4 = datetime(year_int, 1, 4)
                        week1_monday = jan4 - timedelta(days=jan4.weekday())
                        target_monday = week1_monday + timedelta(weeks=week_int - 1)
                        date_str = target_monday.strftime("%Y-%m-%d")
                    else:
                        # Assume date format: "YYYY-MM-DD" - use as-is
                        # Validate it's a valid date
                        datetime.strptime(period, "%Y-%m-%d")
                        date_str = period

                    activity_timeline.append(
                        {
                            "date": date_str,
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
