"""Contributor data processing for report generation."""

import logging
from collections import defaultdict
from typing import Any

logger = logging.getLogger(__name__)

__all__ = [
    "get_engineers_list",
    "populate_activity_timelines",
]


def get_engineers_list(
    leaderboards_data: dict[str, Any], timeseries_data: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Extract engineers list with activity_timeline from leaderboards data.

    Templates expect each engineer to have:
    - user_id, login, avatar_url, rank
    - prs_merged, prs_opened, reviews_submitted, approvals
    - issues_opened, issues_closed, comments_total
    - activity_timeline (array for sparkline chart)

    This function merges data from ALL available leaderboard metrics to create
    a complete contributor list.

    Args:
        leaderboards_data: Leaderboard metrics data.
        timeseries_data: Optional timeseries data for activity sparklines.
    """
    # Handle both nested format (leaderboards: {metrics}) and flat format (metrics at top level)
    if "leaderboards" in leaderboards_data:
        metrics_data = leaderboards_data.get("leaderboards", {})
    else:
        metrics_data = leaderboards_data

    # Build a dictionary of all contributors across all metrics
    contributors: dict[str, dict[str, Any]] = {}

    # Metrics we want to include in the contributor data
    metric_names = [
        "prs_merged",
        "prs_opened",
        "reviews_submitted",
        "approvals",
        "changes_requested",
        "issues_opened",
        "issues_closed",
        "comments_total",
        "review_comments_total",
    ]

    # Process each metric and merge contributor data
    for metric_name in metric_names:
        metric_data = metrics_data.get(metric_name, [])

        # Handle both list format and dict with org key
        if isinstance(metric_data, dict):
            org_data = metric_data.get("org", [])
        elif isinstance(metric_data, list):
            org_data = metric_data
        else:
            org_data = []

        # Add/update contributor data
        for entry in org_data:
            # Handle both formats: user_id or user key
            user_id = entry.get("user_id") or entry.get("user")
            if not user_id:
                continue

            # Initialize contributor if not seen before
            if user_id not in contributors:
                contributors[user_id] = {
                    "user_id": user_id,
                    "login": entry.get("login") or entry.get("user", "unknown"),
                    "avatar_url": entry.get("avatar_url", ""),
                    "display_name": entry.get("display_name"),
                    "prs_merged": 0,
                    "prs_opened": 0,
                    "reviews_submitted": 0,
                    "approvals": 0,
                    "changes_requested": 0,
                    "issues_opened": 0,
                    "issues_closed": 0,
                    "comments_total": 0,
                    "review_comments_total": 0,
                    "activity_timeline": [],
                }

            # Update the specific metric value (handle both value and count keys)
            contributors[user_id][metric_name] = entry.get("value") or entry.get("count", 0)

            # Keep the login/avatar if not already set
            if entry.get("login") or entry.get("user"):
                contributors[user_id]["login"] = entry.get("login") or entry.get("user")
            if entry.get("avatar_url"):
                contributors[user_id]["avatar_url"] = entry.get("avatar_url")

    # Convert to list and sort by total activity (descending)
    result = list(contributors.values())

    # Calculate total contributions for sorting
    for contributor in result:
        total = (
            contributor["prs_merged"]
            + contributor["prs_opened"]
            + contributor["reviews_submitted"]
            + contributor["issues_opened"]
            + contributor["issues_closed"]
            + contributor["comments_total"]
        )
        contributor["contributions_total"] = total

    # Sort by total contributions (descending)
    result.sort(key=lambda x: x["contributions_total"], reverse=True)

    # Assign ranks based on sorted order
    for idx, contributor in enumerate(result):
        contributor["rank"] = idx + 1

    # Populate activity_timeline from timeseries data if available
    if timeseries_data:
        populate_activity_timelines(result, timeseries_data)

    logger.info("Built engineers list with %d contributors", len(result))

    return result


def populate_activity_timelines(
    contributors: list[dict[str, Any]], timeseries_data: dict[str, Any]
) -> None:
    """Populate activity_timeline field for each contributor from timeseries data.

    Creates a weekly activity sparkline by aggregating all contribution types
    (PRs opened, PRs merged, reviews, issues, etc.) for each week.

    Args:
        contributors: List of contributor dictionaries to update in-place.
        timeseries_data: Timeseries data from timeseries.json.
    """
    # Build a mapping of user -> week -> total activity count
    user_weekly_activity: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    weekly_data = timeseries_data.get("weekly", {})
    # Handle case where weekly_data is a list instead of dict (malformed data)
    if not isinstance(weekly_data, dict):
        weekly_data = {}

    # Aggregate activity across all metric types
    metric_types = [
        "prs_opened",
        "prs_merged",
        "reviews_submitted",
        "issues_opened",
        "issues_closed",
        "comments_total",
    ]

    for metric_type in metric_types:
        metric_data = weekly_data.get(metric_type, [])

        for entry in metric_data:
            user = entry.get("user", "")
            period = entry.get("period", "")
            count = entry.get("count", 0)

            if user and period:
                user_weekly_activity[user][period] += count

    # Now populate activity_timeline for each contributor
    for contributor in contributors:
        # Try both login and user_id as keys
        user_key = contributor.get("login") or contributor.get("user_id", "")

        if user_key in user_weekly_activity:
            weekly_counts = user_weekly_activity[user_key]

            # Sort by period and convert to simple array of counts for sparkline
            # Sparklines typically just need the values in chronological order
            sorted_periods = sorted(weekly_counts.keys())
            activity_values = [weekly_counts[period] for period in sorted_periods]

            contributor["activity_timeline"] = activity_values
        else:
            # No activity data for this user
            contributor["activity_timeline"] = []

    logger.info("Populated activity timelines for %d contributors", len(contributors))
