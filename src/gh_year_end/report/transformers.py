"""Data transformation functions for report generation."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

__all__ = [
    "calculate_fun_facts",
    "calculate_highlights",
    "calculate_insights",
    "calculate_risks",
    "generate_chart_data",
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


def calculate_insights(
    summary_data: dict[str, Any],
    leaderboards_data: dict[str, Any],
    repo_health_list: list[dict[str, Any]],
    hygiene_scores_list: list[dict[str, Any]],
) -> dict[str, Any]:
    """Calculate insights from metrics data.

    Args:
        summary_data: Summary statistics from summary.json.
        leaderboards_data: Leaderboard data from leaderboards.json.
        repo_health_list: Repository health metrics.
        hygiene_scores_list: Repository hygiene scores.

    Returns:
        Dictionary with calculated insight values.
    """
    insights: dict[str, Any] = {}

    # Calculate avg_reviewers_per_pr
    try:
        total_prs = summary_data.get("total_prs", 0)
        total_reviews = summary_data.get("total_reviews", 0)

        if total_prs > 0:
            insights["avg_reviewers_per_pr"] = round(total_reviews / total_prs, 1)
        else:
            insights["avg_reviewers_per_pr"] = 0
    except Exception as e:
        logger.warning("Failed to calculate avg_reviewers_per_pr: %s", e)
        insights["avg_reviewers_per_pr"] = 0

    # Calculate review_participation_rate
    try:
        total_contributors = summary_data.get("total_contributors", 0)

        # Count unique reviewers from leaderboards
        nested = leaderboards_data.get("leaderboards", leaderboards_data)
        reviews_data = nested.get("reviews_submitted", [])

        # Data may be nested under "org" key or direct list
        if isinstance(reviews_data, dict):
            reviews_list = reviews_data.get("org", [])
        elif isinstance(reviews_data, list):
            reviews_list = reviews_data
        else:
            reviews_list = []

        reviewers_count = len(reviews_list)

        if total_contributors > 0:
            participation_rate = (reviewers_count / total_contributors) * 100
            insights["review_participation_rate"] = round(participation_rate, 0)
        else:
            insights["review_participation_rate"] = 0
    except Exception as e:
        logger.warning("Failed to calculate review_participation_rate: %s", e)
        insights["review_participation_rate"] = 0

    # Calculate cross_team_reviews
    # This would require team/organization data which we don't have in current metrics
    # For now, set to None to indicate unavailable
    insights["cross_team_reviews"] = None

    # Calculate prs_per_week
    try:
        prs_merged = summary_data.get("prs_merged", 0)
        # Assume 52 weeks per year
        insights["prs_per_week"] = round(prs_merged / 52, 1)
    except Exception as e:
        logger.warning("Failed to calculate prs_per_week: %s", e)
        insights["prs_per_week"] = 0

    # Calculate median_pr_size
    # This would require PR detail data with additions/deletions which we don't have
    # For now, set to None to indicate unavailable
    insights["median_pr_size"] = None

    # Calculate merge_rate
    try:
        total_prs = summary_data.get("total_prs", 0)
        prs_merged = summary_data.get("prs_merged", 0)

        if total_prs > 0:
            merge_rate = (prs_merged / total_prs) * 100
            insights["merge_rate"] = round(merge_rate, 0)
        else:
            insights["merge_rate"] = 0
    except Exception as e:
        logger.warning("Failed to calculate merge_rate: %s", e)
        insights["merge_rate"] = 0

    # Count repos with CI from hygiene scores
    # Hygiene data doesn't currently track CI explicitly, so we'll return None
    insights["repos_with_ci"] = None

    # Count repos with CODEOWNERS from hygiene scores
    # Hygiene data doesn't currently track CODEOWNERS explicitly, so we'll return None
    insights["repos_with_codeowners"] = None

    # Count repos with security policy from hygiene scores
    # Hygiene data doesn't currently track SECURITY.md explicitly, so we'll return None
    insights["repos_with_security_policy"] = None

    # Calculate new_contributors
    # This would require tracking first-time contributors which we don't have in current metrics
    # For now, set to 0
    insights["new_contributors"] = 0

    # Calculate contributor_retention
    # This would require historical contributor data which we don't have
    # For now, set to None to indicate unavailable
    insights["contributor_retention"] = None

    # Calculate bus_factor
    try:
        # Bus factor: number of contributors needed to represent 50% of contributions
        # Use prs_merged as the contribution metric
        nested = leaderboards_data.get("leaderboards", leaderboards_data)
        prs_merged_data = nested.get("prs_merged", [])

        # Data may be nested under "org" key or direct list
        if isinstance(prs_merged_data, dict):
            prs_list = prs_merged_data.get("org", [])
        elif isinstance(prs_merged_data, list):
            prs_list = prs_merged_data
        else:
            prs_list = []

        if prs_list:
            # Sort by count descending (should already be sorted, but ensure)
            sorted_prs = sorted(
                prs_list, key=lambda x: x.get("count", 0) or x.get("value", 0), reverse=True
            )

            # Calculate total PRs
            total_prs = sum(x.get("count", 0) or x.get("value", 0) for x in sorted_prs)

            if total_prs > 0:
                # Count contributors needed to reach 50% of total
                cumulative = 0
                bus_factor = 0
                threshold = total_prs * 0.5

                for contributor in sorted_prs:
                    cumulative += contributor.get("count", 0) or contributor.get("value", 0)
                    bus_factor += 1
                    if cumulative >= threshold:
                        break

                insights["bus_factor"] = bus_factor
            else:
                insights["bus_factor"] = 0
        else:
            insights["bus_factor"] = 0
    except Exception as e:
        logger.warning("Failed to calculate bus_factor: %s", e)
        insights["bus_factor"] = 0

    return insights


def calculate_risks(
    repo_health_list: list[dict[str, Any]],
    hygiene_scores_list: list[dict[str, Any]],
    summary_data: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Identify risks and concerns from repository health metrics.

    Args:
        repo_health_list: Repository health metrics.
        hygiene_scores_list: Repository hygiene scores.
        summary_data: Optional summary statistics.

    Returns:
        List of risk dictionaries with structure:
        {
            "title": str,
            "description": str,
            "severity": "high" | "medium" | "low",
            "repos": [list of affected repo names]
        }
    """
    risks: list[dict[str, Any]] = []

    # Build repo name mappings for quick lookups
    # repo_health_list entries have: repo_id, repo_full_name
    # hygiene_scores_list entries have: repo_id, repo_full_name, score, has_* fields
    hygiene_by_repo: dict[str, dict[str, Any]] = {}
    for hygiene in hygiene_scores_list:
        repo_id = hygiene.get("repo_id", "")
        if repo_id:
            hygiene_by_repo[repo_id] = hygiene

    health_by_repo: dict[str, dict[str, Any]] = {}
    for health in repo_health_list:
        repo_id = health.get("repo_id", "")
        if repo_id:
            health_by_repo[repo_id] = health

    # Risk 1: Missing security policy
    missing_security = []
    for repo_id, hygiene in hygiene_by_repo.items():
        if not hygiene.get("has_security_md", False):
            missing_security.append(hygiene.get("repo_full_name", repo_id))

    if missing_security:
        risks.append(
            {
                "title": "Missing Security Policy",
                "description": f"{len(missing_security)} repositories are missing SECURITY.md",
                "severity": "high",
                "repos": sorted(missing_security),
            }
        )

    # Risk 2: Missing CI/CD workflows
    missing_ci = []
    for repo_id, hygiene in hygiene_by_repo.items():
        if not hygiene.get("has_ci_workflows", False):
            missing_ci.append(hygiene.get("repo_full_name", repo_id))

    if missing_ci:
        risks.append(
            {
                "title": "Missing CI/CD",
                "description": f"{len(missing_ci)} repositories lack CI/CD workflows",
                "severity": "medium",
                "repos": sorted(missing_ci),
            }
        )

    # Risk 3: Low documentation scores (hygiene score < 60)
    low_documentation = []
    for repo_id, hygiene in hygiene_by_repo.items():
        score = hygiene.get("score", 0)
        if score < 60:
            low_documentation.append(hygiene.get("repo_full_name", repo_id))

    if low_documentation:
        risks.append(
            {
                "title": "Low Documentation Score",
                "description": f"{len(low_documentation)} repositories have hygiene scores below 60",
                "severity": "medium",
                "repos": sorted(low_documentation),
            }
        )

    # Risk 4: Missing CODEOWNERS
    missing_codeowners = []
    for repo_id, hygiene in hygiene_by_repo.items():
        if not hygiene.get("has_codeowners", False):
            missing_codeowners.append(hygiene.get("repo_full_name", repo_id))

    if missing_codeowners:
        risks.append(
            {
                "title": "Missing CODEOWNERS",
                "description": f"{len(missing_codeowners)} repositories lack CODEOWNERS file",
                "severity": "low",
                "repos": sorted(missing_codeowners),
            }
        )

    # Risk 5: Long review times (median > 48 hours)
    long_review_times = []
    for repo_id, health in health_by_repo.items():
        median_review_time = health.get("median_time_to_first_review")
        if median_review_time is not None and median_review_time > 172800:  # 48 hours in seconds
            long_review_times.append(health.get("repo_full_name", repo_id))

    if long_review_times:
        risks.append(
            {
                "title": "Long Review Times",
                "description": f"{len(long_review_times)} repositories have median review times exceeding 48 hours",
                "severity": "medium",
                "repos": sorted(long_review_times),
            }
        )

    # Risk 6: Low review coverage (< 50%)
    low_review_coverage = []
    for repo_id, health in health_by_repo.items():
        review_coverage = health.get("review_coverage")
        if review_coverage is not None and review_coverage < 50:
            low_review_coverage.append(health.get("repo_full_name", repo_id))

    if low_review_coverage:
        risks.append(
            {
                "title": "Low Review Coverage",
                "description": f"{len(low_review_coverage)} repositories have less than 50% review coverage",
                "severity": "high",
                "repos": sorted(low_review_coverage),
            }
        )

    # Risk 7: High number of stale PRs (> 5)
    high_stale_prs = []
    for repo_id, health in health_by_repo.items():
        stale_pr_count = health.get("stale_pr_count", 0)
        if stale_pr_count > 5:
            high_stale_prs.append(health.get("repo_full_name", repo_id))

    if high_stale_prs:
        risks.append(
            {
                "title": "High Stale PR Count",
                "description": f"{len(high_stale_prs)} repositories have more than 5 stale PRs",
                "severity": "medium",
                "repos": sorted(high_stale_prs),
            }
        )

    # Risk 8: Low contributor activity (< 2 active contributors in 90 days)
    low_contributor_activity = []
    for repo_id, health in health_by_repo.items():
        active_contributors_90d = health.get("active_contributors_90d", 0)
        if active_contributors_90d < 2:
            low_contributor_activity.append(health.get("repo_full_name", repo_id))

    if low_contributor_activity:
        risks.append(
            {
                "title": "Low Contributor Activity",
                "description": f"{len(low_contributor_activity)} repositories have fewer than 2 active contributors in the last 90 days",
                "severity": "medium",
                "repos": sorted(low_contributor_activity),
            }
        )

    # Risk 9: Disabled security features (dependabot or secret scanning)
    missing_security_features = []
    for repo_id, hygiene in hygiene_by_repo.items():
        dependabot = hygiene.get("dependabot_enabled", False)
        secret_scanning = hygiene.get("secret_scanning_enabled", False)
        if not dependabot or not secret_scanning:
            missing_security_features.append(hygiene.get("repo_full_name", repo_id))

    if missing_security_features:
        risks.append(
            {
                "title": "Disabled Security Features",
                "description": f"{len(missing_security_features)} repositories have Dependabot or secret scanning disabled",
                "severity": "high",
                "repos": sorted(missing_security_features),
            }
        )

    # Risk 10: No branch protection
    no_branch_protection = []
    for repo_id, hygiene in hygiene_by_repo.items():
        if not hygiene.get("branch_protection_enabled", False):
            no_branch_protection.append(hygiene.get("repo_full_name", repo_id))

    if no_branch_protection:
        risks.append(
            {
                "title": "No Branch Protection",
                "description": f"{len(no_branch_protection)} repositories lack branch protection rules",
                "severity": "high",
                "repos": sorted(no_branch_protection),
            }
        )

    logger.info("Identified %d risk patterns from repository metrics", len(risks))
    return risks
