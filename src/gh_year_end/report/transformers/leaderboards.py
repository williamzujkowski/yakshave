"""Leaderboard and awards data transformation functions."""

from typing import Any

__all__ = ["transform_awards_data", "transform_leaderboards"]


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
