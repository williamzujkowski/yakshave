"""Insights and risk analysis functions."""

import logging
from typing import Any

logger = logging.getLogger(__name__)

__all__ = ["calculate_insights", "calculate_risks"]


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
