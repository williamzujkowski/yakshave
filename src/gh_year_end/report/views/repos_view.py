"""Repository health view helpers for repos.html template.

Provides data transformation functions to prepare repository health and hygiene
data for template rendering.
"""

from typing import Any


def merge_repo_data(
    repo_health_data: list[dict[str, Any]],
    hygiene_data: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Merge health and hygiene data for each repository.

    Args:
        repo_health_data: List of repository health metrics.
        hygiene_data: List of repository hygiene scores.

    Returns:
        List of merged repository data for template rendering.
    """
    # Create lookup dict for hygiene data by repo_id
    hygiene_by_repo = {item.get("repo_id"): item for item in hygiene_data}

    merged_repos = []
    for health in repo_health_data:
        repo_id = health.get("repo_id")
        hygiene = hygiene_by_repo.get(repo_id, {})

        # Determine health status based on metrics
        hygiene_score = hygiene.get("score", 0)
        if hygiene_score >= 80:
            health_status = "healthy"
        elif hygiene_score >= 60:
            health_status = "warning"
        else:
            health_status = "critical"

        # Determine hygiene score category
        if hygiene_score >= 80:
            hygiene_score_category = "high"
        elif hygiene_score >= 60:
            hygiene_score_category = "medium"
        else:
            hygiene_score_category = "low"

        merged_repos.append(
            {
                "repo_id": repo_id,
                "full_name": health.get("repo_full_name", ""),
                "name": health.get("repo_full_name", "").split("/")[-1]
                if health.get("repo_full_name")
                else "",
                "is_private": False,  # Not available in current data
                "language": None,  # Not available in current data
                "hygiene_score": hygiene_score,
                "hygiene_score_category": hygiene_score_category,
                "prs_merged": health.get("prs_merged", 0),
                "active_contributors_365d": health.get("active_contributors_365d", 0),
                "review_coverage": health.get("review_coverage", 0),
                "median_time_to_merge": health.get("median_time_to_merge", "N/A"),
                "health_status": health_status,
            }
        )

    return merged_repos


def calculate_hygiene_aggregate(hygiene_data: list[dict[str, Any]]) -> dict[str, Any]:
    """Calculate aggregate hygiene statistics from hygiene data.

    Args:
        hygiene_data: List of repository hygiene scores.

    Returns:
        Dictionary with aggregate counts for template rendering.
    """
    if not hygiene_data:
        return {
            "security_md_count": 0,
            "security_features_count": 0,
            "codeowners_count": 0,
            "branch_protection_count": 0,
            "ci_workflows_count": 0,
            "avg_workflows": 0,
            "readme_count": 0,
            "contributing_count": 0,
        }

    total_repos = len(hygiene_data)
    security_md = sum(1 for r in hygiene_data if r.get("has_security_md"))
    security_features = sum(
        1
        for r in hygiene_data
        if r.get("dependabot_enabled") or r.get("secret_scanning_enabled")
    )
    codeowners = sum(1 for r in hygiene_data if r.get("has_codeowners"))
    branch_protection = sum(1 for r in hygiene_data if r.get("branch_protection_enabled"))
    ci_workflows = sum(1 for r in hygiene_data if r.get("has_ci_workflows"))
    readme = sum(1 for r in hygiene_data if r.get("has_readme"))
    contributing = sum(1 for r in hygiene_data if r.get("has_contributing"))

    return {
        "security_md_count": security_md,
        "security_features_count": security_features,
        "codeowners_count": codeowners,
        "branch_protection_count": branch_protection,
        "ci_workflows_count": ci_workflows,
        "avg_workflows": ci_workflows / total_repos if total_repos > 0 else 0,
        "readme_count": readme,
        "contributing_count": contributing,
    }


def calculate_repo_summary(merged_repos: list[dict[str, Any]]) -> dict[str, Any]:
    """Calculate summary statistics for repositories.

    Args:
        merged_repos: List of merged repository data.

    Returns:
        Dictionary with summary statistics.
    """
    if not merged_repos:
        return {
            "healthy_count": 0,
            "warning_count": 0,
            "critical_count": 0,
            "avg_hygiene_score": 0,
            "avg_contributors": 0,
        }

    healthy = sum(1 for r in merged_repos if r.get("health_status") == "healthy")
    warning = sum(1 for r in merged_repos if r.get("health_status") == "warning")
    critical = sum(1 for r in merged_repos if r.get("health_status") == "critical")

    total_hygiene = sum(r.get("hygiene_score", 0) for r in merged_repos)
    total_contributors = sum(r.get("active_contributors_365d", 0) for r in merged_repos)
    total_repos = len(merged_repos)

    return {
        "healthy_count": healthy,
        "warning_count": warning,
        "critical_count": critical,
        "avg_hygiene_score": total_hygiene / total_repos if total_repos > 0 else 0,
        "avg_contributors": total_contributors / total_repos if total_repos > 0 else 0,
    }


def prepare_repo_activity_data(
    repo_health_data: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Prepare repository activity data for charting.

    Args:
        repo_health_data: List of repository health metrics.

    Returns:
        List of activity data points for D3.js.
    """
    # Placeholder - would need time series data for real implementation
    return []
