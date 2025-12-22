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

        # Calculate hygiene score from available fields
        # If score field exists and is non-zero, use it; otherwise compute from available data
        hygiene_score = hygiene.get("score", 0)

        # Check if we have meaningful hygiene data
        meaningful_hygiene_fields = [
            "has_readme",
            "has_security_md",
            "has_codeowners",
            "has_ci_workflows",
            "has_contributing",
            "dependabot_enabled",
            "secret_scanning_enabled",
        ]

        has_meaningful_hygiene = any(hygiene.get(field) for field in meaningful_hygiene_fields)

        # Compute score from available hygiene fields if not already set
        # This handles cases where score is 0 or None but we have hygiene data
        if not hygiene_score and hygiene and has_meaningful_hygiene:
            # Compute basic score from available fields
            score_components = []

            # Branch protection (25 points)
            if hygiene.get("protected") or hygiene.get("branch_protection_enabled"):
                score_components.append(25)

            # Security features (25 points)
            if hygiene.get("has_security_md"):
                score_components.append(15)
            if hygiene.get("dependabot_enabled") or hygiene.get("secret_scanning_enabled"):
                score_components.append(10)

            # Documentation (25 points)
            if hygiene.get("has_readme"):
                score_components.append(15)
            if hygiene.get("has_contributing"):
                score_components.append(10)

            # Code ownership and CI (25 points)
            if hygiene.get("has_codeowners"):
                score_components.append(15)
            if hygiene.get("has_ci_workflows"):
                score_components.append(10)

            hygiene_score = sum(score_components)

        # Determine health status based on available metrics
        # If we don't have meaningful hygiene data, use activity-based health
        if not has_meaningful_hygiene:
            # Use activity metrics for health status
            prs_merged = health.get("prs_merged", 0)
            active_contributors = health.get("active_contributors_365d", 0)

            # Activity-based health scoring
            # Healthy: PRs merged OR active contributors
            # Warning: Some activity but low
            # Critical: No activity
            if prs_merged >= 5 or active_contributors >= 3:
                health_status = "healthy"
                hygiene_score_category = "high"
            elif prs_merged >= 1 or active_contributors >= 1:
                health_status = "warning"
                hygiene_score_category = "medium"
            else:
                health_status = "critical"
                hygiene_score_category = "low"

            # Set hygiene_score to None to indicate it's not based on hygiene data
            hygiene_score = None
        else:
            # Use hygiene-based health status
            # Lower thresholds to account for personal repos that often lack
            # enterprise hygiene features like CODEOWNERS and branch protection
            if hygiene_score >= 50:
                health_status = "healthy"
                hygiene_score_category = "high"
            elif hygiene_score >= 30:
                health_status = "warning"
                hygiene_score_category = "medium"
            else:
                health_status = "critical"
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


def calculate_hygiene_aggregate(hygiene_data: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Calculate aggregate hygiene statistics from hygiene data.

    Args:
        hygiene_data: List of repository hygiene scores.

    Returns:
        Dictionary with aggregate counts for template rendering, or None if data is unavailable.
    """
    if not hygiene_data:
        return None

    total_repos = len(hygiene_data)

    # Check if we have meaningful hygiene data (not just branch protection status)
    # Meaningful fields: has_readme, has_security_md, has_codeowners, has_ci_workflows, etc.
    meaningful_fields = [
        "has_readme",
        "has_security_md",
        "has_codeowners",
        "has_ci_workflows",
        "has_contributing",
        "dependabot_enabled",
        "secret_scanning_enabled",
    ]

    # Check if ANY repo has ANY meaningful field set to True
    has_meaningful_data = any(r.get(field) for r in hygiene_data for field in meaningful_fields)

    # If we only have minimal data (just branch protection or all False), return None
    if not has_meaningful_data:
        # Check if we at least have branch protection data
        has_branch_protection_data = any(
            r.get("branch_protection_enabled") is not None or r.get("protected") is not None
            for r in hygiene_data
        )

        if not has_branch_protection_data:
            return None

        # We have minimal data - return None to hide the hygiene breakdown
        # Branch protection will still be visible in the repo list
        return None

    # We have full hygiene data - calculate aggregates
    security_md = sum(1 for r in hygiene_data if r.get("has_security_md"))
    security_features = sum(
        1 for r in hygiene_data if r.get("dependabot_enabled") or r.get("secret_scanning_enabled")
    )
    codeowners = sum(1 for r in hygiene_data if r.get("has_codeowners"))

    # For branch protection, check both has_branch_protection and protected fields
    branch_protection = sum(
        1 for r in hygiene_data if r.get("branch_protection_enabled") or r.get("protected")
    )

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

    # Filter out None values when calculating hygiene score average
    hygiene_scores = [
        r.get("hygiene_score", 0) for r in merged_repos if r.get("hygiene_score") is not None
    ]
    total_hygiene = sum(hygiene_scores)
    total_contributors = sum(r.get("active_contributors_365d", 0) for r in merged_repos)
    total_repos = len(merged_repos)

    return {
        "healthy_count": healthy,
        "warning_count": warning,
        "critical_count": critical,
        "avg_hygiene_score": total_hygiene / len(hygiene_scores) if hygiene_scores else 0,
        "avg_contributors": total_contributors / total_repos if total_repos > 0 else 0,
    }
