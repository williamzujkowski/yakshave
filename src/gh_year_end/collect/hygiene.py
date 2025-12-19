"""Repository hygiene collectors for file presence and CI/CD checks.

Collects repository tree data and derives file presence information for
configured hygiene paths (SECURITY.md, README.md, LICENSE, etc.) and CI workflows.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from gh_year_end.storage.writer import AsyncJSONLWriter

if TYPE_CHECKING:
    from gh_year_end.config import Config
    from gh_year_end.github.ratelimit import AdaptiveRateLimiter
    from gh_year_end.github.rest import RestClient
    from gh_year_end.storage.checkpoint import CheckpointManager
    from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


class HygieneCollectionError(Exception):
    """Raised when hygiene collection fails."""


async def collect_repo_hygiene(
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    paths: PathManager,
    rate_limiter: AdaptiveRateLimiter | None = None,
    config: Config | None = None,
) -> dict[str, Any]:
    """Collect repository hygiene data (file presence and CI workflows).

    Fetches repository tree for each repo's default branch and checks for
    presence of configured hygiene files and CI workflows.

    Args:
        repos: List of repository metadata dicts from discovery.
        rest_client: REST API client for GitHub requests.
        paths: Path manager for storage locations.
        rate_limiter: Optional rate limiter for throttling.
        config: Optional configuration for hygiene paths.

    Returns:
        Statistics dict with:
            - repos_processed: Number of repos successfully processed.
            - repos_skipped: Number of repos skipped (empty, no default branch).
            - repos_errored: Number of repos with errors.
            - files_checked: Total number of file presence checks performed.
            - errors: List of error details.

    Raises:
        HygieneCollectionError: If critical collection failure occurs.
    """
    logger.info("Starting hygiene collection for %d repositories", len(repos))

    stats: dict[str, Any] = {
        "repos_processed": 0,
        "repos_skipped": 0,
        "repos_errored": 0,
        "files_checked": 0,
        "errors": [],
    }

    # Extract hygiene paths from config
    hygiene_paths = []
    workflow_prefixes = []
    if config and config.collection.hygiene:
        hygiene_paths = config.collection.hygiene.paths or []
        workflow_prefixes = config.collection.hygiene.workflow_prefixes or []

    logger.info(
        "Checking %d hygiene paths and %d workflow prefixes",
        len(hygiene_paths),
        len(workflow_prefixes),
    )

    for repo in repos:
        try:
            repo_stats = await _collect_repo_hygiene(
                repo=repo,
                rest_client=rest_client,
                paths=paths,
                hygiene_paths=hygiene_paths,
                workflow_prefixes=workflow_prefixes,
            )

            if repo_stats["skipped"]:
                stats["repos_skipped"] += 1
            else:
                stats["repos_processed"] += 1
                stats["files_checked"] += repo_stats["files_checked"]

        except Exception as e:
            stats["repos_errored"] += 1
            error_detail = {
                "repo": repo.get("full_name", "unknown"),
                "error": str(e),
            }
            stats["errors"].append(error_detail)
            logger.error(
                "Failed to collect hygiene for %s: %s",
                repo.get("full_name", "unknown"),
                e,
            )

    logger.info(
        "Hygiene collection complete: %d repos processed, %d files checked, "
        "%d repos skipped, %d repos errored",
        stats["repos_processed"],
        stats["files_checked"],
        stats["repos_skipped"],
        stats["repos_errored"],
    )

    return stats


async def _collect_repo_hygiene(
    repo: dict[str, Any],
    rest_client: RestClient,
    paths: PathManager,
    hygiene_paths: list[str],
    workflow_prefixes: list[str],
) -> dict[str, Any]:
    """Collect hygiene data for a single repository.

    Args:
        repo: Repository metadata dict.
        rest_client: REST API client for GitHub requests.
        paths: Path manager for storage locations.
        hygiene_paths: List of file paths to check for presence.
        workflow_prefixes: List of workflow directory prefixes to check.

    Returns:
        Statistics dict with:
            - files_checked: Number of file presence checks performed.
            - skipped: Whether the repo was skipped.
    """
    full_name = repo.get("full_name")
    if not full_name:
        logger.warning("Repo missing full_name field, skipping")
        return {"files_checked": 0, "skipped": True}

    owner, repo_name = full_name.split("/", 1)
    default_branch = repo.get("default_branch")

    # Skip if no default branch (empty repos)
    if not default_branch:
        logger.info("Repository %s has no default branch, skipping", full_name)
        return {"files_checked": 0, "skipped": True}

    output_path = paths.repo_tree_raw_path(full_name)

    logger.info("Collecting hygiene for %s (branch: %s)", full_name, default_branch)

    files_checked = 0

    async with AsyncJSONLWriter(output_path) as writer:
        try:
            # Fetch repository tree for default branch
            tree_data = await rest_client.get_repository_tree(
                owner=owner,
                repo=repo_name,
                tree_sha=default_branch,
                recursive=True,
            )

            if tree_data is None:
                logger.info(
                    "No tree data for %s (likely empty or inaccessible)",
                    full_name,
                )
                return {"files_checked": 0, "skipped": True}

            # Write raw tree response
            await writer.write(
                source="github_rest",
                endpoint=f"/repos/{full_name}/git/trees/{default_branch}",
                data=tree_data,
            )

            # Build path lookup from tree
            tree_entries = tree_data.get("tree", [])
            path_lookup = {entry["path"]: entry for entry in tree_entries}

            logger.debug(
                "Repository %s tree contains %d entries",
                full_name,
                len(tree_entries),
            )

            # Check presence of each hygiene path
            for hygiene_path in hygiene_paths:
                exists = hygiene_path in path_lookup
                entry = path_lookup.get(hygiene_path)

                presence_data = {
                    "repo": full_name,
                    "path": hygiene_path,
                    "exists": exists,
                    "sha": entry.get("sha") if entry else None,
                    "size": entry.get("size") if entry else None,
                    "type": entry.get("type") if entry else None,
                }

                await writer.write(
                    source="derived",
                    endpoint="file_presence",
                    data=presence_data,
                )
                files_checked += 1

            # Check for CI workflows (any files in workflow directories)
            workflow_files = []
            for prefix in workflow_prefixes:
                matching_files = [
                    entry
                    for entry in tree_entries
                    if entry["path"].startswith(prefix) and entry["type"] == "blob"
                ]
                workflow_files.extend(matching_files)

            workflow_data = {
                "repo": full_name,
                "workflow_prefixes": workflow_prefixes,
                "workflow_files_found": len(workflow_files),
                "workflow_files": [
                    {
                        "path": wf["path"],
                        "sha": wf["sha"],
                        "size": wf.get("size"),
                    }
                    for wf in workflow_files
                ],
            }

            await writer.write(
                source="derived",
                endpoint="workflow_presence",
                data=workflow_data,
            )
            files_checked += 1

        except Exception:
            # Check if this was a 404 (handled gracefully)
            if files_checked == 0:
                logger.info(
                    "No hygiene data for %s (likely empty or inaccessible)",
                    full_name,
                )
                return {"files_checked": 0, "skipped": True}
            # Otherwise, re-raise the exception
            raise

    logger.info("Collected hygiene for %s: %d checks", full_name, files_checked)
    return {"files_checked": files_checked, "skipped": False}


async def collect_security_features(
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    paths: PathManager,
    config: Config,
    checkpoint: CheckpointManager | None = None,
) -> dict[str, Any]:
    """Collect security feature status for all repositories.

    This collector uses best-effort approach - if permissions are denied,
    fields are set to None rather than failing the entire collection.

    Args:
        repos: List of basic repo metadata from discovery.
        rest_client: REST client for API calls.
        paths: Path manager for storage.
        config: Application configuration.
        checkpoint: Optional CheckpointManager for resume support.

    Returns:
        Stats dictionary with counts of repos processed, errors, etc.

    Raises:
        HygieneCollectionError: If collection fails critically.
    """
    logger.info("Starting security features collection for %d repos", len(repos))

    # Check if security features collection is enabled
    if not config.collection.hygiene.security_features.best_effort:
        logger.info("Security features collection disabled in config")
        return {
            "repos_total": len(repos),
            "repos_processed": 0,
            "repos_skipped": len(repos),
            "repos_resumed": 0,
            "repos_with_all_features": 0,
            "repos_with_partial_features": 0,
            "repos_with_no_access": 0,
        }

    stats: dict[str, Any] = {
        "repos_total": len(repos),
        "repos_processed": 0,
        "repos_skipped": 0,
        "repos_resumed": 0,
        "repos_with_all_features": 0,
        "repos_with_partial_features": 0,
        "repos_with_no_access": 0,
        "errors": [],
    }

    for idx, repo in enumerate(repos, 1):
        repo_full_name = repo.get("full_name", "unknown")

        # Check if already complete via checkpoint
        if checkpoint and checkpoint.is_repo_endpoint_complete(repo_full_name, "security_features"):
            logger.debug("Skipping %s - security_features already complete", repo_full_name)
            stats["repos_resumed"] += 1
            continue

        logger.info(
            "Collecting security features for repo %d/%d: %s",
            idx,
            len(repos),
            repo_full_name,
        )

        # Mark as in progress
        if checkpoint:
            checkpoint.mark_repo_endpoint_in_progress(repo_full_name, "security_features")

        try:
            # Parse owner and repo name
            owner, name = _parse_repo_name(repo_full_name)

            # Fetch security features
            features = await _get_security_features(
                owner=owner,
                name=name,
                rest_client=rest_client,
            )

            # Open writer for this repo
            security_features_path = paths.security_features_raw_path(repo_full_name)
            async with AsyncJSONLWriter(security_features_path) as writer:
                await writer.write(
                    source="github_rest",
                    endpoint="security_features",
                    data=features,
                )

            # Update stats
            stats["repos_processed"] += 1

            # Count feature availability
            feature_values = [
                features.get("dependabot_alerts_enabled"),
                features.get("dependabot_security_updates_enabled"),
                features.get("secret_scanning_enabled"),
                features.get("secret_scanning_push_protection_enabled"),
            ]

            none_count = sum(1 for v in feature_values if v is None)
            if none_count == 4:
                stats["repos_with_no_access"] += 1
            elif none_count == 0:
                stats["repos_with_all_features"] += 1
            else:
                stats["repos_with_partial_features"] += 1

            # Mark as complete
            if checkpoint:
                checkpoint.mark_repo_endpoint_complete(repo_full_name, "security_features")

            logger.debug(
                "Successfully collected security features for %s (%d/%d)",
                repo_full_name,
                stats["repos_processed"],
                len(repos),
            )

        except Exception as e:
            logger.error(
                "Failed to collect security features for %s: %s",
                repo_full_name,
                e,
            )
            stats["errors"].append(
                {
                    "repo": repo_full_name,
                    "error": str(e),
                }
            )

            # Mark as failed
            if checkpoint:
                checkpoint.mark_repo_endpoint_failed(
                    repo_full_name, "security_features", str(e), retryable=True
                )
            continue

    logger.info(
        "Security features collection complete: processed=%d, all_features=%d, "
        "partial_features=%d, no_access=%d, resumed=%d",
        stats["repos_processed"],
        stats["repos_with_all_features"],
        stats["repos_with_partial_features"],
        stats["repos_with_no_access"],
        stats["repos_resumed"],
    )

    return stats


async def _get_security_features(
    owner: str,
    name: str,
    rest_client: RestClient,
) -> dict[str, Any]:
    """Fetch security features for a single repository.

    Args:
        owner: Repository owner.
        name: Repository name.
        rest_client: REST client for API calls.

    Returns:
        Dictionary with security feature status and error message if applicable.
    """
    features: dict[str, Any] = {
        "repo": f"{owner}/{name}",
        "dependabot_alerts_enabled": None,
        "dependabot_security_updates_enabled": None,
        "secret_scanning_enabled": None,
        "secret_scanning_push_protection_enabled": None,
        "error": None,
    }

    # Check vulnerability alerts (Dependabot alerts)
    try:
        vuln_alerts_enabled = await rest_client.check_vulnerability_alerts(owner, name)
        features["dependabot_alerts_enabled"] = vuln_alerts_enabled
    except Exception as e:
        logger.debug(
            "Error checking vulnerability alerts for %s/%s: %s",
            owner,
            name,
            e,
        )

    # Get security_and_analysis field from repo metadata
    try:
        repo_data = await rest_client.get_repo_security_analysis(owner, name)
        if repo_data:
            security_analysis = repo_data.get("security_and_analysis", {})

            # Extract Dependabot security updates
            dependabot_security = security_analysis.get("dependabot_security_updates", {})
            if dependabot_security:
                features["dependabot_security_updates_enabled"] = (
                    dependabot_security.get("status") == "enabled"
                )

            # Extract secret scanning
            secret_scanning = security_analysis.get("secret_scanning", {})
            if secret_scanning:
                features["secret_scanning_enabled"] = secret_scanning.get("status") == "enabled"

            # Extract secret scanning push protection
            secret_scanning_push_protection = security_analysis.get(
                "secret_scanning_push_protection", {}
            )
            if secret_scanning_push_protection:
                features["secret_scanning_push_protection_enabled"] = (
                    secret_scanning_push_protection.get("status") == "enabled"
                )
    except Exception as e:
        logger.debug(
            "Error checking security_and_analysis for %s/%s: %s",
            owner,
            name,
            e,
        )

    # Set error message if all features are None (likely permission issue)
    feature_values = [
        features.get("dependabot_alerts_enabled"),
        features.get("dependabot_security_updates_enabled"),
        features.get("secret_scanning_enabled"),
        features.get("secret_scanning_push_protection_enabled"),
    ]

    if all(v is None for v in feature_values):
        features["error"] = "403: Security features not accessible"

    return features


async def collect_branch_protection(
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    path_manager: PathManager,
    config: Config,
    checkpoint: CheckpointManager | None = None,
) -> dict[str, Any]:
    """Collect branch protection settings for repositories.

    Implements three collection modes:
    - skip: Don't collect branch protection
    - best_effort: Try to collect for all repos, handle 404/403 gracefully
    - sample: Only collect for top N repos by a metric (e.g., prs_merged)

    Args:
        repos: List of repository metadata from discovery.
        rest_client: REST client for API calls.
        path_manager: Path manager for file paths.
        config: Application configuration.
        checkpoint: Optional CheckpointManager for resume support.

    Returns:
        Stats dictionary with counts of repos processed, errors, etc.

    Raises:
        HygieneCollectionError: If collection fails critically.
    """
    bp_config = config.collection.hygiene.branch_protection
    mode = bp_config.mode

    logger.info("Starting branch protection collection (mode=%s)", mode)

    stats: dict[str, Any] = {
        "mode": mode,
        "repos_total": len(repos),
        "repos_processed": 0,
        "repos_skipped": 0,
        "repos_resumed": 0,
        "protection_enabled": 0,
        "protection_disabled": 0,
        "permission_denied": 0,
        "errors": [],
    }

    if mode == "skip":
        logger.info("Branch protection collection disabled (mode=skip)")
        stats["repos_skipped"] = len(repos)
        return stats

    # Determine which repos to collect for
    repos_to_collect = _select_repos_for_collection(repos, config)
    stats["repos_selected"] = len(repos_to_collect)
    stats["repos_skipped"] = len(repos) - len(repos_to_collect)

    if not repos_to_collect:
        logger.warning("No repositories selected for branch protection collection")
        return stats

    logger.info(
        "Collecting branch protection for %d/%d repos",
        len(repos_to_collect),
        len(repos),
    )

    for idx, repo in enumerate(repos_to_collect, 1):
        repo_name = repo.get("full_name", repo.get("nameWithOwner", "unknown"))

        # Check if already complete via checkpoint
        if checkpoint and checkpoint.is_repo_endpoint_complete(repo_name, "branch_protection"):
            logger.debug("Skipping %s - branch_protection already complete", repo_name)
            stats["repos_resumed"] += 1
            continue

        logger.info(
            "Collecting branch protection %d/%d: %s",
            idx,
            len(repos_to_collect),
            repo_name,
        )

        # Mark as in progress
        if checkpoint:
            checkpoint.mark_repo_endpoint_in_progress(repo_name, "branch_protection")

        try:
            # Parse owner and repo name
            owner, name = _parse_repo_name(repo_name)

            # Get default branch
            default_branch = _get_default_branch(repo)

            # Fetch branch protection
            protection_data = await _get_branch_protection(
                owner=owner,
                name=name,
                branch=default_branch,
                rest_client=rest_client,
            )

            # Write to JSONL
            output_path = path_manager.branch_protection_raw_path(repo_name)
            async with AsyncJSONLWriter(output_path) as writer:
                await writer.write(
                    source="github_rest",
                    endpoint=f"/repos/{owner}/{name}/branches/{default_branch}/protection",
                    data=protection_data,
                )

            # Update stats
            stats["repos_processed"] += 1

            if protection_data.get("error"):
                error_msg = protection_data["error"]
                if "403" in error_msg:
                    stats["permission_denied"] += 1
                elif "404" in error_msg:
                    stats["protection_disabled"] += 1
            elif protection_data.get("protection_enabled"):
                stats["protection_enabled"] += 1
            else:
                stats["protection_disabled"] += 1

            # Mark as complete
            if checkpoint:
                checkpoint.mark_repo_endpoint_complete(repo_name, "branch_protection")

            logger.debug(
                "Successfully collected branch protection for %s (%d/%d)",
                repo_name,
                stats["repos_processed"],
                len(repos_to_collect),
            )

        except Exception as e:
            logger.error(
                "Failed to collect branch protection for %s: %s",
                repo_name,
                e,
            )
            stats["errors"].append(
                {
                    "repo": repo_name,
                    "error": str(e),
                }
            )

            # Mark as failed
            if checkpoint:
                checkpoint.mark_repo_endpoint_failed(
                    repo_name, "branch_protection", str(e), retryable=True
                )
            continue

    logger.info(
        "Branch protection collection complete: processed=%d, enabled=%d, disabled=%d, "
        "permission_denied=%d, resumed=%d",
        stats["repos_processed"],
        stats["protection_enabled"],
        stats["protection_disabled"],
        stats["permission_denied"],
        stats["repos_resumed"],
    )

    return stats


async def _get_branch_protection(
    owner: str,
    name: str,
    branch: str,
    rest_client: RestClient,
) -> dict[str, Any]:
    """Fetch branch protection for a single branch.

    Args:
        owner: Repository owner.
        name: Repository name.
        branch: Branch name to check.
        rest_client: REST client for API calls.

    Returns:
        Protection data dictionary with standardized structure.
    """
    repo_full_name = f"{owner}/{name}"
    protection_data, status_code = await rest_client.get_branch_protection(owner, name, branch)

    # Build standardized response
    result: dict[str, Any] = {
        "repo": repo_full_name,
        "branch": branch,
        "protection_enabled": None,
        "error": None,
    }

    if status_code == 403:
        # No permission to access
        result["error"] = "403: Resource not accessible by integration"
        logger.debug("Permission denied for %s:%s", repo_full_name, branch)
        return result

    if status_code == 404:
        # No branch protection set
        result["protection_enabled"] = False
        logger.debug("No branch protection for %s:%s", repo_full_name, branch)
        return result

    if protection_data and status_code == 200:
        # Branch protection is enabled
        result["protection_enabled"] = True

        # Extract key protection settings
        result["required_status_checks"] = protection_data.get("required_status_checks")
        result["enforce_admins"] = protection_data.get("enforce_admins", {}).get("enabled")
        result["required_pull_request_reviews"] = protection_data.get(
            "required_pull_request_reviews"
        )
        result["restrictions"] = protection_data.get("restrictions")
        result["allow_force_pushes"] = protection_data.get("allow_force_pushes", {}).get("enabled")
        result["allow_deletions"] = protection_data.get("allow_deletions", {}).get("enabled")
        result["required_linear_history"] = protection_data.get("required_linear_history", {}).get(
            "enabled"
        )
        result["required_conversation_resolution"] = protection_data.get(
            "required_conversation_resolution", {}
        ).get("enabled")

        # Extract review requirements if present
        reviews = result.get("required_pull_request_reviews")
        if reviews:
            result["required_reviews"] = {
                "required_approving_review_count": reviews.get("required_approving_review_count"),
                "dismiss_stale_reviews": reviews.get("dismiss_stale_reviews"),
                "require_code_owner_reviews": reviews.get("require_code_owner_reviews"),
                "require_last_push_approval": reviews.get("require_last_push_approval"),
            }

        logger.debug("Branch protection enabled for %s:%s", repo_full_name, branch)
        return result

    # Unexpected status code
    result["error"] = f"Unexpected status code: {status_code}"
    logger.warning(
        "Unexpected status %d for %s:%s",
        status_code,
        repo_full_name,
        branch,
    )
    return result


def _select_repos_for_collection(
    repos: list[dict[str, Any]],
    config: Config,
) -> list[dict[str, Any]]:
    """Select which repositories to collect branch protection for.

    Args:
        repos: List of all discovered repositories.
        config: Application configuration.

    Returns:
        List of repositories to collect for based on mode.
    """
    bp_config = config.collection.hygiene.branch_protection
    mode = bp_config.mode

    if mode == "skip":
        return []

    if mode == "best_effort":
        # Collect for all repos
        return repos

    if mode == "sample":
        # Sample top N repos by metric
        sample_count = bp_config.sample_count
        metric_key = bp_config.sample_top_repos_by

        # Try to sort by the specified metric
        sorted_repos = _sort_repos_by_metric(repos, metric_key)

        # Take top N
        selected = sorted_repos[:sample_count]
        logger.info(
            "Selected %d/%d repos for branch protection sampling (by %s)",
            len(selected),
            len(repos),
            metric_key,
        )
        return selected

    logger.warning("Unknown branch protection mode: %s, defaulting to skip", mode)
    return []


def _sort_repos_by_metric(
    repos: list[dict[str, Any]],
    metric_key: str,
) -> list[dict[str, Any]]:
    """Sort repositories by a metric for sampling.

    Args:
        repos: List of repositories.
        metric_key: Metric to sort by (e.g., "prs_merged", "stars", "forks").

    Returns:
        Sorted list of repositories (descending order).
    """
    # Map metric keys to repository fields
    metric_mapping = {
        "prs_merged": "mergedPullRequests.totalCount",
        "stars": "stargazerCount",
        "forks": "forkCount",
        "watchers": "watchers.totalCount",
        "issues": "issues.totalCount",
        "pull_requests": "pullRequests.totalCount",
    }

    field_path = metric_mapping.get(metric_key, "stargazerCount")

    def get_metric_value(repo: dict[str, Any]) -> int:
        """Extract metric value from nested dict structure."""
        # Handle dotted path like "mergedPullRequests.totalCount"
        value: Any = repo
        for key in field_path.split("."):
            if isinstance(value, dict):
                value = value.get(key, 0)
            else:
                return 0

        # Ensure we return an int
        try:
            return int(value) if value is not None else 0
        except (ValueError, TypeError):
            return 0

    try:
        # Sort descending by metric value
        sorted_repos = sorted(
            repos,
            key=get_metric_value,
            reverse=True,
        )
        return sorted_repos
    except Exception as e:
        logger.warning("Failed to sort repos by %s: %s, using original order", metric_key, e)
        return repos


def _get_default_branch(repo: dict[str, Any]) -> str:
    """Extract default branch name from repository metadata.

    Args:
        repo: Repository metadata dict.

    Returns:
        Default branch name, defaulting to "main" if not found.
    """
    # Try different possible keys
    if "defaultBranchRef" in repo:
        # GraphQL format
        branch_ref = repo["defaultBranchRef"]
        if isinstance(branch_ref, dict):
            name = branch_ref.get("name", "main")
            return str(name) if name else "main"
        return "main"

    if "default_branch" in repo:
        # REST format
        branch = repo["default_branch"]
        return str(branch) if branch else "main"

    # Fallback to main
    logger.debug("No default branch found in repo metadata, using 'main'")
    return "main"


def _parse_repo_name(full_name: str) -> tuple[str, str]:
    """Parse full repository name into owner and repo name.

    Args:
        full_name: Full repository name in format "owner/repo".

    Returns:
        Tuple of (owner, repo_name).

    Raises:
        ValueError: If name format is invalid.
    """
    parts = full_name.split("/")
    if len(parts) != 2:
        msg = f"Invalid repository name format: {full_name}"
        raise ValueError(msg)

    return parts[0], parts[1]
