"""Repository discovery for GitHub organizations and users.

Discovers repositories based on configuration and applies filters for forks,
archived repos, visibility, activity, size, language, topics, and name patterns.
Supports quick scan mode using GitHub Search API. Writes raw data to JSONL storage.
"""

import logging
from typing import Any

from gh_year_end.collect.filters import FilterChain
from gh_year_end.config import Config
from gh_year_end.github.http import GitHubClient, GitHubResponse
from gh_year_end.storage.paths import PathManager
from gh_year_end.storage.writer import AsyncJSONLWriter

logger = logging.getLogger(__name__)


class DiscoveryError(Exception):
    """Raised when repository discovery fails."""


async def discover_repos(
    config: Config,
    client: GitHubClient,
    paths: PathManager,
) -> list[dict[str, Any]]:
    """Discover repositories based on configuration.

    Discovers all repositories for the configured target (org or user),
    applies discovery filters, writes raw data to JSONL, and returns
    repo metadata for further processing.

    Args:
        config: Application configuration.
        client: GitHub HTTP client.
        paths: Path manager for storage locations.

    Returns:
        List of repo metadata dictionaries.

    Raises:
        DiscoveryError: If discovery fails.
    """
    target_mode = config.github.target.mode
    target_name = config.github.target.name
    discovery_config = config.github.discovery

    logger.info(
        "Starting repository discovery: mode=%s, target=%s, quick_scan=%s",
        target_mode,
        target_name,
        discovery_config.quick_scan.enabled,
    )

    # Create filter chain
    filter_chain = FilterChain(discovery_config)

    # Fetch repositories (quick scan or thorough)
    if discovery_config.quick_scan.enabled:
        logger.info("Using quick scan (Search API) for discovery")
        raw_repos = await _quick_scan_discovery(client, target_mode, target_name, filter_chain)
    else:
        logger.info("Using thorough discovery (List API)")
        raw_repos = await _fetch_repos(client, target_mode, target_name)

    logger.info("Fetched %d raw repositories from %s", len(raw_repos), target_name)

    # Apply filters and track statistics
    filtered_repos, filter_stats = _apply_filters(raw_repos, filter_chain)

    logger.info(
        "Filtered to %d repositories (rejected: %d)",
        len(filtered_repos),
        filter_stats["total_rejected"],
    )

    # Log rejection statistics
    if filter_stats["rejected_by_filter"]:
        logger.info("Filter rejection breakdown:")
        for filter_name, count in sorted(
            filter_stats["rejected_by_filter"].items(),
            key=lambda x: x[1],
            reverse=True,
        ):
            logger.info("  %s: %d", filter_name, count)

    # Write raw data to JSONL
    await _write_raw_repos(filtered_repos, client, paths, filter_stats)

    # Extract and return metadata
    repos_metadata = _extract_metadata(filtered_repos)

    logger.info("Repository discovery complete: %d repos discovered", len(repos_metadata))

    return repos_metadata


async def _quick_scan_discovery(
    client: GitHubClient,
    mode: str,
    name: str,
    filter_chain: FilterChain,
) -> list[dict[str, Any]]:
    """Perform quick discovery using GitHub Search API.

    Falls back to thorough discovery on error.

    Args:
        client: GitHub HTTP client.
        mode: Target mode ("org" or "user").
        name: Target name (organization or username).
        filter_chain: Filter chain for building search query.

    Returns:
        List of raw repository data from API.
    """
    try:
        # Build search query from filters
        query = filter_chain.get_search_query(name, mode)
        logger.debug("Search query: %s", query)

        repos: list[dict[str, Any]] = []
        page = 1
        max_pages = 10  # GitHub Search API limits to 1000 results (10 pages * 100 per page)

        while page <= max_pages:
            logger.debug("Fetching search results page %d", page)

            response: GitHubResponse = await client.get(
                "/search/repositories",
                params={
                    "q": query,
                    "page": page,
                    "per_page": 100,
                    "sort": "updated",
                    "order": "desc",
                },
            )

            if not response.is_success:
                logger.warning(
                    "Search API failed with status %d, falling back to thorough discovery",
                    response.status_code,
                )
                return await _fetch_repos(client, mode, name)

            data = response.data
            if not isinstance(data, dict) or "items" not in data:
                logger.warning("Unexpected search response format, falling back to thorough discovery")
                return await _fetch_repos(client, mode, name)

            items = data["items"]
            if not items:
                break

            repos.extend(items)
            logger.debug(
                "Fetched %d repos on page %d (total: %d, total_count: %d)",
                len(items),
                page,
                len(repos),
                data.get("total_count", 0),
            )

            # Check if we've reached the end
            total_count = data.get("total_count", 0)
            if len(repos) >= total_count or len(repos) >= 1000:
                break

            page += 1

        logger.info("Quick scan discovered %d repositories", len(repos))
        return repos

    except Exception as e:
        logger.warning(
            "Quick scan failed: %s, falling back to thorough discovery",
            e,
            exc_info=True,
        )
        return await _fetch_repos(client, mode, name)


async def _fetch_repos(
    client: GitHubClient,
    mode: str,
    name: str,
) -> list[dict[str, Any]]:
    """Fetch all repositories from GitHub API using list endpoint.

    Args:
        client: GitHub HTTP client.
        mode: Target mode ("org" or "user").
        name: Target name (organization or username).

    Returns:
        List of raw repository data from API.

    Raises:
        DiscoveryError: If API request fails.
    """
    endpoint = f"/orgs/{name}/repos" if mode == "org" else f"/users/{name}/repos"

    repos: list[dict[str, Any]] = []
    page = 1

    while True:
        logger.debug("Fetching repos page %d from %s", page, endpoint)

        try:
            response: GitHubResponse = await client.get(
                endpoint,
                params={"page": page, "per_page": 100, "sort": "created", "direction": "asc"},
            )
        except Exception as e:
            msg = f"Failed to fetch repositories from {endpoint}: {e}"
            logger.error(msg)
            raise DiscoveryError(msg) from e

        if not response.is_success:
            msg = f"API error {response.status_code} for {endpoint}"
            logger.error(msg)
            raise DiscoveryError(msg)

        page_data = response.data
        if not isinstance(page_data, list):
            msg = f"Expected list response from {endpoint}, got {type(page_data)}"
            logger.error(msg)
            raise DiscoveryError(msg)

        if not page_data:
            break

        repos.extend(page_data)
        logger.debug("Fetched %d repos on page %d (total: %d)", len(page_data), page, len(repos))

        page += 1

    return repos


def _apply_filters(
    repos: list[dict[str, Any]],
    filter_chain: FilterChain,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Apply filter chain to repository list.

    Args:
        repos: List of raw repository data.
        filter_chain: Filter chain to evaluate.

    Returns:
        Tuple of (filtered_repos, filter_stats).
    """
    filtered = []
    total_rejected = 0

    for repo in repos:
        result = filter_chain.evaluate(repo)

        if result.passed:
            filtered.append(repo)
        else:
            total_rejected += 1
            if result.filter_name:
                filter_chain.record_rejection(result.filter_name)
                logger.debug(
                    "Rejected %s: %s - %s",
                    repo.get("full_name", "unknown"),
                    result.filter_name,
                    result.reason or "no reason",
                )

    # Build statistics
    filter_stats = {
        "total_discovered": len(repos),
        "passed_filters": len(filtered),
        "total_rejected": total_rejected,
        "rejected_by_filter": filter_chain.get_stats(),
    }

    return filtered, filter_stats


async def _write_raw_repos(
    repos: list[dict[str, Any]],
    client: GitHubClient,
    paths: PathManager,
    filter_stats: dict[str, Any],
) -> None:
    """Write raw repository data to JSONL storage.

    Args:
        repos: List of repository data to write.
        client: GitHub HTTP client (for endpoint info).
        paths: Path manager for storage locations.
        filter_stats: Filter statistics to include in manifest.
    """
    output_path = paths.repos_raw_path

    logger.debug("Writing %d repos to %s", len(repos), output_path)

    async with AsyncJSONLWriter(output_path) as writer:
        for repo in repos:
            await writer.write(
                source="github_rest",
                endpoint=f"/repos/{repo.get('full_name', 'unknown')}",
                data=repo,
            )

    # Write manifest with filter stats
    manifest_path = output_path.parent / "manifest.json"
    import json

    manifest = {
        "repos_discovered": len(repos),
        "timestamp": writer.timestamp.isoformat() if hasattr(writer, "timestamp") else None,
        "filter_stats": filter_stats,
    }

    manifest_path.write_text(json.dumps(manifest, indent=2))

    logger.debug("Wrote %d repos to JSONL storage with manifest", len(repos))


def _extract_metadata(repos: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract relevant metadata from repository data.

    Args:
        repos: List of raw repository data.

    Returns:
        List of metadata dictionaries with essential fields.
    """
    metadata_list = []

    for repo in repos:
        try:
            metadata = {
                "id": repo["id"],
                "name": repo["name"],
                "full_name": repo["full_name"],
                "description": repo.get("description"),
                "default_branch": repo.get("default_branch", "main"),
                "is_fork": repo.get("fork", False),
                "is_archived": repo.get("archived", False),
                "visibility": repo.get("visibility")
                or ("private" if repo.get("private") else "public"),
                "created_at": repo.get("created_at"),
                "updated_at": repo.get("updated_at"),
                "pushed_at": repo.get("pushed_at"),
                "language": repo.get("language"),
                "stargazers_count": repo.get("stargazers_count", 0),
                "forks_count": repo.get("forks_count", 0),
                "open_issues_count": repo.get("open_issues_count", 0),
                "size": repo.get("size", 0),
            }
            metadata_list.append(metadata)
        except KeyError as e:
            logger.warning(
                "Missing required field %s for repo %s, skipping",
                e,
                repo.get("full_name", "unknown"),
            )
            continue

    return metadata_list
