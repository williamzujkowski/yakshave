"""Repository discovery for GitHub organizations and users.

Discovers repositories based on configuration and applies filters for forks,
archived repos, and visibility settings. Writes raw data to JSONL storage.
"""

import logging
from typing import Any

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
        "Starting repository discovery: mode=%s, target=%s",
        target_mode,
        target_name,
    )

    # Fetch all repositories
    raw_repos = await _fetch_repos(client, target_mode, target_name)

    logger.info("Fetched %d raw repositories from %s", len(raw_repos), target_name)

    # Apply discovery filters
    filtered_repos = _apply_filters(raw_repos, discovery_config)

    logger.info(
        "Filtered to %d repositories (forks=%s, archived=%s, visibility=%s)",
        len(filtered_repos),
        discovery_config.include_forks,
        discovery_config.include_archived,
        discovery_config.visibility,
    )

    # Write raw data to JSONL
    await _write_raw_repos(filtered_repos, client, paths)

    # Extract and return metadata
    repos_metadata = _extract_metadata(filtered_repos)

    logger.info("Repository discovery complete: %d repos discovered", len(repos_metadata))

    return repos_metadata


async def _fetch_repos(
    client: GitHubClient,
    mode: str,
    name: str,
) -> list[dict[str, Any]]:
    """Fetch all repositories from GitHub API.

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
    discovery_config: Any,
) -> list[dict[str, Any]]:
    """Apply discovery filters to repository list.

    Args:
        repos: List of raw repository data.
        discovery_config: Discovery configuration with filters.

    Returns:
        Filtered list of repositories.
    """
    filtered = repos

    # Filter forks
    if not discovery_config.include_forks:
        before = len(filtered)
        filtered = [r for r in filtered if not r.get("fork", False)]
        logger.debug("Filtered out %d forks", before - len(filtered))

    # Filter archived
    if not discovery_config.include_archived:
        before = len(filtered)
        filtered = [r for r in filtered if not r.get("archived", False)]
        logger.debug("Filtered out %d archived repos", before - len(filtered))

    # Filter by visibility
    if discovery_config.visibility != "all":
        before = len(filtered)
        filtered = [
            r
            for r in filtered
            if r.get("visibility", (r.get("private") and "private") or "public")
            == discovery_config.visibility
        ]
        logger.debug(
            "Filtered to %d repos with visibility=%s",
            len(filtered),
            discovery_config.visibility,
        )

    return filtered


async def _write_raw_repos(
    repos: list[dict[str, Any]],
    client: GitHubClient,
    paths: PathManager,
) -> None:
    """Write raw repository data to JSONL storage.

    Args:
        repos: List of repository data to write.
        client: GitHub HTTP client (for endpoint info).
        paths: Path manager for storage locations.
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

    logger.debug("Wrote %d repos to JSONL storage", len(repos))


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
