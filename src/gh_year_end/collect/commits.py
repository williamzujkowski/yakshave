"""Commit collection for GitHub repositories.

Collects commit history for all discovered repositories using the GitHub REST API,
applying date filters and writing raw data to JSONL storage.
"""

import logging
from typing import Any

from gh_year_end.config import Config
from gh_year_end.github.ratelimit import AdaptiveRateLimiter
from gh_year_end.github.rest import RestClient
from gh_year_end.storage.paths import PathManager
from gh_year_end.storage.writer import AsyncJSONLWriter

logger = logging.getLogger(__name__)


class CommitCollectionError(Exception):
    """Raised when commit collection fails."""


async def collect_commits(
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    paths: PathManager,
    rate_limiter: AdaptiveRateLimiter | None = None,
    config: Config | None = None,
) -> dict[str, Any]:
    """Collect commits for all discovered repositories.

    Uses the GitHub REST API to fetch commits for each repository within
    the configured date range. Handles pagination automatically via the
    RestClient and respects rate limiting.

    Args:
        repos: List of repository metadata dicts from discovery.
        rest_client: REST API client for GitHub requests.
        paths: Path manager for storage locations.
        rate_limiter: Optional rate limiter for throttling.
        config: Optional configuration for date range filtering.

    Returns:
        Statistics dict with:
            - repos_processed: Number of repos successfully processed.
            - repos_skipped: Number of repos skipped (404, empty, etc).
            - repos_errored: Number of repos with errors.
            - commits_collected: Total number of commits collected.
            - errors: List of error details.

    Raises:
        CommitCollectionError: If critical collection failure occurs.
    """
    logger.info("Starting commit collection for %d repositories", len(repos))

    stats: dict[str, Any] = {
        "repos_processed": 0,
        "repos_skipped": 0,
        "repos_errored": 0,
        "commits_collected": 0,
        "errors": [],
    }

    # Extract date range from config if provided
    since = None
    until = None
    if config:
        since = config.github.windows.since.isoformat()
        until = config.github.windows.until.isoformat()
        logger.info("Using date range: since=%s, until=%s", since, until)

    for repo in repos:
        try:
            repo_stats = await _collect_repo_commits(
                repo=repo,
                rest_client=rest_client,
                paths=paths,
                since=since,
                until=until,
            )

            if repo_stats["skipped"]:
                stats["repos_skipped"] += 1
            else:
                stats["repos_processed"] += 1
                stats["commits_collected"] += repo_stats["commits_count"]

        except Exception as e:
            stats["repos_errored"] += 1
            error_detail = {
                "repo": repo.get("full_name", "unknown"),
                "error": str(e),
            }
            stats["errors"].append(error_detail)
            logger.error(
                "Failed to collect commits for %s: %s",
                repo.get("full_name", "unknown"),
                e,
            )

    logger.info(
        "Commit collection complete: %d repos processed, %d commits collected, "
        "%d repos skipped, %d repos errored",
        stats["repos_processed"],
        stats["commits_collected"],
        stats["repos_skipped"],
        stats["repos_errored"],
    )

    return stats


async def _collect_repo_commits(
    repo: dict[str, Any],
    rest_client: RestClient,
    paths: PathManager,
    since: str | None = None,
    until: str | None = None,
) -> dict[str, Any]:
    """Collect commits for a single repository.

    Args:
        repo: Repository metadata dict.
        rest_client: REST API client for GitHub requests.
        paths: Path manager for storage locations.
        since: ISO 8601 timestamp to filter commits after this date.
        until: ISO 8601 timestamp to filter commits before this date.

    Returns:
        Statistics dict with:
            - commits_count: Number of commits collected.
            - skipped: Whether the repo was skipped.
    """
    full_name = repo.get("full_name")
    if not full_name:
        logger.warning("Repo missing full_name field, skipping")
        return {"commits_count": 0, "skipped": True}

    owner, repo_name = full_name.split("/", 1)
    output_path = paths.commits_raw_path(full_name)

    logger.info("Collecting commits for %s", full_name)

    commits_count = 0
    page_count = 0

    async with AsyncJSONLWriter(output_path) as writer:
        try:
            async for commits_page, metadata in rest_client.list_commits(
                owner=owner,
                repo=repo_name,
                since=since,
                until=until,
            ):
                page_count += 1

                # Write each commit individually
                for commit in commits_page:
                    await writer.write(
                        source="github_rest",
                        endpoint=f"/repos/{full_name}/commits",
                        data=commit,
                        page=metadata["page"],
                    )
                    commits_count += 1

                logger.debug(
                    "Collected page %d for %s: %d commits (total: %d)",
                    metadata["page"],
                    full_name,
                    len(commits_page),
                    commits_count,
                )

        except Exception:
            # Check if this is a 404 (handled gracefully by RestClient)
            # If we got 0 commits and 0 pages, it was likely a 404 or empty repo
            if commits_count == 0 and page_count == 0:
                logger.info("No commits found for %s (likely empty or inaccessible)", full_name)
                return {"commits_count": 0, "skipped": True}
            # Otherwise, re-raise the exception
            raise

    if commits_count == 0:
        logger.info("Repository %s has no commits in date range", full_name)
        return {"commits_count": 0, "skipped": True}

    logger.info("Collected %d commits for %s", commits_count, full_name)
    return {"commits_count": commits_count, "skipped": False}
