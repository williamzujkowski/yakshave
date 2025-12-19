"""Commit collection for GitHub repositories.

Collects commit history for all discovered repositories using the GitHub REST API,
applying date filters and writing raw data to JSONL storage.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from gh_year_end.storage.writer import AsyncJSONLWriter

if TYPE_CHECKING:
    from gh_year_end.config import Config
    from gh_year_end.github.ratelimit import AdaptiveRateLimiter
    from gh_year_end.github.rest import RestClient
    from gh_year_end.storage.checkpoint import CheckpointManager
    from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


class CommitCollectionError(Exception):
    """Raised when commit collection fails."""


async def collect_commits(
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    paths: PathManager,
    rate_limiter: AdaptiveRateLimiter | None = None,
    config: Config | None = None,
    checkpoint: CheckpointManager | None = None,
) -> dict[str, Any]:
    """Collect commits for all discovered repositories.

    Uses the GitHub REST API to fetch commits for each repository within
    the configured date range. Handles pagination automatically via the
    RestClient and respects rate limiting. Supports checkpoint-based resume.

    Args:
        repos: List of repository metadata dicts from discovery.
        rest_client: REST API client for GitHub requests.
        paths: Path manager for storage locations.
        rate_limiter: Optional rate limiter for throttling.
        config: Optional configuration for date range filtering.
        checkpoint: Optional CheckpointManager for resume support.

    Returns:
        Statistics dict with:
            - repos_processed: Number of repos successfully processed.
            - repos_skipped: Number of repos skipped (404, empty, etc).
            - repos_errored: Number of repos with errors.
            - repos_resumed: Number of repos skipped because already complete.
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
        "repos_resumed": 0,
        "commits_collected": 0,
        "errors": [],
    }

    # Extract date range from config if provided
    since = None
    until = None
    max_per_repo = None
    max_pages = None

    if config:
        since = config.github.windows.since.isoformat()
        until = config.github.windows.until.isoformat()

        # Extract commit collection limits
        max_per_repo = config.collection.commits.max_per_repo
        max_pages = config.collection.commits.max_pages

        # Calculate since override if since_days is set
        if config.collection.commits.since_days is not None:
            since_date = config.github.windows.until - timedelta(
                days=config.collection.commits.since_days
            )
            since = since_date.isoformat()
            logger.info(
                "Using since_days=%d, adjusted since=%s",
                config.collection.commits.since_days,
                since,
            )

        logger.info("Using date range: since=%s, until=%s", since, until)
        if max_per_repo is not None:
            logger.info("Max commits per repo: %d", max_per_repo)
        if max_pages is not None:
            logger.info("Max pages per repo: %d", max_pages)

    for repo in repos:
        repo_full_name = repo.get("full_name", "unknown")

        # Check if already complete via checkpoint
        if checkpoint and checkpoint.is_repo_endpoint_complete(repo_full_name, "commits"):
            logger.debug("Skipping %s - commits already complete", repo_full_name)
            stats["repos_resumed"] += 1
            continue

        # Mark as in progress
        if checkpoint:
            checkpoint.mark_repo_endpoint_in_progress(repo_full_name, "commits")

        try:
            repo_stats = await _collect_repo_commits(
                repo=repo,
                rest_client=rest_client,
                paths=paths,
                since=since,
                until=until,
                max_per_repo=max_per_repo,
                max_pages=max_pages,
                checkpoint=checkpoint,
            )

            if repo_stats["skipped"]:
                stats["repos_skipped"] += 1
            else:
                stats["repos_processed"] += 1
                stats["commits_collected"] += repo_stats["commits_count"]

            # Mark as complete
            if checkpoint:
                checkpoint.mark_repo_endpoint_complete(repo_full_name, "commits")

        except Exception as e:
            stats["repos_errored"] += 1
            error_detail = {
                "repo": repo_full_name,
                "error": str(e),
            }
            stats["errors"].append(error_detail)
            logger.error(
                "Failed to collect commits for %s: %s",
                repo_full_name,
                e,
            )

            # Mark as failed
            if checkpoint:
                checkpoint.mark_repo_endpoint_failed(
                    repo_full_name, "commits", str(e), retryable=True
                )

    logger.info(
        "Commit collection complete: %d repos processed, %d commits collected, "
        "%d repos skipped, %d repos errored, %d resumed from checkpoint",
        stats["repos_processed"],
        stats["commits_collected"],
        stats["repos_skipped"],
        stats["repos_errored"],
        stats["repos_resumed"],
    )

    return stats


async def _collect_repo_commits(
    repo: dict[str, Any],
    rest_client: RestClient,
    paths: PathManager,
    since: str | None = None,
    until: str | None = None,
    max_per_repo: int | None = None,
    max_pages: int | None = None,
    checkpoint: CheckpointManager | None = None,
) -> dict[str, Any]:
    """Collect commits for a single repository.

    Args:
        repo: Repository metadata dict.
        rest_client: REST API client for GitHub requests.
        paths: Path manager for storage locations.
        since: ISO 8601 timestamp to filter commits after this date.
        until: ISO 8601 timestamp to filter commits before this date.
        max_per_repo: Maximum commits to collect per repo (None = no limit).
        max_pages: Maximum pages to paginate per repo (None = no limit).
        checkpoint: Optional CheckpointManager for progress tracking.

    Returns:
        Statistics dict with:
            - commits_count: Number of commits collected.
            - skipped: Whether the repo was skipped.
            - limited: Whether collection was limited.
    """
    full_name = repo.get("full_name")
    if not full_name:
        logger.warning("Repo missing full_name field, skipping")
        return {"commits_count": 0, "skipped": True, "limited": False}

    owner, repo_name = full_name.split("/", 1)
    output_path = paths.commits_raw_path(full_name)

    logger.info("Collecting commits for %s", full_name)

    commits_count = 0
    page_count = 0
    was_limited = False

    async with AsyncJSONLWriter(output_path) as writer:
        try:
            async for commits_page, metadata in rest_client.list_commits(
                owner=owner,
                repo=repo_name,
                since=since,
                until=until,
            ):
                page_count += 1

                # Check max_pages limit
                if max_pages is not None and page_count > max_pages:
                    logger.info(
                        "Reached max_pages limit (%d) for %s",
                        max_pages,
                        full_name,
                    )
                    was_limited = True
                    break

                # Write each commit individually
                for commit in commits_page:
                    # Check max_per_repo limit before writing
                    if max_per_repo is not None and commits_count >= max_per_repo:
                        logger.info(
                            "Reached max_per_repo limit (%d) for %s",
                            max_per_repo,
                            full_name,
                        )
                        was_limited = True
                        break

                    await writer.write(
                        source="github_rest",
                        endpoint=f"/repos/{full_name}/commits",
                        data=commit,
                        page=metadata["page"],
                    )
                    commits_count += 1

                # Break outer loop if we hit the per-repo limit
                if was_limited:
                    break

                # Update checkpoint with page progress
                if checkpoint:
                    checkpoint.update_progress(
                        full_name, "commits", metadata["page"], len(commits_page)
                    )

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
                return {"commits_count": 0, "skipped": True, "limited": False}
            # Otherwise, re-raise the exception
            raise

    if commits_count == 0:
        logger.info("Repository %s has no commits in date range", full_name)
        return {"commits_count": 0, "skipped": True, "limited": False}

    if was_limited:
        logger.info(
            "Collected %d commits for %s (limited by configuration)",
            commits_count,
            full_name,
        )
    else:
        logger.info("Collected %d commits for %s", commits_count, full_name)

    return {"commits_count": commits_count, "skipped": False, "limited": was_limited}
