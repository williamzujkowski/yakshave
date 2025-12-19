"""Issue collection for GitHub repositories.

Collects issues for discovered repositories, filtering out pull requests
and applying date range filters. Writes raw data to JSONL storage.
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


class IssueCollectionStats:
    """Statistics for issue collection."""

    def __init__(self) -> None:
        """Initialize collection stats."""
        self.repos_processed = 0
        self.repos_skipped = 0
        self.repos_resumed = 0
        self.issues_collected = 0
        self.pull_requests_filtered = 0
        self.errors = 0

    def to_dict(self) -> dict[str, int]:
        """Convert stats to dictionary.

        Returns:
            Dictionary with stat names and counts.
        """
        return {
            "repos_processed": self.repos_processed,
            "repos_skipped": self.repos_skipped,
            "repos_resumed": self.repos_resumed,
            "issues_collected": self.issues_collected,
            "pull_requests_filtered": self.pull_requests_filtered,
            "errors": self.errors,
        }


async def collect_issues(
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    paths: PathManager,
    rate_limiter: AdaptiveRateLimiter,
    config: Config,
    checkpoint: CheckpointManager | None = None,
) -> dict[str, int]:
    """Collect issues for all discovered repositories.

    Fetches issues from GitHub API, filters out pull requests (GitHub's issue API
    includes PRs), applies date range filters, and writes raw data to JSONL storage.
    Supports checkpoint-based resume.

    Args:
        repos: List of repository metadata dicts from discovery.
        rest_client: REST API client for GitHub requests.
        paths: Path manager for storage locations.
        rate_limiter: Rate limiter for API throttling.
        config: Application configuration with date range settings.
        checkpoint: Optional CheckpointManager for resume support.

    Returns:
        Dictionary with collection statistics (repos_processed, issues_collected, etc.).
    """
    stats = IssueCollectionStats()
    since = config.github.windows.since.isoformat()
    until = config.github.windows.until.isoformat()

    logger.info(
        "Starting issue collection for %d repositories (since=%s, until=%s)",
        len(repos),
        since,
        until,
    )

    for repo in repos:
        repo_name = repo["full_name"]

        # Check if already complete via checkpoint
        if checkpoint and checkpoint.is_repo_endpoint_complete(repo_name, "issues"):
            logger.debug("Skipping %s - issues already complete", repo_name)
            stats.repos_resumed += 1
            continue

        try:
            logger.debug("Collecting issues for %s", repo_name)

            # Parse owner/repo
            parts = repo_name.split("/", 1)
            if len(parts) != 2:
                logger.warning("Invalid repo name format: %s, skipping", repo_name)
                stats.repos_skipped += 1
                continue

            owner, repo_short_name = parts

            # Mark as in progress
            if checkpoint:
                checkpoint.mark_repo_endpoint_in_progress(repo_name, "issues")

            # Collect issues using helper function
            repo_issue_count, repo_pr_count = await _collect_repo_issues(
                owner=owner,
                repo=repo_short_name,
                repo_name=repo_name,
                rest_client=rest_client,
                paths=paths,
                since=since,
                until=until,
                checkpoint=checkpoint,
            )

            logger.debug(
                "Collected %d issues for %s (filtered %d PRs)",
                repo_issue_count,
                repo_name,
                repo_pr_count,
            )

            stats.issues_collected += repo_issue_count
            stats.pull_requests_filtered += repo_pr_count
            stats.repos_processed += 1

            # Mark as complete
            if checkpoint:
                checkpoint.mark_repo_endpoint_complete(repo_name, "issues")

        except Exception as e:
            logger.error("Error collecting issues for %s: %s", repo.get("full_name", "unknown"), e)
            stats.errors += 1

            # Mark as failed
            if checkpoint:
                checkpoint.mark_repo_endpoint_failed(repo_name, "issues", str(e), retryable=True)
            continue

    logger.info(
        "Issue collection complete: repos_processed=%d, issues_collected=%d, "
        "pull_requests_filtered=%d, repos_skipped=%d, repos_resumed=%d, errors=%d",
        stats.repos_processed,
        stats.issues_collected,
        stats.pull_requests_filtered,
        stats.repos_skipped,
        stats.repos_resumed,
        stats.errors,
    )

    return stats.to_dict()


async def _collect_repo_issues(
    owner: str,
    repo: str,
    repo_name: str,
    rest_client: RestClient,
    paths: PathManager,
    since: str,
    until: str,
    checkpoint: CheckpointManager | None = None,
) -> tuple[int, int]:
    """Collect issues for a single repository.

    Args:
        owner: Repository owner.
        repo: Repository name.
        repo_name: Full repository name (owner/repo).
        rest_client: REST API client for GitHub requests.
        paths: Path manager for storage locations.
        since: ISO format date string for filtering issues.
        until: ISO format date string for filtering issues.
        checkpoint: Optional CheckpointManager for progress tracking.

    Returns:
        Tuple of (issue_count, pr_count) collected.

    Raises:
        Exception: If API request fails critically.
    """
    output_path = paths.issues_raw_path(repo_name)
    issue_count = 0
    pr_count = 0

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    async with AsyncJSONLWriter(output_path) as writer:
        async for items, metadata in rest_client.list_issues(
            owner=owner,
            repo=repo,
            state="all",
            since=since,
        ):
            # Filter out pull requests and apply date range
            for item in items:
                # GitHub's /issues endpoint includes PRs - filter them out
                if "pull_request" in item:
                    pr_count += 1
                    continue

                # Check if issue is within date range
                updated_at = item.get("updated_at", "")
                if updated_at and updated_at >= until:
                    # Issue updated after our window, skip
                    continue

                # Write issue to JSONL
                await writer.write(
                    source="github_rest",
                    endpoint=f"/repos/{repo_name}/issues/{item.get('number', 0)}",
                    data=item,
                    page=metadata["page"],
                )
                issue_count += 1

            # Update checkpoint with page progress
            if checkpoint:
                checkpoint.update_progress(repo_name, "issues", metadata["page"], issue_count)

            logger.debug(
                "Fetched page %d: %d items (%d issues, %d PRs filtered)",
                metadata["page"],
                len(items),
                issue_count,
                pr_count,
            )

    return issue_count, pr_count
