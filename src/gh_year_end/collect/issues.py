"""Issue collection for GitHub repositories.

Collects issues for discovered repositories, filtering out pull requests
and applying date range filters. Writes raw data to JSONL storage.
"""

import logging
from typing import Any

from gh_year_end.config import Config
from gh_year_end.github.ratelimit import AdaptiveRateLimiter
from gh_year_end.github.rest import RestClient
from gh_year_end.storage.paths import PathManager
from gh_year_end.storage.writer import AsyncJSONLWriter

logger = logging.getLogger(__name__)


class IssueCollectionStats:
    """Statistics for issue collection."""

    def __init__(self) -> None:
        """Initialize collection stats."""
        self.repos_processed = 0
        self.repos_skipped = 0
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
) -> dict[str, int]:
    """Collect issues for all discovered repositories.

    Fetches issues from GitHub API, filters out pull requests (GitHub's issue API
    includes PRs), applies date range filters, and writes raw data to JSONL storage.

    Args:
        repos: List of repository metadata dicts from discovery.
        rest_client: REST API client for GitHub requests.
        paths: Path manager for storage locations.
        rate_limiter: Rate limiter for API throttling.
        config: Application configuration with date range settings.

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
        try:
            repo_name = repo["full_name"]
            logger.debug("Collecting issues for %s", repo_name)

            # Parse owner/repo
            parts = repo_name.split("/", 1)
            if len(parts) != 2:
                logger.warning("Invalid repo name format: %s, skipping", repo_name)
                stats.repos_skipped += 1
                continue

            owner, repo_short_name = parts

            # Get output path and prepare writer
            output_path = paths.issues_raw_path(repo_name)

            # Check if data already exists
            existing_count = await AsyncJSONLWriter.count_records(output_path)
            if existing_count > 0:
                logger.debug(
                    "Issues already collected for %s (%d records), skipping",
                    repo_name,
                    existing_count,
                )
                stats.repos_skipped += 1
                continue

            # Collect issues
            repo_issue_count = 0
            repo_pr_count = 0

            async with AsyncJSONLWriter(output_path) as writer:
                async for items, _metadata in rest_client.list_issues(
                    owner=owner,
                    repo=repo_short_name,
                    state="all",
                    since=since,
                ):
                    # Filter out pull requests and apply date range
                    for item in items:
                        # GitHub's /issues endpoint includes PRs - filter them out
                        if "pull_request" in item:
                            repo_pr_count += 1
                            stats.pull_requests_filtered += 1
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
                        )
                        repo_issue_count += 1

            logger.debug(
                "Collected %d issues for %s (filtered %d PRs)",
                repo_issue_count,
                repo_name,
                repo_pr_count,
            )

            stats.issues_collected += repo_issue_count
            stats.repos_processed += 1

        except Exception as e:
            logger.error("Error collecting issues for %s: %s", repo.get("full_name", "unknown"), e)
            stats.errors += 1
            continue

    logger.info(
        "Issue collection complete: repos_processed=%d, issues_collected=%d, "
        "pull_requests_filtered=%d, repos_skipped=%d, errors=%d",
        stats.repos_processed,
        stats.issues_collected,
        stats.pull_requests_filtered,
        stats.repos_skipped,
        stats.errors,
    )

    return stats.to_dict()
