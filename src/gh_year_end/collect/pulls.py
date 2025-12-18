"""Pull Request collector for GitHub repositories.

Collects pull requests for all discovered repositories, applying date filters
and writing raw PR data to JSONL storage.
"""

import logging
from datetime import datetime
from typing import Any

from gh_year_end.config import Config
from gh_year_end.github.rest import RestClient
from gh_year_end.storage.paths import PathManager
from gh_year_end.storage.writer import AsyncJSONLWriter

logger = logging.getLogger(__name__)


class PullsCollectorError(Exception):
    """Raised when pull request collection fails."""


async def collect_pulls(
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    paths: PathManager,
    config: Config,
) -> dict[str, Any]:
    """Collect pull requests for all discovered repositories.

    Fetches PRs for each repository, filters by date range, writes to JSONL,
    and tracks progress and errors.

    Args:
        repos: List of repo metadata dicts from discovery.
        rest_client: RestClient for GitHub API access.
        paths: PathManager for storage locations.
        config: Application configuration with date filters.

    Returns:
        Stats dictionary with:
            - repos_processed: Number of repos processed.
            - pulls_collected: Total PRs collected.
            - repos_skipped: Repos skipped due to errors.
            - errors: List of error messages.

    Raises:
        PullsCollectorError: If collection fails critically.
    """
    since = config.github.windows.since
    until = config.github.windows.until

    logger.info(
        "Starting PR collection: %d repos, date range %s to %s",
        len(repos),
        since.isoformat(),
        until.isoformat(),
    )

    stats: dict[str, Any] = {
        "repos_processed": 0,
        "pulls_collected": 0,
        "repos_skipped": 0,
        "errors": [],
    }

    for idx, repo in enumerate(repos, 1):
        repo_full_name = repo["full_name"]
        owner, repo_name = repo_full_name.split("/", 1)

        logger.info(
            "Processing repo %d/%d: %s",
            idx,
            len(repos),
            repo_full_name,
        )

        try:
            # Collect PRs for this repo
            pr_count = await _collect_repo_pulls(
                owner=owner,
                repo=repo_name,
                repo_full_name=repo_full_name,
                rest_client=rest_client,
                paths=paths,
                since=since,
                until=until,
            )

            stats["repos_processed"] += 1
            stats["pulls_collected"] += pr_count

            logger.info(
                "Collected %d PRs from %s (total: %d)",
                pr_count,
                repo_full_name,
                stats["pulls_collected"],
            )

        except Exception as e:
            error_msg = f"Failed to collect PRs from {repo_full_name}: {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)
            stats["repos_skipped"] += 1
            continue

    logger.info(
        "PR collection complete: %d repos processed, %d PRs collected, %d repos skipped",
        stats["repos_processed"],
        stats["pulls_collected"],
        stats["repos_skipped"],
    )

    return stats


async def _collect_repo_pulls(
    owner: str,
    repo: str,
    repo_full_name: str,
    rest_client: RestClient,
    paths: PathManager,
    since: datetime,
    until: datetime,
) -> int:
    """Collect pull requests for a single repository.

    Args:
        owner: Repository owner.
        repo: Repository name.
        repo_full_name: Full repository name (owner/repo).
        rest_client: RestClient for API access.
        paths: PathManager for storage locations.
        since: Filter PRs updated after this date.
        until: Filter PRs updated before this date.

    Returns:
        Number of PRs collected.

    Raises:
        Exception: If API request fails critically (other than 404).
    """
    output_path = paths.pulls_raw_path(repo_full_name)
    pr_count = 0

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    async with AsyncJSONLWriter(output_path) as writer:
        try:
            # Fetch all PRs (state="all" to get both open and closed)
            async for prs_page, metadata in rest_client.list_pulls(
                owner=owner,
                repo=repo,
                state="all",
            ):
                # Filter PRs by date range
                filtered_prs = _filter_prs_by_date(prs_page, since, until)

                # Write each PR to JSONL
                for pr in filtered_prs:
                    await writer.write(
                        source="github_rest",
                        endpoint=f"/repos/{repo_full_name}/pulls/{pr['number']}",
                        data=pr,
                        page=metadata["page"],
                    )
                    pr_count += 1

                logger.debug(
                    "Fetched page %d: %d PRs (%d after date filter)",
                    metadata["page"],
                    len(prs_page),
                    len(filtered_prs),
                )

                # Early termination: if we're getting PRs outside our date range
                # and they're sorted by updated_at DESC, we can stop
                if prs_page and _all_prs_before_date(prs_page, since):
                    logger.debug(
                        "All PRs on page %d are before %s, stopping pagination",
                        metadata["page"],
                        since.isoformat(),
                    )
                    break

        except Exception as e:
            # If it's a 404, the repo might not exist or have no PRs
            # The RestClient already handles 404s by returning empty
            # So we just log and continue
            logger.debug("Error fetching PRs from %s: %s", repo_full_name, e)
            raise

    return pr_count


def _filter_prs_by_date(
    prs: list[dict[str, Any]],
    since: datetime,
    until: datetime,
) -> list[dict[str, Any]]:
    """Filter pull requests by date range.

    Filters based on updated_at timestamp, keeping PRs that were
    updated within the date range [since, until).

    Args:
        prs: List of PR data dicts.
        since: Include PRs updated on or after this date.
        until: Include PRs updated before this date.

    Returns:
        Filtered list of PRs.
    """
    filtered = []

    for pr in prs:
        updated_at_str = pr.get("updated_at")
        if not updated_at_str:
            logger.debug("PR #%d missing updated_at, skipping", pr.get("number", 0))
            continue

        try:
            # Parse ISO 8601 timestamp
            updated_at = datetime.fromisoformat(updated_at_str.replace("Z", "+00:00"))

            # Remove timezone for comparison with config dates
            updated_at_naive = updated_at.replace(tzinfo=None)

            # Check if within range [since, until)
            if since <= updated_at_naive < until:
                filtered.append(pr)

        except (ValueError, AttributeError) as e:
            logger.warning(
                "Failed to parse updated_at '%s' for PR #%d: %s",
                updated_at_str,
                pr.get("number", 0),
                e,
            )
            continue

    return filtered


def _all_prs_before_date(prs: list[dict[str, Any]], since: datetime) -> bool:
    """Check if all PRs in list were updated before the since date.

    Used for early termination of pagination when PRs are sorted
    by updated_at in descending order.

    Args:
        prs: List of PR data dicts.
        since: Date threshold.

    Returns:
        True if all PRs have updated_at before since date.
    """
    for pr in prs:
        updated_at_str = pr.get("updated_at")
        if not updated_at_str:
            continue

        try:
            updated_at = datetime.fromisoformat(updated_at_str.replace("Z", "+00:00"))
            updated_at_naive = updated_at.replace(tzinfo=None)

            # If any PR is on or after since date, return False
            if updated_at_naive >= since:
                return False

        except (ValueError, AttributeError):
            continue

    return True
