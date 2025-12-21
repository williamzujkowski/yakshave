"""Comment collection phase.

Collects issue comments and review comments from all repositories.
"""

import json
import logging
from typing import Any

from gh_year_end.collect.comments import collect_issue_comments, collect_review_comments
from gh_year_end.collect.progress import ProgressTracker
from gh_year_end.config import Config
from gh_year_end.github.ratelimit import AdaptiveRateLimiter
from gh_year_end.github.rest import RestClient
from gh_year_end.storage.checkpoint import CheckpointManager
from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


async def _extract_issue_numbers_from_raw(
    repos: list[dict[str, Any]],
    paths: PathManager,
) -> dict[str, list[int]]:
    """Extract issue numbers from raw issue JSONL files.

    Args:
        repos: List of repository metadata.
        paths: Path manager for storage.

    Returns:
        Dictionary mapping repo full_name to list of issue numbers.
    """
    issue_numbers_by_repo: dict[str, list[int]] = {}

    for repo in repos:
        repo_full_name = repo["full_name"]
        issue_file_path = paths.issues_raw_path(repo_full_name)

        if not issue_file_path.exists():
            continue

        issue_numbers = set()
        try:
            with issue_file_path.open() as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        data = record.get("data", {})
                        number = data.get("number")
                        if number is not None:
                            issue_numbers.add(int(number))
                    except (json.JSONDecodeError, ValueError, KeyError) as e:
                        logger.warning("Failed to parse issue record: %s", e)
                        continue

            if issue_numbers:
                issue_numbers_by_repo[repo_full_name] = sorted(issue_numbers)
                logger.debug("Extracted %d issue numbers from %s", len(issue_numbers), repo_full_name)

        except Exception as e:
            logger.error("Error reading issue file %s: %s", issue_file_path, e)
            continue

    return issue_numbers_by_repo


async def _extract_pr_numbers_from_raw(
    repos: list[dict[str, Any]],
    paths: PathManager,
) -> dict[str, list[int]]:
    """Extract PR numbers from raw PR JSONL files.

    Args:
        repos: List of repository metadata.
        paths: Path manager for storage.

    Returns:
        Dictionary mapping repo full_name to list of PR numbers.
    """
    pr_numbers_by_repo: dict[str, list[int]] = {}

    for repo in repos:
        repo_full_name = repo["full_name"]
        pr_file_path = paths.pulls_raw_path(repo_full_name)

        if not pr_file_path.exists():
            continue

        pr_numbers = set()
        try:
            with pr_file_path.open() as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        data = record.get("data", {})
                        number = data.get("number")
                        if number is not None:
                            pr_numbers.add(int(number))
                    except (json.JSONDecodeError, ValueError, KeyError) as e:
                        logger.warning("Failed to parse PR record: %s", e)
                        continue

            if pr_numbers:
                pr_numbers_by_repo[repo_full_name] = sorted(pr_numbers)
                logger.debug("Extracted %d PR numbers from %s", len(pr_numbers), repo_full_name)

        except Exception as e:
            logger.error("Error reading PR file %s: %s", pr_file_path, e)
            continue

    return pr_numbers_by_repo


async def run_comments_phase(
    config: Config,
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    rate_limiter: AdaptiveRateLimiter,
    paths: PathManager,
    checkpoint: CheckpointManager,
    progress: ProgressTracker,
) -> dict[str, Any]:
    """Run comment collection phase.

    Args:
        config: Application configuration.
        repos: List of discovered repositories.
        rest_client: REST client for API calls.
        rate_limiter: Rate limiter for API calls.
        paths: Path manager for storage.
        checkpoint: Checkpoint manager for resume support.
        progress: Progress tracker.

    Returns:
        Stats dict with collection results.
    """
    if not config.collection.enable.comments:
        logger.info("Skipping comment collection (disabled in config)")
        progress.mark_phase_complete("comments")
        return {"total_comments": 0, "skipped": True}

    progress.set_phase("comments")
    if checkpoint.is_phase_complete("comments"):
        logger.info("Comments phase already complete, skipping")
        progress.mark_phase_complete("comments")
        return {"total_comments": 0, "skipped": True}

    logger.info("=" * 80)
    logger.info("STEP 6: Comment Collection")
    logger.info("=" * 80)
    checkpoint.set_current_phase("comments")

    # Extract issue numbers from collected issues
    logger.info("Extracting issue numbers from collected issues...")
    issue_numbers_by_repo = await _extract_issue_numbers_from_raw(repos, paths)
    logger.info("Found issues in %d repositories", len(issue_numbers_by_repo))

    # Extract PR numbers from collected PRs
    logger.info("Extracting PR numbers from collected PRs...")
    pr_numbers_by_repo = await _extract_pr_numbers_from_raw(repos, paths)
    logger.info("Found PRs in %d repositories", len(pr_numbers_by_repo))

    # Collect issue comments
    logger.info("Collecting issue comments...")
    issue_comment_stats = await collect_issue_comments(
        repos=repos,
        rest_client=rest_client,
        paths=paths,
        rate_limiter=rate_limiter,
        config=config,
        issue_numbers_by_repo=issue_numbers_by_repo,
        checkpoint=checkpoint,
    )

    # Collect review comments
    logger.info("Collecting review comments...")
    review_comment_stats = await collect_review_comments(
        repos=repos,
        rest_client=rest_client,
        paths=paths,
        rate_limiter=rate_limiter,
        config=config,
        pr_numbers_by_repo=pr_numbers_by_repo,
        checkpoint=checkpoint,
    )

    stats = {
        "issue_comments": issue_comment_stats,
        "review_comments": review_comment_stats,
        "total_comments": (
            issue_comment_stats.get("comments_collected", 0)
            + review_comment_stats.get("comments_collected", 0)
        ),
    }

    checkpoint.mark_phase_complete("comments")
    progress.update_items_collected("comments", stats["total_comments"])
    progress.mark_phase_complete("comments")
    logger.info("Comment collection complete: %d total comments collected", stats["total_comments"])

    return stats
