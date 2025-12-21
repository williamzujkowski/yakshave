"""Commit collection phase.

Collects commit history from all repositories.
"""

import logging
from typing import Any

from gh_year_end.collect.commits import collect_commits
from gh_year_end.collect.progress import ProgressTracker
from gh_year_end.config import Config
from gh_year_end.github.ratelimit import AdaptiveRateLimiter
from gh_year_end.github.rest import RestClient
from gh_year_end.storage.checkpoint import CheckpointManager
from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


async def run_commits_phase(
    config: Config,
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    rate_limiter: AdaptiveRateLimiter,
    paths: PathManager,
    checkpoint: CheckpointManager,
    progress: ProgressTracker,
) -> dict[str, Any]:
    """Run commit collection phase.

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
    if not config.collection.enable.commits:
        logger.info("Skipping commit collection (disabled in config)")
        progress.mark_phase_complete("commits")
        return {"commits_collected": 0, "skipped": True}

    progress.set_phase("commits")
    if checkpoint.is_phase_complete("commits"):
        logger.info("Commits phase already complete, skipping")
        progress.mark_phase_complete("commits")
        return {"commits_collected": 0, "skipped": True}

    logger.info("=" * 80)
    logger.info("STEP 7: Commit Collection")
    logger.info("=" * 80)
    checkpoint.set_current_phase("commits")

    commit_stats = await collect_commits(
        repos=repos,
        rest_client=rest_client,
        paths=paths,
        rate_limiter=rate_limiter,
        config=config,
        checkpoint=checkpoint,
    )

    checkpoint.mark_phase_complete("commits")
    progress.update_items_collected("commits", commit_stats.get("commits_collected", 0))
    progress.mark_phase_complete("commits")
    logger.info("Commit collection complete: %d commits collected", commit_stats.get("commits_collected", 0))

    return commit_stats
