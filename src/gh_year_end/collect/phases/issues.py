"""Issue collection phase.

Collects issues from all repositories.
"""

import logging
from typing import Any

from gh_year_end.collect.issues import collect_issues
from gh_year_end.collect.progress import ProgressTracker
from gh_year_end.config import Config
from gh_year_end.github.ratelimit import AdaptiveRateLimiter
from gh_year_end.github.rest import RestClient
from gh_year_end.storage.checkpoint import CheckpointManager
from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


async def run_issues_phase(
    config: Config,
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    rate_limiter: AdaptiveRateLimiter,
    paths: PathManager,
    checkpoint: CheckpointManager,
    progress: ProgressTracker,
) -> dict[str, Any]:
    """Run issue collection phase.

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
    if not config.collection.enable.issues:
        logger.info("Skipping issue collection (disabled in config)")
        progress.mark_phase_complete("issues")
        return {"issues_collected": 0, "skipped": True}

    progress.set_phase("issues")
    if checkpoint.is_phase_complete("issues"):
        logger.info("Issues phase already complete, skipping")
        progress.mark_phase_complete("issues")
        return {"issues_collected": 0, "skipped": True}

    logger.info("=" * 80)
    logger.info("STEP 4: Issue Collection")
    logger.info("=" * 80)
    checkpoint.set_current_phase("issues")

    issue_stats = await collect_issues(
        repos=repos,
        rest_client=rest_client,
        paths=paths,
        rate_limiter=rate_limiter,
        config=config,
        checkpoint=checkpoint,
    )

    checkpoint.mark_phase_complete("issues")
    progress.update_items_collected("issues", issue_stats.get("issues_collected", 0))
    progress.mark_phase_complete("issues")
    logger.info("Issue collection complete: %d issues collected", issue_stats.get("issues_collected", 0))

    return issue_stats
