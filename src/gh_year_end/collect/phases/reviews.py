"""Review collection phase.

Collects pull request reviews from all repositories.
"""

import logging
from typing import Any

from gh_year_end.collect.progress import ProgressTracker
from gh_year_end.collect.reviews import collect_reviews
from gh_year_end.config import Config
from gh_year_end.github.ratelimit import AdaptiveRateLimiter
from gh_year_end.github.rest import RestClient
from gh_year_end.storage.checkpoint import CheckpointManager
from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


async def run_reviews_phase(
    config: Config,
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    rate_limiter: AdaptiveRateLimiter,
    paths: PathManager,
    checkpoint: CheckpointManager,
    progress: ProgressTracker,
) -> dict[str, Any]:
    """Run review collection phase.

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
    if not config.collection.enable.reviews:
        logger.info("Skipping review collection (disabled in config)")
        progress.mark_phase_complete("reviews")
        return {"reviews_collected": 0, "skipped": True}

    progress.set_phase("reviews")
    if checkpoint.is_phase_complete("reviews"):
        logger.info("Reviews phase already complete, skipping")
        progress.mark_phase_complete("reviews")
        return {"reviews_collected": 0, "skipped": True}

    logger.info("=" * 80)
    logger.info("STEP 5: Review Collection")
    logger.info("=" * 80)
    checkpoint.set_current_phase("reviews")

    review_stats = await collect_reviews(
        repos=repos,
        rest_client=rest_client,
        paths=paths,
        rate_limiter=rate_limiter,
        config=config,
        checkpoint=checkpoint,
    )

    checkpoint.mark_phase_complete("reviews")
    progress.update_items_collected("reviews", review_stats.get("reviews_collected", 0))
    progress.mark_phase_complete("reviews")
    logger.info(
        "Review collection complete: %d reviews collected", review_stats.get("reviews_collected", 0)
    )

    return review_stats
