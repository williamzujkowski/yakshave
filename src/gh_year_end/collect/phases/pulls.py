"""Pull request collection phase.

Collects pull requests from all repositories in parallel.
"""

import logging
from typing import Any

from gh_year_end.collect.progress import ProgressTracker
from gh_year_end.collect.pulls import collect_single_repo_pulls
from gh_year_end.config import Config
from gh_year_end.github.rest import RestClient
from gh_year_end.storage.checkpoint import CheckpointManager
from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


async def run_pulls_phase(
    config: Config,
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    paths: PathManager,
    checkpoint: CheckpointManager,
    progress: ProgressTracker,
    collect_repos_parallel: Any,  # Function for parallel processing
) -> dict[str, Any]:
    """Run pull request collection phase.

    Args:
        config: Application configuration.
        repos: List of discovered repositories.
        rest_client: REST client for API calls.
        paths: Path manager for storage.
        checkpoint: Checkpoint manager for resume support.
        progress: Progress tracker.
        collect_repos_parallel: Helper function for parallel repo processing.

    Returns:
        Stats dict with collection results.
    """
    if not config.collection.enable.pulls:
        logger.info("Skipping pull request collection (disabled in config)")
        progress.mark_phase_complete("pulls")
        return {"pulls_collected": 0, "skipped": True}

    progress.set_phase("pulls")
    if checkpoint.is_phase_complete("pulls"):
        logger.info("Pull requests phase already complete, skipping")
        progress.mark_phase_complete("pulls")
        return {"pulls_collected": 0, "skipped": True}

    logger.info("=" * 80)
    logger.info("STEP 3: Pull Request Collection (Parallel)")
    logger.info("=" * 80)
    checkpoint.set_current_phase("pulls")

    # Use parallel processing for faster collection
    logger.info(
        "Processing %d repos in parallel (max_concurrency=%d)",
        len(repos),
        config.rate_limit.max_concurrency,
    )

    pull_stats = await collect_repos_parallel(
        repos=repos,
        collect_fn=collect_single_repo_pulls,
        endpoint_name="pulls",
        checkpoint=checkpoint,
        max_concurrency=config.rate_limit.max_concurrency,
        rest_client=rest_client,
        paths=paths,
        config=config,
    )

    checkpoint.mark_phase_complete("pulls")
    progress.update_items_collected("pulls", pull_stats.get("pulls_collected", 0))
    progress.mark_phase_complete("pulls")
    logger.info(
        "Pull request complete: %d repos processed, %d PRs collected, "
        "%d repos skipped, %d errors",
        pull_stats.get("repos_processed", 0),
        pull_stats.get("pulls_collected", 0),
        pull_stats.get("repos_skipped", 0),
        pull_stats.get("repos_errored", 0),
    )

    return pull_stats
