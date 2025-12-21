"""Repository metadata collection phase.

Collects detailed metadata for all discovered repositories.
"""

import logging
from typing import Any

from gh_year_end.collect.progress import ProgressTracker
from gh_year_end.collect.repos import collect_repo_metadata
from gh_year_end.config import Config
from gh_year_end.github.graphql import GraphQLClient
from gh_year_end.github.ratelimit import AdaptiveRateLimiter
from gh_year_end.storage.checkpoint import CheckpointManager
from gh_year_end.storage.paths import PathManager
from gh_year_end.storage.writer import AsyncJSONLWriter

logger = logging.getLogger(__name__)


async def run_repo_metadata_phase(
    config: Config,
    repos: list[dict[str, Any]],
    graphql_client: GraphQLClient,
    rate_limiter: AdaptiveRateLimiter,
    paths: PathManager,
    checkpoint: CheckpointManager,
    progress: ProgressTracker,
) -> dict[str, Any]:
    """Run repository metadata collection phase.

    Args:
        config: Application configuration.
        repos: List of discovered repositories.
        graphql_client: GraphQL client for API calls.
        rate_limiter: Rate limiter for API calls.
        paths: Path manager for storage.
        checkpoint: Checkpoint manager for resume support.
        progress: Progress tracker.

    Returns:
        Stats dict with collection results.
    """
    if not config.collection.enable.hygiene:
        logger.info("Skipping repo metadata collection (hygiene collection disabled)")
        progress.mark_phase_complete("repo_metadata")
        return {"repos_processed": 0, "skipped": True}

    progress.set_phase("repo_metadata")
    if checkpoint.is_phase_complete("repo_metadata"):
        logger.info("Repo metadata phase already complete, skipping")
        progress.mark_phase_complete("repo_metadata")
        return {"repos_processed": len(repos), "skipped": True}

    logger.info("=" * 80)
    logger.info("STEP 2: Repository Metadata Collection")
    logger.info("=" * 80)
    checkpoint.set_current_phase("repo_metadata")

    # Open writer for repo metadata
    repo_metadata_path = paths.raw_root / "repo_metadata.jsonl"
    async with AsyncJSONLWriter(repo_metadata_path) as writer:
        repo_stats = await collect_repo_metadata(
            repos=repos,
            graphql_client=graphql_client,
            writer=writer,
            rate_limiter=rate_limiter,
            config=config,
        )

    checkpoint.mark_phase_complete("repo_metadata")
    progress.mark_phase_complete("repo_metadata")
    logger.info("Repo metadata complete: %d repos processed", repo_stats.get("repos_processed", 0))

    return repo_stats
