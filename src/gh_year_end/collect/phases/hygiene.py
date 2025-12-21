"""Hygiene collection phases.

Collects branch protection and security features from all repositories.
"""

import logging
from typing import Any

from gh_year_end.collect.hygiene import collect_branch_protection, collect_security_features
from gh_year_end.collect.progress import ProgressTracker
from gh_year_end.config import Config
from gh_year_end.github.rest import RestClient
from gh_year_end.storage.checkpoint import CheckpointManager
from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


async def run_branch_protection_phase(
    config: Config,
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    paths: PathManager,
    checkpoint: CheckpointManager,
    progress: ProgressTracker,
) -> dict[str, Any]:
    """Run branch protection collection phase.

    Args:
        config: Application configuration.
        repos: List of discovered repositories.
        rest_client: REST client for API calls.
        paths: Path manager for storage.
        checkpoint: Checkpoint manager for resume support.
        progress: Progress tracker.

    Returns:
        Stats dict with collection results.
    """
    if not config.collection.enable.hygiene:
        logger.info("Skipping branch protection collection (hygiene disabled in config)")
        progress.mark_phase_complete("branch_protection")
        return {"repos_processed": 0, "skipped": True}

    progress.set_phase("branch_protection")
    if checkpoint.is_phase_complete("branch_protection"):
        logger.info("Branch protection phase already complete, skipping")
        progress.mark_phase_complete("branch_protection")
        return {"repos_processed": 0, "skipped": True}

    logger.info("=" * 80)
    logger.info("STEP 8: Branch Protection Collection")
    logger.info("=" * 80)
    checkpoint.set_current_phase("branch_protection")

    hygiene_stats = await collect_branch_protection(
        repos=repos,
        rest_client=rest_client,
        path_manager=paths,
        config=config,
        checkpoint=checkpoint,
    )

    checkpoint.mark_phase_complete("branch_protection")
    progress.mark_phase_complete("branch_protection")
    logger.info(
        "Branch protection collection complete: %d repos processed, %d with protection enabled",
        hygiene_stats.get("repos_processed", 0),
        hygiene_stats.get("protection_enabled", 0),
    )

    return hygiene_stats


async def run_security_features_phase(
    config: Config,
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    paths: PathManager,
    checkpoint: CheckpointManager,
    progress: ProgressTracker,
) -> dict[str, Any]:
    """Run security features collection phase.

    Args:
        config: Application configuration.
        repos: List of discovered repositories.
        rest_client: REST client for API calls.
        paths: Path manager for storage.
        checkpoint: Checkpoint manager for resume support.
        progress: Progress tracker.

    Returns:
        Stats dict with collection results.
    """
    if not config.collection.enable.hygiene:
        logger.info("Skipping security features collection (hygiene disabled in config)")
        progress.mark_phase_complete("security_features")
        return {"repos_processed": 0, "skipped": True}

    progress.set_phase("security_features")
    if checkpoint.is_phase_complete("security_features"):
        logger.info("Security features phase already complete, skipping")
        progress.mark_phase_complete("security_features")
        return {"repos_processed": 0, "skipped": True}

    logger.info("=" * 80)
    logger.info("STEP 9: Security Features Collection")
    logger.info("=" * 80)
    checkpoint.set_current_phase("security_features")

    security_features_stats = await collect_security_features(
        repos=repos,
        rest_client=rest_client,
        paths=paths,
        config=config,
        checkpoint=checkpoint,
    )

    checkpoint.mark_phase_complete("security_features")
    progress.mark_phase_complete("security_features")
    logger.info(
        "Security features collection complete: %d repos processed, "
        "%d with all features, %d with partial features, %d with no access",
        security_features_stats.get("repos_processed", 0),
        security_features_stats.get("repos_with_all_features", 0),
        security_features_stats.get("repos_with_partial_features", 0),
        security_features_stats.get("repos_with_no_access", 0),
    )

    return security_features_stats
