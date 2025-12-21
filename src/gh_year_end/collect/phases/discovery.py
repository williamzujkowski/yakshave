"""Repository discovery phase.

Discovers all repositories matching the target configuration.
"""

import json
import logging
from typing import Any

from gh_year_end.collect.discovery import discover_repos
from gh_year_end.collect.progress import ProgressTracker
from gh_year_end.config import Config
from gh_year_end.github.http import GitHubClient
from gh_year_end.storage.checkpoint import CheckpointManager
from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


async def run_discovery_phase(
    config: Config,
    http_client: GitHubClient,
    paths: PathManager,
    checkpoint: CheckpointManager,
    progress: ProgressTracker,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run repository discovery phase.

    Args:
        config: Application configuration.
        http_client: GitHub HTTP client.
        paths: Path manager for storage.
        checkpoint: Checkpoint manager for resume support.
        progress: Progress tracker.

    Returns:
        Tuple of (repos list, stats dict).
    """
    logger.info("=" * 80)
    logger.info("STEP 1: Repository Discovery")
    logger.info("=" * 80)
    progress.set_phase("discovery")

    if checkpoint.is_phase_complete("discovery"):
        logger.info("Discovery phase already complete, loading repos from checkpoint")
        # Load repo metadata from existing discovery file
        repos = []
        if paths.repos_raw_path.exists():
            with paths.repos_raw_path.open() as f:
                for line in f:
                    record = json.loads(line)
                    repos.append(record.get("data", {}))
        stats = {"repos_discovered": len(repos), "skipped": True}
        logger.info("Loaded %d repos from checkpoint", len(repos))
        progress.set_total_repos(len(repos))
        progress.mark_phase_complete("discovery")
        return repos, stats

    checkpoint.set_current_phase("discovery")
    repos = await discover_repos(config, http_client, paths)
    stats = {"repos_discovered": len(repos)}

    # Register all repos with checkpoint
    checkpoint.update_repos(repos)
    checkpoint.mark_phase_complete("discovery")
    progress.set_total_repos(len(repos))
    progress.mark_phase_complete("discovery")
    logger.info("Discovery complete: %d repos discovered", len(repos))

    return repos, stats
