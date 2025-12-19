"""PR review collection for GitHub repositories.

Collects reviews for all pull requests across repositories.
Writes raw review data to JSONL storage with proper tracking.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from gh_year_end.storage.writer import AsyncJSONLWriter

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from gh_year_end.config import Config
    from gh_year_end.github.ratelimit import AdaptiveRateLimiter
    from gh_year_end.github.rest import RestClient
    from gh_year_end.storage.checkpoint import CheckpointManager
    from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


class ReviewCollectionStats:
    """Statistics for review collection.

    Tracks PRs processed, reviews collected, and errors encountered.
    """

    def __init__(self) -> None:
        """Initialize stats counters."""
        self.repos_processed = 0
        self.repos_skipped = 0
        self.repos_resumed = 0
        self.prs_processed = 0
        self.reviews_collected = 0
        self.errors = 0
        self.skipped_404 = 0

    def to_dict(self) -> dict[str, int]:
        """Convert stats to dictionary.

        Returns:
            Dictionary with stat counters.
        """
        return {
            "repos_processed": self.repos_processed,
            "repos_skipped": self.repos_skipped,
            "repos_resumed": self.repos_resumed,
            "prs_processed": self.prs_processed,
            "reviews_collected": self.reviews_collected,
            "errors": self.errors,
            "skipped_404": self.skipped_404,
        }


async def collect_reviews(
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    paths: PathManager,
    rate_limiter: AdaptiveRateLimiter | None,
    config: Config,
    pr_numbers_by_repo: dict[str, list[int]] | None = None,
    checkpoint: CheckpointManager | None = None,
) -> dict[str, int]:
    """Collect reviews for all PRs across repositories.

    Args:
        repos: List of repository metadata dictionaries.
        rest_client: REST API client for GitHub.
        paths: Path manager for storage locations.
        rate_limiter: Rate limiter for API throttling (unused, passed for consistency).
        config: Application configuration.
        pr_numbers_by_repo: Optional dict mapping repo full_name to list of PR numbers.
            If not provided, will read from raw PR JSONL files.
        checkpoint: Optional CheckpointManager for resume support.

    Returns:
        Dictionary with collection statistics.

    Note:
        - Uses RestClient.list_reviews() which handles pagination automatically
        - Respects rate limiting through RestClient's integration
        - Handles 404s gracefully (missing PRs/repos)
        - Logs progress at DEBUG level due to high request volume
        - Supports checkpoint-based resume to skip already completed repos
    """
    stats = ReviewCollectionStats()

    logger.info("Starting review collection for %d repositories", len(repos))

    # If PR numbers not provided, extract from raw PR files
    if pr_numbers_by_repo is None:
        logger.info("Extracting PR numbers from raw PR files")
        pr_numbers_by_repo = await _extract_pr_numbers_from_files(repos, paths)
        logger.info(
            "Extracted PR numbers for %d repositories",
            len(pr_numbers_by_repo),
        )

    # Count total PRs to process
    total_prs = sum(len(pr_nums) for pr_nums in pr_numbers_by_repo.values())
    logger.info("Total PRs to process: %d", total_prs)

    # Collect reviews for each repo
    for repo in repos:
        repo_full_name = repo["full_name"]
        pr_numbers = pr_numbers_by_repo.get(repo_full_name, [])

        if not pr_numbers:
            logger.debug("No PRs to process for %s, skipping", repo_full_name)
            continue

        # Check if already complete via checkpoint
        if checkpoint and checkpoint.is_repo_endpoint_complete(repo_full_name, "reviews"):
            logger.debug("Skipping %s - reviews already complete", repo_full_name)
            stats.repos_resumed += 1
            continue

        logger.info(
            "Collecting reviews for %s (%d PRs)",
            repo_full_name,
            len(pr_numbers),
        )

        # Mark as in progress
        if checkpoint:
            checkpoint.mark_repo_endpoint_in_progress(repo_full_name, "reviews")

        try:
            repo_stats = await _collect_reviews_for_repo(
                repo_full_name,
                pr_numbers,
                rest_client,
                paths,
            )

            stats.repos_processed += 1
            stats.prs_processed += repo_stats["prs_processed"]
            stats.reviews_collected += repo_stats["reviews_collected"]
            stats.errors += repo_stats["errors"]
            stats.skipped_404 += repo_stats["skipped_404"]

            # Mark as complete
            if checkpoint:
                checkpoint.mark_repo_endpoint_complete(repo_full_name, "reviews")

            logger.info(
                "Completed %s: %d PRs, %d reviews, %d errors, %d skipped (404)",
                repo_full_name,
                repo_stats["prs_processed"],
                repo_stats["reviews_collected"],
                repo_stats["errors"],
                repo_stats["skipped_404"],
            )

        except Exception as e:
            logger.error(
                "Failed to collect reviews for %s: %s",
                repo_full_name,
                e,
                exc_info=True,
            )
            stats.repos_skipped += 1

            # Mark as failed
            if checkpoint:
                checkpoint.mark_repo_endpoint_failed(
                    repo_full_name, "reviews", str(e), retryable=True
                )
            continue

    logger.info(
        "Review collection complete: %d repos processed, %d PRs processed, %d reviews collected, "
        "%d errors, %d skipped (404), %d resumed from checkpoint",
        stats.repos_processed,
        stats.prs_processed,
        stats.reviews_collected,
        stats.errors,
        stats.skipped_404,
        stats.repos_resumed,
    )

    return stats.to_dict()


async def _collect_reviews_for_repo(
    repo_full_name: str,
    pr_numbers: list[int],
    rest_client: RestClient,
    paths: PathManager,
) -> dict[str, int]:
    """Collect reviews for a single repository.

    Args:
        repo_full_name: Repository full name (owner/repo).
        pr_numbers: List of PR numbers to collect reviews for.
        rest_client: REST API client.
        paths: Path manager for storage.

    Returns:
        Dictionary with collection stats for this repo.
    """
    stats = ReviewCollectionStats()
    owner, repo = repo_full_name.split("/")
    output_path = paths.reviews_raw_path(repo_full_name)

    async with AsyncJSONLWriter(output_path) as writer:
        for pr_number in pr_numbers:
            try:
                logger.debug(
                    "Fetching reviews for %s#%d",
                    repo_full_name,
                    pr_number,
                )

                review_count = 0
                async for reviews, metadata in rest_client.list_reviews(
                    owner,
                    repo,
                    pr_number,
                ):
                    # Write each review individually
                    for review in reviews:
                        await writer.write(
                            source="github_rest",
                            endpoint=f"/repos/{owner}/{repo}/pulls/{pr_number}/reviews",
                            data=review,
                            page=metadata["page"],
                        )
                        review_count += 1

                stats.prs_processed += 1
                stats.reviews_collected += review_count

                if review_count > 0:
                    logger.debug(
                        "Collected %d reviews for %s#%d",
                        review_count,
                        repo_full_name,
                        pr_number,
                    )

            except Exception as e:
                # Check if it's a 404 (PR not found or no access)
                error_msg = str(e).lower()
                if "404" in error_msg or "not found" in error_msg:
                    logger.debug(
                        "PR not found or no access: %s#%d",
                        repo_full_name,
                        pr_number,
                    )
                    stats.skipped_404 += 1
                    stats.prs_processed += 1
                else:
                    logger.error(
                        "Error fetching reviews for %s#%d: %s",
                        repo_full_name,
                        pr_number,
                        e,
                        exc_info=True,
                    )
                    stats.errors += 1

    return stats.to_dict()


async def _extract_pr_numbers_from_files(
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
            logger.debug("No PR file found for %s", repo_full_name)
            continue

        pr_numbers = await _read_pr_numbers_from_file(pr_file_path)
        if pr_numbers:
            pr_numbers_by_repo[repo_full_name] = pr_numbers
            logger.debug(
                "Extracted %d PR numbers from %s",
                len(pr_numbers),
                repo_full_name,
            )

    return pr_numbers_by_repo


async def _read_pr_numbers_from_file(pr_file_path: Path) -> list[int]:
    """Read PR numbers from a JSONL file.

    Args:
        pr_file_path: Path to PR JSONL file.

    Returns:
        List of PR numbers (sorted, deduplicated).
    """
    pr_numbers = set()

    try:
        with pr_file_path.open() as f:
            for line in f:
                try:
                    record = json.loads(line)
                    # Extract PR number from enveloped data
                    pr_data = record.get("data", {})
                    pr_number = pr_data.get("number")
                    if pr_number is not None:
                        pr_numbers.add(pr_number)
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(
                        "Failed to parse PR record from %s: %s",
                        pr_file_path,
                        e,
                    )
                    continue

    except Exception as e:
        logger.error(
            "Error reading PR file %s: %s",
            pr_file_path,
            e,
            exc_info=True,
        )
        return []

    # Return sorted list for deterministic processing
    return sorted(pr_numbers)


async def collect_reviews_from_pr_iterator(
    pr_iterator: AsyncIterator[tuple[str, int]],
    rest_client: RestClient,
    paths: PathManager,
) -> dict[str, int]:
    """Collect reviews from an async iterator of (repo_full_name, pr_number) pairs.

    Alternative interface for cases where PR data is streamed.

    Args:
        pr_iterator: Async iterator yielding (repo_full_name, pr_number) tuples.
        rest_client: REST API client.
        paths: Path manager for storage.

    Returns:
        Dictionary with collection statistics.
    """
    stats = ReviewCollectionStats()
    writers: dict[str, AsyncJSONLWriter] = {}

    try:
        async for repo_full_name, pr_number in pr_iterator:
            # Get or create writer for this repo
            if repo_full_name not in writers:
                output_path = paths.reviews_raw_path(repo_full_name)
                writer = AsyncJSONLWriter(output_path)
                await writer.open()
                writers[repo_full_name] = writer

            writer = writers[repo_full_name]
            owner, repo = repo_full_name.split("/")

            try:
                logger.debug(
                    "Fetching reviews for %s#%d",
                    repo_full_name,
                    pr_number,
                )

                review_count = 0
                async for reviews, metadata in rest_client.list_reviews(
                    owner,
                    repo,
                    pr_number,
                ):
                    for review in reviews:
                        await writer.write(
                            source="github_rest",
                            endpoint=f"/repos/{owner}/{repo}/pulls/{pr_number}/reviews",
                            data=review,
                            page=metadata["page"],
                        )
                        review_count += 1

                stats.prs_processed += 1
                stats.reviews_collected += review_count

            except Exception as e:
                error_msg = str(e).lower()
                if "404" in error_msg or "not found" in error_msg:
                    logger.debug(
                        "PR not found or no access: %s#%d",
                        repo_full_name,
                        pr_number,
                    )
                    stats.skipped_404 += 1
                    stats.prs_processed += 1
                else:
                    logger.error(
                        "Error fetching reviews for %s#%d: %s",
                        repo_full_name,
                        pr_number,
                        e,
                        exc_info=True,
                    )
                    stats.errors += 1

    finally:
        # Close all writers
        for writer in writers.values():
            await writer.close()

    logger.info(
        "Review collection complete: %d PRs processed, %d reviews collected, %d errors, %d skipped",
        stats.prs_processed,
        stats.reviews_collected,
        stats.errors,
        stats.skipped_404,
    )

    return stats.to_dict()
