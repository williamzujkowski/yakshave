"""Collection orchestrator for GitHub data collection.

Coordinates the execution of all collectors in the correct order,
manages clients and rate limiting, and aggregates statistics.
Supports checkpoint-based resume for long-running collections.
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, cast

from gh_year_end.collect.comments import collect_issue_comments, collect_review_comments
from gh_year_end.collect.commits import collect_commits
from gh_year_end.collect.discovery import discover_repos
from gh_year_end.collect.hygiene import collect_branch_protection, collect_security_features
from gh_year_end.collect.issues import collect_issues
from gh_year_end.collect.pulls import collect_pulls
from gh_year_end.collect.repos import collect_repo_metadata
from gh_year_end.collect.reviews import collect_reviews
from gh_year_end.config import Config
from gh_year_end.github.auth import GitHubAuth
from gh_year_end.github.graphql import GraphQLClient
from gh_year_end.github.http import GitHubClient
from gh_year_end.github.ratelimit import AdaptiveRateLimiter
from gh_year_end.github.rest import RestClient
from gh_year_end.storage.checkpoint import CheckpointManager
from gh_year_end.storage.paths import PathManager
from gh_year_end.storage.writer import AsyncJSONLWriter

logger = logging.getLogger(__name__)


class CollectionError(Exception):
    """Raised when collection orchestration fails."""


async def run_collection(
    config: Config,
    force: bool = False,
    resume: bool = False,
    from_repo: str | None = None,
    retry_failed: bool = False,
) -> dict[str, Any]:
    """Run complete data collection pipeline.

    Executes all collectors in the correct order:
    1. Discovery - find all repos
    2. Repos - collect detailed repo metadata
    3. Pulls - collect pull requests
    4. Issues - collect issues
    5. Reviews - collect PR reviews
    6. Comments - collect issue and review comments
    7. Commits - collect commit history

    Args:
        config: Application configuration.
        force: If True, re-fetch data even if raw files exist.
        resume: If True, require existing checkpoint (fail if none exists).
        from_repo: Resume starting from specific repo (e.g., 'owner/repo').
        retry_failed: If True, only retry repos marked as failed.

    Returns:
        Dictionary with aggregated statistics from all collectors:
            - discovery: Discovery stats
            - repos: Repo metadata stats
            - pulls: Pull request stats
            - issues: Issue stats
            - reviews: Review stats
            - comments: Comment stats
            - commits: Commit stats
            - duration_seconds: Total execution time
            - rate_limit_samples: List of rate limit samples

    Raises:
        CollectionError: If critical collection failure occurs.
    """
    start_time = datetime.now()

    logger.info("Starting collection orchestration for %s", config.github.target.name)
    logger.info(
        "Year: %d, Date range: %s to %s",
        config.github.windows.year,
        config.github.windows.since.isoformat(),
        config.github.windows.until.isoformat(),
    )

    # Initialize paths and ensure directories exist
    paths = PathManager(config)
    paths.ensure_directories()

    # Initialize checkpoint manager
    checkpoint = CheckpointManager(paths.checkpoint_path)

    # Handle checkpoint logic based on flags
    if force:
        # Force mode: delete existing checkpoint and start fresh
        logger.info("Force mode: deleting existing checkpoint if present")
        checkpoint.delete_if_exists()
        checkpoint.create_new(config)
    elif resume:
        # Resume mode: require existing checkpoint
        if not checkpoint.exists():
            msg = "Resume requested but no checkpoint found. Start fresh collection first."
            raise CollectionError(msg)
        checkpoint.load()
        if not checkpoint.validate_config(config):
            msg = "Config has changed since checkpoint was created. Use --force to restart."
            raise CollectionError(msg)
        logger.info("Resuming from checkpoint")
        checkpoint_stats = checkpoint.get_stats()
        logger.info(
            "Checkpoint status: %d/%d repos complete, %d in progress, %d failed",
            checkpoint_stats["repos_complete"],
            checkpoint_stats["total_repos"],
            checkpoint_stats["repos_in_progress"],
            checkpoint_stats["repos_failed"],
        )
    elif checkpoint.exists():
        # Check if collection is already complete
        checkpoint.load()
        if checkpoint.validate_config(config):
            checkpoint_stats = checkpoint.get_stats()
            if checkpoint_stats["repos_complete"] == checkpoint_stats["total_repos"]:
                logger.info("Collection already complete. Use --force to re-collect.")
                if paths.manifest_path.exists():
                    with paths.manifest_path.open() as f:
                        existing_manifest = json.load(f)
                    return cast("dict[str, Any]", existing_manifest.get("stats", {}))
            # Auto-resume if partial progress exists
            logger.info("Found partial checkpoint, auto-resuming")
            resume = True
        else:
            logger.warning("Config changed, starting fresh collection")
            checkpoint.delete_if_exists()
            checkpoint.create_new(config)
    else:
        # No checkpoint, check if manifest exists
        if paths.manifest_path.exists():
            logger.warning(
                "Collection manifest exists at %s. Use --force to re-collect.",
                paths.manifest_path,
            )
            logger.info("Loading existing manifest...")
            with paths.manifest_path.open() as f:
                existing_manifest = json.load(f)
            return cast("dict[str, Any]", existing_manifest.get("stats", {}))
        # Fresh start
        checkpoint.create_new(config)

    # Install signal handlers for graceful shutdown
    checkpoint.install_signal_handlers()

    # Initialize auth and clients
    token = os.getenv(config.github.auth.token_env)
    if not token:
        msg = f"GitHub token not found in environment variable {config.github.auth.token_env}"
        raise CollectionError(msg)

    auth = GitHubAuth(token=token)
    http_client = GitHubClient(auth=auth)
    rate_limiter = AdaptiveRateLimiter(config.rate_limit)
    rest_client = RestClient(http_client, rate_limiter)
    graphql_client = GraphQLClient(http_client, rate_limiter)

    # Initialize stats collector
    stats: dict[str, Any] = {
        "discovery": {},
        "repos": {},
        "pulls": {},
        "issues": {},
        "reviews": {},
        "comments": {},
        "commits": {},
        "hygiene": {},
        "duration_seconds": 0.0,
        "rate_limit_samples": [],
    }

    try:
        # Step 1: Discovery
        logger.info("=" * 80)
        logger.info("STEP 1: Repository Discovery")
        logger.info("=" * 80)

        if checkpoint.is_phase_complete("discovery"):
            logger.info("Discovery phase already complete, loading repos from checkpoint")
            # Get repos from checkpoint
            repos_to_process = checkpoint.get_repos_to_process(
                retry_failed=retry_failed, from_repo=from_repo
            )
            # Load repo metadata from existing discovery file
            repos = []
            if paths.repos_raw_path.exists():
                with paths.repos_raw_path.open() as f:
                    for line in f:
                        record = json.loads(line)
                        repos.append(record.get("data", {}))
            stats["discovery"] = {"repos_discovered": len(repos), "skipped": True}
            logger.info("Loaded %d repos from checkpoint", len(repos))
        else:
            checkpoint.set_current_phase("discovery")
            repos = await discover_repos(config, http_client, paths)
            stats["discovery"] = {
                "repos_discovered": len(repos),
            }
            # Register all repos with checkpoint
            checkpoint.update_repos(repos)
            checkpoint.mark_phase_complete("discovery")
            logger.info("Discovery complete: %d repos discovered", len(repos))

        if not repos:
            logger.warning("No repositories discovered. Check target configuration.")
            return stats

        # Get filtered list of repos to process based on resume flags
        repos_to_process_names = checkpoint.get_repos_to_process(
            retry_failed=retry_failed, from_repo=from_repo
        )
        logger.info(
            "Processing %d/%d repos (skipping %d complete)",
            len(repos_to_process_names),
            len(repos),
            len(repos) - len(repos_to_process_names),
        )

        # Step 2: Repo Metadata
        if config.collection.enable.hygiene:
            if checkpoint.is_phase_complete("repo_metadata"):
                logger.info("Repo metadata phase already complete, skipping")
                stats["repos"] = {"repos_processed": len(repos), "skipped": True}
            else:
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
                stats["repos"] = repo_stats
                checkpoint.mark_phase_complete("repo_metadata")
                logger.info(
                    "Repo metadata complete: %d repos processed",
                    repo_stats.get("repos_processed", 0),
                )
        else:
            logger.info("Skipping repo metadata collection (hygiene collection disabled)")
            stats["repos"] = {"repos_processed": 0, "skipped": True}

        # Step 3: Pull Requests
        if config.collection.enable.pulls:
            if checkpoint.is_phase_complete("pulls"):
                logger.info("Pull requests phase already complete, skipping")
                stats["pulls"] = {"pulls_collected": 0, "skipped": True}
            else:
                logger.info("=" * 80)
                logger.info("STEP 3: Pull Request Collection")
                logger.info("=" * 80)
                checkpoint.set_current_phase("pulls")

                pull_stats = await collect_pulls(
                    repos=repos,
                    rest_client=rest_client,
                    paths=paths,
                    config=config,
                    checkpoint=checkpoint,
                )
                stats["pulls"] = pull_stats
                checkpoint.mark_phase_complete("pulls")
                logger.info(
                    "Pull request complete: %d PRs collected", pull_stats.get("pulls_collected", 0)
                )
        else:
            logger.info("Skipping pull request collection (disabled in config)")
            stats["pulls"] = {"pulls_collected": 0, "skipped": True}

        # Step 4: Issues
        if config.collection.enable.issues:
            if checkpoint.is_phase_complete("issues"):
                logger.info("Issues phase already complete, skipping")
                stats["issues"] = {"issues_collected": 0, "skipped": True}
            else:
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
                stats["issues"] = issue_stats
                checkpoint.mark_phase_complete("issues")
                logger.info(
                    "Issue collection complete: %d issues collected",
                    issue_stats.get("issues_collected", 0),
                )
        else:
            logger.info("Skipping issue collection (disabled in config)")
            stats["issues"] = {"issues_collected": 0, "skipped": True}

        # Step 5: Reviews
        if config.collection.enable.reviews:
            if checkpoint.is_phase_complete("reviews"):
                logger.info("Reviews phase already complete, skipping")
                stats["reviews"] = {"reviews_collected": 0, "skipped": True}
            else:
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
                stats["reviews"] = review_stats
                checkpoint.mark_phase_complete("reviews")
                logger.info(
                    "Review collection complete: %d reviews collected",
                    review_stats.get("reviews_collected", 0),
                )
        else:
            logger.info("Skipping review collection (disabled in config)")
            stats["reviews"] = {"reviews_collected": 0, "skipped": True}

        # Step 6: Comments (both issue and review comments)
        if config.collection.enable.comments:
            if checkpoint.is_phase_complete("comments"):
                logger.info("Comments phase already complete, skipping")
                stats["comments"] = {"total_comments": 0, "skipped": True}
            else:
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

                stats["comments"] = {
                    "issue_comments": issue_comment_stats,
                    "review_comments": review_comment_stats,
                    "total_comments": (
                        issue_comment_stats.get("comments_collected", 0)
                        + review_comment_stats.get("comments_collected", 0)
                    ),
                }
                checkpoint.mark_phase_complete("comments")
                logger.info(
                    "Comment collection complete: %d total comments collected",
                    stats["comments"]["total_comments"],
                )
        else:
            logger.info("Skipping comment collection (disabled in config)")
            stats["comments"] = {"total_comments": 0, "skipped": True}

        # Step 7: Commits
        if config.collection.enable.commits:
            if checkpoint.is_phase_complete("commits"):
                logger.info("Commits phase already complete, skipping")
                stats["commits"] = {"commits_collected": 0, "skipped": True}
            else:
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
                stats["commits"] = commit_stats
                checkpoint.mark_phase_complete("commits")
                logger.info(
                    "Commit collection complete: %d commits collected",
                    commit_stats.get("commits_collected", 0),
                )
        else:
            logger.info("Skipping commit collection (disabled in config)")
            stats["commits"] = {"commits_collected": 0, "skipped": True}

        # Step 8: Hygiene - Branch Protection
        if config.collection.enable.hygiene:
            if checkpoint.is_phase_complete("branch_protection"):
                logger.info("Branch protection phase already complete, skipping")
                stats["hygiene"] = {"repos_processed": 0, "skipped": True}
            else:
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
                stats["hygiene"] = hygiene_stats
                checkpoint.mark_phase_complete("branch_protection")
                logger.info(
                    "Branch protection collection complete: %d repos processed, "
                    "%d with protection enabled",
                    hygiene_stats.get("repos_processed", 0),
                    hygiene_stats.get("protection_enabled", 0),
                )
        else:
            logger.info("Skipping branch protection collection (hygiene disabled in config)")
            stats["hygiene"] = {"repos_processed": 0, "skipped": True}

        # Step 9: Security Features
        if config.collection.enable.hygiene:
            if checkpoint.is_phase_complete("security_features"):
                logger.info("Security features phase already complete, skipping")
                stats["security_features"] = {"repos_processed": 0, "skipped": True}
            else:
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
                stats["security_features"] = security_features_stats
                checkpoint.mark_phase_complete("security_features")
                logger.info(
                    "Security features collection complete: %d repos processed, "
                    "%d with all features, %d with partial features, %d with no access",
                    security_features_stats.get("repos_processed", 0),
                    security_features_stats.get("repos_with_all_features", 0),
                    security_features_stats.get("repos_with_partial_features", 0),
                    security_features_stats.get("repos_with_no_access", 0),
                )
        else:
            logger.info("Skipping security features collection (hygiene disabled in config)")
            stats["security_features"] = {"repos_processed": 0, "skipped": True}

        # Collect rate limit samples
        stats["rate_limit_samples"] = rate_limiter.get_samples()

        # Write rate limit samples to JSONL
        if stats["rate_limit_samples"]:
            logger.info(
                "Writing %d rate limit samples to storage", len(stats["rate_limit_samples"])
            )
            async with AsyncJSONLWriter(paths.rate_limit_samples_path) as writer:
                for sample in stats["rate_limit_samples"]:
                    await writer.write(
                        source="github_rest",
                        endpoint="rate_limit_samples",
                        data=sample,
                    )

    finally:
        # Cleanup clients
        await http_client.close()

    # Calculate duration
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    stats["duration_seconds"] = round(duration, 2)

    logger.info("=" * 80)
    logger.info("COLLECTION COMPLETE")
    logger.info("=" * 80)
    logger.info("Total duration: %.2f seconds (%.2f minutes)", duration, duration / 60)
    logger.info("Repos discovered: %d", stats["discovery"].get("repos_discovered", 0))
    logger.info("Repos processed: %d", stats["repos"].get("repos_processed", 0))
    logger.info("PRs collected: %d", stats["pulls"].get("pulls_collected", 0))
    logger.info("Issues collected: %d", stats["issues"].get("issues_collected", 0))
    logger.info("Reviews collected: %d", stats["reviews"].get("reviews_collected", 0))
    logger.info("Comments collected: %d", stats["comments"].get("total_comments", 0))
    logger.info("Commits collected: %d", stats["commits"].get("commits_collected", 0))
    logger.info(
        "Hygiene - branch protection: %d repos processed",
        stats["hygiene"].get("repos_processed", 0),
    )
    logger.info(
        "Security features: %d repos processed, %d with all features",
        stats["security_features"].get("repos_processed", 0),
        stats["security_features"].get("repos_with_all_features", 0),
    )

    # Write manifest
    manifest = {
        "collection_date": datetime.now().isoformat(),
        "config": {
            "target": config.github.target.name,
            "year": config.github.windows.year,
            "since": config.github.windows.since.isoformat(),
            "until": config.github.windows.until.isoformat(),
        },
        "stats": stats,
    }

    logger.info("Writing collection manifest to %s", paths.manifest_path)
    with paths.manifest_path.open("w") as f:
        json.dump(manifest, f, indent=2)

    return stats


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
                logger.debug(
                    "Extracted %d issue numbers from %s", len(issue_numbers), repo_full_name
                )

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
