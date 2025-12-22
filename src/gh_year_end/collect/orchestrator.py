"""Collection orchestrator for GitHub data collection.

Coordinates the execution of all collectors in the correct order,
manages clients and rate limiting, and aggregates statistics.
Supports checkpoint-based resume for long-running collections.
"""

import asyncio
import json
import logging
import os
from collections.abc import Callable
from datetime import datetime
from typing import Any, cast

from gh_year_end.collect.aggregator import MetricsAggregator
from gh_year_end.collect.discovery import discover_repos
from gh_year_end.collect.phases import (
    run_branch_protection_phase,
    run_comments_phase,
    run_commits_phase,
    run_discovery_phase,
    run_issues_phase,
    run_pulls_phase,
    run_repo_metadata_phase,
    run_reviews_phase,
    run_security_features_phase,
)
from gh_year_end.collect.progress import ProgressTracker
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


async def _collect_repos_parallel(
    repos: list[dict[str, Any]],
    collect_fn: Callable[..., Any],
    endpoint_name: str,
    checkpoint: CheckpointManager | None,
    max_concurrency: int,
    **kwargs: Any,
) -> dict[str, Any]:
    """Process multiple repos in parallel with semaphore control.

    Args:
        repos: List of repo dicts to process.
        collect_fn: Async function that takes a single repo and returns stats.
            The function is responsible for checkpoint management.
        endpoint_name: Name of endpoint for checkpoint tracking.
        checkpoint: Checkpoint manager for resume support.
        max_concurrency: Maximum concurrent repos to process.
        **kwargs: Additional args to pass to collect_fn.

    Returns:
        Aggregated stats dict with keys matching the individual collection functions
        (e.g., pulls_collected, repos_processed, errors, etc.).
    """
    semaphore = asyncio.Semaphore(max_concurrency)

    async def process_repo(repo: dict[str, Any]) -> dict[str, Any]:
        repo_full_name = repo["full_name"]

        # Skip if already complete
        if checkpoint and checkpoint.is_repo_endpoint_complete(repo_full_name, endpoint_name):
            logger.debug("Skipping %s - %s already complete", repo_full_name, endpoint_name)
            return {"skipped": True, "repo": repo_full_name}

        async with semaphore:
            try:
                # Call the collection function - it handles checkpoint management
                result = await collect_fn(repo, **kwargs)
                return {"success": True, "repo": repo_full_name, **result}
            except Exception as e:
                logger.error(
                    "Error collecting %s for %s: %s",
                    endpoint_name,
                    repo_full_name,
                    e,
                )
                # Mark as failed in checkpoint if not already done
                if checkpoint:
                    checkpoint.mark_repo_endpoint_failed(
                        repo_full_name,
                        endpoint_name,
                        str(e),
                        retryable=True,
                    )
                return {"error": True, "repo": repo_full_name, "message": str(e)}

    # Create tasks for all repos
    tasks = [process_repo(repo) for repo in repos]

    # Execute in parallel
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Aggregate results
    stats: dict[str, Any] = {
        "repos_processed": 0,
        "repos_skipped": 0,
        "repos_errored": 0,
        "errors": [],
    }

    # Track collection-specific metrics (e.g., pulls_collected, issues_collected)
    collection_metrics: dict[str, Any] = {}

    for result in results:
        if isinstance(result, Exception):
            stats["repos_errored"] += 1
            stats["errors"].append(str(result))
        elif isinstance(result, dict):
            if result.get("skipped"):
                stats["repos_skipped"] += 1
            elif result.get("error"):
                stats["repos_errored"] += 1
                stats["errors"].append(result.get("message", "Unknown error"))
            else:
                stats["repos_processed"] += 1
                # Aggregate collection-specific metrics
                for key, value in result.items():
                    if key not in ("success", "repo") and isinstance(value, (int, float)):
                        collection_metrics[key] = collection_metrics.get(key, 0) + value

    # Merge collection metrics into stats
    stats.update(collection_metrics)

    return stats


class CollectionError(Exception):
    """Raised when collection orchestration fails."""


async def run_collection(
    config: Config,
    force: bool = False,
    resume: bool = False,
    from_repo: str | None = None,
    retry_failed: bool = False,
    verbose: bool = False,
    quiet: bool = False,
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
        verbose: Enable detailed logging output.
        quiet: Minimal output mode (no progress display).

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

    # Initialize progress tracker
    progress = ProgressTracker(
        verbose=verbose,
        quiet=quiet,
        rate_limiter=rate_limiter,
    )

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
        # Start progress display
        progress.start()

        # Step 1: Discovery
        repos, stats["discovery"] = await run_discovery_phase(
            config=config,
            http_client=http_client,
            paths=paths,
            checkpoint=checkpoint,
            progress=progress,
        )

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
        stats["repos"] = await run_repo_metadata_phase(
            config=config,
            repos=repos,
            graphql_client=graphql_client,
            rate_limiter=rate_limiter,
            paths=paths,
            checkpoint=checkpoint,
            progress=progress,
        )

        # Step 3: Pull Requests
        stats["pulls"] = await run_pulls_phase(
            config=config,
            repos=repos,
            rest_client=rest_client,
            paths=paths,
            checkpoint=checkpoint,
            progress=progress,
            collect_repos_parallel=_collect_repos_parallel,
        )

        # Step 4: Issues
        stats["issues"] = await run_issues_phase(
            config=config,
            repos=repos,
            rest_client=rest_client,
            rate_limiter=rate_limiter,
            paths=paths,
            checkpoint=checkpoint,
            progress=progress,
        )

        # Step 5: Reviews
        stats["reviews"] = await run_reviews_phase(
            config=config,
            repos=repos,
            rest_client=rest_client,
            rate_limiter=rate_limiter,
            paths=paths,
            checkpoint=checkpoint,
            progress=progress,
        )

        # Step 6: Comments
        stats["comments"] = await run_comments_phase(
            config=config,
            repos=repos,
            rest_client=rest_client,
            rate_limiter=rate_limiter,
            paths=paths,
            checkpoint=checkpoint,
            progress=progress,
        )

        # Step 7: Commits
        stats["commits"] = await run_commits_phase(
            config=config,
            repos=repos,
            rest_client=rest_client,
            rate_limiter=rate_limiter,
            paths=paths,
            checkpoint=checkpoint,
            progress=progress,
        )

        # Step 8: Hygiene - Branch Protection
        stats["hygiene"] = await run_branch_protection_phase(
            config=config,
            repos=repos,
            rest_client=rest_client,
            paths=paths,
            checkpoint=checkpoint,
            progress=progress,
        )

        # Step 9: Security Features
        stats["security_features"] = await run_security_features_phase(
            config=config,
            repos=repos,
            rest_client=rest_client,
            paths=paths,
            checkpoint=checkpoint,
            progress=progress,
        )

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
        # Stop progress display
        progress.stop()
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


async def _collect_repo_hygiene_inline(
    repo: dict[str, Any],
    owner: str,
    repo_name: str,
    rest_client: RestClient,
    config: Config,
) -> dict[str, Any]:
    """Collect comprehensive hygiene data for a repository inline.

    Args:
        repo: Repository metadata dict.
        owner: Repository owner.
        repo_name: Repository name.
        rest_client: REST client for API calls.
        config: Application configuration.

    Returns:
        Dictionary with hygiene data including file presence, branch protection, and security features.
    """
    repo_full_name = f"{owner}/{repo_name}"
    default_branch = repo.get("default_branch", "main")

    hygiene_data: dict[str, Any] = {
        "repo": repo_full_name,
        "default_branch": default_branch,
        "protected": False,
        "branch_protection_enabled": False,
        "has_readme": False,
        "has_security_md": False,
        "has_codeowners": False,
        "has_contributing": False,
        "has_license": False,
        "has_ci_workflows": False,
        "dependabot_enabled": False,
        "secret_scanning_enabled": False,
        "score": 0,
    }

    # Check branch protection
    try:
        protection_data, status_code = await rest_client.get_branch_protection(
            owner=owner,
            repo=repo_name,
            branch=default_branch,
        )
        if status_code == 200 and protection_data:
            hygiene_data["protected"] = True
            hygiene_data["branch_protection_enabled"] = True
            hygiene_data["protection"] = protection_data
    except Exception as e:
        logger.debug("Error checking branch protection for %s: %s", repo_full_name, e)

    # Fetch repository tree to check file presence
    try:
        tree_data = await rest_client.get_repository_tree(
            owner=owner,
            repo=repo_name,
            tree_sha=default_branch,
            recursive=True,
        )

        if tree_data and "tree" in tree_data:
            tree_entries = tree_data.get("tree", [])
            paths = {entry.get("path", ""): entry for entry in tree_entries}

            # Check for key hygiene files
            hygiene_data["has_readme"] = any(
                p.upper() == "README.MD" or p.upper() == "README" for p in paths
            )
            hygiene_data["has_security_md"] = (
                "SECURITY.md" in paths or "security.md" in paths or ".github/SECURITY.md" in paths
            )
            hygiene_data["has_codeowners"] = (
                "CODEOWNERS" in paths or ".github/CODEOWNERS" in paths or "docs/CODEOWNERS" in paths
            )
            hygiene_data["has_contributing"] = (
                "CONTRIBUTING.md" in paths
                or "contributing.md" in paths
                or ".github/CONTRIBUTING.md" in paths
            )
            hygiene_data["has_license"] = any(p.upper().startswith("LICENSE") for p in paths)

            # Check for CI workflows
            ci_workflow_paths = [
                ".github/workflows/",
                ".gitlab-ci.yml",
                "circle.yml",
                ".circleci/config.yml",
                ".travis.yml",
                "Jenkinsfile",
            ]
            hygiene_data["has_ci_workflows"] = any(
                any(p.startswith(prefix) for prefix in ci_workflow_paths if "/" in prefix)
                or p in ci_workflow_paths
                for p in paths
            )

    except Exception as e:
        logger.debug("Error fetching repository tree for %s: %s", repo_full_name, e)

    # Check security features (Dependabot, secret scanning)
    try:
        repo_data = await rest_client.get_repo_security_analysis(owner, repo_name)
        if repo_data:
            security_analysis = repo_data.get("security_and_analysis", {})

            # Check Dependabot
            dependabot_alerts = security_analysis.get("dependabot_security_updates", {})
            hygiene_data["dependabot_enabled"] = dependabot_alerts.get("status") == "enabled"

            # Check secret scanning
            secret_scanning = security_analysis.get("secret_scanning", {})
            hygiene_data["secret_scanning_enabled"] = secret_scanning.get("status") == "enabled"
    except Exception as e:
        logger.debug("Error checking security features for %s: %s", repo_full_name, e)

    # Calculate hygiene score (0-100)
    score = 0

    # Branch protection (25 points)
    if hygiene_data["branch_protection_enabled"]:
        score += 25

    # Security features (25 points)
    if hygiene_data["has_security_md"]:
        score += 10
    if hygiene_data["dependabot_enabled"]:
        score += 8
    if hygiene_data["secret_scanning_enabled"]:
        score += 7

    # Documentation (25 points)
    if hygiene_data["has_readme"]:
        score += 15
    if hygiene_data["has_contributing"]:
        score += 5
    if hygiene_data["has_license"]:
        score += 5

    # Code ownership and CI (25 points)
    if hygiene_data["has_codeowners"]:
        score += 10
    if hygiene_data["has_ci_workflows"]:
        score += 15

    hygiene_data["score"] = score

    return hygiene_data


async def collect_and_aggregate(
    config: Config,
    force: bool = False,
    verbose: bool = False,
    quiet: bool = False,
) -> dict[str, Any]:
    """Single-pass collection with inline metric aggregation.

    This function replaces the separate collect, normalize, and metrics phases
    with a single pass that aggregates metrics during collection. No raw JSONL
    files are written - metrics are computed in-memory and returned directly.

    Args:
        config: Application configuration.
        force: Force re-collection even if cached data exists.
        verbose: Enable detailed logging output.
        quiet: Minimal output mode (no progress display).

    Returns:
        Dictionary containing all metrics in the format expected by the website:
        {
            'summary': {...},
            'leaderboards': {...},
            'timeseries': {...},
            'repo_health': [...],
            'hygiene_scores': {...},
            'awards': {...}
        }

    Raises:
        CollectionError: If critical collection failure occurs.
    """
    start_time = datetime.now()

    logger.info(
        "Starting single-pass collection with metric aggregation for %s", config.github.target.name
    )
    logger.info(
        "Year: %d, Date range: %s to %s",
        config.github.windows.year,
        config.github.windows.since.isoformat(),
        config.github.windows.until.isoformat(),
    )

    # Initialize MetricsAggregator
    aggregator = MetricsAggregator(
        year=config.github.windows.year,
        target_name=config.github.target.name,
        target_mode=config.github.target.mode,
    )

    # Initialize auth and clients
    token = os.getenv(config.github.auth.token_env)
    if not token:
        msg = f"GitHub token not found in environment variable {config.github.auth.token_env}"
        raise CollectionError(msg)

    auth = GitHubAuth(token=token)
    http_client = GitHubClient(auth=auth)
    rate_limiter = AdaptiveRateLimiter(config.rate_limit)
    rest_client = RestClient(http_client, rate_limiter)

    # Initialize progress tracker (simplified - no checkpoint tracking)
    progress = ProgressTracker(
        verbose=verbose,
        quiet=quiet,
        rate_limiter=rate_limiter,
    )

    try:
        # Start progress display
        progress.start()

        # Step 1: Discovery
        logger.info("=" * 80)
        logger.info("STEP 1: Repository Discovery")
        logger.info("=" * 80)
        progress.set_phase("discovery")

        # Discover repos using existing discovery module
        # Note: We still need PathManager for discovery, but we won't write JSONL
        paths = PathManager(config)
        paths.ensure_directories()

        repos = await discover_repos(config, http_client, paths)
        progress.set_total_repos(len(repos))
        progress.mark_phase_complete("discovery")

        logger.info("Discovery complete: %d repos discovered", len(repos))

        if not repos:
            logger.warning("No repositories discovered. Check target configuration.")
            return aggregator.export()

        # Step 2: Collect PRs, Issues, Reviews with inline aggregation
        logger.info("=" * 80)
        logger.info("STEP 2: Data Collection with Metric Aggregation")
        logger.info("=" * 80)
        progress.set_phase("collection")

        total_prs = 0
        total_issues = 0
        total_reviews = 0
        total_comments = 0

        for idx, repo in enumerate(repos, 1):
            repo_full_name = repo["full_name"]
            owner, repo_name = repo_full_name.split("/", 1)

            logger.info("[%d/%d] Processing %s", idx, len(repos), repo_full_name)

            # Add repo to aggregator
            aggregator.add_repo(repo)

            # Track PR numbers for comment collection
            pr_numbers = []
            # Track issue numbers for comment collection
            issue_numbers = []

            try:
                # Collect PRs
                if config.collection.enable.pulls:
                    logger.debug("  Collecting PRs...")
                    async for prs_page, _metadata in rest_client.list_pulls(
                        owner=owner,
                        repo=repo_name,
                        state="all",
                    ):
                        for pr in prs_page:
                            # Apply date filter
                            created_at = pr.get("created_at")
                            if created_at:
                                created_dt = datetime.fromisoformat(
                                    created_at.replace("Z", "+00:00")
                                )
                                if (
                                    config.github.windows.since
                                    <= created_dt
                                    < config.github.windows.until
                                ):
                                    aggregator.add_pr(repo_full_name, pr)
                                    pr_numbers.append(pr["number"])
                                    total_prs += 1

                                    # Collect reviews for this PR
                                    if config.collection.enable.reviews:
                                        async for (
                                            reviews_page,
                                            _review_meta,
                                        ) in rest_client.list_reviews(
                                            owner=owner,
                                            repo=repo_name,
                                            pull_number=pr["number"],
                                        ):
                                            for review in reviews_page:
                                                aggregator.add_review(
                                                    repo_full_name, pr["number"], review
                                                )
                                                total_reviews += 1

                # Collect issues
                if config.collection.enable.issues:
                    logger.debug("  Collecting issues...")
                    async for issues_page, _metadata in rest_client.list_issues(
                        owner=owner,
                        repo=repo_name,
                        state="all",
                    ):
                        for issue in issues_page:
                            # Skip PRs (GitHub issues API includes PRs)
                            if "pull_request" in issue:
                                continue

                            # Apply date filter
                            created_at = issue.get("created_at")
                            if created_at:
                                created_dt = datetime.fromisoformat(
                                    created_at.replace("Z", "+00:00")
                                )
                                if (
                                    config.github.windows.since
                                    <= created_dt
                                    < config.github.windows.until
                                ):
                                    aggregator.add_issue(repo_full_name, issue)
                                    issue_numbers.append(issue["number"])
                                    total_issues += 1

                # Collect comments
                if config.collection.enable.comments:
                    # Collect issue comments
                    if issue_numbers:
                        logger.debug(
                            "  Collecting issue comments for %d issues...", len(issue_numbers)
                        )
                        for issue_number in issue_numbers:
                            async for comments_page, _metadata in rest_client.list_issue_comments(
                                owner=owner,
                                repo=repo_name,
                                issue_number=issue_number,
                            ):
                                for comment in comments_page:
                                    aggregator.add_comment(
                                        repo_full_name, comment, comment_type="issue"
                                    )
                                    total_comments += 1

                    # Collect review comments (inline PR comments)
                    if pr_numbers:
                        logger.debug("  Collecting review comments for %d PRs...", len(pr_numbers))
                        for pr_number in pr_numbers:
                            async for comments_page, _metadata in rest_client.list_review_comments(
                                owner=owner,
                                repo=repo_name,
                                pull_number=pr_number,
                            ):
                                for comment in comments_page:
                                    aggregator.add_comment(
                                        repo_full_name, comment, comment_type="review"
                                    )
                                    total_comments += 1

                # Collect hygiene data
                if config.collection.enable.hygiene:
                    logger.debug("  Collecting hygiene data...")
                    hygiene_data = await _collect_repo_hygiene_inline(
                        repo=repo,
                        owner=owner,
                        repo_name=repo_name,
                        rest_client=rest_client,
                        config=config,
                    )
                    aggregator.set_hygiene(repo_full_name, hygiene_data)

                logger.info(
                    "  Processed: %d PRs, %d issues, %d reviews, %d comments",
                    total_prs,
                    total_issues,
                    total_reviews,
                    total_comments,
                )

            except Exception as e:
                logger.error("Error processing %s: %s", repo_full_name, e)
                continue

        progress.mark_phase_complete("collection")

        logger.info("=" * 80)
        logger.info("COLLECTION COMPLETE")
        logger.info("=" * 80)
        logger.info("Total PRs: %d", total_prs)
        logger.info("Total issues: %d", total_issues)
        logger.info("Total reviews: %d", total_reviews)
        logger.info("Total comments: %d", total_comments)

        # Export aggregated metrics
        metrics = aggregator.export()

        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info("Total duration: %.2f seconds (%.2f minutes)", duration, duration / 60)

        return metrics

    finally:
        # Stop progress display
        progress.stop()
        # Cleanup clients
        await http_client.close()
