"""Comment collectors for issues and pull requests.

Collects issue comments and review comments (inline code review comments) from
GitHub repositories. Requires pre-collected issues and PRs to extract numbers.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from gh_year_end.config import Config
from gh_year_end.github.ratelimit import AdaptiveRateLimiter
from gh_year_end.github.rest import RestClient
from gh_year_end.storage.paths import PathManager
from gh_year_end.storage.writer import AsyncJSONLWriter

if TYPE_CHECKING:
    from gh_year_end.storage.checkpoint import CheckpointManager

logger = logging.getLogger(__name__)


class CommentCollectionError(Exception):
    """Raised when comment collection fails."""


async def collect_issue_comments(
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    paths: PathManager,
    rate_limiter: AdaptiveRateLimiter,
    config: Config,
    issue_numbers_by_repo: dict[str, list[int]],
    checkpoint: CheckpointManager | None = None,
) -> dict[str, Any]:
    """Collect issue comments for repositories.

    Args:
        repos: List of repository metadata dicts.
        rest_client: RestClient for GitHub API access.
        paths: PathManager for storage paths.
        rate_limiter: AdaptiveRateLimiter for throttling.
        config: Application configuration.
        issue_numbers_by_repo: Dict mapping repo full_name to list of issue numbers.
        checkpoint: Optional CheckpointManager for resume support.

    Returns:
        Dict with collection statistics:
            - repos_processed: Number of repos processed.
            - issues_processed: Number of issues processed.
            - comments_collected: Total comments collected.
            - repos_resumed: Repos skipped because already complete.
            - errors: Number of errors encountered.
    """
    logger.info("Starting issue comment collection for %d repositories", len(repos))

    repos_processed = 0
    issues_processed = 0
    comments_collected = 0
    repos_resumed = 0
    errors = 0

    for repo in repos:
        repo_name = repo["full_name"]
        owner, repo_short = repo_name.split("/", 1)

        # Check if already complete via checkpoint
        if checkpoint and checkpoint.is_repo_endpoint_complete(repo_name, "issue_comments"):
            logger.debug("Skipping %s - issue_comments already complete", repo_name)
            repos_resumed += 1
            continue

        # Get issue numbers for this repo
        issue_numbers = issue_numbers_by_repo.get(repo_name, [])
        if not issue_numbers:
            logger.debug("No issues found for %s, skipping issue comments", repo_name)
            repos_processed += 1
            continue

        logger.info(
            "Collecting issue comments for %s (%d issues)",
            repo_name,
            len(issue_numbers),
        )

        # Mark as in progress
        if checkpoint:
            checkpoint.mark_repo_endpoint_in_progress(repo_name, "issue_comments")

        output_path = paths.issue_comments_raw_path(repo_name)

        try:
            async with AsyncJSONLWriter(output_path) as writer:
                for issue_number in issue_numbers:
                    try:
                        issue_comments = 0
                        async for comments_page, metadata in rest_client.list_issue_comments(
                            owner=owner,
                            repo=repo_short,
                            issue_number=issue_number,
                        ):
                            # Write each comment individually
                            for comment in comments_page:
                                await writer.write(
                                    source="github_rest",
                                    endpoint=f"/repos/{repo_name}/issues/{issue_number}/comments",
                                    data=comment,
                                    page=metadata["page"],
                                )
                                issue_comments += 1
                                comments_collected += 1

                            logger.debug(
                                "Collected %d comments from %s#%d (page %d)",
                                len(comments_page),
                                repo_name,
                                issue_number,
                                metadata["page"],
                            )

                        if issue_comments > 0:
                            logger.debug(
                                "Collected %d total comments for issue %s#%d",
                                issue_comments,
                                repo_name,
                                issue_number,
                            )

                        issues_processed += 1

                    except Exception as e:
                        logger.error(
                            "Failed to collect comments for issue %s#%d: %s",
                            repo_name,
                            issue_number,
                            e,
                        )
                        errors += 1

            repos_processed += 1

            # Mark as complete
            if checkpoint:
                checkpoint.mark_repo_endpoint_complete(repo_name, "issue_comments")

            logger.info(
                "Completed issue comment collection for %s: %d issues, %d comments",
                repo_name,
                len(issue_numbers),
                comments_collected,
            )

        except Exception as e:
            logger.error("Failed to collect issue comments for %s: %s", repo_name, e)
            errors += 1
            repos_processed += 1

            # Mark as failed
            if checkpoint:
                checkpoint.mark_repo_endpoint_failed(
                    repo_name, "issue_comments", str(e), retryable=True
                )

    logger.info(
        "Issue comment collection complete: %d repos, %d issues, %d comments, "
        "%d resumed from checkpoint, %d errors",
        repos_processed,
        issues_processed,
        comments_collected,
        repos_resumed,
        errors,
    )

    return {
        "repos_processed": repos_processed,
        "issues_processed": issues_processed,
        "comments_collected": comments_collected,
        "repos_resumed": repos_resumed,
        "errors": errors,
    }


async def collect_review_comments(
    repos: list[dict[str, Any]],
    rest_client: RestClient,
    paths: PathManager,
    rate_limiter: AdaptiveRateLimiter,
    config: Config,
    pr_numbers_by_repo: dict[str, list[int]],
    checkpoint: CheckpointManager | None = None,
) -> dict[str, Any]:
    """Collect review comments (inline code review comments) for pull requests.

    Args:
        repos: List of repository metadata dicts.
        rest_client: RestClient for GitHub API access.
        paths: PathManager for storage paths.
        rate_limiter: AdaptiveRateLimiter for throttling.
        config: Application configuration.
        pr_numbers_by_repo: Dict mapping repo full_name to list of PR numbers.
        checkpoint: Optional CheckpointManager for resume support.

    Returns:
        Dict with collection statistics:
            - repos_processed: Number of repos processed.
            - prs_processed: Number of PRs processed.
            - comments_collected: Total review comments collected.
            - repos_resumed: Repos skipped because already complete.
            - errors: Number of errors encountered.
    """
    logger.info("Starting review comment collection for %d repositories", len(repos))

    repos_processed = 0
    prs_processed = 0
    comments_collected = 0
    repos_resumed = 0
    errors = 0

    for repo in repos:
        repo_name = repo["full_name"]
        owner, repo_short = repo_name.split("/", 1)

        # Check if already complete via checkpoint
        if checkpoint and checkpoint.is_repo_endpoint_complete(repo_name, "review_comments"):
            logger.debug("Skipping %s - review_comments already complete", repo_name)
            repos_resumed += 1
            continue

        # Get PR numbers for this repo
        pr_numbers = pr_numbers_by_repo.get(repo_name, [])
        if not pr_numbers:
            logger.debug("No PRs found for %s, skipping review comments", repo_name)
            repos_processed += 1
            continue

        logger.info(
            "Collecting review comments for %s (%d PRs)",
            repo_name,
            len(pr_numbers),
        )

        # Mark as in progress
        if checkpoint:
            checkpoint.mark_repo_endpoint_in_progress(repo_name, "review_comments")

        output_path = paths.review_comments_raw_path(repo_name)

        try:
            async with AsyncJSONLWriter(output_path) as writer:
                for pr_number in pr_numbers:
                    try:
                        pr_comments = 0
                        async for comments_page, metadata in rest_client.list_review_comments(
                            owner=owner,
                            repo=repo_short,
                            pull_number=pr_number,
                        ):
                            # Write each comment individually
                            for comment in comments_page:
                                await writer.write(
                                    source="github_rest",
                                    endpoint=f"/repos/{repo_name}/pulls/{pr_number}/comments",
                                    data=comment,
                                    page=metadata["page"],
                                )
                                pr_comments += 1
                                comments_collected += 1

                            logger.debug(
                                "Collected %d review comments from %s#%d (page %d)",
                                len(comments_page),
                                repo_name,
                                pr_number,
                                metadata["page"],
                            )

                        if pr_comments > 0:
                            logger.debug(
                                "Collected %d total review comments for PR %s#%d",
                                pr_comments,
                                repo_name,
                                pr_number,
                            )

                        prs_processed += 1

                    except Exception as e:
                        logger.error(
                            "Failed to collect review comments for PR %s#%d: %s",
                            repo_name,
                            pr_number,
                            e,
                        )
                        errors += 1

            repos_processed += 1

            # Mark as complete
            if checkpoint:
                checkpoint.mark_repo_endpoint_complete(repo_name, "review_comments")

            logger.info(
                "Completed review comment collection for %s: %d PRs, %d comments",
                repo_name,
                len(pr_numbers),
                comments_collected,
            )

        except Exception as e:
            logger.error("Failed to collect review comments for %s: %s", repo_name, e)
            errors += 1
            repos_processed += 1

            # Mark as failed
            if checkpoint:
                checkpoint.mark_repo_endpoint_failed(
                    repo_name, "review_comments", str(e), retryable=True
                )

    logger.info(
        "Review comment collection complete: %d repos, %d PRs, %d comments, "
        "%d resumed from checkpoint, %d errors",
        repos_processed,
        prs_processed,
        comments_collected,
        repos_resumed,
        errors,
    )

    return {
        "repos_processed": repos_processed,
        "prs_processed": prs_processed,
        "comments_collected": comments_collected,
        "repos_resumed": repos_resumed,
        "errors": errors,
    }


def read_issue_numbers(path: Path) -> dict[str, list[int]]:
    """Read issue numbers from issues JSONL file.

    Extracts issue numbers from enveloped JSONL records for use in comment collection.

    Args:
        path: Path to issues JSONL file (can be a directory or file).

    Returns:
        Dict mapping repo full_name to list of issue numbers.

    Raises:
        FileNotFoundError: If path doesn't exist.
    """
    issue_numbers_by_repo: dict[str, list[int]] = {}

    if not path.exists():
        raise FileNotFoundError(f"Issues path not found: {path}")

    # Handle directory of per-repo files
    if path.is_dir():
        for issues_file in path.glob("*.jsonl"):
            # Extract repo name from filename (e.g., "owner__repo.jsonl")
            repo_name = issues_file.stem.replace("__", "/")
            issue_numbers = _extract_issue_numbers_from_file(issues_file)
            if issue_numbers:
                issue_numbers_by_repo[repo_name] = issue_numbers
                logger.debug(
                    "Found %d issues in %s for repo %s",
                    len(issue_numbers),
                    issues_file.name,
                    repo_name,
                )
    else:
        # Handle single file (extract repo from each record)
        issue_numbers_by_repo = _extract_issue_numbers_by_repo(path)

    logger.info(
        "Loaded issue numbers for %d repositories (total %d issues)",
        len(issue_numbers_by_repo),
        sum(len(nums) for nums in issue_numbers_by_repo.values()),
    )

    return issue_numbers_by_repo


def read_pr_numbers(path: Path) -> dict[str, list[int]]:
    """Read PR numbers from pulls JSONL file.

    Extracts PR numbers from enveloped JSONL records for use in review comment collection.

    Args:
        path: Path to pulls JSONL file (can be a directory or file).

    Returns:
        Dict mapping repo full_name to list of PR numbers.

    Raises:
        FileNotFoundError: If path doesn't exist.
    """
    pr_numbers_by_repo: dict[str, list[int]] = {}

    if not path.exists():
        raise FileNotFoundError(f"Pulls path not found: {path}")

    # Handle directory of per-repo files
    if path.is_dir():
        for pulls_file in path.glob("*.jsonl"):
            # Extract repo name from filename (e.g., "owner__repo.jsonl")
            repo_name = pulls_file.stem.replace("__", "/")
            pr_numbers = _extract_pr_numbers_from_file(pulls_file)
            if pr_numbers:
                pr_numbers_by_repo[repo_name] = pr_numbers
                logger.debug(
                    "Found %d PRs in %s for repo %s",
                    len(pr_numbers),
                    pulls_file.name,
                    repo_name,
                )
    else:
        # Handle single file (extract repo from each record)
        pr_numbers_by_repo = _extract_pr_numbers_by_repo(path)

    logger.info(
        "Loaded PR numbers for %d repositories (total %d PRs)",
        len(pr_numbers_by_repo),
        sum(len(nums) for nums in pr_numbers_by_repo.values()),
    )

    return pr_numbers_by_repo


def _extract_issue_numbers_from_file(file_path: Path) -> list[int]:
    """Extract issue numbers from a single JSONL file.

    Args:
        file_path: Path to JSONL file.

    Returns:
        List of unique issue numbers, sorted.
    """
    issue_numbers = set()

    with file_path.open() as f:
        for line in f:
            try:
                record = json.loads(line)
                # Enveloped format: data is nested
                data = record.get("data", {})
                number = data.get("number")
                if number is not None:
                    issue_numbers.add(int(number))
            except (json.JSONDecodeError, ValueError, KeyError) as e:
                logger.warning("Failed to parse issue record in %s: %s", file_path, e)

    return sorted(issue_numbers)


def _extract_pr_numbers_from_file(file_path: Path) -> list[int]:
    """Extract PR numbers from a single JSONL file.

    Args:
        file_path: Path to JSONL file.

    Returns:
        List of unique PR numbers, sorted.
    """
    pr_numbers = set()

    with file_path.open() as f:
        for line in f:
            try:
                record = json.loads(line)
                # Enveloped format: data is nested
                data = record.get("data", {})
                number = data.get("number")
                if number is not None:
                    pr_numbers.add(int(number))
            except (json.JSONDecodeError, ValueError, KeyError) as e:
                logger.warning("Failed to parse PR record in %s: %s", file_path, e)

    return sorted(pr_numbers)


def _extract_issue_numbers_by_repo(file_path: Path) -> dict[str, list[int]]:
    """Extract issue numbers grouped by repo from a single JSONL file.

    Args:
        file_path: Path to JSONL file.

    Returns:
        Dict mapping repo full_name to list of issue numbers.
    """
    issues_by_repo: dict[str, set[int]] = {}

    with file_path.open() as f:
        for line in f:
            try:
                record = json.loads(line)
                data = record.get("data", {})

                # Extract repo name from URL or repository object
                repo_name = None
                if "repository" in data:
                    repo_name = data["repository"].get("full_name")
                elif "url" in data:
                    # Parse from URL: https://api.github.com/repos/owner/repo/issues/123
                    parts = data["url"].split("/")
                    if len(parts) >= 6 and "repos" in parts:
                        repo_idx = parts.index("repos")
                        repo_name = f"{parts[repo_idx + 1]}/{parts[repo_idx + 2]}"

                number = data.get("number")
                if repo_name and number is not None:
                    if repo_name not in issues_by_repo:
                        issues_by_repo[repo_name] = set()
                    issues_by_repo[repo_name].add(int(number))

            except (json.JSONDecodeError, ValueError, KeyError) as e:
                logger.warning("Failed to parse issue record in %s: %s", file_path, e)

    # Convert sets to sorted lists
    return {repo: sorted(numbers) for repo, numbers in issues_by_repo.items()}


def _extract_pr_numbers_by_repo(file_path: Path) -> dict[str, list[int]]:
    """Extract PR numbers grouped by repo from a single JSONL file.

    Args:
        file_path: Path to JSONL file.

    Returns:
        Dict mapping repo full_name to list of PR numbers.
    """
    prs_by_repo: dict[str, set[int]] = {}

    with file_path.open() as f:
        for line in f:
            try:
                record = json.loads(line)
                data = record.get("data", {})

                # Extract repo name from URL or repository object
                repo_name = None
                if "base" in data and "repo" in data["base"]:
                    repo_name = data["base"]["repo"].get("full_name")
                elif "url" in data:
                    # Parse from URL: https://api.github.com/repos/owner/repo/pulls/123
                    parts = data["url"].split("/")
                    if len(parts) >= 6 and "repos" in parts:
                        repo_idx = parts.index("repos")
                        repo_name = f"{parts[repo_idx + 1]}/{parts[repo_idx + 2]}"

                number = data.get("number")
                if repo_name and number is not None:
                    if repo_name not in prs_by_repo:
                        prs_by_repo[repo_name] = set()
                    prs_by_repo[repo_name].add(int(number))

            except (json.JSONDecodeError, ValueError, KeyError) as e:
                logger.warning("Failed to parse PR record in %s: %s", file_path, e)

    # Convert sets to sorted lists
    return {repo: sorted(numbers) for repo, numbers in prs_by_repo.items()}
