"""Review normalizer.

Converts raw PR review data from JSONL to normalized fact_review table.

Schema:
    - review_id: GitHub node ID (string)
    - repo_id: Repository node ID (string, nullable - derived from file path)
    - pr_id: Pull request node ID (string, nullable)
    - reviewer_user_id: Reviewer's user node ID (string, nullable)
    - submitted_at: Review submission timestamp (datetime UTC, nullable)
    - state: Review state - APPROVED, CHANGES_REQUESTED, COMMENTED, etc. (string)
    - body_len: Length of review body (int)
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from gh_year_end.config import Config
from gh_year_end.normalize.common import (
    get_user_id,
    normalize_timestamp,
    read_jsonl_data,
    safe_len,
)

logger = logging.getLogger(__name__)


def normalize_reviews(raw_data_path: Path, config: Config) -> pd.DataFrame:
    """Normalize review data from raw JSONL files.

    Reads all review JSONL files from raw_data_path/reviews/ directory
    and returns a normalized DataFrame.

    Note: The repo_id is not directly in the review object, but we can infer
    it from the PR data if needed. For now, we'll leave it nullable.

    Args:
        raw_data_path: Path to raw data directory (e.g., data/raw/year=2025/.../target=org/)
        config: Application configuration.

    Returns:
        DataFrame with normalized review data matching fact_review schema.

    Raises:
        FileNotFoundError: If reviews directory doesn't exist.
    """
    reviews_dir = raw_data_path / "reviews"

    if not reviews_dir.exists():
        msg = f"Reviews directory not found: {reviews_dir}"
        raise FileNotFoundError(msg)

    logger.info("Normalizing reviews from %s", reviews_dir)

    records: list[dict[str, Any]] = []

    # Build a mapping of repo_full_name to repo_id by reading PR files
    # This allows us to populate repo_id for reviews
    repo_id_map = _build_repo_id_map(raw_data_path)

    # Process each repo's review file
    for review_file in sorted(reviews_dir.glob("*.jsonl")):
        logger.debug("Processing review file: %s", review_file.name)

        # Extract repo full name from filename (format: owner__repo.jsonl)
        repo_full_name = review_file.stem.replace("__", "/")
        repo_id = repo_id_map.get(repo_full_name)

        if repo_id is None:
            logger.warning(
                "Could not determine repo_id for %s, reviews will have null repo_id",
                repo_full_name,
            )

        try:
            for review in read_jsonl_data(review_file):
                record = _normalize_review_record(review, repo_id)
                if record:
                    records.append(record)

        except Exception as e:
            logger.error("Error processing review file %s: %s", review_file, e)
            continue

    logger.info("Normalized %d reviews", len(records))

    # Convert to DataFrame
    if not records:
        # Return empty DataFrame with correct schema
        return pd.DataFrame(columns=_get_schema_columns())

    df = pd.DataFrame(records)

    # Ensure deterministic ordering by review ID
    df = df.sort_values("review_id").reset_index(drop=True)

    return df


def _normalize_review_record(
    review: dict[str, Any],
    repo_id: str | None,
) -> dict[str, Any] | None:
    """Normalize a single review record.

    Args:
        review: Raw review data dictionary from GitHub API.
        repo_id: Repository node ID (derived from filename or PR data).

    Returns:
        Normalized review record or None if required fields are missing.
    """
    # Required fields
    review_id = review.get("node_id")

    if review_id is None:
        logger.warning("Skipping review with missing review_id")
        return None

    # Extract fields
    pr_id = (
        review.get("pull_request_url", "").split("/")[-1]
        if review.get("pull_request_url")
        else None
    )
    # Note: pr_id from URL is the PR number, not node_id
    # We should use the "id" from the review which might have pr info
    # For now, we'll leave pr_id as potentially null - should be linked in metrics layer

    # Actually, GitHub reviews don't have direct pr_id node_id in the response
    # We'll need to link this through PR normalization or keep null
    pr_id = None  # Will be linked in metrics layer or through separate join

    reviewer_user_id = get_user_id(review.get("user"))

    # Timestamp - reviews use "submitted_at"
    submitted_at = normalize_timestamp(review.get("submitted_at"))

    # State - APPROVED, CHANGES_REQUESTED, COMMENTED, DISMISSED, PENDING
    state = review.get("state", "COMMENTED")

    # Body length
    body_len = safe_len(review.get("body"))

    return {
        "review_id": review_id,
        "repo_id": repo_id,
        "pr_id": pr_id,  # Will be null for now, linked later
        "reviewer_user_id": reviewer_user_id,
        "submitted_at": submitted_at,
        "state": state,
        "body_len": body_len,
    }


def _build_repo_id_map(raw_data_path: Path) -> dict[str, str]:
    """Build mapping of repo full_name to repo_id from PR files.

    Reads PR files to extract repo_id for each repository.

    Args:
        raw_data_path: Path to raw data directory.

    Returns:
        Dictionary mapping repo full_name to repo_id.
    """
    repo_id_map: dict[str, str] = {}
    pulls_dir = raw_data_path / "pulls"

    if not pulls_dir.exists():
        logger.warning("Pulls directory not found, repo_id mapping will be empty")
        return repo_id_map

    for pr_file in pulls_dir.glob("*.jsonl"):
        # Extract repo full name from filename
        repo_full_name = pr_file.stem.replace("__", "/")

        try:
            # Read first PR to get repo_id
            for pr in read_jsonl_data(pr_file):
                base = pr.get("base", {})
                repo = base.get("repo", {})
                repo_id = repo.get("node_id")

                if repo_id:
                    repo_id_map[repo_full_name] = repo_id
                    logger.debug("Mapped %s -> %s", repo_full_name, repo_id)
                    break  # Only need first PR

        except Exception as e:
            logger.debug("Error reading PR file %s for repo_id mapping: %s", pr_file, e)
            continue

    logger.debug("Built repo_id map with %d repositories", len(repo_id_map))
    return repo_id_map


def _get_schema_columns() -> list[str]:
    """Get ordered list of schema columns.

    Returns:
        List of column names in schema order.
    """
    return [
        "review_id",
        "repo_id",
        "pr_id",
        "reviewer_user_id",
        "submitted_at",
        "state",
        "body_len",
    ]
