"""Pull Request normalizer.

Converts raw PR data from JSONL to normalized fact_pull_request table.

Schema:
    - pr_id: GitHub node ID (string)
    - repo_id: Repository node ID (string)
    - number: PR number (int)
    - author_user_id: Author's user node ID (string, nullable)
    - created_at: Creation timestamp (datetime UTC, nullable)
    - updated_at: Last update timestamp (datetime UTC, nullable)
    - closed_at: Close timestamp (datetime UTC, nullable)
    - merged_at: Merge timestamp (datetime UTC, nullable)
    - state: PR state - "open", "closed", or "merged" (string)
    - is_draft: Draft status (bool)
    - labels: Comma-separated label names (string)
    - milestone: Milestone title (string, nullable)
    - additions: Lines added (int, nullable)
    - deletions: Lines deleted (int, nullable)
    - changed_files: Number of files changed (int, nullable)
    - title_len: Length of title (int)
    - body_len: Length of body (int)
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from gh_year_end.config import Config
from gh_year_end.normalize.common import (
    determine_pr_state,
    extract_labels,
    get_repo_id,
    get_user_id,
    normalize_timestamp,
    read_jsonl_data,
    safe_len,
)

logger = logging.getLogger(__name__)


def normalize_pulls(raw_data_path: Path, config: Config) -> pd.DataFrame:
    """Normalize pull request data from raw JSONL files.

    Reads all pull request JSONL files from raw_data_path/pulls/ directory,
    extracts relevant fields, and returns a normalized DataFrame.

    Args:
        raw_data_path: Path to raw data directory (e.g., data/raw/year=2025/.../target=org/)
        config: Application configuration.

    Returns:
        DataFrame with normalized PR data matching fact_pull_request schema.

    Raises:
        FileNotFoundError: If pulls directory doesn't exist.
    """
    pulls_dir = raw_data_path / "pulls"

    if not pulls_dir.exists():
        msg = f"Pulls directory not found: {pulls_dir}"
        raise FileNotFoundError(msg)

    logger.info("Normalizing pull requests from %s", pulls_dir)

    records: list[dict[str, Any]] = []

    # Process each repo's PR file
    for pr_file in sorted(pulls_dir.glob("*.jsonl")):
        logger.debug("Processing PR file: %s", pr_file.name)

        try:
            for pr in read_jsonl_data(pr_file):
                record = _normalize_pr_record(pr)
                if record:
                    records.append(record)

        except Exception as e:
            logger.error("Error processing PR file %s: %s", pr_file, e)
            continue

    logger.info("Normalized %d pull requests", len(records))

    # Convert to DataFrame
    if not records:
        # Return empty DataFrame with correct schema
        return pd.DataFrame(columns=_get_schema_columns())

    df = pd.DataFrame(records)

    # Ensure deterministic ordering by PR ID
    df = df.sort_values("pr_id").reset_index(drop=True)

    return df


def _normalize_pr_record(pr: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize a single PR record.

    Args:
        pr: Raw PR data dictionary from GitHub API.

    Returns:
        Normalized PR record or None if required fields are missing.
    """
    # Required fields
    pr_id = pr.get("node_id")
    repo_id = get_repo_id(pr.get("base", {}).get("repo"))
    number = pr.get("number")

    if pr_id is None or repo_id is None or number is None:
        logger.warning(
            "Skipping PR with missing required fields (pr_id=%s, repo_id=%s, number=%s)",
            pr_id,
            repo_id,
            number,
        )
        return None

    # Extract fields
    author_user_id = get_user_id(pr.get("user"))

    # Timestamps
    created_at = normalize_timestamp(pr.get("created_at"))
    updated_at = normalize_timestamp(pr.get("updated_at"))
    closed_at = normalize_timestamp(pr.get("closed_at"))
    merged_at = normalize_timestamp(pr.get("merged_at"))

    state = determine_pr_state(pr)

    is_draft = pr.get("draft", False)

    labels = extract_labels(pr.get("labels"))

    # Milestone
    milestone_obj = pr.get("milestone")
    milestone = milestone_obj.get("title") if milestone_obj else None

    # Code change stats (may be null if not fetched)
    additions = pr.get("additions")
    deletions = pr.get("deletions")
    changed_files = pr.get("changed_files")

    # Text lengths
    title_len = safe_len(pr.get("title"))
    body_len = safe_len(pr.get("body"))

    return {
        "pr_id": pr_id,
        "repo_id": repo_id,
        "number": number,
        "author_user_id": author_user_id,
        "created_at": created_at,
        "updated_at": updated_at,
        "closed_at": closed_at,
        "merged_at": merged_at,
        "state": state,
        "is_draft": is_draft,
        "labels": labels,
        "milestone": milestone,
        "additions": additions,
        "deletions": deletions,
        "changed_files": changed_files,
        "title_len": title_len,
        "body_len": body_len,
    }


def _get_schema_columns() -> list[str]:
    """Get ordered list of schema columns.

    Returns:
        List of column names in schema order.
    """
    return [
        "pr_id",
        "repo_id",
        "number",
        "author_user_id",
        "created_at",
        "updated_at",
        "closed_at",
        "merged_at",
        "state",
        "is_draft",
        "labels",
        "milestone",
        "additions",
        "deletions",
        "changed_files",
        "title_len",
        "body_len",
    ]
