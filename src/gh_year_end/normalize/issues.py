"""Issue normalizer.

Converts raw issue data from JSONL to normalized fact_issue table.
Filters out pull requests (GitHub's issues API includes PRs).

Schema:
    - issue_id: GitHub node ID (string)
    - repo_id: Repository node ID (string)
    - number: Issue number (int)
    - author_user_id: Author's user node ID (string, nullable)
    - created_at: Creation timestamp (datetime UTC, nullable)
    - updated_at: Last update timestamp (datetime UTC, nullable)
    - closed_at: Close timestamp (datetime UTC, nullable)
    - state: Issue state - "open" or "closed" (string)
    - labels: Comma-separated label names (string)
    - title_len: Length of title (int)
    - body_len: Length of body (int)
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from gh_year_end.config import Config
from gh_year_end.normalize.common import (
    extract_labels,
    get_repo_id,
    get_user_id,
    normalize_timestamp,
    read_jsonl_data,
    safe_len,
)

logger = logging.getLogger(__name__)


def normalize_issues(raw_data_path: Path, config: Config) -> pd.DataFrame:
    """Normalize issue data from raw JSONL files.

    Reads all issue JSONL files from raw_data_path/issues/ directory,
    filters out pull requests, and returns a normalized DataFrame.

    Note: GitHub's /issues endpoint returns both issues and PRs.
    We filter out PRs by checking for the "pull_request" field.

    Args:
        raw_data_path: Path to raw data directory (e.g., data/raw/year=2025/.../target=org/)
        config: Application configuration.

    Returns:
        DataFrame with normalized issue data matching fact_issue schema.

    Raises:
        FileNotFoundError: If issues directory doesn't exist.
    """
    issues_dir = raw_data_path / "issues"

    if not issues_dir.exists():
        msg = f"Issues directory not found: {issues_dir}"
        raise FileNotFoundError(msg)

    logger.info("Normalizing issues from %s", issues_dir)

    records: list[dict[str, Any]] = []
    pr_count = 0

    # Process each repo's issue file
    for issue_file in sorted(issues_dir.glob("*.jsonl")):
        logger.debug("Processing issue file: %s", issue_file.name)

        try:
            for issue in read_jsonl_data(issue_file):
                # Filter out pull requests
                if "pull_request" in issue:
                    pr_count += 1
                    continue

                record = _normalize_issue_record(issue)
                if record:
                    records.append(record)

        except Exception as e:
            logger.error("Error processing issue file %s: %s", issue_file, e)
            continue

    logger.info(
        "Normalized %d issues (filtered %d pull requests)",
        len(records),
        pr_count,
    )

    # Convert to DataFrame
    if not records:
        # Return empty DataFrame with correct schema
        return pd.DataFrame(columns=_get_schema_columns())

    df = pd.DataFrame(records)

    # Ensure deterministic ordering by issue ID
    df = df.sort_values("issue_id").reset_index(drop=True)

    return df


def _normalize_issue_record(issue: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize a single issue record.

    Args:
        issue: Raw issue data dictionary from GitHub API.

    Returns:
        Normalized issue record or None if required fields are missing.
    """
    # Required fields
    issue_id = issue.get("node_id")
    repo_id = get_repo_id(issue.get("repository"))
    number = issue.get("number")

    # If repo_id is not in "repository" field, try extracting from URL
    if repo_id is None:
        repo_url = issue.get("repository_url")
        if repo_url:
            # URL format: https://api.github.com/repos/owner/repo
            # For now, we'll skip issues without direct repo info
            # In practice, the collector should ensure this is present
            logger.debug(
                "Issue #%s missing repository object, attempting URL parse",
                number,
            )
            # Note: We don't have repo node_id from URL, skip this issue
            # The collector should have already attached proper repo info

    if issue_id is None or number is None:
        logger.warning(
            "Skipping issue with missing required fields (issue_id=%s, number=%s)",
            issue_id,
            number,
        )
        return None

    # Note: repo_id can be None if not available - we'll still include the record
    # The downstream processes should handle nullable repo_id

    # Extract fields
    author_user_id = get_user_id(issue.get("user"))

    # Timestamps
    created_at = normalize_timestamp(issue.get("created_at"))
    updated_at = normalize_timestamp(issue.get("updated_at"))
    closed_at = normalize_timestamp(issue.get("closed_at"))

    state = issue.get("state", "open")

    labels = extract_labels(issue.get("labels"))

    # Text lengths
    title_len = safe_len(issue.get("title"))
    body_len = safe_len(issue.get("body"))

    return {
        "issue_id": issue_id,
        "repo_id": repo_id,
        "number": number,
        "author_user_id": author_user_id,
        "created_at": created_at,
        "updated_at": updated_at,
        "closed_at": closed_at,
        "state": state,
        "labels": labels,
        "title_len": title_len,
        "body_len": body_len,
    }


def _get_schema_columns() -> list[str]:
    """Get ordered list of schema columns.

    Returns:
        List of column names in schema order.
    """
    return [
        "issue_id",
        "repo_id",
        "number",
        "author_user_id",
        "created_at",
        "updated_at",
        "closed_at",
        "state",
        "labels",
        "title_len",
        "body_len",
    ]
