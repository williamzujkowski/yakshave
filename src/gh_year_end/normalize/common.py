"""Common utilities for data normalization.

Provides shared functions for reading raw JSONL data, normalizing timestamps,
and handling common data transformations across all normalizers.
"""

import json
import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def read_jsonl_data(path: Path) -> Iterator[dict[str, Any]]:
    """Read data from enveloped JSONL file.

    Reads JSONL records and yields the data field from each envelope.
    The envelope structure is:
    {
        "timestamp": "...",
        "source": "github_rest",
        "endpoint": "...",
        "request_id": "...",
        "page": 1,
        "data": {...}  <- this is what we yield
    }

    Args:
        path: Path to JSONL file.

    Yields:
        Data dictionaries from each envelope.

    Raises:
        FileNotFoundError: If file doesn't exist.
    """
    if not path.exists():
        msg = f"File not found: {path}"
        raise FileNotFoundError(msg)

    with path.open() as f:
        for line_num, line in enumerate(f, 1):
            try:
                envelope = json.loads(line)
                data = envelope.get("data")
                if data is not None:
                    yield data
                else:
                    logger.warning(
                        "Missing 'data' field in envelope at %s:%d",
                        path,
                        line_num,
                    )
            except json.JSONDecodeError as e:
                logger.warning(
                    "Failed to parse JSON at %s:%d: %s",
                    path,
                    line_num,
                    e,
                )
                continue


def normalize_timestamp(ts: str | None) -> datetime | None:
    """Normalize GitHub API timestamp to UTC datetime.

    Handles GitHub's ISO 8601 timestamps (e.g., "2025-01-15T10:30:00Z").

    Args:
        ts: ISO 8601 timestamp string or None.

    Returns:
        UTC datetime object or None if input is None or invalid.
    """
    if ts is None:
        return None

    try:
        # GitHub timestamps are in format: 2025-01-15T10:30:00Z
        # Replace Z with +00:00 for fromisoformat compatibility
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))

        # Ensure it's in UTC
        dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)

        return dt

    except (ValueError, AttributeError) as e:
        logger.warning("Failed to parse timestamp '%s': %s", ts, e)
        return None


def safe_len(text: str | None) -> int:
    """Calculate length of text, returning 0 for None.

    Args:
        text: Text string or None.

    Returns:
        Length of text, or 0 if None.
    """
    return len(text) if text is not None else 0


def extract_labels(labels_list: list[dict[str, Any]] | None) -> str:
    """Extract label names from GitHub labels array.

    GitHub returns labels as: [{"name": "bug"}, {"name": "enhancement"}]
    We convert this to comma-separated string: "bug,enhancement"

    Args:
        labels_list: List of label dictionaries from GitHub API.

    Returns:
        Comma-separated string of label names, or empty string if no labels.
    """
    if not labels_list:
        return ""

    label_names = [label.get("name", "") for label in labels_list if label.get("name")]
    return ",".join(sorted(label_names))  # Sort for deterministic output


def get_repo_id(repo_dict: dict[str, Any] | None) -> str | None:
    """Extract repository node ID from repo object.

    Args:
        repo_dict: Repository dictionary from GitHub API.

    Returns:
        Repository node ID or None.
    """
    if repo_dict is None:
        return None
    return repo_dict.get("node_id")


def get_user_id(user_dict: dict[str, Any] | None) -> str | None:
    """Extract user node ID from user object.

    Args:
        user_dict: User dictionary from GitHub API.

    Returns:
        User node ID or None.
    """
    if user_dict is None:
        return None
    return user_dict.get("node_id")


def determine_pr_state(pr: dict[str, Any]) -> str:
    """Determine PR state (open/closed/merged).

    GitHub's PR state field only shows "open" or "closed", but we want
    to distinguish between closed and merged PRs.

    Args:
        pr: Pull request data dictionary.

    Returns:
        One of: "open", "closed", "merged"
    """
    state: str = str(pr.get("state", "open"))

    # If merged_at exists and is not null, it's merged
    if pr.get("merged_at") is not None:
        return "merged"

    return state
