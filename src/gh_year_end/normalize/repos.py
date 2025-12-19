"""Repository dimension normalizer.

Converts raw repository data from GitHub API into normalized dim_repo table.
"""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from gh_year_end.config import Config

logger = logging.getLogger(__name__)


def normalize_repos(raw_data_path: Path, config: Config) -> pd.DataFrame:
    """Normalize repository data into dim_repo dimension table.

    Reads repository data from raw JSONL files (discovery and detailed metadata)
    and produces a normalized DataFrame with stable schema and deterministic ordering.

    Schema:
        - repo_id: GitHub node_id (unique identifier)
        - owner: Repository owner login
        - name: Repository name
        - full_name: Full repository name (owner/name)
        - is_archived: Whether repository is archived
        - is_fork: Whether repository is a fork
        - is_private: Whether repository is private
        - default_branch: Default branch name
        - stars: Stargazers count
        - forks: Forks count
        - watchers: Watchers count
        - topics: Comma-separated list of topics (or empty string)
        - language: Primary programming language (nullable)
        - created_at: Repository creation timestamp (UTC)
        - pushed_at: Last push timestamp (UTC, nullable)

    Args:
        raw_data_path: Path to raw data directory (year=YYYY/source=github/target=<name>/).
        config: Configuration object with target and window information.

    Returns:
        DataFrame with normalized repository dimension data, sorted by repo_id.

    Raises:
        FileNotFoundError: If repos.jsonl file is not found.
    """
    # raw_data_path is already the full path (e.g., data/raw/year=2024/source=github/target=foo)
    # so we just append repos.jsonl
    repos_file = raw_data_path / "repos.jsonl"

    if not repos_file.exists():
        msg = f"Repository data file not found: {repos_file}"
        raise FileNotFoundError(msg)

    logger.info("Normalizing repository data from %s", repos_file)

    # Read and extract repository records
    repos_data = []
    with repos_file.open() as f:
        for line_num, line in enumerate(f, start=1):
            try:
                envelope = json.loads(line)
                # Extract the actual repo data from envelope
                repo_data = envelope.get("data", {})

                # Handle both single repo and list of repos
                if isinstance(repo_data, list):
                    # Paginated list of repos from discovery
                    repos_data.extend(repo_data)
                elif isinstance(repo_data, dict):
                    # Single repo metadata
                    repos_data.append(repo_data)
            except json.JSONDecodeError as e:
                logger.warning("Invalid JSON on line %d: %s", line_num, e)
                continue

    if not repos_data:
        logger.warning("No repository data found in %s", repos_file)
        return _empty_repo_dataframe()

    logger.info("Processing %d repository records", len(repos_data))

    # Normalize each repository record
    normalized_records = []
    for repo in repos_data:
        normalized = _normalize_repo_record(repo)
        if normalized:
            normalized_records.append(normalized)

    if not normalized_records:
        logger.warning("No valid repository records after normalization")
        return _empty_repo_dataframe()

    # Create DataFrame
    df = pd.DataFrame(normalized_records)

    # Deduplicate by repo_id (keep first occurrence)
    df = df.drop_duplicates(subset=["repo_id"], keep="first")

    # Sort by repo_id for deterministic output
    df = df.sort_values("repo_id").reset_index(drop=True)

    logger.info("Normalized %d unique repositories", len(df))

    return df


def _normalize_repo_record(repo: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize a single repository record.

    Args:
        repo: Raw repository data from GitHub API.

    Returns:
        Normalized repository dict, or None if required fields are missing.
    """
    # Required fields
    repo_id = repo.get("node_id")
    owner_data = repo.get("owner", {})
    owner_login = owner_data.get("login") if isinstance(owner_data, dict) else None
    name = repo.get("name")
    full_name = repo.get("full_name")

    if not all([repo_id, owner_login, name, full_name]):
        logger.debug(
            "Skipping repository with missing required fields: %s",
            repo.get("full_name", "unknown"),
        )
        return None

    # Boolean flags
    is_archived = bool(repo.get("archived", False))
    is_fork = bool(repo.get("fork", False))
    is_private = bool(repo.get("private", False))

    # Metadata
    default_branch = repo.get("default_branch", "main")
    stars = repo.get("stargazers_count", 0)
    forks = repo.get("forks_count", 0)
    watchers = repo.get("watchers_count", 0)

    topics_list = repo.get("topics", [])
    topics = ",".join(topics_list) if topics_list else ""

    language = repo.get("language")

    # Timestamps (ISO 8601 strings, normalize to UTC)
    created_at = _normalize_timestamp(repo.get("created_at"))
    pushed_at = _normalize_timestamp(repo.get("pushed_at"))

    return {
        "repo_id": repo_id,
        "owner": owner_login,
        "name": name,
        "full_name": full_name,
        "is_archived": is_archived,
        "is_fork": is_fork,
        "is_private": is_private,
        "default_branch": default_branch,
        "stars": stars,
        "forks": forks,
        "watchers": watchers,
        "topics": topics,
        "language": language,
        "created_at": created_at,
        "pushed_at": pushed_at,
    }


def _normalize_timestamp(ts: str | None) -> str | None:
    """Normalize timestamp to UTC ISO 8601 format.

    Args:
        ts: ISO 8601 timestamp string (with or without timezone).

    Returns:
        Normalized UTC timestamp string, or None if input is None/invalid.
    """
    if not ts:
        return None

    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
        return dt.isoformat()
    except (ValueError, AttributeError) as e:
        logger.debug("Invalid timestamp format: %s - %s", ts, e)
        return None


def _empty_repo_dataframe() -> pd.DataFrame:
    """Create empty DataFrame with correct schema.

    Returns:
        Empty DataFrame with dim_repo schema.
    """
    return pd.DataFrame(
        columns=[
            "repo_id",
            "owner",
            "name",
            "full_name",
            "is_archived",
            "is_fork",
            "is_private",
            "default_branch",
            "stars",
            "forks",
            "watchers",
            "topics",
            "language",
            "created_at",
            "pushed_at",
        ]
    )
