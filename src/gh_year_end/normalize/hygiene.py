"""Hygiene fact normalizers for repository health and security features.

Normalizes raw data from:
- repo_tree/ (file presence checks)
- branch_protection/ (branch protection rules)
- security_features/ (Dependabot, secret scanning, etc.)
"""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from gh_year_end.config import Config

logger = logging.getLogger(__name__)


def normalize_file_presence(
    raw_data_path: Path,
    config: Config,
) -> pd.DataFrame:
    """Normalize file presence data from repo_tree snapshots.

    Reads file_presence records from repo_tree/<repo>.jsonl files and produces
    a normalized DataFrame with one row per repo/file combination.

    Args:
        raw_data_path: Path to raw data directory (year=YYYY/source=github/target=<name>/).
        config: Application configuration.

    Returns:
        DataFrame with schema:
            - repo_id: Repository node ID (str)
            - repo_full_name: Full repository name (str)
            - captured_at: UTC timestamp of capture (datetime)
            - path: File path (str)
            - exists: Whether file exists (bool)
            - sha: Git SHA if exists (str, nullable)
            - size_bytes: File size in bytes (int, nullable)

    Raises:
        ValueError: If raw data path doesn't exist or is invalid.
    """
    logger.info("Starting file presence normalization from %s", raw_data_path)

    repo_tree_dir = raw_data_path / "repo_tree"
    if not repo_tree_dir.exists():
        logger.warning("No repo_tree directory found at %s", repo_tree_dir)
        return _empty_file_presence_dataframe()

    # Load repository metadata for repo_id lookup
    repos_file = raw_data_path / "repos.jsonl"
    if not repos_file.exists():
        logger.warning("No repos.jsonl found at %s", repos_file)
        return _empty_file_presence_dataframe()

    repo_id_map = _load_repo_id_map(repos_file)

    records = []
    jsonl_files = list(repo_tree_dir.glob("*.jsonl"))
    logger.info("Processing %d repo_tree files", len(jsonl_files))

    for jsonl_file in jsonl_files:
        try:
            file_records = _extract_file_presence_from_jsonl(
                jsonl_file,
                repo_id_map,
            )
            records.extend(file_records)
        except Exception as e:
            logger.error("Failed to process %s: %s", jsonl_file, e)
            continue

    if not records:
        logger.warning("No file presence records found")
        return _empty_file_presence_dataframe()

    df = pd.DataFrame(records)

    # Ensure correct types
    df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True)
    df["exists"] = df["exists"].astype(bool)
    df["size_bytes"] = df["size_bytes"].astype("Int64")  # Nullable integer

    # Sort for deterministic output
    df = df.sort_values(
        by=["repo_full_name", "path", "captured_at"],
    ).reset_index(drop=True)

    logger.info("Normalized %d file presence records", len(df))
    return df


def normalize_branch_protection(
    raw_data_path: Path,
    config: Config,
) -> pd.DataFrame:
    """Normalize branch protection data from branch_protection snapshots.

    Reads branch protection records and produces a normalized DataFrame with
    nullable fields for permission errors.

    Args:
        raw_data_path: Path to raw data directory.
        config: Application configuration.

    Returns:
        DataFrame with schema:
            - repo_id: Repository node ID (str)
            - repo_full_name: Full repository name (str)
            - captured_at: UTC timestamp of capture (datetime)
            - default_branch: Default branch name (str)
            - requires_reviews: Requires PR reviews (bool, nullable)
            - required_approving_review_count: Number of required reviews (int, nullable)
            - requires_status_checks: Requires status checks (bool, nullable)
            - allows_force_pushes: Allows force pushes (bool, nullable)
            - allows_deletions: Allows branch deletions (bool, nullable)
            - enforce_admins: Enforces rules for admins (bool, nullable)
            - error: Error message if collection failed (str, nullable)

    Raises:
        ValueError: If raw data path doesn't exist or is invalid.
    """
    logger.info("Starting branch protection normalization from %s", raw_data_path)

    bp_dir = raw_data_path / "branch_protection"
    if not bp_dir.exists():
        logger.warning("No branch_protection directory found at %s", bp_dir)
        return _empty_branch_protection_dataframe()

    # Load repository metadata
    repos_file = raw_data_path / "repos.jsonl"
    if not repos_file.exists():
        logger.warning("No repos.jsonl found at %s", repos_file)
        return _empty_branch_protection_dataframe()

    repo_id_map = _load_repo_id_map(repos_file)

    records = []
    jsonl_files = list(bp_dir.glob("*.jsonl"))
    logger.info("Processing %d branch_protection files", len(jsonl_files))

    for jsonl_file in jsonl_files:
        try:
            bp_records = _extract_branch_protection_from_jsonl(
                jsonl_file,
                repo_id_map,
            )
            records.extend(bp_records)
        except Exception as e:
            logger.error("Failed to process %s: %s", jsonl_file, e)
            continue

    if not records:
        logger.warning("No branch protection records found")
        return _empty_branch_protection_dataframe()

    df = pd.DataFrame(records)

    # Ensure correct types
    df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True)

    # Convert boolean columns with nullable types
    bool_cols = [
        "requires_reviews",
        "requires_status_checks",
        "allows_force_pushes",
        "allows_deletions",
        "enforce_admins",
    ]
    for col in bool_cols:
        df[col] = df[col].astype("boolean")  # Pandas nullable boolean

    df["required_approving_review_count"] = df["required_approving_review_count"].astype("Int64")

    # Sort for deterministic output
    df = df.sort_values(
        by=["repo_full_name", "captured_at"],
    ).reset_index(drop=True)

    logger.info("Normalized %d branch protection records", len(df))
    return df


def normalize_security_features(
    raw_data_path: Path,
    config: Config,
) -> pd.DataFrame:
    """Normalize security features data from security_features snapshots.

    Reads security features records and produces a normalized DataFrame with
    nullable fields for permission errors.

    Args:
        raw_data_path: Path to raw data directory.
        config: Application configuration.

    Returns:
        DataFrame with schema:
            - repo_id: Repository node ID (str)
            - repo_full_name: Full repository name (str)
            - captured_at: UTC timestamp of capture (datetime)
            - dependabot_alerts_enabled: Dependabot alerts enabled (bool, nullable)
            - secret_scanning_enabled: Secret scanning enabled (bool, nullable)
            - push_protection_enabled: Push protection enabled (bool, nullable)
            - error: Error message if collection failed (str, nullable)

    Raises:
        ValueError: If raw data path doesn't exist or is invalid.
    """
    logger.info("Starting security features normalization from %s", raw_data_path)

    sf_dir = raw_data_path / "security_features"
    if not sf_dir.exists():
        logger.warning("No security_features directory found at %s", sf_dir)
        return _empty_security_features_dataframe()

    # Load repository metadata
    repos_file = raw_data_path / "repos.jsonl"
    if not repos_file.exists():
        logger.warning("No repos.jsonl found at %s", repos_file)
        return _empty_security_features_dataframe()

    repo_id_map = _load_repo_id_map(repos_file)

    records = []
    jsonl_files = list(sf_dir.glob("*.jsonl"))
    logger.info("Processing %d security_features files", len(jsonl_files))

    for jsonl_file in jsonl_files:
        try:
            sf_records = _extract_security_features_from_jsonl(
                jsonl_file,
                repo_id_map,
            )
            records.extend(sf_records)
        except Exception as e:
            logger.error("Failed to process %s: %s", jsonl_file, e)
            continue

    if not records:
        logger.warning("No security features records found")
        return _empty_security_features_dataframe()

    df = pd.DataFrame(records)

    # Ensure correct types
    df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True)

    # Convert boolean columns with nullable types
    bool_cols = [
        "dependabot_alerts_enabled",
        "secret_scanning_enabled",
        "push_protection_enabled",
    ]
    for col in bool_cols:
        df[col] = df[col].astype("boolean")  # Pandas nullable boolean

    # Sort for deterministic output
    df = df.sort_values(
        by=["repo_full_name", "captured_at"],
    ).reset_index(drop=True)

    logger.info("Normalized %d security features records", len(df))
    return df


# Helper functions


def _load_repo_id_map(repos_file: Path) -> dict[str, str]:
    """Load mapping of repo full_name to node_id from repos.jsonl.

    Args:
        repos_file: Path to repos.jsonl file.

    Returns:
        Dictionary mapping full_name to node_id.
    """
    repo_map = {}
    with repos_file.open() as f:
        for line in f:
            envelope = json.loads(line)
            data = envelope.get("data", {})

            # Handle both single repo and list of repos
            repos = [data] if isinstance(data, dict) else data

            for repo in repos:
                if not isinstance(repo, dict):
                    continue

                # Handle both REST and GraphQL formats
                full_name = repo.get("full_name") or repo.get("nameWithOwner")
                node_id = repo.get("node_id") or repo.get("id")

                if full_name and node_id:
                    repo_map[full_name] = node_id

    logger.debug("Loaded %d repos from %s", len(repo_map), repos_file)
    return repo_map


def _extract_file_presence_from_jsonl(
    jsonl_file: Path,
    repo_id_map: dict[str, str],
) -> list[dict[str, Any]]:
    """Extract file presence records from a repo_tree JSONL file.

    Args:
        jsonl_file: Path to JSONL file.
        repo_id_map: Mapping of full_name to node_id.

    Returns:
        List of file presence records.
    """
    records = []
    with jsonl_file.open() as f:
        for line in f:
            envelope = json.loads(line)

            # Only process "file_presence" records (derived from tree)
            if envelope.get("endpoint") != "file_presence":
                continue

            data = envelope.get("data", {})
            repo_full_name = data.get("repo")

            if not repo_full_name:
                continue

            repo_id = repo_id_map.get(repo_full_name)
            if not repo_id:
                logger.warning("No repo_id found for %s", repo_full_name)
                continue

            captured_at = envelope.get("timestamp")
            if not captured_at:
                captured_at = datetime.now(UTC).isoformat()

            records.append(
                {
                    "repo_id": repo_id,
                    "repo_full_name": repo_full_name,
                    "captured_at": captured_at,
                    "path": data.get("path"),
                    "exists": data.get("exists", False),
                    "sha": data.get("sha"),
                    "size_bytes": data.get("size"),
                }
            )

    return records


def _extract_branch_protection_from_jsonl(
    jsonl_file: Path,
    repo_id_map: dict[str, str],
) -> list[dict[str, Any]]:
    """Extract branch protection records from a branch_protection JSONL file.

    Args:
        jsonl_file: Path to JSONL file.
        repo_id_map: Mapping of full_name to node_id.

    Returns:
        List of branch protection records.
    """
    records = []
    with jsonl_file.open() as f:
        for line in f:
            envelope = json.loads(line)
            data = envelope.get("data", {})
            repo_full_name = data.get("repo")

            if not repo_full_name:
                continue

            repo_id = repo_id_map.get(repo_full_name)
            if not repo_id:
                logger.warning("No repo_id found for %s", repo_full_name)
                continue

            captured_at = envelope.get("timestamp")
            if not captured_at:
                captured_at = datetime.now(UTC).isoformat()

            # Handle error cases (403, 404, etc.)
            error = data.get("error")
            protection_enabled = data.get("protection_enabled")

            record: dict[str, Any] = {
                "repo_id": repo_id,
                "repo_full_name": repo_full_name,
                "captured_at": captured_at,
                "default_branch": data.get("branch", "main"),
                "error": error,
            }

            # If error is set, all protection fields should be None
            if error or protection_enabled is False:
                record.update(
                    {
                        "requires_reviews": None,
                        "required_approving_review_count": None,
                        "requires_status_checks": None,
                        "allows_force_pushes": None,
                        "allows_deletions": None,
                        "enforce_admins": None,
                    }
                )
            elif protection_enabled:
                # Extract protection settings
                required_reviews = data.get("required_reviews", {})
                required_status_checks = data.get("required_status_checks")

                record.update(
                    {
                        "requires_reviews": required_reviews is not None,
                        "required_approving_review_count": (
                            required_reviews.get("required_approving_review_count")
                            if required_reviews
                            else None
                        ),
                        "requires_status_checks": required_status_checks is not None,
                        "allows_force_pushes": data.get("allow_force_pushes"),
                        "allows_deletions": data.get("allow_deletions"),
                        "enforce_admins": data.get("enforce_admins"),
                    }
                )

            records.append(record)

    return records


def _extract_security_features_from_jsonl(
    jsonl_file: Path,
    repo_id_map: dict[str, str],
) -> list[dict[str, Any]]:
    """Extract security features records from a security_features JSONL file.

    Args:
        jsonl_file: Path to JSONL file.
        repo_id_map: Mapping of full_name to node_id.

    Returns:
        List of security features records.
    """
    records = []
    with jsonl_file.open() as f:
        for line in f:
            envelope = json.loads(line)
            data = envelope.get("data", {})
            repo_full_name = data.get("repo")

            if not repo_full_name:
                continue

            repo_id = repo_id_map.get(repo_full_name)
            if not repo_id:
                logger.warning("No repo_id found for %s", repo_full_name)
                continue

            captured_at = envelope.get("timestamp")
            if not captured_at:
                captured_at = datetime.now(UTC).isoformat()

            # Handle error cases
            error = data.get("error")

            record: dict[str, Any] = {
                "repo_id": repo_id,
                "repo_full_name": repo_full_name,
                "captured_at": captured_at,
                "dependabot_alerts_enabled": data.get("dependabot_alerts_enabled"),
                "secret_scanning_enabled": data.get("secret_scanning_enabled"),
                "push_protection_enabled": data.get("secret_scanning_push_protection_enabled"),
                "error": error,
            }

            records.append(record)

    return records


def _empty_file_presence_dataframe() -> pd.DataFrame:
    """Create empty DataFrame with file presence schema.

    Returns:
        Empty DataFrame with correct columns and types.
    """
    return pd.DataFrame(
        columns=[
            "repo_id",
            "repo_full_name",
            "captured_at",
            "path",
            "exists",
            "sha",
            "size_bytes",
        ]
    ).astype(
        {
            "repo_id": "string",
            "repo_full_name": "string",
            "captured_at": "datetime64[ns, UTC]",
            "path": "string",
            "exists": "bool",
            "sha": "string",
            "size_bytes": "Int64",
        }
    )


def _empty_branch_protection_dataframe() -> pd.DataFrame:
    """Create empty DataFrame with branch protection schema.

    Returns:
        Empty DataFrame with correct columns and types.
    """
    return pd.DataFrame(
        columns=[
            "repo_id",
            "repo_full_name",
            "captured_at",
            "default_branch",
            "requires_reviews",
            "required_approving_review_count",
            "requires_status_checks",
            "allows_force_pushes",
            "allows_deletions",
            "enforce_admins",
            "error",
        ]
    ).astype(
        {
            "repo_id": "string",
            "repo_full_name": "string",
            "captured_at": "datetime64[ns, UTC]",
            "default_branch": "string",
            "requires_reviews": "boolean",
            "required_approving_review_count": "Int64",
            "requires_status_checks": "boolean",
            "allows_force_pushes": "boolean",
            "allows_deletions": "boolean",
            "enforce_admins": "boolean",
            "error": "string",
        }
    )


def _empty_security_features_dataframe() -> pd.DataFrame:
    """Create empty DataFrame with security features schema.

    Returns:
        Empty DataFrame with correct columns and types.
    """
    return pd.DataFrame(
        columns=[
            "repo_id",
            "repo_full_name",
            "captured_at",
            "dependabot_alerts_enabled",
            "secret_scanning_enabled",
            "push_protection_enabled",
            "error",
        ]
    ).astype(
        {
            "repo_id": "string",
            "repo_full_name": "string",
            "captured_at": "datetime64[ns, UTC]",
            "dependabot_alerts_enabled": "boolean",
            "secret_scanning_enabled": "boolean",
            "push_protection_enabled": "boolean",
            "error": "string",
        }
    )
