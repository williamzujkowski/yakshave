"""User dimension normalizer with bot detection.

This module extracts unique users from all raw data sources and applies
bot detection rules from the configuration.
"""

import json
from pathlib import Path
from typing import Any

import polars as pl

from gh_year_end.config import Config
from gh_year_end.normalize.identity import BotDetector
from gh_year_end.storage.paths import PathManager


def normalize_identity_rules(config: Config) -> pl.DataFrame:
    """Create dim_identity_rule table from bot detection configuration.

    Args:
        config: Application configuration containing identity rules.

    Returns:
        DataFrame with columns:
        - rule_id: Unique rule identifier
        - type: Rule type (regex_exclude, allowlist)
        - pattern: Pattern or value
        - description: Human-readable description
    """
    rules = []
    rule_id = 1

    # Add exclude patterns
    for pattern in config.identity.bots.exclude_patterns:
        rules.append(
            {
                "rule_id": f"R_{rule_id:04d}",
                "type": "regex_exclude",
                "pattern": pattern,
                "description": f"Bot pattern: {pattern}",
            }
        )
        rule_id += 1

    # Add include overrides
    for login in config.identity.bots.include_overrides:
        rules.append(
            {
                "rule_id": f"R_{rule_id:04d}",
                "type": "allowlist",
                "pattern": login,
                "description": f"Force as human: {login}",
            }
        )
        rule_id += 1

    if not rules:
        return pl.DataFrame(
            schema={
                "rule_id": pl.Utf8,
                "type": pl.Utf8,
                "pattern": pl.Utf8,
                "description": pl.Utf8,
            }
        )

    return pl.DataFrame(rules)


def normalize_users(config: Config) -> pl.DataFrame:
    """Normalize users from raw data with bot detection.

    Extracts unique users from all raw JSONL files (repos, pulls, issues,
    reviews, comments, commits) and applies bot detection rules from config.

    Args:
        config: Application configuration containing identity rules.

    Returns:
        DataFrame with columns:
        - user_id: GitHub node_id (unique identifier)
        - login: Username
        - type: User type (User, Bot, Organization)
        - profile_url: GitHub profile URL
        - is_bot: Boolean flag indicating bot status
        - bot_reason: Explanation for bot classification (null if human)
        - display_name: Display name if available (nullable)

    Raises:
        FileNotFoundError: If raw data directory doesn't exist.
    """
    paths = PathManager(config)

    # Initialize bot detector
    bot_detector = BotDetector(
        exclude_patterns=config.identity.bots.exclude_patterns,
        include_overrides=config.identity.bots.include_overrides,
    )

    # Collect all unique users from all sources
    users: dict[str, dict[str, Any]] = {}

    # Extract from repos
    if paths.repos_raw_path.exists():
        _extract_users_from_repos(paths.repos_raw_path, users)

    # Extract from pulls
    _extract_users_from_directory(paths.raw_root / "pulls", users)

    # Extract from issues
    _extract_users_from_directory(paths.raw_root / "issues", users)

    # Extract from reviews
    _extract_users_from_directory(paths.raw_root / "reviews", users)

    # Extract from issue_comments
    _extract_users_from_directory(paths.raw_root / "issue_comments", users)

    # Extract from review_comments
    _extract_users_from_directory(paths.raw_root / "review_comments", users)

    # Extract from commits
    _extract_users_from_directory(paths.raw_root / "commits", users)

    # Convert to list and apply bot detection
    user_records = []
    for user_id, user_data in users.items():
        login = user_data["login"]
        user_type = user_data["type"]

        # Detect bot
        bot_result = bot_detector.detect(login, user_type)

        user_records.append(
            {
                "user_id": user_id,
                "login": login,
                "type": user_type,
                "profile_url": user_data["profile_url"],
                "is_bot": bot_result.is_bot,
                "bot_reason": bot_result.reason,
                "display_name": user_data.get("display_name"),
            }
        )

    # Create DataFrame and sort by user_id for determinism
    if not user_records:
        # Return empty DataFrame with correct schema
        return pl.DataFrame(
            schema={
                "user_id": pl.Utf8,
                "login": pl.Utf8,
                "type": pl.Utf8,
                "profile_url": pl.Utf8,
                "is_bot": pl.Boolean,
                "bot_reason": pl.Utf8,
                "display_name": pl.Utf8,
            }
        )

    df = pl.DataFrame(user_records)
    return df.sort("user_id")


def _extract_users_from_repos(path: Path, users: dict[str, dict[str, Any]]) -> None:
    """Extract users from repos JSONL file.

    Args:
        path: Path to repos.jsonl file.
        users: Dictionary to accumulate users (keyed by user_id).
    """
    if not path.exists():
        return

    with path.open() as f:
        for line in f:
            record = json.loads(line)
            data = record.get("data", {})

            # Extract owner
            owner = data.get("owner")
            if owner and isinstance(owner, dict):
                user_id = owner.get("node_id")
                if user_id and user_id not in users:
                    users[user_id] = {
                        "login": owner.get("login", ""),
                        "type": owner.get("type", "User"),
                        "profile_url": owner.get("html_url", ""),
                        "display_name": owner.get("name"),
                    }


def _extract_users_from_directory(directory: Path, users: dict[str, dict[str, Any]]) -> None:
    """Extract users from all JSONL files in a directory.

    Args:
        directory: Path to directory containing JSONL files.
        users: Dictionary to accumulate users (keyed by user_id).
    """
    if not directory.exists():
        return

    for jsonl_file in directory.glob("*.jsonl"):
        _extract_users_from_jsonl(jsonl_file, users)


def _extract_users_from_jsonl(path: Path, users: dict[str, dict[str, Any]]) -> None:
    """Extract users from a JSONL file.

    Handles pulls, issues, reviews, comments, and commits.

    Args:
        path: Path to JSONL file.
        users: Dictionary to accumulate users (keyed by user_id).
    """
    with path.open() as f:
        for line in f:
            record = json.loads(line)
            data = record.get("data", {})

            # Handle different data structures
            if isinstance(data, list):
                # List of items (pulls, issues, reviews, comments, commits)
                for item in data:
                    _extract_user_from_item(item, users)
            elif isinstance(data, dict):
                # Single item
                _extract_user_from_item(data, users)


def _extract_user_from_item(item: dict[str, Any], users: dict[str, dict[str, Any]]) -> None:
    """Extract user from a single item.

    Args:
        item: Item dictionary (PR, issue, review, comment, commit).
        users: Dictionary to accumulate users (keyed by user_id).
    """
    # Standard user fields
    user_fields = ["user", "author", "reviewer", "committer"]

    for field in user_fields:
        user = item.get(field)
        if user and isinstance(user, dict):
            user_id = user.get("node_id") or user.get("id")
            if user_id and user_id not in users:
                users[str(user_id)] = {
                    "login": user.get("login", ""),
                    "type": user.get("type", "User"),
                    "profile_url": user.get("html_url", ""),
                    "display_name": user.get("name"),
                }

    # Handle nested structures
    if "author" in item and isinstance(item["author"], dict):
        # Commit author might have nested user
        author = item["author"]
        if "user" in author and isinstance(author["user"], dict):
            user = author["user"]
            user_id = user.get("node_id") or user.get("id")
            if user_id and user_id not in users:
                users[str(user_id)] = {
                    "login": user.get("login", ""),
                    "type": user.get("type", "User"),
                    "profile_url": user.get("html_url", ""),
                    "display_name": user.get("name"),
                }

    if "committer" in item and isinstance(item["committer"], dict):
        # Commit committer might have nested user
        committer = item["committer"]
        if "user" in committer and isinstance(committer["user"], dict):
            user = committer["user"]
            user_id = user.get("node_id") or user.get("id")
            if user_id and user_id not in users:
                users[str(user_id)] = {
                    "login": user.get("login", ""),
                    "type": user.get("type", "User"),
                    "profile_url": user.get("html_url", ""),
                    "display_name": user.get("name"),
                }

    # Handle assignees (list)
    assignees = item.get("assignees", [])
    if isinstance(assignees, list):
        for assignee in assignees:
            if isinstance(assignee, dict):
                user_id = assignee.get("node_id") or assignee.get("id")
                if user_id and user_id not in users:
                    users[str(user_id)] = {
                        "login": assignee.get("login", ""),
                        "type": assignee.get("type", "User"),
                        "profile_url": assignee.get("html_url", ""),
                        "display_name": assignee.get("name"),
                    }

    # Handle requested_reviewers (list)
    reviewers = item.get("requested_reviewers", [])
    if isinstance(reviewers, list):
        for reviewer in reviewers:
            if isinstance(reviewer, dict):
                user_id = reviewer.get("node_id") or reviewer.get("id")
                if user_id and user_id not in users:
                    users[str(user_id)] = {
                        "login": reviewer.get("login", ""),
                        "type": reviewer.get("type", "User"),
                        "profile_url": reviewer.get("html_url", ""),
                        "display_name": reviewer.get("name"),
                    }
