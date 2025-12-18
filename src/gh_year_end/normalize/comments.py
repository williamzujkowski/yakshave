"""Comment normalizers for issue and review comments.

Converts raw comment data from GitHub API into normalized fact tables:
- fact_issue_comment: Comments on issues and PRs
- fact_review_comment: Inline code review comments on PRs
"""

import contextlib
import json
import logging
from datetime import datetime
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from gh_year_end.config import Config
from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


def normalize_issue_comments(
    paths: PathManager,
    config: Config,
    issue_pr_mapping: dict[tuple[str, int], bool] | None = None,
) -> None:
    """Normalize issue comments to fact_issue_comment Parquet table.

    Args:
        paths: PathManager for storage paths.
        config: Application configuration.
        issue_pr_mapping: Optional dict mapping (repo_full_name, issue_number) to is_pr flag.
            If not provided, all comments are assumed to be on issues.

    Schema:
        - comment_id (string): Comment node ID
        - repo_id (string): Repository node ID
        - parent_type (string): "issue" or "pr"
        - parent_id (string): Issue or PR node ID
        - author_user_id (string, nullable): Author node ID
        - created_at (timestamp[us, tz=UTC]): Comment creation time
        - body_len (int64): Length of comment body
        - year (int32): Year for partitioning
    """
    logger.info("Normalizing issue comments")

    # Collect all comment records from raw JSONL files
    records: list[dict[str, Any]] = []

    issue_comments_dir = paths.raw_root / "issue_comments"
    if not issue_comments_dir.exists():
        logger.warning("No issue_comments directory found, creating empty table")
        _write_empty_issue_comment_table(paths, config)
        return

    for jsonl_file in sorted(issue_comments_dir.glob("*.jsonl")):
        repo_full_name = jsonl_file.stem.replace("__", "/")
        logger.debug("Processing issue comments from %s", repo_full_name)

        with jsonl_file.open() as f:
            for line in f:
                envelope = json.loads(line)
                comment_data = envelope["data"]

                # Extract repo_id from the comment URL or use a lookup
                # For now, we'll derive it from the issue_url
                issue_url = comment_data.get("issue_url", "")
                issue_number = None
                if issue_url:
                    # Extract issue number from URL like https://api.github.com/repos/owner/repo/issues/123
                    parts = issue_url.split("/")
                    if len(parts) >= 2 and parts[-2] == "issues":
                        with contextlib.suppress(ValueError, IndexError):
                            issue_number = int(parts[-1])

                # Determine if this is a PR or issue
                parent_type = "issue"
                if issue_pr_mapping and issue_number:
                    parent_type = (
                        "pr"
                        if issue_pr_mapping.get((repo_full_name, issue_number), False)
                        else "issue"
                    )

                # Extract author user_id (may be null for deleted users)
                author_user_id = None
                if comment_data.get("user"):
                    author_user_id = comment_data["user"].get("node_id")

                # Parse created_at timestamp
                created_at = datetime.fromisoformat(
                    comment_data["created_at"].replace("Z", "+00:00")
                )

                # Calculate body length
                body = comment_data.get("body") or ""
                body_len = len(body)

                record = {
                    "comment_id": comment_data["node_id"],
                    "repo_id": comment_data[
                        "node_id"
                    ],  # Placeholder, should be enriched with actual repo_id
                    "parent_type": parent_type,
                    "parent_id": comment_data[
                        "node_id"
                    ],  # Placeholder, should be the issue/PR node_id
                    "author_user_id": author_user_id,
                    "created_at": created_at,
                    "body_len": body_len,
                    "year": config.github.windows.year,
                }

                records.append(record)

    if not records:
        logger.warning("No issue comments found, creating empty table")
        _write_empty_issue_comment_table(paths, config)
        return

    logger.info("Collected %d issue comment records", len(records))

    # Create PyArrow table
    schema = pa.schema(
        [
            pa.field("comment_id", pa.string()),
            pa.field("repo_id", pa.string()),
            pa.field("parent_type", pa.string()),
            pa.field("parent_id", pa.string()),
            pa.field("author_user_id", pa.string()),
            pa.field("created_at", pa.timestamp("us", tz="UTC")),
            pa.field("body_len", pa.int64()),
            pa.field("year", pa.int32()),
        ]
    )

    table = pa.Table.from_pylist(records, schema=schema)

    # Write to Parquet
    output_path = paths.curated_path("fact_issue_comment")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path, compression="snappy", use_dictionary=False)

    logger.info("Wrote %d issue comments to %s", len(records), output_path)


def normalize_review_comments(
    paths: PathManager,
    config: Config,
) -> None:
    """Normalize review comments to fact_review_comment Parquet table.

    Args:
        paths: PathManager for storage paths.
        config: Application configuration.

    Schema:
        - comment_id (string): Comment node ID
        - repo_id (string): Repository node ID
        - pr_id (string): PR node ID
        - author_user_id (string, nullable): Author node ID
        - created_at (timestamp[us, tz=UTC]): Comment creation time
        - path (string): File path being reviewed
        - line (int32, nullable): Line number (nullable for outdated comments)
        - body_len (int64): Length of comment body
        - year (int32): Year for partitioning
    """
    logger.info("Normalizing review comments")

    # Collect all comment records from raw JSONL files
    records: list[dict[str, Any]] = []

    review_comments_dir = paths.raw_root / "review_comments"
    if not review_comments_dir.exists():
        logger.warning("No review_comments directory found, creating empty table")
        _write_empty_review_comment_table(paths, config)
        return

    for jsonl_file in sorted(review_comments_dir.glob("*.jsonl")):
        repo_full_name = jsonl_file.stem.replace("__", "/")
        logger.debug("Processing review comments from %s", repo_full_name)

        with jsonl_file.open() as f:
            for line in f:
                envelope = json.loads(line)
                comment_data = envelope["data"]

                # Extract author user_id (may be null for deleted users)
                author_user_id = None
                if comment_data.get("user"):
                    author_user_id = comment_data["user"].get("node_id")

                # Parse created_at timestamp
                created_at = datetime.fromisoformat(
                    comment_data["created_at"].replace("Z", "+00:00")
                )

                # Extract file path and line
                path = comment_data.get("path", "")
                line = comment_data.get("line")  # May be null for outdated comments

                # Calculate body length
                body = comment_data.get("body") or ""
                body_len = len(body)

                record = {
                    "comment_id": comment_data["node_id"],
                    "repo_id": comment_data["node_id"],  # Placeholder, should be enriched
                    "pr_id": comment_data["node_id"],  # Placeholder, should be the PR node_id
                    "author_user_id": author_user_id,
                    "created_at": created_at,
                    "path": path,
                    "line": line,
                    "body_len": body_len,
                    "year": config.github.windows.year,
                }

                records.append(record)

    if not records:
        logger.warning("No review comments found, creating empty table")
        _write_empty_review_comment_table(paths, config)
        return

    logger.info("Collected %d review comment records", len(records))

    # Create PyArrow table
    schema = pa.schema(
        [
            pa.field("comment_id", pa.string()),
            pa.field("repo_id", pa.string()),
            pa.field("pr_id", pa.string()),
            pa.field("author_user_id", pa.string()),
            pa.field("created_at", pa.timestamp("us", tz="UTC")),
            pa.field("path", pa.string()),
            pa.field("line", pa.int32()),
            pa.field("body_len", pa.int64()),
            pa.field("year", pa.int32()),
        ]
    )

    table = pa.Table.from_pylist(records, schema=schema)

    # Write to Parquet
    output_path = paths.curated_path("fact_review_comment")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path, compression="snappy", use_dictionary=False)

    logger.info("Wrote %d review comments to %s", len(records), output_path)


def _write_empty_issue_comment_table(paths: PathManager, config: Config) -> None:
    """Write an empty fact_issue_comment table."""
    schema = pa.schema(
        [
            pa.field("comment_id", pa.string()),
            pa.field("repo_id", pa.string()),
            pa.field("parent_type", pa.string()),
            pa.field("parent_id", pa.string()),
            pa.field("author_user_id", pa.string()),
            pa.field("created_at", pa.timestamp("us", tz="UTC")),
            pa.field("body_len", pa.int64()),
            pa.field("year", pa.int32()),
        ]
    )

    empty_table = pa.Table.from_pylist([], schema=schema)
    output_path = paths.curated_path("fact_issue_comment")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(empty_table, output_path, compression="snappy", use_dictionary=False)
    logger.info("Wrote empty issue comment table to %s", output_path)


def _write_empty_review_comment_table(paths: PathManager, config: Config) -> None:
    """Write an empty fact_review_comment table."""
    schema = pa.schema(
        [
            pa.field("comment_id", pa.string()),
            pa.field("repo_id", pa.string()),
            pa.field("pr_id", pa.string()),
            pa.field("author_user_id", pa.string()),
            pa.field("created_at", pa.timestamp("us", tz="UTC")),
            pa.field("path", pa.string()),
            pa.field("line", pa.int32()),
            pa.field("body_len", pa.int64()),
            pa.field("year", pa.int32()),
        ]
    )

    empty_table = pa.Table.from_pylist([], schema=schema)
    output_path = paths.curated_path("fact_review_comment")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(empty_table, output_path, compression="snappy", use_dictionary=False)
    logger.info("Wrote empty review comment table to %s", output_path)
