"""Commit normalizers for commits and commit files.

Converts raw commit data from GitHub API into normalized fact tables:
- fact_commit: Commit metadata with author/committer info
- fact_commit_file: Files changed in commits with classifications
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from gh_year_end.config import Config
from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


def normalize_commits(
    paths: PathManager,
    config: Config,
) -> None:
    """Normalize commits to fact_commit Parquet table.

    Args:
        paths: PathManager for storage paths.
        config: Application configuration.

    Schema:
        - commit_sha (string): Commit SHA
        - repo_id (string): Repository node ID (placeholder)
        - author_user_id (string, nullable): Author node ID (may be null for unsigned commits)
        - committer_user_id (string, nullable): Committer node ID (may be null)
        - authored_at (timestamp[us, tz=UTC]): Author timestamp
        - committed_at (timestamp[us, tz=UTC]): Commit timestamp
        - message_len (int64): Length of commit message
        - year (int32): Year for partitioning
    """
    logger.info("Normalizing commits")

    # Collect all commit records from raw JSONL files
    records: list[dict[str, Any]] = []

    commits_dir = paths.raw_root / "commits"
    if not commits_dir.exists():
        logger.warning("No commits directory found, creating empty table")
        _write_empty_commit_table(paths, config)
        return

    for jsonl_file in sorted(commits_dir.glob("*.jsonl")):
        repo_full_name = jsonl_file.stem.replace("__", "/")
        logger.debug("Processing commits from %s", repo_full_name)

        with jsonl_file.open() as f:
            for line in f:
                envelope = json.loads(line)
                commit_data = envelope["data"]

                # Extract commit SHA
                commit_sha = commit_data.get("sha", "")

                # Extract author and committer user IDs
                # Note: author/committer can be null for unsigned commits
                author_user_id = None
                if commit_data.get("author"):
                    author_user_id = commit_data["author"].get("node_id")

                committer_user_id = None
                if commit_data.get("committer"):
                    committer_user_id = commit_data["committer"].get("node_id")

                # Extract timestamps from commit object (not the GitHub user)
                commit_obj = commit_data.get("commit", {})

                author_info = commit_obj.get("author", {})
                authored_at_str = author_info.get("date", "")
                authored_at = (
                    datetime.fromisoformat(authored_at_str.replace("Z", "+00:00"))
                    if authored_at_str
                    else None
                )

                committer_info = commit_obj.get("committer", {})
                committed_at_str = committer_info.get("date", "")
                committed_at = (
                    datetime.fromisoformat(committed_at_str.replace("Z", "+00:00"))
                    if committed_at_str
                    else None
                )

                # Calculate message length
                message = commit_obj.get("message", "")
                message_len = len(message)

                # Skip commits without required timestamps
                if not authored_at or not committed_at:
                    logger.debug("Skipping commit %s with missing timestamp", commit_sha)
                    continue

                record = {
                    "commit_sha": commit_sha,
                    "repo_id": commit_data.get("node_id", ""),  # Placeholder
                    "author_user_id": author_user_id,
                    "committer_user_id": committer_user_id,
                    "authored_at": authored_at,
                    "committed_at": committed_at,
                    "message_len": message_len,
                    "year": config.github.windows.year,
                }

                records.append(record)

    if not records:
        logger.warning("No commits found, creating empty table")
        _write_empty_commit_table(paths, config)
        return

    logger.info("Collected %d commit records", len(records))

    # Create PyArrow table
    schema = pa.schema(
        [
            pa.field("commit_sha", pa.string()),
            pa.field("repo_id", pa.string()),
            pa.field("author_user_id", pa.string()),
            pa.field("committer_user_id", pa.string()),
            pa.field("authored_at", pa.timestamp("us", tz="UTC")),
            pa.field("committed_at", pa.timestamp("us", tz="UTC")),
            pa.field("message_len", pa.int64()),
            pa.field("year", pa.int32()),
        ]
    )

    table = pa.Table.from_pylist(records, schema=schema)

    # Write to Parquet
    output_path = paths.curated_path("fact_commit")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path, compression="snappy", use_dictionary=False)

    logger.info("Wrote %d commits to %s", len(records), output_path)


def normalize_commit_files(
    paths: PathManager,
    config: Config,
) -> None:
    """Normalize commit files to fact_commit_file Parquet table.

    Args:
        paths: PathManager for storage paths.
        config: Application configuration.

    Schema:
        - commit_sha (string): Commit SHA
        - repo_id (string): Repository node ID (placeholder)
        - path (string): File path
        - file_ext (string): File extension (derived)
        - additions (int32): Lines added
        - deletions (int32): Lines deleted
        - changes (int32): Total changes (additions + deletions)
        - is_docs (bool): Whether file is documentation
        - is_iac (bool): Whether file is infrastructure as code
        - is_test (bool): Whether file is a test
        - is_ci (bool): Whether file is CI/CD configuration
        - year (int32): Year for partitioning
    """
    logger.info("Normalizing commit files")

    # Only process if config enables file collection and classification
    if not config.collection.commits.include_files:
        logger.info("Commit file collection disabled, creating empty table")
        _write_empty_commit_file_table(paths, config)
        return

    # Collect all file records from raw JSONL files
    records: list[dict[str, Any]] = []

    commits_dir = paths.raw_root / "commits"
    if not commits_dir.exists():
        logger.warning("No commits directory found, creating empty table")
        _write_empty_commit_file_table(paths, config)
        return

    for jsonl_file in sorted(commits_dir.glob("*.jsonl")):
        repo_full_name = jsonl_file.stem.replace("__", "/")
        logger.debug("Processing commit files from %s", repo_full_name)

        with jsonl_file.open() as f:
            for line in f:
                envelope = json.loads(line)
                commit_data = envelope["data"]

                commit_sha = commit_data.get("sha", "")
                files = commit_data.get("files", [])

                for file_data in files:
                    file_path = file_data.get("filename", "")

                    # Extract file extension
                    file_ext = Path(file_path).suffix.lstrip(".") if file_path else ""

                    # Get change stats
                    additions = file_data.get("additions", 0)
                    deletions = file_data.get("deletions", 0)
                    changes = additions + deletions

                    # Classify file if classification is enabled
                    is_docs = False
                    is_iac = False
                    is_test = False
                    is_ci = False

                    if config.collection.commits.classify_files:
                        is_docs = _is_docs_file(file_path)
                        is_iac = _is_iac_file(file_path)
                        is_test = _is_test_file(file_path)
                        is_ci = _is_ci_file(file_path)

                    record = {
                        "commit_sha": commit_sha,
                        "repo_id": commit_data.get("node_id", ""),  # Placeholder
                        "path": file_path,
                        "file_ext": file_ext,
                        "additions": additions,
                        "deletions": deletions,
                        "changes": changes,
                        "is_docs": is_docs,
                        "is_iac": is_iac,
                        "is_test": is_test,
                        "is_ci": is_ci,
                        "year": config.github.windows.year,
                    }

                    records.append(record)

    if not records:
        logger.warning("No commit files found, creating empty table")
        _write_empty_commit_file_table(paths, config)
        return

    logger.info("Collected %d commit file records", len(records))

    # Create PyArrow table
    schema = pa.schema(
        [
            pa.field("commit_sha", pa.string()),
            pa.field("repo_id", pa.string()),
            pa.field("path", pa.string()),
            pa.field("file_ext", pa.string()),
            pa.field("additions", pa.int32()),
            pa.field("deletions", pa.int32()),
            pa.field("changes", pa.int32()),
            pa.field("is_docs", pa.bool_()),
            pa.field("is_iac", pa.bool_()),
            pa.field("is_test", pa.bool_()),
            pa.field("is_ci", pa.bool_()),
            pa.field("year", pa.int32()),
        ]
    )

    table = pa.Table.from_pylist(records, schema=schema)

    # Write to Parquet
    output_path = paths.curated_path("fact_commit_file")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path, compression="snappy", use_dictionary=False)

    logger.info("Wrote %d commit files to %s", len(records), output_path)


# File classification helpers


def _is_docs_file(file_path: str) -> bool:
    """Check if a file is documentation.

    Args:
        file_path: File path to check.

    Returns:
        True if file is documentation.

    Rules:
        - README files (any case, any extension)
        - Files in docs/ directory
        - Markdown files in root directory
        - Common doc files: CHANGELOG, CONTRIBUTING, LICENSE, etc.
    """
    path_lower = file_path.lower()
    path_obj = Path(file_path)

    # README files
    if path_obj.name.lower().startswith("readme"):
        return True

    # Files in docs/ directory
    if "docs/" in path_lower or path_lower.startswith("docs/"):
        return True

    # Markdown files in root (no subdirectory)
    if "/" not in file_path and path_obj.suffix.lower() in {".md", ".markdown", ".rst"}:
        return True

    # Common documentation files
    doc_names = {
        "changelog",
        "contributing",
        "license",
        "authors",
        "notice",
        "history",
        "changes",
        "code_of_conduct",
    }
    name_lower = path_obj.stem.lower()
    return name_lower in doc_names


def _is_iac_file(file_path: str) -> bool:
    """Check if a file is Infrastructure as Code.

    Args:
        file_path: File path to check.

    Returns:
        True if file is IaC.

    Rules:
        - Terraform files (*.tf, *.tfvars)
        - Terraform directories
        - Dockerfiles
        - Docker Compose files
        - Kubernetes manifests (*.yaml in k8s directories)
        - CloudFormation templates
        - Ansible playbooks
    """
    path_lower = file_path.lower()
    path_obj = Path(file_path)

    # Terraform
    if path_obj.suffix.lower() in {".tf", ".tfvars"}:
        return True
    if "terraform/" in path_lower or path_lower.startswith("terraform/"):
        return True

    # Docker
    if path_obj.name.lower() in {"dockerfile", "dockerfile.dev", "dockerfile.prod"}:
        return True
    if "docker-compose" in path_obj.name.lower():
        return True

    # Kubernetes
    if any(
        k8s_dir in path_lower for k8s_dir in ["k8s/", "kubernetes/", "kube/"]
    ) and path_obj.suffix.lower() in {".yaml", ".yml"}:
        return True

    # CloudFormation
    if "cloudformation" in path_lower and path_obj.suffix.lower() in {".yaml", ".yml", ".json"}:
        return True

    # Ansible
    return (
        "ansible/" in path_lower or path_lower.startswith("ansible/")
    ) and path_obj.suffix.lower() in {".yaml", ".yml"}


def _is_test_file(file_path: str) -> bool:
    """Check if a file is a test.

    Args:
        file_path: File path to check.

    Returns:
        True if file is a test.

    Rules:
        - Files in tests/ or test/ directories
        - Files matching test_*.py or *_test.py patterns
        - Files matching *.spec.ts, *.spec.js patterns
        - Files matching *.test.ts, *.test.js, *.test.tsx, *.test.jsx patterns
        - Files in __tests__/ directories
    """
    path_lower = file_path.lower()
    path_obj = Path(file_path)

    # Test directories
    if any(test_dir in path_lower for test_dir in ["tests/", "test/", "__tests__/"]):
        return True

    # Python test patterns
    name_lower = path_obj.name.lower()
    if name_lower.startswith("test_") and path_obj.suffix == ".py":
        return True
    if name_lower.endswith("_test.py"):
        return True

    # JavaScript/TypeScript test patterns
    if re.match(r".*\.(spec|test)\.(ts|js|tsx|jsx)$", name_lower):
        return True

    # Go test patterns
    if name_lower.endswith("_test.go"):
        return True

    # Java test patterns
    return bool("test" in name_lower and path_obj.suffix == ".java")


def _is_ci_file(file_path: str) -> bool:
    """Check if a file is CI/CD configuration.

    Args:
        file_path: File path to check.

    Returns:
        True if file is CI/CD.

    Rules:
        - .github/workflows/ files
        - .circleci/ files
        - Jenkinsfile
        - .gitlab-ci.yml
        - .travis.yml
        - azure-pipelines.yml
        - bitbucket-pipelines.yml
    """
    path_lower = file_path.lower()

    # GitHub Actions
    if ".github/workflows/" in path_lower:
        return True

    # CircleCI
    if ".circleci/" in path_lower:
        return True

    # Jenkins
    if "jenkinsfile" in path_lower:
        return True

    # GitLab CI
    if ".gitlab-ci.yml" in path_lower:
        return True

    # Travis CI
    if ".travis.yml" in path_lower:
        return True

    # Azure Pipelines
    if "azure-pipelines" in path_lower:
        return True

    # Bitbucket Pipelines
    return "bitbucket-pipelines" in path_lower


def _write_empty_commit_table(paths: PathManager, config: Config) -> None:
    """Write an empty fact_commit table."""
    schema = pa.schema(
        [
            pa.field("commit_sha", pa.string()),
            pa.field("repo_id", pa.string()),
            pa.field("author_user_id", pa.string()),
            pa.field("committer_user_id", pa.string()),
            pa.field("authored_at", pa.timestamp("us", tz="UTC")),
            pa.field("committed_at", pa.timestamp("us", tz="UTC")),
            pa.field("message_len", pa.int64()),
            pa.field("year", pa.int32()),
        ]
    )

    empty_table = pa.Table.from_pylist([], schema=schema)
    output_path = paths.curated_path("fact_commit")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(empty_table, output_path, compression="snappy", use_dictionary=False)
    logger.info("Wrote empty commit table to %s", output_path)


def _write_empty_commit_file_table(paths: PathManager, config: Config) -> None:
    """Write an empty fact_commit_file table."""
    schema = pa.schema(
        [
            pa.field("commit_sha", pa.string()),
            pa.field("repo_id", pa.string()),
            pa.field("path", pa.string()),
            pa.field("file_ext", pa.string()),
            pa.field("additions", pa.int32()),
            pa.field("deletions", pa.int32()),
            pa.field("changes", pa.int32()),
            pa.field("is_docs", pa.bool_()),
            pa.field("is_iac", pa.bool_()),
            pa.field("is_test", pa.bool_()),
            pa.field("is_ci", pa.bool_()),
            pa.field("year", pa.int32()),
        ]
    )

    empty_table = pa.Table.from_pylist([], schema=schema)
    output_path = paths.curated_path("fact_commit_file")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(empty_table, output_path, compression="snappy", use_dictionary=False)
    logger.info("Wrote empty commit file table to %s", output_path)
