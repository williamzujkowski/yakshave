"""Tests for comment normalizers."""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from gh_year_end.config import Config
from gh_year_end.normalize.comments import (
    normalize_issue_comments,
    normalize_review_comments,
)
from gh_year_end.storage.paths import PathManager


@pytest.fixture
def temp_config(tmp_path: Path, request: pytest.FixtureRequest) -> Config:
    """Create a temporary config for testing."""
    # Use test name to create unique directory
    test_name = request.node.name
    config_dict = {
        "github": {
            "target": {"mode": "org", "name": "test-org"},
            "windows": {
                "year": 2025,
                "since": "2025-01-01T00:00:00Z",
                "until": "2026-01-01T00:00:00Z",
            },
        },
        "storage": {"root": str(tmp_path / test_name / "data")},
    }
    return Config.model_validate(config_dict)


@pytest.fixture
def paths(temp_config: Config) -> PathManager:
    """Create path manager with temp config."""
    paths = PathManager(temp_config)
    paths.ensure_directories()
    return paths


def test_normalize_issue_comments_empty(paths: PathManager, temp_config: Config) -> None:
    """Test normalizing issue comments with no data."""
    normalize_issue_comments(paths, temp_config)

    output_path = paths.curated_path("fact_issue_comment")
    assert output_path.exists()

    # Use ParquetFile to read single file (not dataset)
    parquet_file = pq.ParquetFile(output_path)
    table = parquet_file.read()
    assert len(table) == 0


def test_normalize_issue_comments_with_data(
    paths: PathManager,
    temp_config: Config,
) -> None:
    """Test normalizing issue comments with sample data."""
    # Create sample issue_comments data
    issue_comments_dir = paths.raw_root / "issue_comments"
    issue_comments_dir.mkdir(parents=True, exist_ok=True)

    sample_comment = {
        "timestamp": "2025-01-15T10:30:00Z",
        "source": "github_rest",
        "endpoint": "/repos/test-org/repo1/issues/1/comments",
        "request_id": "test-id-1",
        "page": 1,
        "data": {
            "id": 1234,
            "node_id": "IC_comment123",
            "user": {
                "login": "testuser",
                "node_id": "U_user123",
                "type": "User",
            },
            "created_at": "2025-01-15T10:30:00Z",
            "updated_at": "2025-01-15T10:30:00Z",
            "body": "This is a test comment",
            "issue_url": "https://api.github.com/repos/test-org/repo1/issues/1",
        },
    }

    jsonl_path = issue_comments_dir / "test-org__repo1.jsonl"
    with jsonl_path.open("w") as f:
        f.write(json.dumps(sample_comment) + "\n")

    normalize_issue_comments(paths, temp_config)

    output_path = paths.curated_path("fact_issue_comment")
    assert output_path.exists()

    parquet_file = pq.ParquetFile(output_path)
    table = parquet_file.read()
    assert len(table) == 1

    # Verify schema
    assert "comment_id" in table.column_names
    assert "repo_id" in table.column_names
    assert "parent_type" in table.column_names
    assert "parent_id" in table.column_names
    assert "author_user_id" in table.column_names
    assert "created_at" in table.column_names
    assert "body_len" in table.column_names
    assert "year" in table.column_names

    # Verify data
    row = table.to_pylist()[0]
    assert row["comment_id"] == "IC_comment123"
    assert row["author_user_id"] == "U_user123"
    assert row["body_len"] == len("This is a test comment")
    assert row["year"] == 2025


def test_normalize_review_comments_empty(paths: PathManager, temp_config: Config) -> None:
    """Test normalizing review comments with no data."""
    normalize_review_comments(paths, temp_config)

    output_path = paths.curated_path("fact_review_comment")
    assert output_path.exists()

    parquet_file = pq.ParquetFile(output_path)
    table = parquet_file.read()
    assert len(table) == 0


def test_normalize_review_comments_with_data(
    paths: PathManager,
    temp_config: Config,
) -> None:
    """Test normalizing review comments with sample data."""
    # Create sample review_comments data
    review_comments_dir = paths.raw_root / "review_comments"
    review_comments_dir.mkdir(parents=True, exist_ok=True)

    sample_comment = {
        "timestamp": "2025-01-15T11:00:00Z",
        "source": "github_rest",
        "endpoint": "/repos/test-org/repo1/pulls/2/comments",
        "request_id": "test-id-2",
        "page": 1,
        "data": {
            "id": 5678,
            "node_id": "RC_comment456",
            "user": {
                "login": "reviewer",
                "node_id": "U_user456",
                "type": "User",
            },
            "created_at": "2025-01-15T11:00:00Z",
            "updated_at": "2025-01-15T11:00:00Z",
            "body": "Please fix this",
            "path": "src/main.py",
            "line": 42,
            "pull_request_url": "https://api.github.com/repos/test-org/repo1/pulls/2",
        },
    }

    jsonl_path = review_comments_dir / "test-org__repo1.jsonl"
    with jsonl_path.open("w") as f:
        f.write(json.dumps(sample_comment) + "\n")

    normalize_review_comments(paths, temp_config)

    output_path = paths.curated_path("fact_review_comment")
    assert output_path.exists()

    parquet_file = pq.ParquetFile(output_path)
    table = parquet_file.read()
    assert len(table) == 1

    # Verify schema
    assert "comment_id" in table.column_names
    assert "repo_id" in table.column_names
    assert "pr_id" in table.column_names
    assert "author_user_id" in table.column_names
    assert "created_at" in table.column_names
    assert "path" in table.column_names
    assert "line" in table.column_names
    assert "body_len" in table.column_names
    assert "year" in table.column_names

    # Verify data
    row = table.to_pylist()[0]
    assert row["comment_id"] == "RC_comment456"
    assert row["author_user_id"] == "U_user456"
    assert row["path"] == "src/main.py"
    assert row["line"] == 42
    assert row["body_len"] == len("Please fix this")
    assert row["year"] == 2025


def test_normalize_issue_comments_null_user(
    paths: PathManager,
    temp_config: Config,
) -> None:
    """Test normalizing issue comments with null user (deleted account)."""
    issue_comments_dir = paths.raw_root / "issue_comments"
    issue_comments_dir.mkdir(parents=True, exist_ok=True)

    sample_comment = {
        "timestamp": "2025-01-15T10:30:00Z",
        "source": "github_rest",
        "endpoint": "/repos/test-org/repo1/issues/1/comments",
        "request_id": "test-id-3",
        "page": 1,
        "data": {
            "id": 1234,
            "node_id": "IC_comment789",
            "user": None,  # Deleted user
            "created_at": "2025-01-15T10:30:00Z",
            "updated_at": "2025-01-15T10:30:00Z",
            "body": "Comment from deleted user",
            "issue_url": "https://api.github.com/repos/test-org/repo1/issues/1",
        },
    }

    jsonl_path = issue_comments_dir / "test-org__repo1.jsonl"
    with jsonl_path.open("w") as f:
        f.write(json.dumps(sample_comment) + "\n")

    normalize_issue_comments(paths, temp_config)

    output_path = paths.curated_path("fact_issue_comment")
    parquet_file = pq.ParquetFile(output_path)
    table = parquet_file.read()
    assert len(table) == 1

    row = table.to_pylist()[0]
    assert row["author_user_id"] is None


def test_normalize_review_comments_null_line(
    paths: PathManager,
    temp_config: Config,
) -> None:
    """Test normalizing review comments with null line (outdated comment)."""
    review_comments_dir = paths.raw_root / "review_comments"
    review_comments_dir.mkdir(parents=True, exist_ok=True)

    sample_comment = {
        "timestamp": "2025-01-15T11:00:00Z",
        "source": "github_rest",
        "endpoint": "/repos/test-org/repo1/pulls/2/comments",
        "request_id": "test-id-4",
        "page": 1,
        "data": {
            "id": 5678,
            "node_id": "RC_comment999",
            "user": {
                "login": "reviewer",
                "node_id": "U_user999",
                "type": "User",
            },
            "created_at": "2025-01-15T11:00:00Z",
            "updated_at": "2025-01-15T11:00:00Z",
            "body": "Outdated comment",
            "path": "src/main.py",
            "line": None,  # Outdated comment
        },
    }

    jsonl_path = review_comments_dir / "test-org__repo1.jsonl"
    with jsonl_path.open("w") as f:
        f.write(json.dumps(sample_comment) + "\n")

    normalize_review_comments(paths, temp_config)

    output_path = paths.curated_path("fact_review_comment")
    parquet_file = pq.ParquetFile(output_path)
    table = parquet_file.read()
    assert len(table) == 1

    row = table.to_pylist()[0]
    assert row["line"] is None
