"""Tests for repository dimension normalizer."""

import json
from pathlib import Path

import pandas as pd
import pytest

from gh_year_end.config import Config
from gh_year_end.normalize.repos import normalize_repos


@pytest.fixture
def test_config() -> Config:
    """Create test configuration.

    Returns:
        Config instance for testing.
    """
    return Config.model_validate(
        {
            "github": {
                "target": {"mode": "org", "name": "test-org"},
                "windows": {
                    "year": 2024,
                    "since": "2024-01-01T00:00:00Z",
                    "until": "2025-01-01T00:00:00Z",
                },
            }
        }
    )


@pytest.fixture
def temp_raw_data_dir(tmp_path: Path) -> Path:
    """Create temporary raw data directory structure.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        Path to raw data directory root.
    """
    raw_dir = tmp_path / "year=2024" / "source=github" / "target=test-org"
    raw_dir.mkdir(parents=True, exist_ok=True)
    return tmp_path


def test_normalize_repos_basic(temp_raw_data_dir: Path, test_config: Config) -> None:
    """Test basic repository normalization."""
    repos_file = (
        temp_raw_data_dir / "year=2024" / "source=github" / "target=test-org" / "repos.jsonl"
    )
    repos_file.parent.mkdir(parents=True, exist_ok=True)

    # Create sample repository data
    sample_repos = [
        {
            "node_id": "R_123",
            "name": "test-repo",
            "full_name": "test-org/test-repo",
            "owner": {"login": "test-org"},
            "archived": False,
            "fork": False,
            "private": False,
            "default_branch": "main",
            "stargazers_count": 42,
            "forks_count": 5,
            "watchers_count": 38,
            "topics": ["python", "testing"],
            "language": "Python",
            "created_at": "2023-01-15T10:30:00Z",
            "pushed_at": "2024-06-20T14:45:00Z",
        }
    ]

    # Write enveloped data
    with repos_file.open("w") as f:
        envelope = {
            "timestamp": "2024-12-18T00:00:00Z",
            "source": "github_rest",
            "endpoint": "/orgs/test-org/repos",
            "request_id": "test-123",
            "page": 1,
            "data": sample_repos,
        }
        f.write(json.dumps(envelope) + "\n")

    # Normalize
    df = normalize_repos(temp_raw_data_dir, test_config)

    # Assertions
    assert len(df) == 1
    assert df.iloc[0]["repo_id"] == "R_123"
    assert df.iloc[0]["owner"] == "test-org"
    assert df.iloc[0]["name"] == "test-repo"
    assert df.iloc[0]["full_name"] == "test-org/test-repo"
    assert df.iloc[0]["is_archived"] == False  # noqa: E712
    assert df.iloc[0]["is_fork"] == False  # noqa: E712
    assert df.iloc[0]["is_private"] == False  # noqa: E712
    assert df.iloc[0]["default_branch"] == "main"
    assert df.iloc[0]["stars"] == 42
    assert df.iloc[0]["forks"] == 5
    assert df.iloc[0]["watchers"] == 38
    assert df.iloc[0]["topics"] == "python,testing"
    assert df.iloc[0]["language"] == "Python"
    assert df.iloc[0]["created_at"] == "2023-01-15T10:30:00+00:00"
    assert df.iloc[0]["pushed_at"] == "2024-06-20T14:45:00+00:00"


def test_normalize_repos_multiple_records(temp_raw_data_dir: Path, test_config: Config) -> None:
    """Test normalization with multiple repository records."""
    repos_file = (
        temp_raw_data_dir / "year=2024" / "source=github" / "target=test-org" / "repos.jsonl"
    )
    repos_file.parent.mkdir(parents=True, exist_ok=True)

    # Create multiple pages of data
    with repos_file.open("w") as f:
        # Page 1
        envelope1 = {
            "timestamp": "2024-12-18T00:00:00Z",
            "source": "github_rest",
            "endpoint": "/orgs/test-org/repos",
            "request_id": "test-1",
            "page": 1,
            "data": [
                {
                    "node_id": "R_200",
                    "name": "alpha",
                    "full_name": "test-org/alpha",
                    "owner": {"login": "test-org"},
                    "archived": False,
                    "fork": False,
                    "private": False,
                    "default_branch": "main",
                    "stargazers_count": 10,
                    "forks_count": 2,
                    "watchers_count": 8,
                    "topics": [],
                    "language": "Go",
                    "created_at": "2023-01-01T00:00:00Z",
                    "pushed_at": "2024-01-01T00:00:00Z",
                }
            ],
        }
        f.write(json.dumps(envelope1) + "\n")

        # Page 2
        envelope2 = {
            "timestamp": "2024-12-18T00:01:00Z",
            "source": "github_rest",
            "endpoint": "/orgs/test-org/repos",
            "request_id": "test-2",
            "page": 2,
            "data": [
                {
                    "node_id": "R_100",
                    "name": "beta",
                    "full_name": "test-org/beta",
                    "owner": {"login": "test-org"},
                    "archived": True,
                    "fork": True,
                    "private": True,
                    "default_branch": "develop",
                    "stargazers_count": 0,
                    "forks_count": 0,
                    "watchers_count": 0,
                    "topics": ["archived"],
                    "language": None,
                    "created_at": "2022-06-15T12:00:00Z",
                    "pushed_at": None,
                }
            ],
        }
        f.write(json.dumps(envelope2) + "\n")

    # Normalize
    df = normalize_repos(temp_raw_data_dir, test_config)

    # Assertions
    assert len(df) == 2

    # Should be sorted by repo_id (R_100 before R_200)
    assert df.iloc[0]["repo_id"] == "R_100"
    assert df.iloc[1]["repo_id"] == "R_200"

    # Check second record details
    assert df.iloc[0]["name"] == "beta"
    assert df.iloc[0]["is_archived"] == True  # noqa: E712
    assert df.iloc[0]["is_fork"] == True  # noqa: E712
    assert df.iloc[0]["is_private"] == True  # noqa: E712
    assert df.iloc[0]["topics"] == "archived"
    assert pd.isna(df.iloc[0]["language"])
    assert pd.isna(df.iloc[0]["pushed_at"])


def test_normalize_repos_deduplication(temp_raw_data_dir: Path, test_config: Config) -> None:
    """Test that duplicate repo_ids are deduplicated (keeping first)."""
    repos_file = (
        temp_raw_data_dir / "year=2024" / "source=github" / "target=test-org" / "repos.jsonl"
    )
    repos_file.parent.mkdir(parents=True, exist_ok=True)

    # Create duplicate records
    with repos_file.open("w") as f:
        # First occurrence
        envelope1 = {
            "timestamp": "2024-12-18T00:00:00Z",
            "source": "github_rest",
            "endpoint": "/orgs/test-org/repos",
            "request_id": "test-1",
            "page": 1,
            "data": [
                {
                    "node_id": "R_DUP",
                    "name": "duplicate",
                    "full_name": "test-org/duplicate",
                    "owner": {"login": "test-org"},
                    "archived": False,
                    "fork": False,
                    "private": False,
                    "default_branch": "main",
                    "stargazers_count": 100,
                    "forks_count": 10,
                    "watchers_count": 90,
                    "topics": ["first"],
                    "language": "Python",
                    "created_at": "2023-01-01T00:00:00Z",
                    "pushed_at": "2024-01-01T00:00:00Z",
                }
            ],
        }
        f.write(json.dumps(envelope1) + "\n")

        # Duplicate occurrence (should be ignored)
        envelope2 = {
            "timestamp": "2024-12-18T00:01:00Z",
            "source": "github_rest",
            "endpoint": "/repos/test-org/duplicate",
            "request_id": "test-2",
            "page": 1,
            "data": {
                "node_id": "R_DUP",
                "name": "duplicate",
                "full_name": "test-org/duplicate",
                "owner": {"login": "test-org"},
                "archived": False,
                "fork": False,
                "private": False,
                "default_branch": "main",
                "stargazers_count": 200,  # Different value
                "forks_count": 20,
                "watchers_count": 180,
                "topics": ["second"],  # Different value
                "language": "Python",
                "created_at": "2023-01-01T00:00:00Z",
                "pushed_at": "2024-06-01T00:00:00Z",
            },
        }
        f.write(json.dumps(envelope2) + "\n")

    # Normalize
    df = normalize_repos(temp_raw_data_dir, test_config)

    # Should only have one record (first occurrence kept)
    assert len(df) == 1
    assert df.iloc[0]["repo_id"] == "R_DUP"
    assert df.iloc[0]["stars"] == 100  # From first occurrence
    assert df.iloc[0]["topics"] == "first"  # From first occurrence


def test_normalize_repos_missing_optional_fields(
    temp_raw_data_dir: Path, test_config: Config
) -> None:
    """Test handling of missing optional fields."""
    repos_file = (
        temp_raw_data_dir / "year=2024" / "source=github" / "target=test-org" / "repos.jsonl"
    )
    repos_file.parent.mkdir(parents=True, exist_ok=True)

    # Minimal repository record
    with repos_file.open("w") as f:
        envelope = {
            "timestamp": "2024-12-18T00:00:00Z",
            "source": "github_rest",
            "endpoint": "/orgs/test-org/repos",
            "request_id": "test-1",
            "page": 1,
            "data": [
                {
                    "node_id": "R_MIN",
                    "name": "minimal",
                    "full_name": "test-org/minimal",
                    "owner": {"login": "test-org"},
                    # Missing most optional fields
                }
            ],
        }
        f.write(json.dumps(envelope) + "\n")

    # Normalize
    df = normalize_repos(temp_raw_data_dir, test_config)

    # Should still create record with defaults
    assert len(df) == 1
    assert df.iloc[0]["repo_id"] == "R_MIN"
    assert df.iloc[0]["is_archived"] == False  # noqa: E712
    assert df.iloc[0]["is_fork"] == False  # noqa: E712
    assert df.iloc[0]["is_private"] == False  # noqa: E712
    assert df.iloc[0]["default_branch"] == "main"
    assert df.iloc[0]["stars"] == 0
    assert df.iloc[0]["forks"] == 0
    assert df.iloc[0]["watchers"] == 0
    assert df.iloc[0]["topics"] == ""
    assert pd.isna(df.iloc[0]["language"])
    assert pd.isna(df.iloc[0]["created_at"])
    assert pd.isna(df.iloc[0]["pushed_at"])


def test_normalize_repos_missing_required_fields(
    temp_raw_data_dir: Path, test_config: Config
) -> None:
    """Test that records with missing required fields are skipped."""
    repos_file = (
        temp_raw_data_dir / "year=2024" / "source=github" / "target=test-org" / "repos.jsonl"
    )
    repos_file.parent.mkdir(parents=True, exist_ok=True)

    with repos_file.open("w") as f:
        # Valid record
        envelope1 = {
            "timestamp": "2024-12-18T00:00:00Z",
            "source": "github_rest",
            "endpoint": "/orgs/test-org/repos",
            "request_id": "test-1",
            "page": 1,
            "data": [
                {
                    "node_id": "R_VALID",
                    "name": "valid",
                    "full_name": "test-org/valid",
                    "owner": {"login": "test-org"},
                }
            ],
        }
        f.write(json.dumps(envelope1) + "\n")

        # Missing node_id
        envelope2 = {
            "timestamp": "2024-12-18T00:00:00Z",
            "source": "github_rest",
            "endpoint": "/orgs/test-org/repos",
            "request_id": "test-2",
            "page": 1,
            "data": [
                {
                    "name": "invalid1",
                    "full_name": "test-org/invalid1",
                    "owner": {"login": "test-org"},
                }
            ],
        }
        f.write(json.dumps(envelope2) + "\n")

        # Missing owner
        envelope3 = {
            "timestamp": "2024-12-18T00:00:00Z",
            "source": "github_rest",
            "endpoint": "/orgs/test-org/repos",
            "request_id": "test-3",
            "page": 1,
            "data": [
                {"node_id": "R_INVALID2", "name": "invalid2", "full_name": "test-org/invalid2"}
            ],
        }
        f.write(json.dumps(envelope3) + "\n")

    # Normalize
    df = normalize_repos(temp_raw_data_dir, test_config)

    # Should only have the valid record
    assert len(df) == 1
    assert df.iloc[0]["repo_id"] == "R_VALID"


def test_normalize_repos_empty_file(temp_raw_data_dir: Path, test_config: Config) -> None:
    """Test handling of empty JSONL file."""
    repos_file = (
        temp_raw_data_dir / "year=2024" / "source=github" / "target=test-org" / "repos.jsonl"
    )
    repos_file.parent.mkdir(parents=True, exist_ok=True)
    repos_file.touch()  # Create empty file

    # Normalize
    df = normalize_repos(temp_raw_data_dir, test_config)

    # Should return empty DataFrame with correct schema
    assert len(df) == 0
    assert list(df.columns) == [
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


def test_normalize_repos_file_not_found(temp_raw_data_dir: Path, test_config: Config) -> None:
    """Test error when repos.jsonl file is missing."""
    # Don't create the file

    with pytest.raises(FileNotFoundError, match="Repository data file not found"):
        normalize_repos(temp_raw_data_dir, test_config)


def test_normalize_repos_invalid_json(temp_raw_data_dir: Path, test_config: Config) -> None:
    """Test handling of malformed JSON lines."""
    repos_file = (
        temp_raw_data_dir / "year=2024" / "source=github" / "target=test-org" / "repos.jsonl"
    )
    repos_file.parent.mkdir(parents=True, exist_ok=True)

    with repos_file.open("w") as f:
        # Valid line
        envelope1 = {
            "timestamp": "2024-12-18T00:00:00Z",
            "source": "github_rest",
            "endpoint": "/orgs/test-org/repos",
            "request_id": "test-1",
            "page": 1,
            "data": [
                {
                    "node_id": "R_VALID",
                    "name": "valid",
                    "full_name": "test-org/valid",
                    "owner": {"login": "test-org"},
                }
            ],
        }
        f.write(json.dumps(envelope1) + "\n")

        # Invalid JSON line
        f.write("{ invalid json }\n")

        # Another valid line
        envelope2 = {
            "timestamp": "2024-12-18T00:01:00Z",
            "source": "github_rest",
            "endpoint": "/orgs/test-org/repos",
            "request_id": "test-2",
            "page": 2,
            "data": [
                {
                    "node_id": "R_VALID2",
                    "name": "valid2",
                    "full_name": "test-org/valid2",
                    "owner": {"login": "test-org"},
                }
            ],
        }
        f.write(json.dumps(envelope2) + "\n")

    # Normalize (should skip invalid line)
    df = normalize_repos(temp_raw_data_dir, test_config)

    # Should have two valid records
    assert len(df) == 2
    assert "R_VALID" in df["repo_id"].values
    assert "R_VALID2" in df["repo_id"].values


def test_normalize_repos_timestamp_normalization(
    temp_raw_data_dir: Path, test_config: Config
) -> None:
    """Test timestamp normalization to UTC."""
    repos_file = (
        temp_raw_data_dir / "year=2024" / "source=github" / "target=test-org" / "repos.jsonl"
    )
    repos_file.parent.mkdir(parents=True, exist_ok=True)

    with repos_file.open("w") as f:
        envelope = {
            "timestamp": "2024-12-18T00:00:00Z",
            "source": "github_rest",
            "endpoint": "/orgs/test-org/repos",
            "request_id": "test-1",
            "page": 1,
            "data": [
                {
                    "node_id": "R_TS",
                    "name": "timestamps",
                    "full_name": "test-org/timestamps",
                    "owner": {"login": "test-org"},
                    "created_at": "2023-01-15T10:30:00Z",  # Z notation
                    "pushed_at": "2024-06-20T14:45:00+00:00",  # +00:00 notation
                }
            ],
        }
        f.write(json.dumps(envelope) + "\n")

    # Normalize
    df = normalize_repos(temp_raw_data_dir, test_config)

    # Both should be normalized to UTC ISO 8601 format
    assert df.iloc[0]["created_at"] == "2023-01-15T10:30:00+00:00"
    assert df.iloc[0]["pushed_at"] == "2024-06-20T14:45:00+00:00"


def test_normalize_repos_deterministic_ordering(
    temp_raw_data_dir: Path, test_config: Config
) -> None:
    """Test that output is sorted by repo_id for determinism."""
    repos_file = (
        temp_raw_data_dir / "year=2024" / "source=github" / "target=test-org" / "repos.jsonl"
    )
    repos_file.parent.mkdir(parents=True, exist_ok=True)

    # Create records in non-sorted order
    with repos_file.open("w") as f:
        envelope = {
            "timestamp": "2024-12-18T00:00:00Z",
            "source": "github_rest",
            "endpoint": "/orgs/test-org/repos",
            "request_id": "test-1",
            "page": 1,
            "data": [
                {
                    "node_id": "R_300",
                    "name": "charlie",
                    "full_name": "test-org/charlie",
                    "owner": {"login": "test-org"},
                },
                {
                    "node_id": "R_100",
                    "name": "alpha",
                    "full_name": "test-org/alpha",
                    "owner": {"login": "test-org"},
                },
                {
                    "node_id": "R_200",
                    "name": "beta",
                    "full_name": "test-org/beta",
                    "owner": {"login": "test-org"},
                },
            ],
        }
        f.write(json.dumps(envelope) + "\n")

    # Normalize
    df = normalize_repos(temp_raw_data_dir, test_config)

    # Should be sorted by repo_id
    assert len(df) == 3
    assert df.iloc[0]["repo_id"] == "R_100"
    assert df.iloc[1]["repo_id"] == "R_200"
    assert df.iloc[2]["repo_id"] == "R_300"
