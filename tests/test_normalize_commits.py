"""Tests for commit normalizers."""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from gh_year_end.config import Config
from gh_year_end.normalize.commits import (
    _is_ci_file,
    _is_docs_file,
    _is_iac_file,
    _is_test_file,
    normalize_commit_files,
    normalize_commits,
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
        "collection": {
            "commits": {
                "include_files": True,
                "classify_files": True,
            },
        },
    }
    return Config.model_validate(config_dict)


@pytest.fixture
def paths(temp_config: Config) -> PathManager:
    """Create path manager with temp config."""
    paths = PathManager(temp_config)
    paths.ensure_directories()
    return paths


def test_normalize_commits_empty(paths: PathManager, temp_config: Config) -> None:
    """Test normalizing commits with no data."""
    normalize_commits(paths, temp_config)

    output_path = paths.curated_path("fact_commit")
    assert output_path.exists()

    parquet_file = pq.ParquetFile(output_path)
    table = parquet_file.read()
    assert len(table) == 0


def test_normalize_commits_with_data(
    paths: PathManager,
    temp_config: Config,
) -> None:
    """Test normalizing commits with sample data."""
    # Create sample commits data
    commits_dir = paths.raw_root / "commits"
    commits_dir.mkdir(parents=True, exist_ok=True)

    sample_commit = {
        "timestamp": "2025-01-15T12:00:00Z",
        "source": "github_rest",
        "endpoint": "/repos/test-org/repo1/commits",
        "request_id": "test-id-1",
        "page": 1,
        "data": {
            "sha": "abc123def456",
            "node_id": "C_commit123",
            "author": {
                "login": "author",
                "node_id": "U_author123",
                "type": "User",
            },
            "committer": {
                "login": "committer",
                "node_id": "U_committer123",
                "type": "User",
            },
            "commit": {
                "author": {
                    "name": "Author Name",
                    "email": "author@example.com",
                    "date": "2025-01-15T12:00:00Z",
                },
                "committer": {
                    "name": "Committer Name",
                    "email": "committer@example.com",
                    "date": "2025-01-15T12:00:00Z",
                },
                "message": "Fix bug in feature X",
            },
        },
    }

    jsonl_path = commits_dir / "test-org__repo1.jsonl"
    with jsonl_path.open("w") as f:
        f.write(json.dumps(sample_commit) + "\n")

    normalize_commits(paths, temp_config)

    output_path = paths.curated_path("fact_commit")
    assert output_path.exists()

    parquet_file = pq.ParquetFile(output_path)
    table = parquet_file.read()
    assert len(table) == 1

    # Verify schema
    assert "commit_sha" in table.column_names
    assert "repo_id" in table.column_names
    assert "author_user_id" in table.column_names
    assert "committer_user_id" in table.column_names
    assert "authored_at" in table.column_names
    assert "committed_at" in table.column_names
    assert "message_len" in table.column_names
    assert "year" in table.column_names

    # Verify data
    row = table.to_pylist()[0]
    assert row["commit_sha"] == "abc123def456"
    assert row["author_user_id"] == "U_author123"
    assert row["committer_user_id"] == "U_committer123"
    assert row["message_len"] == len("Fix bug in feature X")
    assert row["year"] == 2025


def test_normalize_commits_null_author(
    paths: PathManager,
    temp_config: Config,
) -> None:
    """Test normalizing commits with null author (unsigned commit)."""
    commits_dir = paths.raw_root / "commits"
    commits_dir.mkdir(parents=True, exist_ok=True)

    sample_commit = {
        "timestamp": "2025-01-15T12:00:00Z",
        "source": "github_rest",
        "endpoint": "/repos/test-org/repo1/commits",
        "request_id": "test-id-2",
        "page": 1,
        "data": {
            "sha": "xyz789abc456",
            "node_id": "C_commit456",
            "author": None,  # Unsigned commit
            "committer": None,
            "commit": {
                "author": {
                    "name": "Unknown Author",
                    "email": "unknown@example.com",
                    "date": "2025-01-15T12:00:00Z",
                },
                "committer": {
                    "name": "Unknown Committer",
                    "email": "unknown@example.com",
                    "date": "2025-01-15T12:00:00Z",
                },
                "message": "Unsigned commit",
            },
        },
    }

    jsonl_path = commits_dir / "test-org__repo1.jsonl"
    with jsonl_path.open("w") as f:
        f.write(json.dumps(sample_commit) + "\n")

    normalize_commits(paths, temp_config)

    output_path = paths.curated_path("fact_commit")
    parquet_file = pq.ParquetFile(output_path)
    table = parquet_file.read()
    assert len(table) == 1

    row = table.to_pylist()[0]
    assert row["author_user_id"] is None
    assert row["committer_user_id"] is None


def test_normalize_commit_files_empty(paths: PathManager, temp_config: Config) -> None:
    """Test normalizing commit files with no data."""
    normalize_commit_files(paths, temp_config)

    output_path = paths.curated_path("fact_commit_file")
    assert output_path.exists()

    parquet_file = pq.ParquetFile(output_path)
    table = parquet_file.read()
    assert len(table) == 0


def test_normalize_commit_files_with_data(
    paths: PathManager,
    temp_config: Config,
) -> None:
    """Test normalizing commit files with sample data."""
    commits_dir = paths.raw_root / "commits"
    commits_dir.mkdir(parents=True, exist_ok=True)

    sample_commit = {
        "timestamp": "2025-01-15T12:00:00Z",
        "source": "github_rest",
        "endpoint": "/repos/test-org/repo1/commits",
        "request_id": "test-id-3",
        "page": 1,
        "data": {
            "sha": "file123abc",
            "node_id": "C_commit789",
            "files": [
                {
                    "filename": "README.md",
                    "additions": 10,
                    "deletions": 2,
                    "changes": 12,
                },
                {
                    "filename": "src/main.py",
                    "additions": 50,
                    "deletions": 10,
                    "changes": 60,
                },
                {
                    "filename": "tests/test_main.py",
                    "additions": 30,
                    "deletions": 5,
                    "changes": 35,
                },
            ],
            "commit": {
                "author": {"date": "2025-01-15T12:00:00Z"},
                "committer": {"date": "2025-01-15T12:00:00Z"},
                "message": "Add tests",
            },
        },
    }

    jsonl_path = commits_dir / "test-org__repo1.jsonl"
    with jsonl_path.open("w") as f:
        f.write(json.dumps(sample_commit) + "\n")

    normalize_commit_files(paths, temp_config)

    output_path = paths.curated_path("fact_commit_file")
    assert output_path.exists()

    parquet_file = pq.ParquetFile(output_path)
    table = parquet_file.read()
    assert len(table) == 3

    # Verify schema
    assert "commit_sha" in table.column_names
    assert "repo_id" in table.column_names
    assert "path" in table.column_names
    assert "file_ext" in table.column_names
    assert "additions" in table.column_names
    assert "deletions" in table.column_names
    assert "changes" in table.column_names
    assert "is_docs" in table.column_names
    assert "is_iac" in table.column_names
    assert "is_test" in table.column_names
    assert "is_ci" in table.column_names
    assert "year" in table.column_names

    # Verify data
    rows = table.to_pylist()

    # README.md should be classified as docs
    readme_row = next(r for r in rows if r["path"] == "README.md")
    assert readme_row["file_ext"] == "md"
    assert readme_row["is_docs"] is True
    assert readme_row["is_test"] is False

    # test_main.py should be classified as test
    test_row = next(r for r in rows if r["path"] == "tests/test_main.py")
    assert test_row["file_ext"] == "py"
    assert test_row["is_test"] is True
    assert test_row["is_docs"] is False


def test_normalize_commit_files_disabled(
    tmp_path: Path,
) -> None:
    """Test normalizing commit files when disabled in config."""
    config_dict = {
        "github": {
            "target": {"mode": "org", "name": "test-org"},
            "windows": {
                "year": 2025,
                "since": "2025-01-01T00:00:00Z",
                "until": "2026-01-01T00:00:00Z",
            },
        },
        "storage": {"root": str(tmp_path / "disabled_test" / "data")},
        "collection": {
            "commits": {
                "include_files": False,  # Disabled
                "classify_files": True,
            },
        },
    }
    config = Config.model_validate(config_dict)
    paths_disabled = PathManager(config)
    paths_disabled.ensure_directories()

    normalize_commit_files(paths_disabled, config)

    output_path = paths_disabled.curated_path("fact_commit_file")
    assert output_path.exists()

    parquet_file = pq.ParquetFile(output_path)
    table = parquet_file.read()
    assert len(table) == 0


# File classification tests


def test_is_docs_file() -> None:
    """Test documentation file classification."""
    assert _is_docs_file("README.md") is True
    assert _is_docs_file("readme.txt") is True
    assert _is_docs_file("docs/api.md") is True
    assert _is_docs_file("CHANGELOG.md") is True
    assert _is_docs_file("CONTRIBUTING.md") is True
    assert _is_docs_file("LICENSE") is True

    # Not docs
    assert _is_docs_file("src/main.py") is False
    assert _is_docs_file("test.py") is False


def test_is_iac_file() -> None:
    """Test Infrastructure as Code file classification."""
    assert _is_iac_file("main.tf") is True
    assert _is_iac_file("variables.tfvars") is True
    assert _is_iac_file("Dockerfile") is True
    assert _is_iac_file("docker-compose.yml") is True
    assert _is_iac_file("k8s/deployment.yaml") is True
    assert _is_iac_file("terraform/main.tf") is True

    # Not IaC
    assert _is_iac_file("README.md") is False
    assert _is_iac_file("src/main.py") is False


def test_is_test_file() -> None:
    """Test test file classification."""
    assert _is_test_file("test_main.py") is True
    assert _is_test_file("main_test.py") is True
    assert _is_test_file("tests/test_app.py") is True
    assert _is_test_file("app.spec.ts") is True
    assert _is_test_file("app.test.js") is True
    assert _is_test_file("main_test.go") is True

    # Not test
    assert _is_test_file("README.md") is False
    assert _is_test_file("src/main.py") is False


def test_is_ci_file() -> None:
    """Test CI/CD file classification."""
    assert _is_ci_file(".github/workflows/ci.yml") is True
    assert _is_ci_file(".circleci/config.yml") is True
    assert _is_ci_file("Jenkinsfile") is True
    assert _is_ci_file(".gitlab-ci.yml") is True
    assert _is_ci_file(".travis.yml") is True
    assert _is_ci_file("azure-pipelines.yml") is True

    # Not CI
    assert _is_ci_file("README.md") is False
    assert _is_ci_file("src/main.py") is False
