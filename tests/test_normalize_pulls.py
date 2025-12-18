"""Tests for pull request normalization."""

import json
from pathlib import Path

import pandas as pd
import pytest

from gh_year_end.config import Config
from gh_year_end.normalize.pulls import normalize_pulls


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    """Create temporary data directory structure."""
    raw_dir = tmp_path / "raw/year=2025/source=github/target=test-org"
    raw_dir.mkdir(parents=True)
    (raw_dir / "pulls").mkdir()
    return raw_dir


@pytest.fixture
def config(tmp_path: Path) -> Config:
    """Create test configuration."""
    return Config.model_validate(
        {
            "github": {
                "target": {"mode": "org", "name": "test-org"},
                "windows": {
                    "year": 2025,
                    "since": "2025-01-01T00:00:00Z",
                    "until": "2026-01-01T00:00:00Z",
                },
            },
            "storage": {"root": str(tmp_path)},
        }
    )


class TestNormalizePulls:
    """Tests for normalize_pulls function."""

    def test_empty_directory_returns_empty_dataframe(
        self, config: Config, temp_data_dir: Path
    ) -> None:
        """Test that empty pulls directory returns empty DataFrame."""
        df = normalize_pulls(temp_data_dir, config)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert "pr_id" in df.columns
        assert "repo_id" in df.columns
        assert "state" in df.columns

    def test_normalizes_single_pr(self, config: Config, temp_data_dir: Path) -> None:
        """Test normalizing a single PR."""
        pulls_file = temp_data_dir / "pulls" / "test-org__test-repo.jsonl"
        with pulls_file.open("w") as f:
            envelope = {
                "timestamp": "2025-01-15T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls/1",
                "request_id": "123",
                "page": 1,
                "data": {
                    "node_id": "PR_1",
                    "number": 1,
                    "state": "open",
                    "draft": False,
                    "title": "Test PR",
                    "body": "Test description",
                    "user": {"node_id": "U_alice", "login": "alice"},
                    "base": {"repo": {"node_id": "R_test", "full_name": "test-org/test-repo"}},
                    "created_at": "2025-01-01T10:00:00Z",
                    "updated_at": "2025-01-02T10:00:00Z",
                    "closed_at": None,
                    "merged_at": None,
                    "labels": [{"name": "bug"}],
                    "milestone": None,
                    "additions": 10,
                    "deletions": 5,
                    "changed_files": 2,
                },
            }
            f.write(json.dumps(envelope) + "\n")

        df = normalize_pulls(temp_data_dir, config)

        assert len(df) == 1
        row = df.iloc[0]
        assert row["pr_id"] == "PR_1"
        assert row["repo_id"] == "R_test"
        assert row["number"] == 1
        assert row["author_user_id"] == "U_alice"
        assert row["state"] == "open"
        assert row["is_draft"] == False  # noqa: E712
        assert row["labels"] == "bug"
        assert row["title_len"] == 7  # len("Test PR")
        assert row["body_len"] == 16  # len("Test description")
        assert row["additions"] == 10
        assert row["deletions"] == 5
        assert row["changed_files"] == 2

    def test_normalizes_merged_pr_state(self, config: Config, temp_data_dir: Path) -> None:
        """Test that merged PRs get state='merged'."""
        pulls_file = temp_data_dir / "pulls" / "test-org__test-repo.jsonl"
        with pulls_file.open("w") as f:
            envelope = {
                "timestamp": "2025-01-15T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls/1",
                "request_id": "123",
                "page": 1,
                "data": {
                    "node_id": "PR_1",
                    "number": 1,
                    "state": "closed",
                    "draft": False,
                    "title": "Test PR",
                    "body": None,
                    "user": {"node_id": "U_alice", "login": "alice"},
                    "base": {"repo": {"node_id": "R_test", "full_name": "test-org/test-repo"}},
                    "created_at": "2025-01-01T10:00:00Z",
                    "updated_at": "2025-01-02T10:00:00Z",
                    "closed_at": "2025-01-02T10:00:00Z",
                    "merged_at": "2025-01-02T10:00:00Z",
                    "labels": [],
                    "milestone": None,
                },
            }
            f.write(json.dumps(envelope) + "\n")

        df = normalize_pulls(temp_data_dir, config)

        assert len(df) == 1
        assert df.iloc[0]["state"] == "merged"
        assert df.iloc[0]["body_len"] == 0  # None body should be length 0

    def test_normalizes_multiple_prs(self, config: Config, temp_data_dir: Path) -> None:
        """Test normalizing multiple PRs."""
        pulls_file = temp_data_dir / "pulls" / "test-org__test-repo.jsonl"
        with pulls_file.open("w") as f:
            for i in range(1, 4):
                envelope = {
                    "timestamp": "2025-01-15T00:00:00Z",
                    "source": "github_rest",
                    "endpoint": f"/repos/test-org/test-repo/pulls/{i}",
                    "request_id": f"123-{i}",
                    "page": 1,
                    "data": {
                        "node_id": f"PR_{i}",
                        "number": i,
                        "state": "open",
                        "draft": False,
                        "title": f"PR {i}",
                        "body": "",
                        "user": {"node_id": "U_alice", "login": "alice"},
                        "base": {
                            "repo": {
                                "node_id": "R_test",
                                "full_name": "test-org/test-repo",
                            }
                        },
                        "created_at": "2025-01-01T10:00:00Z",
                        "updated_at": "2025-01-02T10:00:00Z",
                        "closed_at": None,
                        "merged_at": None,
                        "labels": [],
                        "milestone": None,
                    },
                }
                f.write(json.dumps(envelope) + "\n")

        df = normalize_pulls(temp_data_dir, config)

        assert len(df) == 3
        # Check deterministic ordering by pr_id
        assert df["pr_id"].tolist() == ["PR_1", "PR_2", "PR_3"]

    def test_skips_pr_with_missing_required_fields(
        self, config: Config, temp_data_dir: Path
    ) -> None:
        """Test that PRs with missing required fields are skipped."""
        pulls_file = temp_data_dir / "pulls" / "test-org__test-repo.jsonl"
        with pulls_file.open("w") as f:
            # PR missing node_id
            envelope = {
                "timestamp": "2025-01-15T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls/1",
                "request_id": "123",
                "page": 1,
                "data": {
                    "number": 1,
                    "state": "open",
                    "title": "Test PR",
                    "user": {"node_id": "U_alice"},
                    "base": {"repo": {"node_id": "R_test"}},
                },
            }
            f.write(json.dumps(envelope) + "\n")

        df = normalize_pulls(temp_data_dir, config)

        assert len(df) == 0

    def test_handles_multiple_labels(self, config: Config, temp_data_dir: Path) -> None:
        """Test handling multiple labels."""
        pulls_file = temp_data_dir / "pulls" / "test-org__test-repo.jsonl"
        with pulls_file.open("w") as f:
            envelope = {
                "timestamp": "2025-01-15T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls/1",
                "request_id": "123",
                "page": 1,
                "data": {
                    "node_id": "PR_1",
                    "number": 1,
                    "state": "open",
                    "draft": False,
                    "title": "Test",
                    "body": None,
                    "user": {"node_id": "U_alice"},
                    "base": {"repo": {"node_id": "R_test"}},
                    "created_at": "2025-01-01T10:00:00Z",
                    "updated_at": "2025-01-02T10:00:00Z",
                    "labels": [{"name": "bug"}, {"name": "enhancement"}, {"name": "docs"}],
                },
            }
            f.write(json.dumps(envelope) + "\n")

        df = normalize_pulls(temp_data_dir, config)

        assert len(df) == 1
        # Labels should be sorted
        assert df.iloc[0]["labels"] == "bug,docs,enhancement"
