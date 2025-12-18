"""Tests for issue normalization."""

import json
from pathlib import Path

import pandas as pd
import pytest

from gh_year_end.config import Config
from gh_year_end.normalize.issues import normalize_issues


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    """Create temporary data directory structure."""
    raw_dir = tmp_path / "raw/year=2025/source=github/target=test-org"
    raw_dir.mkdir(parents=True)
    (raw_dir / "issues").mkdir()
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


class TestNormalizeIssues:
    """Tests for normalize_issues function."""

    def test_empty_directory_returns_empty_dataframe(
        self, config: Config, temp_data_dir: Path
    ) -> None:
        """Test that empty issues directory returns empty DataFrame."""
        df = normalize_issues(temp_data_dir, config)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert "issue_id" in df.columns
        assert "state" in df.columns

    def test_normalizes_single_issue(self, config: Config, temp_data_dir: Path) -> None:
        """Test normalizing a single issue."""
        issues_file = temp_data_dir / "issues" / "test-org__test-repo.jsonl"
        with issues_file.open("w") as f:
            envelope = {
                "timestamp": "2025-01-15T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/issues/1",
                "request_id": "123",
                "page": 1,
                "data": {
                    "node_id": "I_1",
                    "number": 1,
                    "state": "open",
                    "title": "Test Issue",
                    "body": "Test description",
                    "user": {"node_id": "U_alice", "login": "alice"},
                    "repository": {
                        "node_id": "R_test",
                        "full_name": "test-org/test-repo",
                    },
                    "created_at": "2025-01-01T10:00:00Z",
                    "updated_at": "2025-01-02T10:00:00Z",
                    "closed_at": None,
                    "labels": [{"name": "bug"}],
                },
            }
            f.write(json.dumps(envelope) + "\n")

        df = normalize_issues(temp_data_dir, config)

        assert len(df) == 1
        row = df.iloc[0]
        assert row["issue_id"] == "I_1"
        assert row["repo_id"] == "R_test"
        assert row["number"] == 1
        assert row["author_user_id"] == "U_alice"
        assert row["state"] == "open"
        assert row["labels"] == "bug"
        assert row["title_len"] == 10
        assert row["body_len"] == 16

    def test_filters_out_pull_requests(self, config: Config, temp_data_dir: Path) -> None:
        """Test that pull requests are filtered out."""
        issues_file = temp_data_dir / "issues" / "test-org__test-repo.jsonl"
        with issues_file.open("w") as f:
            # Regular issue
            envelope1 = {
                "timestamp": "2025-01-15T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/issues/1",
                "request_id": "123",
                "page": 1,
                "data": {
                    "node_id": "I_1",
                    "number": 1,
                    "state": "open",
                    "title": "Issue",
                    "body": "",
                    "user": {"node_id": "U_alice"},
                    "repository": {"node_id": "R_test"},
                    "created_at": "2025-01-01T10:00:00Z",
                    "updated_at": "2025-01-02T10:00:00Z",
                    "labels": [],
                },
            }
            # Pull request (has pull_request field)
            envelope2 = {
                "timestamp": "2025-01-15T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/issues/2",
                "request_id": "124",
                "page": 1,
                "data": {
                    "node_id": "I_2",
                    "number": 2,
                    "state": "open",
                    "title": "PR",
                    "body": "",
                    "user": {"node_id": "U_bob"},
                    "repository": {"node_id": "R_test"},
                    "created_at": "2025-01-01T10:00:00Z",
                    "updated_at": "2025-01-02T10:00:00Z",
                    "labels": [],
                    "pull_request": {"url": "https://api.github.com/repos/..."},
                },
            }
            f.write(json.dumps(envelope1) + "\n")
            f.write(json.dumps(envelope2) + "\n")

        df = normalize_issues(temp_data_dir, config)

        # Should only have the issue, not the PR
        assert len(df) == 1
        assert df.iloc[0]["issue_id"] == "I_1"
