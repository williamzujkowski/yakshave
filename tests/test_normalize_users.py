"""Tests for user dimension normalization."""

import json
from pathlib import Path

import polars as pl
import pytest

from gh_year_end.config import Config
from gh_year_end.normalize.users import normalize_users


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    """Create temporary data directory structure."""
    raw_dir = tmp_path / "raw/year=2025/source=github/target=test-org"
    raw_dir.mkdir(parents=True)
    (raw_dir / "pulls").mkdir()
    (raw_dir / "issues").mkdir()
    (raw_dir / "reviews").mkdir()
    (raw_dir / "issue_comments").mkdir()
    (raw_dir / "review_comments").mkdir()
    (raw_dir / "commits").mkdir()
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
            "identity": {
                "bots": {
                    "exclude_patterns": [r".*\[bot\]$", r"^dependabot$"],
                    "include_overrides": ["trusted-bot[bot]"],
                }
            },
        }
    )


class TestNormalizeUsers:
    """Tests for normalize_users function."""

    def test_empty_data_returns_empty_dataframe(
        self,
        config: Config,
        temp_data_dir: Path,  # noqa: ARG002
    ) -> None:
        """Test that normalize_users returns empty DataFrame when no data."""
        df = normalize_users(config)

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0
        assert df.columns == [
            "user_id",
            "login",
            "type",
            "profile_url",
            "is_bot",
            "bot_reason",
            "display_name",
        ]

    def test_extract_users_from_repos(self, config: Config, temp_data_dir: Path) -> None:
        """Test extracting users from repos.jsonl."""
        repos_file = temp_data_dir / "repos.jsonl"
        with repos_file.open("w") as f:
            record = {
                "timestamp": "2025-01-01T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/orgs/test-org/repos",
                "request_id": "123",
                "page": 1,
                "data": {
                    "id": 1,
                    "name": "test-repo",
                    "owner": {
                        "node_id": "U_alice",
                        "login": "alice",
                        "type": "User",
                        "html_url": "https://github.com/alice",
                        "name": "Alice Smith",
                    },
                },
            }
            f.write(json.dumps(record) + "\n")

        df = normalize_users(config)

        assert len(df) == 1
        row = df.row(0, named=True)
        assert row["user_id"] == "U_alice"
        assert row["login"] == "alice"
        assert row["type"] == "User"
        assert row["profile_url"] == "https://github.com/alice"
        assert row["is_bot"] is False
        assert row["bot_reason"] is None
        assert row["display_name"] == "Alice Smith"

    def test_extract_users_from_pulls(self, config: Config, temp_data_dir: Path) -> None:
        """Test extracting users from pulls."""
        pulls_file = temp_data_dir / "pulls/test-org__test-repo.jsonl"
        with pulls_file.open("w") as f:
            record = {
                "timestamp": "2025-01-01T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls",
                "request_id": "123",
                "page": 1,
                "data": [
                    {
                        "id": 1,
                        "number": 1,
                        "user": {
                            "node_id": "U_bob",
                            "login": "bob",
                            "type": "User",
                            "html_url": "https://github.com/bob",
                        },
                    }
                ],
            }
            f.write(json.dumps(record) + "\n")

        df = normalize_users(config)

        assert len(df) == 1
        row = df.row(0, named=True)
        assert row["user_id"] == "U_bob"
        assert row["login"] == "bob"
        assert row["is_bot"] is False

    def test_bot_detection_by_pattern(self, config: Config, temp_data_dir: Path) -> None:
        """Test bot detection using exclude patterns."""
        pulls_file = temp_data_dir / "pulls/test-org__test-repo.jsonl"
        with pulls_file.open("w") as f:
            record = {
                "timestamp": "2025-01-01T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls",
                "request_id": "123",
                "page": 1,
                "data": [
                    {
                        "id": 1,
                        "number": 1,
                        "user": {
                            "node_id": "U_bot1",
                            "login": "dependabot[bot]",
                            "type": "User",
                            "html_url": "https://github.com/apps/dependabot",
                        },
                    },
                    {
                        "id": 2,
                        "number": 2,
                        "user": {
                            "node_id": "U_bot2",
                            "login": "dependabot",
                            "type": "User",
                            "html_url": "https://github.com/dependabot",
                        },
                    },
                ],
            }
            f.write(json.dumps(record) + "\n")

        df = normalize_users(config)

        assert len(df) == 2

        # Both should be detected as bots
        bot1 = df.filter(pl.col("login") == "dependabot[bot]").row(0, named=True)
        assert bot1["is_bot"] is True
        assert "matches pattern" in bot1["bot_reason"]

        bot2 = df.filter(pl.col("login") == "dependabot").row(0, named=True)
        assert bot2["is_bot"] is True
        assert "matches pattern" in bot2["bot_reason"]

    def test_bot_detection_by_type(self, config: Config, temp_data_dir: Path) -> None:
        """Test bot detection by type field."""
        pulls_file = temp_data_dir / "pulls/test-org__test-repo.jsonl"
        with pulls_file.open("w") as f:
            record = {
                "timestamp": "2025-01-01T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls",
                "request_id": "123",
                "page": 1,
                "data": [
                    {
                        "id": 1,
                        "number": 1,
                        "user": {
                            "node_id": "U_bot",
                            "login": "some-bot",
                            "type": "Bot",
                            "html_url": "https://github.com/apps/some-bot",
                        },
                    }
                ],
            }
            f.write(json.dumps(record) + "\n")

        df = normalize_users(config)

        assert len(df) == 1
        row = df.row(0, named=True)
        assert row["is_bot"] is True
        assert row["bot_reason"] == "type is Bot"

    def test_include_override(self, config: Config, temp_data_dir: Path) -> None:
        """Test that include_overrides force users to be treated as humans."""
        pulls_file = temp_data_dir / "pulls/test-org__test-repo.jsonl"
        with pulls_file.open("w") as f:
            record = {
                "timestamp": "2025-01-01T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls",
                "request_id": "123",
                "page": 1,
                "data": [
                    {
                        "id": 1,
                        "number": 1,
                        "user": {
                            "node_id": "U_trusted",
                            "login": "trusted-bot[bot]",
                            "type": "User",
                            "html_url": "https://github.com/trusted-bot",
                        },
                    }
                ],
            }
            f.write(json.dumps(record) + "\n")

        df = normalize_users(config)

        assert len(df) == 1
        row = df.row(0, named=True)
        assert row["login"] == "trusted-bot[bot]"
        assert row["is_bot"] is False
        assert row["bot_reason"] is None

    def test_deduplication(self, config: Config, temp_data_dir: Path) -> None:
        """Test that duplicate users are deduplicated by user_id."""
        pulls_file = temp_data_dir / "pulls/test-org__test-repo.jsonl"
        with pulls_file.open("w") as f:
            # Same user appears in multiple PRs
            for i in range(3):
                record = {
                    "timestamp": "2025-01-01T00:00:00Z",
                    "source": "github_rest",
                    "endpoint": "/repos/test-org/test-repo/pulls",
                    "request_id": f"123-{i}",
                    "page": 1,
                    "data": [
                        {
                            "id": i + 1,
                            "number": i + 1,
                            "user": {
                                "node_id": "U_alice",
                                "login": "alice",
                                "type": "User",
                                "html_url": "https://github.com/alice",
                            },
                        }
                    ],
                }
                f.write(json.dumps(record) + "\n")

        df = normalize_users(config)

        # Should only have one user despite appearing 3 times
        assert len(df) == 1
        assert df.row(0, named=True)["user_id"] == "U_alice"

    def test_deterministic_sorting(self, config: Config, temp_data_dir: Path) -> None:
        """Test that output is sorted by user_id for determinism."""
        pulls_file = temp_data_dir / "pulls/test-org__test-repo.jsonl"
        with pulls_file.open("w") as f:
            record = {
                "timestamp": "2025-01-01T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls",
                "request_id": "123",
                "page": 1,
                "data": [
                    {
                        "id": 3,
                        "number": 3,
                        "user": {
                            "node_id": "U_charlie",
                            "login": "charlie",
                            "type": "User",
                            "html_url": "https://github.com/charlie",
                        },
                    },
                    {
                        "id": 1,
                        "number": 1,
                        "user": {
                            "node_id": "U_alice",
                            "login": "alice",
                            "type": "User",
                            "html_url": "https://github.com/alice",
                        },
                    },
                    {
                        "id": 2,
                        "number": 2,
                        "user": {
                            "node_id": "U_bob",
                            "login": "bob",
                            "type": "User",
                            "html_url": "https://github.com/bob",
                        },
                    },
                ],
            }
            f.write(json.dumps(record) + "\n")

        df = normalize_users(config)

        # Should be sorted by user_id
        assert len(df) == 3
        user_ids = df.select("user_id").to_series().to_list()
        assert user_ids == ["U_alice", "U_bob", "U_charlie"]

    def test_extract_from_multiple_sources(self, config: Config, temp_data_dir: Path) -> None:
        """Test extracting users from multiple data sources."""
        # Add users in pulls
        pulls_file = temp_data_dir / "pulls/test-org__test-repo.jsonl"
        with pulls_file.open("w") as f:
            record = {
                "timestamp": "2025-01-01T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls",
                "request_id": "123",
                "page": 1,
                "data": [
                    {
                        "id": 1,
                        "number": 1,
                        "user": {
                            "node_id": "U_alice",
                            "login": "alice",
                            "type": "User",
                            "html_url": "https://github.com/alice",
                        },
                    }
                ],
            }
            f.write(json.dumps(record) + "\n")

        # Add users in issues
        issues_file = temp_data_dir / "issues/test-org__test-repo.jsonl"
        with issues_file.open("w") as f:
            record = {
                "timestamp": "2025-01-01T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/issues",
                "request_id": "456",
                "page": 1,
                "data": [
                    {
                        "id": 10,
                        "number": 10,
                        "user": {
                            "node_id": "U_bob",
                            "login": "bob",
                            "type": "User",
                            "html_url": "https://github.com/bob",
                        },
                    }
                ],
            }
            f.write(json.dumps(record) + "\n")

        # Add users in reviews
        reviews_file = temp_data_dir / "reviews/test-org__test-repo.jsonl"
        with reviews_file.open("w") as f:
            record = {
                "timestamp": "2025-01-01T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls/1/reviews",
                "request_id": "789",
                "page": 1,
                "data": [
                    {
                        "id": 100,
                        "user": {
                            "node_id": "U_charlie",
                            "login": "charlie",
                            "type": "User",
                            "html_url": "https://github.com/charlie",
                        },
                    }
                ],
            }
            f.write(json.dumps(record) + "\n")

        df = normalize_users(config)

        # Should have all unique users from all sources
        assert len(df) == 3
        logins = df.select("login").to_series().to_list()
        assert sorted(logins) == ["alice", "bob", "charlie"]

    def test_handle_assignees_and_reviewers(self, config: Config, temp_data_dir: Path) -> None:
        """Test extracting users from assignees and requested_reviewers lists."""
        pulls_file = temp_data_dir / "pulls/test-org__test-repo.jsonl"
        with pulls_file.open("w") as f:
            record = {
                "timestamp": "2025-01-01T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls",
                "request_id": "123",
                "page": 1,
                "data": [
                    {
                        "id": 1,
                        "number": 1,
                        "user": {
                            "node_id": "U_alice",
                            "login": "alice",
                            "type": "User",
                            "html_url": "https://github.com/alice",
                        },
                        "assignees": [
                            {
                                "node_id": "U_bob",
                                "login": "bob",
                                "type": "User",
                                "html_url": "https://github.com/bob",
                            }
                        ],
                        "requested_reviewers": [
                            {
                                "node_id": "U_charlie",
                                "login": "charlie",
                                "type": "User",
                                "html_url": "https://github.com/charlie",
                            }
                        ],
                    }
                ],
            }
            f.write(json.dumps(record) + "\n")

        df = normalize_users(config)

        # Should extract all users: author, assignee, and reviewer
        assert len(df) == 3
        logins = df.select("login").to_series().to_list()
        assert sorted(logins) == ["alice", "bob", "charlie"]
