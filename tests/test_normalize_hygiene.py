"""Tests for hygiene normalization."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from gh_year_end.config import Config
from gh_year_end.normalize.hygiene import (
    normalize_branch_protection,
    normalize_file_presence,
    normalize_security_features,
)


@pytest.fixture
def mock_config() -> Config:
    """Create a mock configuration for testing."""
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
            "storage": {"root": "./data"},
            "report": {"output_dir": "./site"},
        }
    )


@pytest.fixture
def temp_raw_data_dir(tmp_path: Path) -> Path:
    """Create temporary raw data directory structure."""
    raw_dir = tmp_path / "raw" / "year=2025" / "source=github" / "target=test-org"
    raw_dir.mkdir(parents=True)

    # Create subdirectories
    (raw_dir / "repo_tree").mkdir()
    (raw_dir / "branch_protection").mkdir()
    (raw_dir / "security_features").mkdir()

    return raw_dir


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    """Write enveloped records to JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for record in records:
            envelope = {
                "timestamp": record.get("timestamp", datetime.now(UTC).isoformat()),
                "source": record.get("source", "github_rest"),
                "endpoint": record.get("endpoint", "test"),
                "request_id": record.get("request_id", "test-id"),
                "page": record.get("page", 1),
                "data": record["data"],
            }
            f.write(json.dumps(envelope) + "\n")


class TestNormalizeFilePresence:
    """Tests for normalize_file_presence."""

    def test_normalize_file_presence_basic(
        self,
        temp_raw_data_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test basic file presence normalization."""
        # Write repos.jsonl
        write_jsonl(
            temp_raw_data_dir / "repos.jsonl",
            [
                {
                    "data": {
                        "full_name": "test-org/repo1",
                        "node_id": "R_001",
                    }
                }
            ],
        )

        # Write repo_tree JSONL with file presence records
        write_jsonl(
            temp_raw_data_dir / "repo_tree" / "test-org__repo1.jsonl",
            [
                {
                    "endpoint": "file_presence",
                    "timestamp": "2025-01-15T10:00:00Z",
                    "data": {
                        "repo": "test-org/repo1",
                        "path": "SECURITY.md",
                        "exists": True,
                        "sha": "abc123",
                        "size": 1024,
                    },
                },
                {
                    "endpoint": "file_presence",
                    "timestamp": "2025-01-15T10:00:00Z",
                    "data": {
                        "repo": "test-org/repo1",
                        "path": "README.md",
                        "exists": True,
                        "sha": "def456",
                        "size": 2048,
                    },
                },
                {
                    "endpoint": "file_presence",
                    "timestamp": "2025-01-15T10:00:00Z",
                    "data": {
                        "repo": "test-org/repo1",
                        "path": "CODEOWNERS",
                        "exists": False,
                        "sha": None,
                        "size": None,
                    },
                },
            ],
        )

        df = normalize_file_presence(temp_raw_data_dir, mock_config)

        assert len(df) == 3
        assert list(df.columns) == [
            "repo_id",
            "repo_full_name",
            "captured_at",
            "path",
            "exists",
            "sha",
            "size_bytes",
        ]

        # Check first row (CODEOWNERS - sorted by path)
        row = df.iloc[0]
        assert row["repo_id"] == "R_001"
        assert row["repo_full_name"] == "test-org/repo1"
        assert row["path"] == "CODEOWNERS"
        assert row["exists"] == False  # noqa: E712
        assert pd.isna(row["sha"])
        assert pd.isna(row["size_bytes"])

        # Check second row (README.md)
        row = df.iloc[1]
        assert row["path"] == "README.md"
        assert row["exists"] == True  # noqa: E712
        assert row["sha"] == "def456"
        assert row["size_bytes"] == 2048

        # Check third row (SECURITY.md)
        row = df.iloc[2]
        assert row["path"] == "SECURITY.md"
        assert row["exists"] == True  # noqa: E712
        assert row["sha"] == "abc123"
        assert row["size_bytes"] == 1024

    def test_normalize_file_presence_empty(
        self,
        temp_raw_data_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test normalization with no data."""
        # Write empty repos.jsonl
        write_jsonl(temp_raw_data_dir / "repos.jsonl", [])

        df = normalize_file_presence(temp_raw_data_dir, mock_config)

        assert len(df) == 0
        assert list(df.columns) == [
            "repo_id",
            "repo_full_name",
            "captured_at",
            "path",
            "exists",
            "sha",
            "size_bytes",
        ]

    def test_normalize_file_presence_multiple_repos(
        self,
        temp_raw_data_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test normalization with multiple repositories."""
        # Write repos.jsonl
        write_jsonl(
            temp_raw_data_dir / "repos.jsonl",
            [
                {"data": {"full_name": "test-org/repo1", "node_id": "R_001"}},
                {"data": {"full_name": "test-org/repo2", "node_id": "R_002"}},
            ],
        )

        # Write repo_tree for repo1
        write_jsonl(
            temp_raw_data_dir / "repo_tree" / "test-org__repo1.jsonl",
            [
                {
                    "endpoint": "file_presence",
                    "data": {
                        "repo": "test-org/repo1",
                        "path": "SECURITY.md",
                        "exists": True,
                        "sha": "abc123",
                        "size": 1024,
                    },
                }
            ],
        )

        # Write repo_tree for repo2
        write_jsonl(
            temp_raw_data_dir / "repo_tree" / "test-org__repo2.jsonl",
            [
                {
                    "endpoint": "file_presence",
                    "data": {
                        "repo": "test-org/repo2",
                        "path": "SECURITY.md",
                        "exists": False,
                        "sha": None,
                        "size": None,
                    },
                }
            ],
        )

        df = normalize_file_presence(temp_raw_data_dir, mock_config)

        assert len(df) == 2
        assert df["repo_full_name"].tolist() == ["test-org/repo1", "test-org/repo2"]


class TestNormalizeBranchProtection:
    """Tests for normalize_branch_protection."""

    def test_normalize_branch_protection_enabled(
        self,
        temp_raw_data_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test normalization with branch protection enabled."""
        # Write repos.jsonl
        write_jsonl(
            temp_raw_data_dir / "repos.jsonl",
            [{"data": {"full_name": "test-org/repo1", "node_id": "R_001"}}],
        )

        # Write branch_protection JSONL
        write_jsonl(
            temp_raw_data_dir / "branch_protection" / "test-org__repo1.jsonl",
            [
                {
                    "timestamp": "2025-01-15T10:00:00Z",
                    "data": {
                        "repo": "test-org/repo1",
                        "branch": "main",
                        "protection_enabled": True,
                        "required_reviews": {
                            "required_approving_review_count": 2,
                        },
                        "required_status_checks": {},
                        "allow_force_pushes": False,
                        "allow_deletions": False,
                        "enforce_admins": True,
                    },
                }
            ],
        )

        df = normalize_branch_protection(temp_raw_data_dir, mock_config)

        assert len(df) == 1
        row = df.iloc[0]

        assert row["repo_id"] == "R_001"
        assert row["repo_full_name"] == "test-org/repo1"
        assert row["default_branch"] == "main"
        assert row["requires_reviews"] == True  # noqa: E712
        assert row["required_approving_review_count"] == 2
        assert row["requires_status_checks"] == True  # noqa: E712
        assert row["allows_force_pushes"] == False  # noqa: E712
        assert row["allows_deletions"] == False  # noqa: E712
        assert row["enforce_admins"] == True  # noqa: E712
        assert pd.isna(row["error"])

    def test_normalize_branch_protection_disabled(
        self,
        temp_raw_data_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test normalization with branch protection disabled (404)."""
        # Write repos.jsonl
        write_jsonl(
            temp_raw_data_dir / "repos.jsonl",
            [{"data": {"full_name": "test-org/repo1", "node_id": "R_001"}}],
        )

        # Write branch_protection JSONL (404 case)
        write_jsonl(
            temp_raw_data_dir / "branch_protection" / "test-org__repo1.jsonl",
            [
                {
                    "timestamp": "2025-01-15T10:00:00Z",
                    "data": {
                        "repo": "test-org/repo1",
                        "branch": "main",
                        "protection_enabled": False,
                    },
                }
            ],
        )

        df = normalize_branch_protection(temp_raw_data_dir, mock_config)

        assert len(df) == 1
        row = df.iloc[0]

        assert row["repo_id"] == "R_001"
        assert row["default_branch"] == "main"
        assert pd.isna(row["requires_reviews"])
        assert pd.isna(row["required_approving_review_count"])
        assert pd.isna(row["requires_status_checks"])
        assert pd.isna(row["allows_force_pushes"])
        assert pd.isna(row["allows_deletions"])
        assert pd.isna(row["enforce_admins"])

    def test_normalize_branch_protection_permission_denied(
        self,
        temp_raw_data_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test normalization with permission denied (403)."""
        # Write repos.jsonl
        write_jsonl(
            temp_raw_data_dir / "repos.jsonl",
            [{"data": {"full_name": "test-org/repo1", "node_id": "R_001"}}],
        )

        # Write branch_protection JSONL (403 case)
        write_jsonl(
            temp_raw_data_dir / "branch_protection" / "test-org__repo1.jsonl",
            [
                {
                    "timestamp": "2025-01-15T10:00:00Z",
                    "data": {
                        "repo": "test-org/repo1",
                        "branch": "main",
                        "error": "403: Resource not accessible by integration",
                    },
                }
            ],
        )

        df = normalize_branch_protection(temp_raw_data_dir, mock_config)

        assert len(df) == 1
        row = df.iloc[0]

        assert row["repo_id"] == "R_001"
        assert row["error"] == "403: Resource not accessible by integration"
        # All protection fields should be None
        assert pd.isna(row["requires_reviews"])
        assert pd.isna(row["required_approving_review_count"])


class TestNormalizeSecurityFeatures:
    """Tests for normalize_security_features."""

    def test_normalize_security_features_all_enabled(
        self,
        temp_raw_data_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test normalization with all features enabled."""
        # Write repos.jsonl
        write_jsonl(
            temp_raw_data_dir / "repos.jsonl",
            [{"data": {"full_name": "test-org/repo1", "node_id": "R_001"}}],
        )

        # Write security_features JSONL
        write_jsonl(
            temp_raw_data_dir / "security_features" / "test-org__repo1.jsonl",
            [
                {
                    "timestamp": "2025-01-15T10:00:00Z",
                    "data": {
                        "repo": "test-org/repo1",
                        "dependabot_alerts_enabled": True,
                        "secret_scanning_enabled": True,
                        "secret_scanning_push_protection_enabled": True,
                    },
                }
            ],
        )

        df = normalize_security_features(temp_raw_data_dir, mock_config)

        assert len(df) == 1
        row = df.iloc[0]

        assert row["repo_id"] == "R_001"
        assert row["repo_full_name"] == "test-org/repo1"
        assert row["dependabot_alerts_enabled"] == True  # noqa: E712
        assert row["secret_scanning_enabled"] == True  # noqa: E712
        assert row["push_protection_enabled"] == True  # noqa: E712
        assert pd.isna(row["error"])

    def test_normalize_security_features_mixed(
        self,
        temp_raw_data_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test normalization with mixed feature states."""
        # Write repos.jsonl
        write_jsonl(
            temp_raw_data_dir / "repos.jsonl",
            [{"data": {"full_name": "test-org/repo1", "node_id": "R_001"}}],
        )

        # Write security_features JSONL
        write_jsonl(
            temp_raw_data_dir / "security_features" / "test-org__repo1.jsonl",
            [
                {
                    "timestamp": "2025-01-15T10:00:00Z",
                    "data": {
                        "repo": "test-org/repo1",
                        "dependabot_alerts_enabled": True,
                        "secret_scanning_enabled": False,
                        "secret_scanning_push_protection_enabled": None,
                    },
                }
            ],
        )

        df = normalize_security_features(temp_raw_data_dir, mock_config)

        assert len(df) == 1
        row = df.iloc[0]

        assert row["dependabot_alerts_enabled"] == True  # noqa: E712
        assert row["secret_scanning_enabled"] == False  # noqa: E712
        assert pd.isna(row["push_protection_enabled"])

    def test_normalize_security_features_permission_denied(
        self,
        temp_raw_data_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test normalization with permission denied."""
        # Write repos.jsonl
        write_jsonl(
            temp_raw_data_dir / "repos.jsonl",
            [{"data": {"full_name": "test-org/repo1", "node_id": "R_001"}}],
        )

        # Write security_features JSONL (403 case)
        write_jsonl(
            temp_raw_data_dir / "security_features" / "test-org__repo1.jsonl",
            [
                {
                    "timestamp": "2025-01-15T10:00:00Z",
                    "data": {
                        "repo": "test-org/repo1",
                        "dependabot_alerts_enabled": None,
                        "secret_scanning_enabled": None,
                        "secret_scanning_push_protection_enabled": None,
                        "error": "403: Security features not accessible",
                    },
                }
            ],
        )

        df = normalize_security_features(temp_raw_data_dir, mock_config)

        assert len(df) == 1
        row = df.iloc[0]

        assert row["error"] == "403: Security features not accessible"
        # All feature fields should be None
        assert pd.isna(row["dependabot_alerts_enabled"])
        assert pd.isna(row["secret_scanning_enabled"])
        assert pd.isna(row["push_protection_enabled"])

    def test_normalize_security_features_empty(
        self,
        temp_raw_data_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test normalization with no data."""
        # Write empty repos.jsonl
        write_jsonl(temp_raw_data_dir / "repos.jsonl", [])

        df = normalize_security_features(temp_raw_data_dir, mock_config)

        assert len(df) == 0
        assert list(df.columns) == [
            "repo_id",
            "repo_full_name",
            "captured_at",
            "dependabot_alerts_enabled",
            "secret_scanning_enabled",
            "push_protection_enabled",
            "error",
        ]


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_missing_repo_tree_directory(
        self,
        temp_raw_data_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test when repo_tree directory doesn't exist."""
        # Remove repo_tree directory
        (temp_raw_data_dir / "repo_tree").rmdir()

        df = normalize_file_presence(temp_raw_data_dir, mock_config)

        assert len(df) == 0
        assert list(df.columns) == [
            "repo_id",
            "repo_full_name",
            "captured_at",
            "path",
            "exists",
            "sha",
            "size_bytes",
        ]

    def test_missing_repos_jsonl(
        self,
        temp_raw_data_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test when repos.jsonl doesn't exist."""
        df = normalize_file_presence(temp_raw_data_dir, mock_config)

        assert len(df) == 0

    def test_deterministic_sorting(
        self,
        temp_raw_data_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test that output is deterministically sorted."""
        # Write repos.jsonl
        write_jsonl(
            temp_raw_data_dir / "repos.jsonl",
            [
                {"data": {"full_name": "test-org/repo-z", "node_id": "R_003"}},
                {"data": {"full_name": "test-org/repo-a", "node_id": "R_001"}},
                {"data": {"full_name": "test-org/repo-m", "node_id": "R_002"}},
            ],
        )

        # Write security features in random order
        for repo_name, repo_id in [
            ("test-org__repo-z", "test-org/repo-z"),
            ("test-org__repo-a", "test-org/repo-a"),
            ("test-org__repo-m", "test-org/repo-m"),
        ]:
            write_jsonl(
                temp_raw_data_dir / "security_features" / f"{repo_name}.jsonl",
                [
                    {
                        "data": {
                            "repo": repo_id,
                            "dependabot_alerts_enabled": True,
                            "secret_scanning_enabled": True,
                            "secret_scanning_push_protection_enabled": True,
                        }
                    }
                ],
            )

        df = normalize_security_features(temp_raw_data_dir, mock_config)

        # Should be sorted by repo_full_name
        assert df["repo_full_name"].tolist() == [
            "test-org/repo-a",
            "test-org/repo-m",
            "test-org/repo-z",
        ]
