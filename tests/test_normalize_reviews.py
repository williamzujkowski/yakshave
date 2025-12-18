"""Tests for review normalization."""

import json
from pathlib import Path

import pandas as pd
import pytest

from gh_year_end.config import Config
from gh_year_end.normalize.reviews import normalize_reviews


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    """Create temporary data directory structure."""
    raw_dir = tmp_path / "raw/year=2025/source=github/target=test-org"
    raw_dir.mkdir(parents=True)
    (raw_dir / "reviews").mkdir()
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


class TestNormalizeReviews:
    """Tests for normalize_reviews function."""

    def test_empty_directory_returns_empty_dataframe(
        self, config: Config, temp_data_dir: Path
    ) -> None:
        """Test that empty reviews directory returns empty DataFrame."""
        df = normalize_reviews(temp_data_dir, config)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert "review_id" in df.columns
        assert "state" in df.columns

    def test_normalizes_single_review(self, config: Config, temp_data_dir: Path) -> None:
        """Test normalizing a single review."""
        # Create PR file to get repo_id mapping
        pr_file = temp_data_dir / "pulls" / "test-org__test-repo.jsonl"
        with pr_file.open("w") as f:
            pr_envelope = {
                "timestamp": "2025-01-15T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls/1",
                "request_id": "pr-123",
                "page": 1,
                "data": {
                    "node_id": "PR_1",
                    "number": 1,
                    "base": {"repo": {"node_id": "R_test"}},
                },
            }
            f.write(json.dumps(pr_envelope) + "\n")

        # Create review file
        reviews_file = temp_data_dir / "reviews" / "test-org__test-repo.jsonl"
        with reviews_file.open("w") as f:
            envelope = {
                "timestamp": "2025-01-15T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls/1/reviews",
                "request_id": "123",
                "page": 1,
                "data": {
                    "node_id": "REV_1",
                    "user": {"node_id": "U_alice", "login": "alice"},
                    "body": "Looks good!",
                    "state": "APPROVED",
                    "submitted_at": "2025-01-02T10:00:00Z",
                    "pull_request_url": "https://api.github.com/repos/test-org/test-repo/pulls/1",
                },
            }
            f.write(json.dumps(envelope) + "\n")

        df = normalize_reviews(temp_data_dir, config)

        assert len(df) == 1
        row = df.iloc[0]
        assert row["review_id"] == "REV_1"
        assert row["repo_id"] == "R_test"
        assert row["reviewer_user_id"] == "U_alice"
        assert row["state"] == "APPROVED"
        assert row["body_len"] == 11  # len("Looks good!")

    def test_handles_multiple_review_states(self, config: Config, temp_data_dir: Path) -> None:
        """Test handling different review states."""
        reviews_file = temp_data_dir / "reviews" / "test-org__test-repo.jsonl"
        with reviews_file.open("w") as f:
            states = ["APPROVED", "CHANGES_REQUESTED", "COMMENTED"]
            for i, state in enumerate(states, 1):
                envelope = {
                    "timestamp": "2025-01-15T00:00:00Z",
                    "source": "github_rest",
                    "endpoint": f"/repos/test-org/test-repo/pulls/1/reviews/{i}",
                    "request_id": f"123-{i}",
                    "page": 1,
                    "data": {
                        "node_id": f"REV_{i}",
                        "user": {"node_id": "U_alice"},
                        "body": "",
                        "state": state,
                        "submitted_at": "2025-01-02T10:00:00Z",
                    },
                }
                f.write(json.dumps(envelope) + "\n")

        df = normalize_reviews(temp_data_dir, config)

        assert len(df) == 3
        assert set(df["state"].tolist()) == {"APPROVED", "CHANGES_REQUESTED", "COMMENTED"}

    def test_handles_missing_repo_id(self, config: Config, temp_data_dir: Path) -> None:
        """Test handling reviews when repo_id cannot be determined."""
        # No PR file created, so repo_id_map will be empty
        reviews_file = temp_data_dir / "reviews" / "test-org__test-repo.jsonl"
        with reviews_file.open("w") as f:
            envelope = {
                "timestamp": "2025-01-15T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/test-org/test-repo/pulls/1/reviews",
                "request_id": "123",
                "page": 1,
                "data": {
                    "node_id": "REV_1",
                    "user": {"node_id": "U_alice"},
                    "body": "Test",
                    "state": "APPROVED",
                    "submitted_at": "2025-01-02T10:00:00Z",
                },
            }
            f.write(json.dumps(envelope) + "\n")

        df = normalize_reviews(temp_data_dir, config)

        assert len(df) == 1
        # repo_id should be None/NaN
        assert pd.isna(df.iloc[0]["repo_id"]) or df.iloc[0]["repo_id"] is None
