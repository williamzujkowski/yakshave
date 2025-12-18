"""Tests for hygiene score calculation."""

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from gh_year_end.config import Config
from gh_year_end.metrics.hygiene_score import (
    WEIGHTS,
    calculate_hygiene_scores,
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
def temp_curated_dir(tmp_path: Path) -> Path:
    """Create temporary curated data directory."""
    curated_dir = tmp_path / "curated" / "year=2025"
    curated_dir.mkdir(parents=True)
    return curated_dir


class TestCalculateHygieneScores:
    """Tests for calculate_hygiene_scores."""

    def test_perfect_score(
        self,
        temp_curated_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test repository with perfect hygiene score (100/100)."""
        # Create file presence data with all required files
        file_presence = pd.DataFrame(
            [
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/perfect-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "README.md",
                    "exists": True,
                    "sha": "abc123",
                    "size_bytes": 1000,
                },
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/perfect-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "LICENSE",
                    "exists": True,
                    "sha": "def456",
                    "size_bytes": 500,
                },
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/perfect-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "CONTRIBUTING.md",
                    "exists": True,
                    "sha": "ghi789",
                    "size_bytes": 800,
                },
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/perfect-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "CODE_OF_CONDUCT.md",
                    "exists": True,
                    "sha": "jkl012",
                    "size_bytes": 600,
                },
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/perfect-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "SECURITY.md",
                    "exists": True,
                    "sha": "mno345",
                    "size_bytes": 700,
                },
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/perfect-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "CODEOWNERS",
                    "exists": True,
                    "sha": "pqr678",
                    "size_bytes": 200,
                },
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/perfect-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": ".github/workflows/ci.yml",
                    "exists": True,
                    "sha": "stu901",
                    "size_bytes": 300,
                },
            ]
        )

        # Create branch protection data
        branch_protection = pd.DataFrame(
            [
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/perfect-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "default_branch": "main",
                    "requires_reviews": True,
                    "required_approving_review_count": 2,
                    "requires_status_checks": True,
                    "allows_force_pushes": False,
                    "allows_deletions": False,
                    "enforce_admins": True,
                    "error": None,
                }
            ]
        )

        # Create security features data
        security_features = pd.DataFrame(
            [
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/perfect-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "dependabot_alerts_enabled": True,
                    "secret_scanning_enabled": True,
                    "push_protection_enabled": True,
                    "error": None,
                }
            ]
        )

        # Write to parquet
        file_presence.to_parquet(temp_curated_dir / "fact_repo_files_presence.parquet")
        branch_protection.to_parquet(temp_curated_dir / "fact_repo_hygiene.parquet")
        security_features.to_parquet(temp_curated_dir / "fact_repo_security_features.parquet")

        # Calculate scores
        result = calculate_hygiene_scores(temp_curated_dir, mock_config)

        # Verify
        assert len(result) == 1
        assert result.iloc[0]["repo_id"] == "R_001"
        assert result.iloc[0]["score"] == 100
        assert result.iloc[0]["has_readme"]
        assert result.iloc[0]["has_license"]
        assert result.iloc[0]["has_contributing"]
        assert result.iloc[0]["has_code_of_conduct"]
        assert result.iloc[0]["has_security_md"]
        assert result.iloc[0]["has_codeowners"]
        assert result.iloc[0]["has_ci_workflows"]
        assert result.iloc[0]["branch_protection_enabled"]
        assert result.iloc[0]["requires_reviews"]
        assert result.iloc[0]["dependabot_enabled"]
        assert result.iloc[0]["secret_scanning_enabled"]
        assert result.iloc[0]["notes"] == ""

    def test_minimal_score(
        self,
        temp_curated_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test repository with minimal hygiene (no files, no features)."""
        # Create file presence data with no files
        file_presence = pd.DataFrame(
            [
                {
                    "repo_id": "R_002",
                    "repo_full_name": "test-org/minimal-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "README.md",
                    "exists": False,
                    "sha": None,
                    "size_bytes": None,
                },
            ]
        )

        # Create branch protection data with no protection
        branch_protection = pd.DataFrame(
            [
                {
                    "repo_id": "R_002",
                    "repo_full_name": "test-org/minimal-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "default_branch": "main",
                    "requires_reviews": None,
                    "required_approving_review_count": None,
                    "requires_status_checks": None,
                    "allows_force_pushes": None,
                    "allows_deletions": None,
                    "enforce_admins": None,
                    "error": "Branch not protected",
                }
            ]
        )

        # Create security features data with no features
        security_features = pd.DataFrame(
            [
                {
                    "repo_id": "R_002",
                    "repo_full_name": "test-org/minimal-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "dependabot_alerts_enabled": False,
                    "secret_scanning_enabled": False,
                    "push_protection_enabled": False,
                    "error": None,
                }
            ]
        )

        # Write to parquet
        file_presence.to_parquet(temp_curated_dir / "fact_repo_files_presence.parquet")
        branch_protection.to_parquet(temp_curated_dir / "fact_repo_hygiene.parquet")
        security_features.to_parquet(temp_curated_dir / "fact_repo_security_features.parquet")

        # Calculate scores
        result = calculate_hygiene_scores(temp_curated_dir, mock_config)

        # Verify
        assert len(result) == 1
        assert result.iloc[0]["repo_id"] == "R_002"
        assert result.iloc[0]["score"] == 0
        assert not result.iloc[0]["has_readme"]
        assert pd.isna(result.iloc[0]["branch_protection_enabled"])
        assert not result.iloc[0]["dependabot_enabled"]
        assert not result.iloc[0]["secret_scanning_enabled"]

        # Check notes contain expected warnings
        notes = result.iloc[0]["notes"]
        assert "missing README" in notes
        assert "missing LICENSE" in notes
        assert "branch protection status unknown" in notes
        assert "Dependabot not enabled" in notes

    def test_partial_score(
        self,
        temp_curated_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test repository with partial hygiene (some files, some features)."""
        # Create file presence data with some files
        file_presence = pd.DataFrame(
            [
                {
                    "repo_id": "R_003",
                    "repo_full_name": "test-org/partial-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "README.md",
                    "exists": True,
                    "sha": "abc123",
                    "size_bytes": 1000,
                },
                {
                    "repo_id": "R_003",
                    "repo_full_name": "test-org/partial-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "LICENSE",
                    "exists": True,
                    "sha": "def456",
                    "size_bytes": 500,
                },
                {
                    "repo_id": "R_003",
                    "repo_full_name": "test-org/partial-repo",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": ".github/workflows/test.yml",
                    "exists": True,
                    "sha": "ghi789",
                    "size_bytes": 300,
                },
            ]
        )

        # No branch protection or security features
        branch_protection = pd.DataFrame(
            columns=[
                "repo_id",
                "repo_full_name",
                "captured_at",
                "default_branch",
                "requires_reviews",
                "required_approving_review_count",
                "requires_status_checks",
                "allows_force_pushes",
                "allows_deletions",
                "enforce_admins",
                "error",
            ]
        )

        security_features = pd.DataFrame(
            columns=[
                "repo_id",
                "repo_full_name",
                "captured_at",
                "dependabot_alerts_enabled",
                "secret_scanning_enabled",
                "push_protection_enabled",
                "error",
            ]
        )

        # Write to parquet
        file_presence.to_parquet(temp_curated_dir / "fact_repo_files_presence.parquet")
        branch_protection.to_parquet(temp_curated_dir / "fact_repo_hygiene.parquet")
        security_features.to_parquet(temp_curated_dir / "fact_repo_security_features.parquet")

        # Calculate scores
        result = calculate_hygiene_scores(temp_curated_dir, mock_config)

        # Verify
        assert len(result) == 1
        assert result.iloc[0]["repo_id"] == "R_003"

        # Score should be: README (15) + LICENSE (10) + CI (15) = 40
        expected_score = WEIGHTS["readme"] + WEIGHTS["license"] + WEIGHTS["ci_workflows"]
        assert result.iloc[0]["score"] == expected_score

        assert result.iloc[0]["has_readme"]
        assert result.iloc[0]["has_license"]
        assert result.iloc[0]["has_ci_workflows"]
        assert not result.iloc[0]["has_contributing"]
        assert pd.isna(result.iloc[0]["branch_protection_enabled"])
        assert pd.isna(result.iloc[0]["dependabot_enabled"])

    def test_multiple_repos(
        self,
        temp_curated_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test calculation for multiple repositories."""
        # Create file presence data for two repos
        file_presence = pd.DataFrame(
            [
                # Repo 1: Has README and LICENSE
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/repo1",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "README.md",
                    "exists": True,
                    "sha": "abc123",
                    "size_bytes": 1000,
                },
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/repo1",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "LICENSE",
                    "exists": True,
                    "sha": "def456",
                    "size_bytes": 500,
                },
                # Repo 2: Has only README
                {
                    "repo_id": "R_002",
                    "repo_full_name": "test-org/repo2",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "README.md",
                    "exists": True,
                    "sha": "ghi789",
                    "size_bytes": 800,
                },
            ]
        )

        branch_protection = pd.DataFrame(
            columns=[
                "repo_id",
                "repo_full_name",
                "captured_at",
                "default_branch",
                "requires_reviews",
                "required_approving_review_count",
                "requires_status_checks",
                "allows_force_pushes",
                "allows_deletions",
                "enforce_admins",
                "error",
            ]
        )

        security_features = pd.DataFrame(
            columns=[
                "repo_id",
                "repo_full_name",
                "captured_at",
                "dependabot_alerts_enabled",
                "secret_scanning_enabled",
                "push_protection_enabled",
                "error",
            ]
        )

        # Write to parquet
        file_presence.to_parquet(temp_curated_dir / "fact_repo_files_presence.parquet")
        branch_protection.to_parquet(temp_curated_dir / "fact_repo_hygiene.parquet")
        security_features.to_parquet(temp_curated_dir / "fact_repo_security_features.parquet")

        # Calculate scores
        result = calculate_hygiene_scores(temp_curated_dir, mock_config)

        # Verify
        assert len(result) == 2

        # Results should be sorted by repo_full_name
        assert result.iloc[0]["repo_full_name"] == "test-org/repo1"
        assert result.iloc[1]["repo_full_name"] == "test-org/repo2"

        # Verify score calculation
        assert result.iloc[0]["score"] == WEIGHTS["readme"] + WEIGHTS["license"]
        assert result.iloc[1]["score"] == WEIGHTS["readme"]

    def test_case_insensitive_file_matching(
        self,
        temp_curated_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test that file matching is case-insensitive."""
        # Create file presence data with various cases
        file_presence = pd.DataFrame(
            [
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/repo1",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "readme.MD",  # Mixed case
                    "exists": True,
                    "sha": "abc123",
                    "size_bytes": 1000,
                },
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/repo1",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "LICENCE",  # British spelling, uppercase
                    "exists": True,
                    "sha": "def456",
                    "size_bytes": 500,
                },
            ]
        )

        branch_protection = pd.DataFrame(
            columns=[
                "repo_id",
                "repo_full_name",
                "captured_at",
                "default_branch",
                "requires_reviews",
                "required_approving_review_count",
                "requires_status_checks",
                "allows_force_pushes",
                "allows_deletions",
                "enforce_admins",
                "error",
            ]
        )

        security_features = pd.DataFrame(
            columns=[
                "repo_id",
                "repo_full_name",
                "captured_at",
                "dependabot_alerts_enabled",
                "secret_scanning_enabled",
                "push_protection_enabled",
                "error",
            ]
        )

        # Write to parquet
        file_presence.to_parquet(temp_curated_dir / "fact_repo_files_presence.parquet")
        branch_protection.to_parquet(temp_curated_dir / "fact_repo_hygiene.parquet")
        security_features.to_parquet(temp_curated_dir / "fact_repo_security_features.parquet")

        # Calculate scores
        result = calculate_hygiene_scores(temp_curated_dir, mock_config)

        # Verify case-insensitive matching works
        assert result.iloc[0]["has_readme"]
        assert result.iloc[0]["has_license"]

    def test_empty_curated_data(
        self,
        temp_curated_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test handling of missing curated data."""
        # Don't create any parquet files

        # Calculate scores
        result = calculate_hygiene_scores(temp_curated_dir, mock_config)

        # Verify empty result
        assert len(result) == 0
        assert list(result.columns) == [
            "repo_id",
            "repo_full_name",
            "year",
            "score",
            "has_readme",
            "has_license",
            "has_contributing",
            "has_code_of_conduct",
            "has_security_md",
            "has_codeowners",
            "has_ci_workflows",
            "branch_protection_enabled",
            "requires_reviews",
            "dependabot_enabled",
            "secret_scanning_enabled",
            "notes",
        ]

    def test_branch_protection_with_no_reviews(
        self,
        temp_curated_dir: Path,
        mock_config: Config,
    ) -> None:
        """Test branch protection enabled but no required reviews."""
        file_presence = pd.DataFrame(
            [
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/repo1",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "path": "README.md",
                    "exists": True,
                    "sha": "abc123",
                    "size_bytes": 1000,
                },
            ]
        )

        # Branch protection enabled but no required reviews
        branch_protection = pd.DataFrame(
            [
                {
                    "repo_id": "R_001",
                    "repo_full_name": "test-org/repo1",
                    "captured_at": datetime(2025, 1, 15, tzinfo=UTC),
                    "default_branch": "main",
                    "requires_reviews": False,
                    "required_approving_review_count": 0,
                    "requires_status_checks": True,
                    "allows_force_pushes": False,
                    "allows_deletions": False,
                    "enforce_admins": False,
                    "error": None,
                }
            ]
        )

        security_features = pd.DataFrame(
            columns=[
                "repo_id",
                "repo_full_name",
                "captured_at",
                "dependabot_alerts_enabled",
                "secret_scanning_enabled",
                "push_protection_enabled",
                "error",
            ]
        )

        # Write to parquet
        file_presence.to_parquet(temp_curated_dir / "fact_repo_files_presence.parquet")
        branch_protection.to_parquet(temp_curated_dir / "fact_repo_hygiene.parquet")
        security_features.to_parquet(temp_curated_dir / "fact_repo_security_features.parquet")

        # Calculate scores
        result = calculate_hygiene_scores(temp_curated_dir, mock_config)

        # Verify
        assert result.iloc[0]["branch_protection_enabled"]
        assert not result.iloc[0]["requires_reviews"]

        # Score should include branch protection but not review requirement
        expected_score = WEIGHTS["readme"] + WEIGHTS["branch_protection"]
        assert result.iloc[0]["score"] == expected_score

        # Notes should mention no required reviews
        assert "no required reviews" in result.iloc[0]["notes"]
