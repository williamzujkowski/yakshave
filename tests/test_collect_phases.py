"""Tests for collection phase modules."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gh_year_end.collect.phases.comments import (
    _extract_issue_numbers_from_raw,
    _extract_pr_numbers_from_raw,
    run_comments_phase,
)
from gh_year_end.collect.phases.commits import run_commits_phase
from gh_year_end.collect.phases.discovery import run_discovery_phase
from gh_year_end.collect.phases.hygiene import (
    run_branch_protection_phase,
    run_security_features_phase,
)
from gh_year_end.collect.phases.issues import run_issues_phase
from gh_year_end.collect.phases.pulls import run_pulls_phase
from gh_year_end.collect.phases.repos import run_repo_metadata_phase
from gh_year_end.collect.phases.reviews import run_reviews_phase
from gh_year_end.config import Config


@pytest.fixture
def sample_config(tmp_path: Path) -> Config:
    """Create sample config for testing."""
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
            "rate_limit": {
                "strategy": "adaptive",
                "max_concurrency": 2,
            },
            "collection": {
                "enable": {
                    "pulls": True,
                    "issues": True,
                    "reviews": True,
                    "comments": True,
                    "commits": True,
                    "hygiene": True,
                }
            },
            "storage": {
                "root": str(tmp_path / "data"),
            },
        }
    )


@pytest.fixture
def sample_repos() -> list[dict]:
    """Sample repository data."""
    return [
        {"full_name": "test-org/repo1", "name": "repo1"},
        {"full_name": "test-org/repo2", "name": "repo2"},
    ]


@pytest.fixture
def mock_http_client():
    """Create mock HTTP client."""
    return AsyncMock()


@pytest.fixture
def mock_graphql_client():
    """Create mock GraphQL client."""
    return AsyncMock()


@pytest.fixture
def mock_rest_client():
    """Create mock REST client."""
    return AsyncMock()


@pytest.fixture
def mock_rate_limiter():
    """Create mock rate limiter."""
    return AsyncMock()


@pytest.fixture
def mock_paths(tmp_path: Path):
    """Create mock PathManager."""
    paths = MagicMock()
    paths.repos_raw_path = tmp_path / "data" / "raw" / "repos.jsonl"
    paths.raw_root = tmp_path / "data" / "raw"
    paths.site_data_path = tmp_path / "data" / "site"
    paths.pulls_raw_path = MagicMock(
        return_value=tmp_path / "data" / "raw" / "pulls" / "repo.jsonl"
    )
    paths.issues_raw_path = MagicMock(
        return_value=tmp_path / "data" / "raw" / "issues" / "repo.jsonl"
    )
    return paths


@pytest.fixture
def mock_checkpoint():
    """Create mock CheckpointManager."""
    checkpoint = MagicMock()
    checkpoint.is_phase_complete = MagicMock(return_value=False)
    checkpoint.set_current_phase = MagicMock()
    checkpoint.mark_phase_complete = MagicMock()
    checkpoint.update_repos = MagicMock()
    return checkpoint


@pytest.fixture
def mock_progress():
    """Create mock ProgressTracker."""
    progress = MagicMock()
    progress.set_phase = MagicMock()
    progress.set_total_repos = MagicMock()
    progress.mark_phase_complete = MagicMock()
    progress.update_items_collected = MagicMock()
    return progress


class TestRunDiscoveryPhase:
    """Tests for run_discovery_phase function."""

    @pytest.mark.asyncio
    async def test_discovery_phase_fresh_run(
        self,
        sample_config,
        mock_http_client,
        mock_paths,
        mock_checkpoint,
        mock_progress,
        sample_repos,
    ):
        """Test running discovery phase for the first time."""
        with patch(
            "gh_year_end.collect.phases.discovery.discover_repos",
            new=AsyncMock(return_value=sample_repos),
        ):
            repos, stats = await run_discovery_phase(
                config=sample_config,
                http_client=mock_http_client,
                paths=mock_paths,
                checkpoint=mock_checkpoint,
                progress=mock_progress,
            )

        assert repos == sample_repos
        assert stats["repos_discovered"] == 2
        mock_checkpoint.set_current_phase.assert_called_once_with("discovery")
        mock_checkpoint.update_repos.assert_called_once_with(sample_repos)
        mock_checkpoint.mark_phase_complete.assert_called_once_with("discovery")
        mock_progress.set_total_repos.assert_called_once_with(2)
        mock_progress.mark_phase_complete.assert_called_once_with("discovery")

    @pytest.mark.asyncio
    async def test_discovery_phase_already_complete(
        self,
        sample_config,
        mock_http_client,
        mock_paths,
        mock_checkpoint,
        mock_progress,
        tmp_path,
    ):
        """Test skipping discovery phase if already complete."""
        mock_checkpoint.is_phase_complete.return_value = True

        # Create sample repos.jsonl file
        repos_file = tmp_path / "data" / "raw" / "repos.jsonl"
        repos_file.parent.mkdir(parents=True, exist_ok=True)
        mock_paths.repos_raw_path = repos_file

        with repos_file.open("w") as f:
            f.write(json.dumps({"data": {"full_name": "test-org/repo1"}}) + "\n")
            f.write(json.dumps({"data": {"full_name": "test-org/repo2"}}) + "\n")

        repos, stats = await run_discovery_phase(
            config=sample_config,
            http_client=mock_http_client,
            paths=mock_paths,
            checkpoint=mock_checkpoint,
            progress=mock_progress,
        )

        assert len(repos) == 2
        assert stats["skipped"] is True
        mock_progress.set_total_repos.assert_called_once_with(2)


class TestRunRepoMetadataPhase:
    """Tests for run_repo_metadata_phase function."""

    @pytest.mark.asyncio
    async def test_repo_metadata_phase_fresh_run(
        self,
        sample_config,
        sample_repos,
        mock_graphql_client,
        mock_rate_limiter,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test running repo metadata phase for the first time."""
        with patch(
            "gh_year_end.collect.phases.repos.collect_repo_metadata",
            new=AsyncMock(return_value={"repos_processed": 2}),
        ):
            stats = await run_repo_metadata_phase(
                config=sample_config,
                repos=sample_repos,
                graphql_client=mock_graphql_client,
                rate_limiter=mock_rate_limiter,
                paths=mock_paths,
                checkpoint=mock_checkpoint,
                progress=mock_progress,
            )

        assert stats["repos_processed"] == 2
        mock_checkpoint.set_current_phase.assert_called_once_with("repo_metadata")
        mock_checkpoint.mark_phase_complete.assert_called_once_with("repo_metadata")
        mock_progress.mark_phase_complete.assert_called_once_with("repo_metadata")

    @pytest.mark.asyncio
    async def test_repo_metadata_phase_disabled(
        self,
        sample_config,
        sample_repos,
        mock_graphql_client,
        mock_rate_limiter,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test skipping repo metadata phase when hygiene disabled."""
        sample_config.collection.enable.hygiene = False

        stats = await run_repo_metadata_phase(
            config=sample_config,
            repos=sample_repos,
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            paths=mock_paths,
            checkpoint=mock_checkpoint,
            progress=mock_progress,
        )

        assert stats["skipped"] is True
        assert stats["repos_processed"] == 0

    @pytest.mark.asyncio
    async def test_repo_metadata_phase_already_complete(
        self,
        sample_config,
        sample_repos,
        mock_graphql_client,
        mock_rate_limiter,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test skipping repo metadata phase if already complete."""
        mock_checkpoint.is_phase_complete.return_value = True

        stats = await run_repo_metadata_phase(
            config=sample_config,
            repos=sample_repos,
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            paths=mock_paths,
            checkpoint=mock_checkpoint,
            progress=mock_progress,
        )

        assert stats["skipped"] is True


class TestRunPullsPhase:
    """Tests for run_pulls_phase function."""

    @pytest.mark.asyncio
    async def test_pulls_phase_fresh_run(
        self,
        sample_config,
        sample_repos,
        mock_rest_client,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test running pulls phase for the first time."""
        mock_collect_parallel = AsyncMock(
            return_value={
                "repos_processed": 2,
                "pulls_collected": 42,
                "repos_skipped": 0,
                "repos_errored": 0,
            }
        )

        stats = await run_pulls_phase(
            config=sample_config,
            repos=sample_repos,
            rest_client=mock_rest_client,
            paths=mock_paths,
            checkpoint=mock_checkpoint,
            progress=mock_progress,
            collect_repos_parallel=mock_collect_parallel,
        )

        assert stats["pulls_collected"] == 42
        assert stats["repos_processed"] == 2
        mock_checkpoint.set_current_phase.assert_called_once_with("pulls")
        mock_checkpoint.mark_phase_complete.assert_called_once_with("pulls")
        mock_progress.update_items_collected.assert_called_once_with("pulls", 42)

    @pytest.mark.asyncio
    async def test_pulls_phase_disabled(
        self,
        sample_config,
        sample_repos,
        mock_rest_client,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test skipping pulls phase when disabled."""
        sample_config.collection.enable.pulls = False
        mock_collect_parallel = AsyncMock()

        stats = await run_pulls_phase(
            config=sample_config,
            repos=sample_repos,
            rest_client=mock_rest_client,
            paths=mock_paths,
            checkpoint=mock_checkpoint,
            progress=mock_progress,
            collect_repos_parallel=mock_collect_parallel,
        )

        assert stats["skipped"] is True
        assert stats["pulls_collected"] == 0
        mock_collect_parallel.assert_not_called()


class TestRunIssuesPhase:
    """Tests for run_issues_phase function."""

    @pytest.mark.asyncio
    async def test_issues_phase_fresh_run(
        self,
        sample_config,
        sample_repos,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test running issues phase for the first time."""
        with patch(
            "gh_year_end.collect.phases.issues.collect_issues",
            new=AsyncMock(return_value={"issues_collected": 25}),
        ):
            stats = await run_issues_phase(
                config=sample_config,
                repos=sample_repos,
                rest_client=mock_rest_client,
                rate_limiter=mock_rate_limiter,
                paths=mock_paths,
                checkpoint=mock_checkpoint,
                progress=mock_progress,
            )

        assert stats["issues_collected"] == 25
        mock_checkpoint.set_current_phase.assert_called_once_with("issues")
        mock_checkpoint.mark_phase_complete.assert_called_once_with("issues")
        mock_progress.update_items_collected.assert_called_once_with("issues", 25)

    @pytest.mark.asyncio
    async def test_issues_phase_disabled(
        self,
        sample_config,
        sample_repos,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test skipping issues phase when disabled."""
        sample_config.collection.enable.issues = False

        stats = await run_issues_phase(
            config=sample_config,
            repos=sample_repos,
            rest_client=mock_rest_client,
            rate_limiter=mock_rate_limiter,
            paths=mock_paths,
            checkpoint=mock_checkpoint,
            progress=mock_progress,
        )

        assert stats["skipped"] is True


class TestRunReviewsPhase:
    """Tests for run_reviews_phase function."""

    @pytest.mark.asyncio
    async def test_reviews_phase_fresh_run(
        self,
        sample_config,
        sample_repos,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test running reviews phase for the first time."""
        with patch(
            "gh_year_end.collect.phases.reviews.collect_reviews",
            new=AsyncMock(return_value={"reviews_collected": 100}),
        ):
            stats = await run_reviews_phase(
                config=sample_config,
                repos=sample_repos,
                rest_client=mock_rest_client,
                rate_limiter=mock_rate_limiter,
                paths=mock_paths,
                checkpoint=mock_checkpoint,
                progress=mock_progress,
            )

        assert stats["reviews_collected"] == 100
        mock_checkpoint.set_current_phase.assert_called_once_with("reviews")
        mock_checkpoint.mark_phase_complete.assert_called_once_with("reviews")

    @pytest.mark.asyncio
    async def test_reviews_phase_disabled(
        self,
        sample_config,
        sample_repos,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test skipping reviews phase when disabled."""
        sample_config.collection.enable.reviews = False

        stats = await run_reviews_phase(
            config=sample_config,
            repos=sample_repos,
            rest_client=mock_rest_client,
            rate_limiter=mock_rate_limiter,
            paths=mock_paths,
            checkpoint=mock_checkpoint,
            progress=mock_progress,
        )

        assert stats["skipped"] is True


class TestRunCommentsPhase:
    """Tests for run_comments_phase function."""

    @pytest.mark.asyncio
    async def test_comments_phase_fresh_run(
        self,
        sample_config,
        sample_repos,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test running comments phase for the first time."""
        with (
            patch(
                "gh_year_end.collect.phases.comments._extract_issue_numbers_from_raw",
                new=AsyncMock(return_value={"test-org/repo1": [1, 2, 3]}),
            ),
            patch(
                "gh_year_end.collect.phases.comments._extract_pr_numbers_from_raw",
                new=AsyncMock(return_value={"test-org/repo1": [10, 20]}),
            ),
            patch(
                "gh_year_end.collect.phases.comments.collect_issue_comments",
                new=AsyncMock(return_value={"comments_collected": 50}),
            ),
            patch(
                "gh_year_end.collect.phases.comments.collect_review_comments",
                new=AsyncMock(return_value={"comments_collected": 30}),
            ),
        ):
            stats = await run_comments_phase(
                config=sample_config,
                repos=sample_repos,
                rest_client=mock_rest_client,
                rate_limiter=mock_rate_limiter,
                paths=mock_paths,
                checkpoint=mock_checkpoint,
                progress=mock_progress,
            )

        assert stats["total_comments"] == 80  # 50 + 30
        assert stats["issue_comments"]["comments_collected"] == 50
        assert stats["review_comments"]["comments_collected"] == 30
        mock_checkpoint.mark_phase_complete.assert_called_once_with("comments")

    @pytest.mark.asyncio
    async def test_comments_phase_disabled(
        self,
        sample_config,
        sample_repos,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test skipping comments phase when disabled."""
        sample_config.collection.enable.comments = False

        stats = await run_comments_phase(
            config=sample_config,
            repos=sample_repos,
            rest_client=mock_rest_client,
            rate_limiter=mock_rate_limiter,
            paths=mock_paths,
            checkpoint=mock_checkpoint,
            progress=mock_progress,
        )

        assert stats["skipped"] is True
        assert stats["total_comments"] == 0


class TestRunCommitsPhase:
    """Tests for run_commits_phase function."""

    @pytest.mark.asyncio
    async def test_commits_phase_fresh_run(
        self,
        sample_config,
        sample_repos,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test running commits phase for the first time."""
        with patch(
            "gh_year_end.collect.phases.commits.collect_commits",
            new=AsyncMock(return_value={"commits_collected": 500}),
        ):
            stats = await run_commits_phase(
                config=sample_config,
                repos=sample_repos,
                rest_client=mock_rest_client,
                rate_limiter=mock_rate_limiter,
                paths=mock_paths,
                checkpoint=mock_checkpoint,
                progress=mock_progress,
            )

        assert stats["commits_collected"] == 500
        mock_checkpoint.set_current_phase.assert_called_once_with("commits")
        mock_checkpoint.mark_phase_complete.assert_called_once_with("commits")

    @pytest.mark.asyncio
    async def test_commits_phase_disabled(
        self,
        sample_config,
        sample_repos,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test skipping commits phase when disabled."""
        sample_config.collection.enable.commits = False

        stats = await run_commits_phase(
            config=sample_config,
            repos=sample_repos,
            rest_client=mock_rest_client,
            rate_limiter=mock_rate_limiter,
            paths=mock_paths,
            checkpoint=mock_checkpoint,
            progress=mock_progress,
        )

        assert stats["skipped"] is True


class TestRunBranchProtectionPhase:
    """Tests for run_branch_protection_phase function."""

    @pytest.mark.asyncio
    async def test_branch_protection_phase_fresh_run(
        self,
        sample_config,
        sample_repos,
        mock_rest_client,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test running branch protection phase for the first time."""
        with patch(
            "gh_year_end.collect.phases.hygiene.collect_branch_protection",
            new=AsyncMock(return_value={"repos_processed": 2, "protection_enabled": 1}),
        ):
            stats = await run_branch_protection_phase(
                config=sample_config,
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                checkpoint=mock_checkpoint,
                progress=mock_progress,
            )

        assert stats["repos_processed"] == 2
        assert stats["protection_enabled"] == 1
        mock_checkpoint.set_current_phase.assert_called_once_with("branch_protection")
        mock_checkpoint.mark_phase_complete.assert_called_once_with("branch_protection")

    @pytest.mark.asyncio
    async def test_branch_protection_phase_disabled(
        self,
        sample_config,
        sample_repos,
        mock_rest_client,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test skipping branch protection phase when hygiene disabled."""
        sample_config.collection.enable.hygiene = False

        stats = await run_branch_protection_phase(
            config=sample_config,
            repos=sample_repos,
            rest_client=mock_rest_client,
            paths=mock_paths,
            checkpoint=mock_checkpoint,
            progress=mock_progress,
        )

        assert stats["skipped"] is True


class TestRunSecurityFeaturesPhase:
    """Tests for run_security_features_phase function."""

    @pytest.mark.asyncio
    async def test_security_features_phase_fresh_run(
        self,
        sample_config,
        sample_repos,
        mock_rest_client,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test running security features phase for the first time."""
        with patch(
            "gh_year_end.collect.phases.hygiene.collect_security_features",
            new=AsyncMock(
                return_value={
                    "repos_processed": 2,
                    "repos_with_all_features": 1,
                    "repos_with_partial_features": 1,
                    "repos_with_no_access": 0,
                }
            ),
        ):
            stats = await run_security_features_phase(
                config=sample_config,
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                checkpoint=mock_checkpoint,
                progress=mock_progress,
            )

        assert stats["repos_processed"] == 2
        assert stats["repos_with_all_features"] == 1
        mock_checkpoint.set_current_phase.assert_called_once_with("security_features")

    @pytest.mark.asyncio
    async def test_security_features_phase_disabled(
        self,
        sample_config,
        sample_repos,
        mock_rest_client,
        mock_paths,
        mock_checkpoint,
        mock_progress,
    ):
        """Test skipping security features phase when hygiene disabled."""
        sample_config.collection.enable.hygiene = False

        stats = await run_security_features_phase(
            config=sample_config,
            repos=sample_repos,
            rest_client=mock_rest_client,
            paths=mock_paths,
            checkpoint=mock_checkpoint,
            progress=mock_progress,
        )

        assert stats["skipped"] is True


class TestExtractIssueNumbersFromRaw:
    """Tests for _extract_issue_numbers_from_raw function."""

    @pytest.mark.asyncio
    async def test_extract_issue_numbers(self, sample_repos, mock_paths, tmp_path):
        """Test extracting issue numbers from JSONL files."""
        # Setup mock paths
        issue_file = tmp_path / "issues" / "test-org_repo1.jsonl"
        issue_file.parent.mkdir(parents=True, exist_ok=True)
        mock_paths.issues_raw_path = MagicMock(return_value=issue_file)

        # Write sample issue data
        with issue_file.open("w") as f:
            f.write(json.dumps({"data": {"number": 1, "title": "Issue 1"}}) + "\n")
            f.write(json.dumps({"data": {"number": 3, "title": "Issue 3"}}) + "\n")
            f.write(json.dumps({"data": {"number": 2, "title": "Issue 2"}}) + "\n")

        result = await _extract_issue_numbers_from_raw(sample_repos, mock_paths)

        assert "test-org/repo1" in result
        assert result["test-org/repo1"] == [1, 2, 3]  # Should be sorted

    @pytest.mark.asyncio
    async def test_extract_issue_numbers_file_not_exist(self, sample_repos, mock_paths, tmp_path):
        """Test handling missing issue files."""
        mock_paths.issues_raw_path = MagicMock(return_value=tmp_path / "nonexistent.jsonl")

        result = await _extract_issue_numbers_from_raw(sample_repos, mock_paths)

        assert result == {}

    @pytest.mark.asyncio
    async def test_extract_issue_numbers_invalid_json(self, sample_repos, mock_paths, tmp_path):
        """Test handling invalid JSON in issue files."""
        issue_file = tmp_path / "issues" / "test-org_repo1.jsonl"
        issue_file.parent.mkdir(parents=True, exist_ok=True)
        mock_paths.issues_raw_path = MagicMock(return_value=issue_file)

        with issue_file.open("w") as f:
            f.write(json.dumps({"data": {"number": 1}}) + "\n")
            f.write("invalid json\n")
            f.write(json.dumps({"data": {"number": 2}}) + "\n")

        result = await _extract_issue_numbers_from_raw(sample_repos, mock_paths)

        # Should skip invalid line but continue
        assert "test-org/repo1" in result
        assert result["test-org/repo1"] == [1, 2]


class TestExtractPRNumbersFromRaw:
    """Tests for _extract_pr_numbers_from_raw function."""

    @pytest.mark.asyncio
    async def test_extract_pr_numbers(self, sample_repos, mock_paths, tmp_path):
        """Test extracting PR numbers from JSONL files."""
        pr_file = tmp_path / "pulls" / "test-org_repo1.jsonl"
        pr_file.parent.mkdir(parents=True, exist_ok=True)
        mock_paths.pulls_raw_path = MagicMock(return_value=pr_file)

        with pr_file.open("w") as f:
            f.write(json.dumps({"data": {"number": 10, "title": "PR 10"}}) + "\n")
            f.write(json.dumps({"data": {"number": 20, "title": "PR 20"}}) + "\n")

        result = await _extract_pr_numbers_from_raw(sample_repos, mock_paths)

        assert "test-org/repo1" in result
        assert result["test-org/repo1"] == [10, 20]

    @pytest.mark.asyncio
    async def test_extract_pr_numbers_file_not_exist(self, sample_repos, mock_paths, tmp_path):
        """Test handling missing PR files."""
        mock_paths.pulls_raw_path = MagicMock(return_value=tmp_path / "nonexistent.jsonl")

        result = await _extract_pr_numbers_from_raw(sample_repos, mock_paths)

        assert result == {}

    @pytest.mark.asyncio
    async def test_extract_pr_numbers_missing_number_field(
        self, sample_repos, mock_paths, tmp_path
    ):
        """Test handling records without number field."""
        pr_file = tmp_path / "pulls" / "test-org_repo1.jsonl"
        pr_file.parent.mkdir(parents=True, exist_ok=True)
        mock_paths.pulls_raw_path = MagicMock(return_value=pr_file)

        with pr_file.open("w") as f:
            f.write(json.dumps({"data": {"number": 10}}) + "\n")
            f.write(json.dumps({"data": {"title": "No number"}}) + "\n")
            f.write(json.dumps({"data": {"number": 20}}) + "\n")

        result = await _extract_pr_numbers_from_raw(sample_repos, mock_paths)

        # Should skip record without number
        assert result["test-org/repo1"] == [10, 20]
