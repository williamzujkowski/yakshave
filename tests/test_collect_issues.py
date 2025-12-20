"""Tests for issue collector module."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gh_year_end.collect.issues import IssueCollectionStats, collect_issues
from gh_year_end.config import Config


@pytest.fixture
def sample_config(tmp_path: Path) -> Config:
    """Create sample config for testing."""
    return Config.model_validate(
        {
            "github": {
                "target": {"mode": "org", "name": "test-org"},
                "windows": {
                    "year": 2024,
                    "since": "2024-01-01T00:00:00Z",
                    "until": "2025-01-01T00:00:00Z",
                },
            },
            "rate_limit": {
                "strategy": "adaptive",
                "max_concurrency": 1,
            },
            "collection": {
                "enable": {
                    "pulls": True,
                    "issues": True,
                    "reviews": True,
                    "comments": False,
                    "commits": False,
                    "hygiene": False,
                }
            },
            "storage": {
                "root": str(tmp_path / "data"),
            },
        }
    )


@pytest.fixture
def mock_rest_client():
    """Create mock REST client."""
    return AsyncMock()


@pytest.fixture
def mock_rate_limiter():
    """Create mock rate limiter."""
    return MagicMock()


@pytest.fixture
def mock_paths(tmp_path: Path):
    """Create mock PathManager."""
    paths = MagicMock()
    paths.issues_raw_path.return_value = tmp_path / "data" / "raw" / "issues.jsonl"
    return paths


@pytest.fixture
def sample_repos() -> list[dict]:
    """Sample repository data."""
    return [
        {"full_name": "owner/repo1", "name": "repo1"},
        {"full_name": "owner/repo2", "name": "repo2"},
    ]


@pytest.fixture
def sample_issues() -> list[dict]:
    """Sample issue data (includes both issues and PRs)."""
    return [
        {
            "number": 1,
            "title": "Bug report",
            "updated_at": "2024-06-15T10:00:00Z",
            "state": "open",
        },
        {
            "number": 2,
            "title": "Feature request",
            "updated_at": "2024-07-20T14:30:00Z",
            "state": "closed",
            "pull_request": {"url": "https://api.github.com/repos/owner/repo/pulls/2"},
        },
        {
            "number": 3,
            "title": "Documentation issue",
            "updated_at": "2024-08-10T09:00:00Z",
            "state": "open",
        },
        {
            "number": 4,
            "title": "Another PR",
            "updated_at": "2025-02-01T12:00:00Z",  # After window
            "state": "open",
            "pull_request": {"url": "https://api.github.com/repos/owner/repo/pulls/4"},
        },
    ]


class TestIssueCollectionStats:
    """Tests for IssueCollectionStats class."""

    def test_init_stats(self):
        """Test stats initialization."""
        stats = IssueCollectionStats()

        assert stats.repos_processed == 0
        assert stats.repos_skipped == 0
        assert stats.repos_resumed == 0
        assert stats.issues_collected == 0
        assert stats.pull_requests_filtered == 0
        assert stats.errors == 0

    def test_to_dict(self):
        """Test converting stats to dict."""
        stats = IssueCollectionStats()
        stats.repos_processed = 5
        stats.issues_collected = 25
        stats.pull_requests_filtered = 10

        result = stats.to_dict()

        assert result["repos_processed"] == 5
        assert result["issues_collected"] == 25
        assert result["pull_requests_filtered"] == 10
        assert result["repos_skipped"] == 0


@pytest.mark.asyncio
class TestCollectIssues:
    """Tests for collect_issues function."""

    async def test_collect_issues_success(
        self,
        sample_repos,
        sample_issues,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test successful issue collection."""

        async def mock_list_issues(*args, **kwargs):
            """Mock list_issues to return sample issues."""
            yield sample_issues, {"page": 1}

        mock_rest_client.list_issues = mock_list_issues

        with patch("gh_year_end.collect.issues.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_issues(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
            )

            # Verify stats - PRs should be filtered out
            assert result["repos_processed"] == 2
            # Each repo gets sample_issues: 2 PRs filtered, 2 issues (but 1 is after window)
            assert result["issues_collected"] > 0
            assert result["pull_requests_filtered"] > 0

    async def test_collect_issues_empty_repos(
        self, mock_rest_client, mock_rate_limiter, mock_paths, sample_config
    ):
        """Test collection with no repos."""
        result = await collect_issues(
            repos=[],
            rest_client=mock_rest_client,
            paths=mock_paths,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert result["repos_processed"] == 0
        assert result["issues_collected"] == 0
        assert result["pull_requests_filtered"] == 0

    async def test_collect_issues_filters_pull_requests(
        self,
        sample_repos,
        sample_issues,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test that pull requests are filtered out."""

        async def mock_list_issues(*args, **kwargs):
            yield sample_issues, {"page": 1}

        mock_rest_client.list_issues = mock_list_issues

        with patch("gh_year_end.collect.issues.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_issues(
                repos=sample_repos[:1],  # Just one repo
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
            )

            # Should filter out items with "pull_request" key
            assert result["pull_requests_filtered"] == 2

    async def test_collect_issues_with_error(
        self, sample_repos, mock_rest_client, mock_rate_limiter, mock_paths, sample_config
    ):
        """Test collection handles errors gracefully."""

        async def mock_list_issues(*args, **kwargs):
            raise Exception("API error")

        mock_rest_client.list_issues = mock_list_issues

        with patch("gh_year_end.collect.issues.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_issues(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
            )

            # Errors should be tracked
            assert result["errors"] > 0

    async def test_collect_issues_invalid_repo_name(
        self, mock_rest_client, mock_rate_limiter, mock_paths, sample_config
    ):
        """Test handling of invalid repo name format."""
        invalid_repos = [{"full_name": "invalid-name-no-slash"}]

        result = await collect_issues(
            repos=invalid_repos,
            rest_client=mock_rest_client,
            paths=mock_paths,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        # Invalid repo should be skipped
        assert result["repos_skipped"] == 1
        assert result["repos_processed"] == 0

    async def test_collect_issues_with_checkpoint_resume(
        self,
        sample_repos,
        sample_issues,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test collection resumes from checkpoint."""
        mock_checkpoint = MagicMock()
        mock_checkpoint.is_repo_endpoint_complete.side_effect = [
            True,  # First repo complete
            False,  # Second repo not complete
        ]

        async def mock_list_issues(*args, **kwargs):
            yield sample_issues, {"page": 1}

        mock_rest_client.list_issues = mock_list_issues

        with patch("gh_year_end.collect.issues.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_issues(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                checkpoint=mock_checkpoint,
            )

            # First repo resumed, second processed
            assert result["repos_resumed"] == 1

    async def test_collect_issues_checkpoint_marks_progress(
        self,
        sample_repos,
        sample_issues,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test that checkpoint progress is marked."""
        mock_checkpoint = MagicMock()
        mock_checkpoint.is_repo_endpoint_complete.return_value = False

        async def mock_list_issues(*args, **kwargs):
            yield sample_issues, {"page": 1}

        mock_rest_client.list_issues = mock_list_issues

        with patch("gh_year_end.collect.issues.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            await collect_issues(
                repos=sample_repos[:1],
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                checkpoint=mock_checkpoint,
            )

            # Verify checkpoint methods were called
            mock_checkpoint.mark_repo_endpoint_in_progress.assert_called()
            mock_checkpoint.mark_repo_endpoint_complete.assert_called()
