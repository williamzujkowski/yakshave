"""Tests for commit collector module."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gh_year_end.collect.commits import CommitCollectionError, collect_commits
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
                    "commits": True,
                    "hygiene": False,
                },
                "commits": {
                    "max_per_repo": 1000,
                    "max_pages": 10,
                    "since_days": None,
                },
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
    paths.commits_raw_path.return_value = tmp_path / "data" / "raw" / "commits.jsonl"
    return paths


@pytest.fixture
def sample_repos() -> list[dict]:
    """Sample repository data."""
    return [
        {"full_name": "owner/repo1", "name": "repo1"},
        {"full_name": "owner/repo2", "name": "repo2"},
    ]


@pytest.fixture
def sample_commits() -> list[dict]:
    """Sample commit data."""
    return [
        {
            "sha": "abc123",
            "commit": {
                "message": "Fix bug",
                "author": {"name": "Alice", "date": "2024-06-15T10:00:00Z"},
            },
        },
        {
            "sha": "def456",
            "commit": {
                "message": "Add feature",
                "author": {"name": "Bob", "date": "2024-07-20T14:30:00Z"},
            },
        },
        {
            "sha": "ghi789",
            "commit": {
                "message": "Update docs",
                "author": {"name": "Charlie", "date": "2024-08-10T09:00:00Z"},
            },
        },
    ]


@pytest.mark.asyncio
class TestCollectCommits:
    """Tests for collect_commits function."""

    async def test_collect_commits_success(
        self,
        sample_repos,
        sample_commits,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test successful commit collection."""

        async def mock_list_commits(*args, **kwargs):
            """Mock list_commits to return sample commits."""
            yield sample_commits, {"page": 1}

        mock_rest_client.list_commits = mock_list_commits

        with patch("gh_year_end.collect.commits.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_commits(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
            )

            # Verify stats
            assert result["repos_processed"] == 2
            assert result["commits_collected"] == 6  # 3 commits per repo
            assert result["repos_skipped"] == 0
            assert result["repos_errored"] == 0

    async def test_collect_commits_empty_repos(
        self, mock_rest_client, mock_rate_limiter, mock_paths, sample_config
    ):
        """Test collection with no repos."""
        result = await collect_commits(
            repos=[],
            rest_client=mock_rest_client,
            paths=mock_paths,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert result["repos_processed"] == 0
        assert result["commits_collected"] == 0

    async def test_collect_commits_no_commits_in_repo(
        self, sample_repos, mock_rest_client, mock_rate_limiter, mock_paths, sample_config
    ):
        """Test handling repo with no commits."""

        async def mock_list_commits(*args, **kwargs):
            """Mock list_commits that returns empty."""
            # Return nothing - simulate empty repo
            if False:
                yield

        mock_rest_client.list_commits = mock_list_commits

        with patch("gh_year_end.collect.commits.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_commits(
                repos=sample_repos[:1],
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
            )

            # Repo with no commits should be skipped
            assert result["repos_skipped"] == 1

    async def test_collect_commits_with_error(
        self, sample_repos, mock_rest_client, mock_rate_limiter, mock_paths, sample_config
    ):
        """Test collection handles errors gracefully."""

        with patch("gh_year_end.collect.commits._collect_repo_commits") as mock_collect:
            mock_collect.side_effect = Exception("API error")

            result = await collect_commits(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
            )

            # Both repos should error
            assert result["repos_errored"] == 2
            assert len(result["errors"]) == 2

    async def test_collect_commits_respects_max_per_repo(
        self,
        sample_repos,
        sample_commits,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        tmp_path,
    ):
        """Test that max_per_repo limit is respected."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test-org"},
                    "windows": {
                        "year": 2024,
                        "since": "2024-01-01T00:00:00Z",
                        "until": "2025-01-01T00:00:00Z",
                    },
                },
                "rate_limit": {"strategy": "adaptive", "max_concurrency": 1},
                "collection": {
                    "enable": {"commits": True},
                    "commits": {"max_per_repo": 1, "max_pages": None},
                },
                "storage": {"root": str(tmp_path / "data")},
            }
        )

        async def mock_list_commits(*args, **kwargs):
            yield sample_commits, {"page": 1}

        mock_rest_client.list_commits = mock_list_commits

        with patch("gh_year_end.collect.commits.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_commits(
                repos=sample_repos[:1],
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=config,
            )

            # Should be limited to 1 commit per repo
            assert result["commits_collected"] <= 1

    async def test_collect_commits_with_checkpoint_resume(
        self,
        sample_repos,
        sample_commits,
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

        async def mock_list_commits(*args, **kwargs):
            yield sample_commits, {"page": 1}

        mock_rest_client.list_commits = mock_list_commits

        with patch("gh_year_end.collect.commits.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_commits(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                checkpoint=mock_checkpoint,
            )

            # First repo resumed, second processed
            assert result["repos_resumed"] == 1
            assert result["repos_processed"] == 1

    async def test_collect_commits_checkpoint_marks_failed(
        self, sample_repos, mock_rest_client, mock_rate_limiter, mock_paths, sample_config
    ):
        """Test that checkpoint marks failed repos."""
        mock_checkpoint = MagicMock()
        mock_checkpoint.is_repo_endpoint_complete.return_value = False

        with patch("gh_year_end.collect.commits._collect_repo_commits") as mock_collect:
            mock_collect.side_effect = Exception("API error")

            await collect_commits(
                repos=sample_repos[:1],
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                checkpoint=mock_checkpoint,
            )

            # Verify checkpoint marked as failed
            mock_checkpoint.mark_repo_endpoint_failed.assert_called_once()

    async def test_collect_commits_missing_full_name(
        self, mock_rest_client, mock_rate_limiter, mock_paths, sample_config
    ):
        """Test handling of repo missing full_name field."""
        invalid_repos = [{"name": "repo1"}]  # Missing full_name

        with patch("gh_year_end.collect.commits.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_commits(
                repos=invalid_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
            )

            # Should be skipped
            assert result["repos_skipped"] == 1

    async def test_collect_commits_without_config(
        self, sample_repos, sample_commits, mock_rest_client, mock_rate_limiter, mock_paths
    ):
        """Test collection works without config (no date filtering)."""

        async def mock_list_commits(*args, **kwargs):
            yield sample_commits, {"page": 1}

        mock_rest_client.list_commits = mock_list_commits

        with patch("gh_year_end.collect.commits.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_commits(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=None,  # No config
            )

            # Should still work
            assert result["repos_processed"] == 2
