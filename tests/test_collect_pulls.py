"""Tests for pull request collector module."""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gh_year_end.collect.pulls import (
    PullsCollectorError,
    _all_prs_before_date,
    _filter_prs_by_date,
    collect_pulls,
    collect_single_repo_pulls,
)
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
    client = AsyncMock()
    return client


@pytest.fixture
def mock_paths(tmp_path: Path):
    """Create mock PathManager."""
    paths = MagicMock()
    paths.pulls_raw_path.return_value = tmp_path / "data" / "raw" / "pulls.jsonl"
    return paths


@pytest.fixture
def sample_repos() -> list[dict]:
    """Sample repository data."""
    return [
        {"full_name": "owner/repo1", "name": "repo1"},
        {"full_name": "owner/repo2", "name": "repo2"},
    ]


@pytest.fixture
def sample_prs() -> list[dict]:
    """Sample pull request data."""
    return [
        {
            "number": 1,
            "title": "Fix bug",
            "updated_at": "2024-06-15T10:00:00Z",
            "state": "closed",
        },
        {
            "number": 2,
            "title": "Add feature",
            "updated_at": "2024-07-20T14:30:00Z",
            "state": "open",
        },
        {
            "number": 3,
            "title": "Refactor code",
            "updated_at": "2023-12-15T08:00:00Z",  # Before window
            "state": "closed",
        },
        {
            "number": 4,
            "title": "Update docs",
            "updated_at": "2025-02-01T12:00:00Z",  # After window
            "state": "open",
        },
    ]


class TestFilterPrsByDate:
    """Tests for _filter_prs_by_date function."""

    def test_filter_prs_within_date_range(self, sample_prs):
        """Test filtering PRs within date range."""
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2025, 1, 1, tzinfo=timezone.utc)

        result = _filter_prs_by_date(sample_prs, since, until)

        # Should include PRs 1 and 2 (within window), exclude 3 (before) and 4 (after)
        assert len(result) == 2
        assert result[0]["number"] == 1
        assert result[1]["number"] == 2

    def test_filter_prs_empty_list(self):
        """Test filtering empty list."""
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2025, 1, 1, tzinfo=timezone.utc)

        result = _filter_prs_by_date([], since, until)

        assert result == []

    def test_filter_prs_missing_updated_at(self):
        """Test filtering PRs with missing updated_at field."""
        prs = [
            {"number": 1, "title": "No timestamp"},
            {"number": 2, "title": "Has timestamp", "updated_at": "2024-06-15T10:00:00Z"},
        ]
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2025, 1, 1, tzinfo=timezone.utc)

        result = _filter_prs_by_date(prs, since, until)

        # Should only include PR with valid timestamp
        assert len(result) == 1
        assert result[0]["number"] == 2

    def test_filter_prs_invalid_timestamp(self):
        """Test filtering PRs with invalid timestamp."""
        prs = [
            {"number": 1, "title": "Invalid", "updated_at": "invalid-date"},
            {"number": 2, "title": "Valid", "updated_at": "2024-06-15T10:00:00Z"},
        ]
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2025, 1, 1, tzinfo=timezone.utc)

        result = _filter_prs_by_date(prs, since, until)

        # Should skip invalid timestamp
        assert len(result) == 1
        assert result[0]["number"] == 2

    def test_filter_prs_edge_case_exact_since(self):
        """Test PR exactly at since boundary is included."""
        prs = [{"number": 1, "updated_at": "2024-01-01T00:00:00Z"}]
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2025, 1, 1, tzinfo=timezone.utc)

        result = _filter_prs_by_date(prs, since, until)

        assert len(result) == 1

    def test_filter_prs_edge_case_exact_until(self):
        """Test PR exactly at until boundary is excluded."""
        prs = [{"number": 1, "updated_at": "2025-01-01T00:00:00Z"}]
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2025, 1, 1, tzinfo=timezone.utc)

        result = _filter_prs_by_date(prs, since, until)

        assert len(result) == 0


class TestAllPrsBeforeDate:
    """Tests for _all_prs_before_date function."""

    def test_all_prs_before_date_true(self):
        """Test when all PRs are before date."""
        prs = [
            {"updated_at": "2023-06-15T10:00:00Z"},
            {"updated_at": "2023-07-20T14:30:00Z"},
            {"updated_at": "2023-12-15T08:00:00Z"},
        ]
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)

        result = _all_prs_before_date(prs, since)

        assert result is True

    def test_all_prs_before_date_false(self):
        """Test when some PRs are on or after date."""
        prs = [
            {"updated_at": "2023-12-15T08:00:00Z"},
            {"updated_at": "2024-01-01T00:00:00Z"},  # On boundary
        ]
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)

        result = _all_prs_before_date(prs, since)

        assert result is False

    def test_all_prs_before_date_empty(self):
        """Test with empty list."""
        result = _all_prs_before_date([], datetime(2024, 1, 1, tzinfo=timezone.utc))

        assert result is True

    def test_all_prs_before_date_missing_timestamp(self):
        """Test with PRs missing updated_at."""
        prs = [
            {"number": 1},
            {"updated_at": "2023-12-15T08:00:00Z"},
        ]
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)

        result = _all_prs_before_date(prs, since)

        # Should skip PR without timestamp and return True if others are before
        assert result is True


@pytest.mark.asyncio
class TestCollectPulls:
    """Tests for collect_pulls function."""

    async def test_collect_pulls_success(
        self, sample_repos, sample_prs, mock_rest_client, mock_paths, sample_config
    ):
        """Test successful PR collection."""

        async def mock_list_pulls(*args, **kwargs):
            """Mock list_pulls to return sample PRs."""
            yield sample_prs, {"page": 1}

        mock_rest_client.list_pulls = mock_list_pulls

        with patch("gh_year_end.collect.pulls.AsyncJSONLWriter") as mock_writer_class:
            # Setup mock writer
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_pulls(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                config=sample_config,
            )

            # Verify stats
            assert result["repos_processed"] == 2
            assert result["pulls_collected"] == 4  # 2 PRs per repo (filtered)
            assert result["repos_skipped"] == 0
            assert result["repos_resumed"] == 0
            assert result["errors"] == []

    async def test_collect_pulls_empty_repos(
        self, mock_rest_client, mock_paths, sample_config
    ):
        """Test collection with no repos."""
        result = await collect_pulls(
            repos=[],
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=sample_config,
        )

        assert result["repos_processed"] == 0
        assert result["pulls_collected"] == 0

    async def test_collect_pulls_with_error(
        self, sample_repos, mock_rest_client, mock_paths, sample_config
    ):
        """Test collection handles errors gracefully."""

        async def mock_list_pulls(*args, **kwargs):
            """Mock list_pulls that raises error."""
            raise Exception("API error")

        mock_rest_client.list_pulls = mock_list_pulls

        with patch("gh_year_end.collect.pulls.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_pulls(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                config=sample_config,
            )

            # Both repos should fail
            assert result["repos_processed"] == 0
            assert result["repos_skipped"] == 2
            assert len(result["errors"]) == 2

    async def test_collect_pulls_with_checkpoint_resume(
        self, sample_repos, mock_rest_client, mock_paths, sample_config
    ):
        """Test collection resumes from checkpoint."""
        # Mock checkpoint that says first repo is complete
        mock_checkpoint = MagicMock()
        mock_checkpoint.is_repo_endpoint_complete.side_effect = [
            True,  # First repo complete
            False,  # Second repo not complete
        ]

        async def mock_list_pulls(*args, **kwargs):
            """Mock list_pulls."""
            yield [], {"page": 1}

        mock_rest_client.list_pulls = mock_list_pulls

        with patch("gh_year_end.collect.pulls.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_pulls(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                config=sample_config,
                checkpoint=mock_checkpoint,
            )

            # First repo resumed, second processed
            assert result["repos_resumed"] == 1
            assert result["repos_processed"] == 1


@pytest.mark.asyncio
class TestCollectSingleRepoPulls:
    """Tests for collect_single_repo_pulls function."""

    async def test_collect_single_repo_pulls_success(
        self, sample_prs, mock_rest_client, mock_paths, sample_config
    ):
        """Test collecting PRs for a single repo."""
        repo = {"full_name": "owner/repo", "name": "repo"}

        async def mock_list_pulls(*args, **kwargs):
            yield sample_prs[:2], {"page": 1}  # Return only first 2 PRs

        mock_rest_client.list_pulls = mock_list_pulls

        with patch("gh_year_end.collect.pulls._collect_repo_pulls") as mock_collect:
            mock_collect.return_value = 2

            result = await collect_single_repo_pulls(
                repo=repo,
                rest_client=mock_rest_client,
                paths=mock_paths,
                config=sample_config,
            )

            assert result["pulls_collected"] == 2
            mock_collect.assert_called_once()

    async def test_collect_single_repo_pulls_with_checkpoint(
        self, sample_prs, mock_rest_client, mock_paths, sample_config
    ):
        """Test collecting PRs with checkpoint tracking."""
        repo = {"full_name": "owner/repo", "name": "repo"}
        mock_checkpoint = MagicMock()

        async def mock_list_pulls(*args, **kwargs):
            yield sample_prs[:2], {"page": 1}

        mock_rest_client.list_pulls = mock_list_pulls

        with patch("gh_year_end.collect.pulls._collect_repo_pulls") as mock_collect:
            mock_collect.return_value = 2

            await collect_single_repo_pulls(
                repo=repo,
                rest_client=mock_rest_client,
                paths=mock_paths,
                config=sample_config,
                checkpoint=mock_checkpoint,
            )

            # Verify checkpoint was called
            mock_checkpoint.mark_repo_endpoint_in_progress.assert_called_once_with(
                "owner/repo", "pulls"
            )
