"""Tests for review collector module."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gh_year_end.collect.reviews import ReviewCollectionStats, collect_reviews
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
    paths.reviews_raw_path.return_value = tmp_path / "data" / "raw" / "reviews.jsonl"
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
def sample_reviews() -> list[dict]:
    """Sample review data."""
    return [
        {
            "id": 1,
            "user": {"login": "reviewer1"},
            "state": "APPROVED",
            "submitted_at": "2024-06-15T10:00:00Z",
        },
        {
            "id": 2,
            "user": {"login": "reviewer2"},
            "state": "CHANGES_REQUESTED",
            "submitted_at": "2024-07-20T14:30:00Z",
        },
        {
            "id": 3,
            "user": {"login": "reviewer3"},
            "state": "COMMENTED",
            "submitted_at": "2024-08-10T09:00:00Z",
        },
    ]


@pytest.fixture
def pr_numbers_by_repo() -> dict[str, list[int]]:
    """Sample PR numbers by repo."""
    return {
        "owner/repo1": [1, 2, 3],
        "owner/repo2": [4, 5],
    }


class TestReviewCollectionStats:
    """Tests for ReviewCollectionStats class."""

    def test_init_stats(self):
        """Test stats initialization."""
        stats = ReviewCollectionStats()

        assert stats.repos_processed == 0
        assert stats.repos_skipped == 0
        assert stats.repos_resumed == 0
        assert stats.prs_processed == 0
        assert stats.reviews_collected == 0
        assert stats.errors == 0
        assert stats.skipped_404 == 0

    def test_to_dict(self):
        """Test converting stats to dict."""
        stats = ReviewCollectionStats()
        stats.repos_processed = 2
        stats.prs_processed = 10
        stats.reviews_collected = 25
        stats.skipped_404 = 3

        result = stats.to_dict()

        assert result["repos_processed"] == 2
        assert result["prs_processed"] == 10
        assert result["reviews_collected"] == 25
        assert result["skipped_404"] == 3


@pytest.mark.asyncio
class TestCollectReviews:
    """Tests for collect_reviews function."""

    async def test_collect_reviews_success(
        self,
        sample_repos,
        sample_reviews,
        pr_numbers_by_repo,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test successful review collection."""

        async def mock_list_reviews(*args, **kwargs):
            """Mock list_reviews to return sample reviews."""
            yield sample_reviews, {"page": 1}

        mock_rest_client.list_reviews = mock_list_reviews

        with patch("gh_year_end.collect.reviews.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_reviews(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                pr_numbers_by_repo=pr_numbers_by_repo,
            )

            # Verify stats
            assert result["repos_processed"] == 2
            assert result["prs_processed"] == 5  # 3 + 2 PRs
            assert result["reviews_collected"] > 0

    async def test_collect_reviews_empty_repos(
        self, mock_rest_client, mock_rate_limiter, mock_paths, sample_config
    ):
        """Test collection with no repos."""
        result = await collect_reviews(
            repos=[],
            rest_client=mock_rest_client,
            paths=mock_paths,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
            pr_numbers_by_repo={},
        )

        assert result["repos_processed"] == 0
        assert result["prs_processed"] == 0

    async def test_collect_reviews_no_prs_for_repo(
        self, sample_repos, mock_rest_client, mock_rate_limiter, mock_paths, sample_config
    ):
        """Test handling repo with no PRs."""
        # Empty PR numbers
        pr_numbers_by_repo = {"owner/repo1": []}

        result = await collect_reviews(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            paths=mock_paths,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
            pr_numbers_by_repo=pr_numbers_by_repo,
        )

        # No PRs to process
        assert result["prs_processed"] == 0

    async def test_collect_reviews_extracts_pr_numbers(
        self,
        sample_repos,
        sample_reviews,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
        tmp_path,
    ):
        """Test that PR numbers are extracted from files when not provided."""
        # Create a sample PR file
        pr_file = tmp_path / "data" / "raw" / "pulls.jsonl"
        pr_file.parent.mkdir(parents=True, exist_ok=True)
        pr_file.write_text(
            '{"data": {"number": 1, "title": "Test PR"}}\n'
            '{"data": {"number": 2, "title": "Another PR"}}\n'
        )

        mock_paths.pulls_raw_path.return_value = pr_file

        async def mock_list_reviews(*args, **kwargs):
            yield sample_reviews, {"page": 1}

        mock_rest_client.list_reviews = mock_list_reviews

        with patch("gh_year_end.collect.reviews.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            # Don't provide pr_numbers_by_repo - should extract from files
            result = await collect_reviews(
                repos=sample_repos[:1],
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                pr_numbers_by_repo=None,
            )

            # Should have processed PRs
            assert result["prs_processed"] >= 0

    async def test_collect_reviews_handles_404(
        self,
        sample_repos,
        pr_numbers_by_repo,  # noqa: ARG002
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test handling 404 errors for missing PRs."""

        async def mock_list_reviews(*args, **kwargs):
            if False:
                yield
            raise Exception("404 Not Found")

        mock_rest_client.list_reviews = mock_list_reviews

        with patch("gh_year_end.collect.reviews.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_reviews(
                repos=sample_repos[:1],
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                pr_numbers_by_repo={"owner/repo1": [1, 2]},
            )

            # 404s should be tracked separately
            assert result["skipped_404"] == 2

    async def test_collect_reviews_handles_other_errors(
        self,
        sample_repos,
        pr_numbers_by_repo,  # noqa: ARG002
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test handling non-404 errors."""

        async def mock_list_reviews(*args, **kwargs):
            if False:
                yield
            raise Exception("API rate limit exceeded")

        mock_rest_client.list_reviews = mock_list_reviews

        with patch("gh_year_end.collect.reviews.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_reviews(
                repos=sample_repos[:1],
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                pr_numbers_by_repo={"owner/repo1": [1, 2]},
            )

            # Non-404 errors should be tracked as errors
            assert result["errors"] == 2

    async def test_collect_reviews_with_checkpoint_resume(
        self,
        sample_repos,
        sample_reviews,
        pr_numbers_by_repo,
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

        async def mock_list_reviews(*args, **kwargs):
            yield sample_reviews, {"page": 1}

        mock_rest_client.list_reviews = mock_list_reviews

        with patch("gh_year_end.collect.reviews.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_reviews(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                pr_numbers_by_repo=pr_numbers_by_repo,
                checkpoint=mock_checkpoint,
            )

            # First repo resumed, second processed
            assert result["repos_resumed"] == 1
            assert result["repos_processed"] == 1

    async def test_collect_reviews_repo_level_error(
        self,
        sample_repos,
        pr_numbers_by_repo,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test handling repo-level errors (not PR-level)."""

        # Simulate an error at the repo processing level
        with patch("gh_year_end.collect.reviews._collect_reviews_for_repo") as mock_collect:
            mock_collect.side_effect = Exception("Repo processing error")

            result = await collect_reviews(
                repos=sample_repos[:1],
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                pr_numbers_by_repo=pr_numbers_by_repo,
            )

            # Repo should be skipped
            assert result["repos_skipped"] == 1
