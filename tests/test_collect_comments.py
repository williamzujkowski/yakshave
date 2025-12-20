"""Tests for comment collector module."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gh_year_end.collect.comments import (
    CommentCollectionError,
    collect_issue_comments,
    collect_review_comments,
    read_issue_numbers,
    read_pr_numbers,
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
                    "comments": True,
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
    paths.issue_comments_raw_path.return_value = (
        tmp_path / "data" / "raw" / "issue_comments.jsonl"
    )
    paths.review_comments_raw_path.return_value = (
        tmp_path / "data" / "raw" / "review_comments.jsonl"
    )
    return paths


@pytest.fixture
def sample_repos() -> list[dict]:
    """Sample repository data."""
    return [
        {"full_name": "owner/repo1", "name": "repo1"},
        {"full_name": "owner/repo2", "name": "repo2"},
    ]


@pytest.fixture
def sample_issue_comments() -> list[dict]:
    """Sample issue comment data."""
    return [
        {
            "id": 1,
            "user": {"login": "commenter1"},
            "body": "Great idea!",
            "created_at": "2024-06-15T10:00:00Z",
        },
        {
            "id": 2,
            "user": {"login": "commenter2"},
            "body": "I agree",
            "created_at": "2024-07-20T14:30:00Z",
        },
    ]


@pytest.fixture
def sample_review_comments() -> list[dict]:
    """Sample review comment data."""
    return [
        {
            "id": 1,
            "user": {"login": "reviewer1"},
            "body": "Nice fix",
            "path": "src/main.py",
            "created_at": "2024-06-15T10:00:00Z",
        },
        {
            "id": 2,
            "user": {"login": "reviewer2"},
            "body": "Consider using a different approach",
            "path": "src/utils.py",
            "created_at": "2024-07-20T14:30:00Z",
        },
    ]


@pytest.fixture
def issue_numbers_by_repo() -> dict[str, list[int]]:
    """Sample issue numbers by repo."""
    return {
        "owner/repo1": [1, 2, 3],
        "owner/repo2": [4, 5],
    }


@pytest.fixture
def pr_numbers_by_repo() -> dict[str, list[int]]:
    """Sample PR numbers by repo."""
    return {
        "owner/repo1": [10, 11],
        "owner/repo2": [12, 13, 14],
    }


@pytest.mark.asyncio
class TestCollectIssueComments:
    """Tests for collect_issue_comments function."""

    async def test_collect_issue_comments_success(
        self,
        sample_repos,
        sample_issue_comments,
        issue_numbers_by_repo,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test successful issue comment collection."""

        async def mock_list_issue_comments(*args, **kwargs):
            """Mock list_issue_comments."""
            yield sample_issue_comments, {"page": 1}

        mock_rest_client.list_issue_comments = mock_list_issue_comments

        with patch("gh_year_end.collect.comments.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_issue_comments(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                issue_numbers_by_repo=issue_numbers_by_repo,
            )

            # Verify stats
            assert result["repos_processed"] == 2
            assert result["issues_processed"] == 5  # 3 + 2 issues
            assert result["comments_collected"] > 0

    async def test_collect_issue_comments_no_issues(
        self, sample_repos, mock_rest_client, mock_rate_limiter, mock_paths, sample_config
    ):
        """Test handling repo with no issues."""
        result = await collect_issue_comments(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            paths=mock_paths,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
            issue_numbers_by_repo={"owner/repo1": []},
        )

        # Should process repo but no issues
        assert result["repos_processed"] == 1
        assert result["issues_processed"] == 0

    async def test_collect_issue_comments_error_handling(
        self,
        sample_repos,
        issue_numbers_by_repo,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test error handling in issue comment collection."""

        async def mock_list_issue_comments(*args, **kwargs):
            raise Exception("API error")

        mock_rest_client.list_issue_comments = mock_list_issue_comments

        with patch("gh_year_end.collect.comments.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_issue_comments(
                repos=sample_repos[:1],
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                issue_numbers_by_repo={"owner/repo1": [1, 2]},
            )

            # Should track errors per issue
            assert result["errors"] == 2

    async def test_collect_issue_comments_with_checkpoint(
        self,
        sample_repos,
        sample_issue_comments,
        issue_numbers_by_repo,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test issue comment collection with checkpoint."""
        mock_checkpoint = MagicMock()
        mock_checkpoint.is_repo_endpoint_complete.side_effect = [
            True,  # First repo complete
            False,  # Second repo not complete
        ]

        async def mock_list_issue_comments(*args, **kwargs):
            yield sample_issue_comments, {"page": 1}

        mock_rest_client.list_issue_comments = mock_list_issue_comments

        with patch("gh_year_end.collect.comments.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_issue_comments(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                issue_numbers_by_repo=issue_numbers_by_repo,
                checkpoint=mock_checkpoint,
            )

            # First repo resumed
            assert result["repos_resumed"] == 1


@pytest.mark.asyncio
class TestCollectReviewComments:
    """Tests for collect_review_comments function."""

    async def test_collect_review_comments_success(
        self,
        sample_repos,
        sample_review_comments,
        pr_numbers_by_repo,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test successful review comment collection."""

        async def mock_list_review_comments(*args, **kwargs):
            """Mock list_review_comments."""
            yield sample_review_comments, {"page": 1}

        mock_rest_client.list_review_comments = mock_list_review_comments

        with patch("gh_year_end.collect.comments.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_review_comments(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                pr_numbers_by_repo=pr_numbers_by_repo,
            )

            # Verify stats
            assert result["repos_processed"] == 2
            assert result["prs_processed"] == 5  # 2 + 3 PRs
            assert result["comments_collected"] > 0

    async def test_collect_review_comments_no_prs(
        self, sample_repos, mock_rest_client, mock_rate_limiter, mock_paths, sample_config
    ):
        """Test handling repo with no PRs."""
        result = await collect_review_comments(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            paths=mock_paths,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
            pr_numbers_by_repo={"owner/repo1": []},
        )

        # Should process repo but no PRs
        assert result["repos_processed"] == 1
        assert result["prs_processed"] == 0

    async def test_collect_review_comments_error_handling(
        self,
        sample_repos,
        pr_numbers_by_repo,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test error handling in review comment collection."""

        async def mock_list_review_comments(*args, **kwargs):
            raise Exception("API error")

        mock_rest_client.list_review_comments = mock_list_review_comments

        with patch("gh_year_end.collect.comments.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_review_comments(
                repos=sample_repos[:1],
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                pr_numbers_by_repo={"owner/repo1": [10, 11]},
            )

            # Should track errors per PR
            assert result["errors"] == 2

    async def test_collect_review_comments_with_checkpoint(
        self,
        sample_repos,
        sample_review_comments,
        pr_numbers_by_repo,
        mock_rest_client,
        mock_rate_limiter,
        mock_paths,
        sample_config,
    ):
        """Test review comment collection with checkpoint."""
        mock_checkpoint = MagicMock()
        mock_checkpoint.is_repo_endpoint_complete.side_effect = [
            True,  # First repo complete
            False,  # Second repo not complete
        ]

        async def mock_list_review_comments(*args, **kwargs):
            yield sample_review_comments, {"page": 1}

        mock_rest_client.list_review_comments = mock_list_review_comments

        with patch("gh_year_end.collect.comments.AsyncJSONLWriter") as mock_writer_class:
            mock_writer = AsyncMock()
            mock_writer_class.return_value.__aenter__.return_value = mock_writer

            result = await collect_review_comments(
                repos=sample_repos,
                rest_client=mock_rest_client,
                paths=mock_paths,
                rate_limiter=mock_rate_limiter,
                config=sample_config,
                pr_numbers_by_repo=pr_numbers_by_repo,
                checkpoint=mock_checkpoint,
            )

            # First repo resumed
            assert result["repos_resumed"] == 1


class TestReadIssueNumbers:
    """Tests for read_issue_numbers function."""

    def test_read_issue_numbers_from_directory(self, tmp_path: Path):
        """Test reading issue numbers from directory of JSONL files."""
        issues_dir = tmp_path / "issues"
        issues_dir.mkdir()

        # Create sample issue files
        repo1_file = issues_dir / "owner__repo1.jsonl"
        repo1_file.write_text(
            '{"data": {"number": 1}}\n'
            '{"data": {"number": 2}}\n'
            '{"data": {"number": 3}}\n'
        )

        repo2_file = issues_dir / "owner__repo2.jsonl"
        repo2_file.write_text('{"data": {"number": 10}}\n' '{"data": {"number": 20}}\n')

        result = read_issue_numbers(issues_dir)

        assert "owner/repo1" in result
        assert "owner/repo2" in result
        assert result["owner/repo1"] == [1, 2, 3]
        assert result["owner/repo2"] == [10, 20]

    def test_read_issue_numbers_from_single_file(self, tmp_path: Path):
        """Test reading issue numbers from single JSONL file."""
        issues_file = tmp_path / "issues.jsonl"
        issues_file.write_text(
            '{"data": {"number": 1, "repository": {"full_name": "owner/repo1"}}}\n'
            '{"data": {"number": 2, "repository": {"full_name": "owner/repo1"}}}\n'
            '{"data": {"number": 10, "repository": {"full_name": "owner/repo2"}}}\n'
        )

        result = read_issue_numbers(issues_file)

        assert result["owner/repo1"] == [1, 2]
        assert result["owner/repo2"] == [10]

    def test_read_issue_numbers_invalid_json(self, tmp_path: Path):
        """Test handling invalid JSON in issue file."""
        issues_dir = tmp_path / "issues"
        issues_dir.mkdir()

        file = issues_dir / "owner__repo.jsonl"
        file.write_text('{"data": {"number": 1}}\n' 'invalid json\n' '{"data": {"number": 2}}\n')

        result = read_issue_numbers(issues_dir)

        # Should skip invalid line but keep valid ones
        assert result["owner/repo"] == [1, 2]

    def test_read_issue_numbers_file_not_found(self, tmp_path: Path):
        """Test handling missing file."""
        with pytest.raises(FileNotFoundError):
            read_issue_numbers(tmp_path / "nonexistent.jsonl")


class TestReadPrNumbers:
    """Tests for read_pr_numbers function."""

    def test_read_pr_numbers_from_directory(self, tmp_path: Path):
        """Test reading PR numbers from directory of JSONL files."""
        prs_dir = tmp_path / "prs"
        prs_dir.mkdir()

        # Create sample PR files
        repo1_file = prs_dir / "owner__repo1.jsonl"
        repo1_file.write_text(
            '{"data": {"number": 1}}\n'
            '{"data": {"number": 2}}\n'
            '{"data": {"number": 3}}\n'
        )

        result = read_pr_numbers(prs_dir)

        assert "owner/repo1" in result
        assert result["owner/repo1"] == [1, 2, 3]

    def test_read_pr_numbers_from_single_file(self, tmp_path: Path):
        """Test reading PR numbers from single JSONL file."""
        prs_file = tmp_path / "prs.jsonl"
        prs_file.write_text(
            '{"data": {"number": 1, "base": {"repo": {"full_name": "owner/repo1"}}}}\n'
            '{"data": {"number": 2, "base": {"repo": {"full_name": "owner/repo1"}}}}\n'
        )

        result = read_pr_numbers(prs_file)

        assert result["owner/repo1"] == [1, 2]

    def test_read_pr_numbers_deduplication(self, tmp_path: Path):
        """Test that duplicate PR numbers are deduplicated."""
        prs_dir = tmp_path / "prs"
        prs_dir.mkdir()

        file = prs_dir / "owner__repo.jsonl"
        file.write_text(
            '{"data": {"number": 1}}\n'
            '{"data": {"number": 2}}\n'
            '{"data": {"number": 1}}\n'  # Duplicate
        )

        result = read_pr_numbers(prs_dir)

        # Should be deduplicated and sorted
        assert result["owner/repo"] == [1, 2]

    def test_read_pr_numbers_sorted(self, tmp_path: Path):
        """Test that PR numbers are returned sorted."""
        prs_dir = tmp_path / "prs"
        prs_dir.mkdir()

        file = prs_dir / "owner__repo.jsonl"
        file.write_text('{"data": {"number": 5}}\n' '{"data": {"number": 1}}\n' '{"data": {"number": 3}}\n')

        result = read_pr_numbers(prs_dir)

        assert result["owner/repo"] == [1, 3, 5]
