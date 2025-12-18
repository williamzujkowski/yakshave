"""Unit tests for collection orchestrator.

Tests orchestrator logic by mocking all collectors and dependencies.
Does not make real API calls.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gh_year_end.collect.orchestrator import (
    CollectionError,
    _extract_issue_numbers_from_raw,
    _extract_pr_numbers_from_raw,
    run_collection,
)
from gh_year_end.config import Config
from gh_year_end.storage.paths import PathManager


@pytest.fixture
def test_config() -> Config:
    """Create test configuration.

    Returns:
        Config instance for testing.
    """
    config = Config.model_validate(
        {
            "github": {
                "target": {
                    "mode": "org",
                    "name": "test-org",
                },
                "discovery": {
                    "include_forks": False,
                    "include_archived": False,
                    "visibility": "all",
                },
                "windows": {
                    "year": 2024,
                    "since": "2024-01-01T00:00:00Z",
                    "until": "2025-01-01T00:00:00Z",
                },
            },
            "rate_limit": {
                "strategy": "adaptive",
                "max_concurrency": 2,
            },
            "identity": {
                "humans_only": False,
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
                "root": "./test_data",
            },
            "report": {
                "title": "Test Report",
                "output_dir": "./test_site",
            },
        }
    )
    return config


@pytest.fixture
def temp_dir() -> Path:
    """Create temporary directory for testing.

    Yields:
        Path to temporary directory.
    """
    tmpdir = Path(tempfile.mkdtemp(prefix="gh_year_end_test_"))
    yield tmpdir
    # Cleanup handled by pytest or test


@pytest.fixture
def mock_repos() -> list[dict]:
    """Create mock repository list.

    Returns:
        List of mock repository dictionaries.
    """
    return [
        {
            "id": 1,
            "name": "repo1",
            "full_name": "test-org/repo1",
            "default_branch": "main",
        },
        {
            "id": 2,
            "name": "repo2",
            "full_name": "test-org/repo2",
            "default_branch": "main",
        },
    ]


class TestRunCollection:
    """Tests for run_collection orchestrator function."""

    @pytest.mark.asyncio
    async def test_run_collection_success(
        self,
        test_config: Config,
        mock_repos: list[dict],
    ) -> None:
        """Test successful collection orchestration.

        Args:
            test_config: Test configuration.
            mock_repos: Mock repository list.
        """
        with (
            patch("gh_year_end.collect.orchestrator.discover_repos") as mock_discover,
            patch("gh_year_end.collect.orchestrator.collect_repo_metadata") as mock_repo_meta,
            patch("gh_year_end.collect.orchestrator.collect_pulls") as mock_pulls,
            patch("gh_year_end.collect.orchestrator.collect_issues") as mock_issues,
            patch("gh_year_end.collect.orchestrator.collect_reviews") as mock_reviews,
            patch("gh_year_end.collect.orchestrator.collect_issue_comments") as mock_issue_comments,
            patch(
                "gh_year_end.collect.orchestrator.collect_review_comments"
            ) as mock_review_comments,
            patch("gh_year_end.collect.orchestrator.collect_commits") as mock_commits,
            patch(
                "gh_year_end.collect.orchestrator._extract_issue_numbers_from_raw"
            ) as mock_extract_issues,
            patch(
                "gh_year_end.collect.orchestrator._extract_pr_numbers_from_raw"
            ) as mock_extract_prs,
            patch("gh_year_end.collect.orchestrator.GitHubClient") as mock_client,
            patch("gh_year_end.collect.orchestrator.RestClient") as _mock_rest_client,
            patch("gh_year_end.collect.orchestrator.GraphQLClient") as _mock_graphql_client,
            patch("gh_year_end.collect.orchestrator.AdaptiveRateLimiter") as mock_rate_limiter,
            patch("gh_year_end.collect.orchestrator.AsyncJSONLWriter") as _mock_writer,
            patch("os.getenv") as mock_getenv,
            tempfile.TemporaryDirectory() as tmpdir,
        ):
            # Setup config with temp dir
            test_config.storage.root = Path(tmpdir)

            # Setup mocks
            mock_getenv.return_value = "ghp_" + "a" * 36
            mock_discover.return_value = mock_repos

            # Setup collector return values
            mock_repo_meta.return_value = {"repos_processed": 2}
            mock_pulls.return_value = {"pulls_collected": 10}
            mock_issues.return_value = {"issues_collected": 5}
            mock_reviews.return_value = {"reviews_collected": 8}
            mock_issue_comments.return_value = {"comments_collected": 15}
            mock_review_comments.return_value = {"comments_collected": 12}
            mock_commits.return_value = {"commits_collected": 50}

            # Setup extraction mocks
            mock_extract_issues.return_value = {"test-org/repo1": [1, 2, 3]}
            mock_extract_prs.return_value = {"test-org/repo1": [1, 2]}

            # Setup rate limiter mock
            mock_rate_limiter_instance = MagicMock()
            mock_rate_limiter_instance.get_samples.return_value = []
            mock_rate_limiter.return_value = mock_rate_limiter_instance

            # Setup client mock
            mock_client_instance = AsyncMock()
            mock_client_instance.close = AsyncMock()
            mock_client.return_value = mock_client_instance

            # Run collection
            stats = await run_collection(test_config, force=True)

            # Verify discovery was called
            mock_discover.assert_called_once()

            # Verify all collectors were called
            mock_repo_meta.assert_called_once()
            mock_pulls.assert_called_once()
            mock_issues.assert_called_once()
            mock_reviews.assert_called_once()
            mock_issue_comments.assert_called_once()
            mock_review_comments.assert_called_once()
            mock_commits.assert_called_once()

            # Verify stats structure
            assert "discovery" in stats
            assert "repos" in stats
            assert "pulls" in stats
            assert "issues" in stats
            assert "reviews" in stats
            assert "comments" in stats
            assert "commits" in stats
            assert "duration_seconds" in stats
            assert "rate_limit_samples" in stats

            # Verify stat values
            assert stats["discovery"]["repos_discovered"] == 2
            assert stats["repos"]["repos_processed"] == 2
            assert stats["pulls"]["pulls_collected"] == 10
            assert stats["issues"]["issues_collected"] == 5
            assert stats["reviews"]["reviews_collected"] == 8
            assert stats["comments"]["total_comments"] == 27  # 15 + 12
            assert stats["commits"]["commits_collected"] == 50

            # Verify client was closed
            mock_client_instance.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_collection_no_repos(
        self,
        test_config: Config,
    ) -> None:
        """Test collection with no repositories discovered.

        Args:
            test_config: Test configuration.
        """
        with (
            patch("gh_year_end.collect.orchestrator.discover_repos") as mock_discover,
            patch("gh_year_end.collect.orchestrator.GitHubClient") as mock_client,
            patch("os.getenv") as mock_getenv,
            tempfile.TemporaryDirectory() as tmpdir,
        ):
            # Setup config with temp dir
            test_config.storage.root = Path(tmpdir)

            # Setup mocks
            mock_getenv.return_value = "ghp_" + "a" * 36
            mock_discover.return_value = []  # No repos

            # Setup client mock
            mock_client_instance = AsyncMock()
            mock_client_instance.close = AsyncMock()
            mock_client.return_value = mock_client_instance

            # Run collection
            stats = await run_collection(test_config, force=True)

            # Verify discovery was called
            mock_discover.assert_called_once()

            # Verify stats show zero repos
            assert stats["discovery"]["repos_discovered"] == 0

            # Verify client was closed
            mock_client_instance.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_collection_missing_token(
        self,
        test_config: Config,
    ) -> None:
        """Test collection fails with missing token.

        Args:
            test_config: Test configuration.
        """
        with (
            patch("os.getenv") as mock_getenv,
            tempfile.TemporaryDirectory() as tmpdir,
        ):
            # Setup config with temp dir
            test_config.storage.root = Path(tmpdir)

            # Setup mock to return None (no token)
            mock_getenv.return_value = None

            # Run collection and expect error
            with pytest.raises(CollectionError, match="GitHub token not found"):
                await run_collection(test_config, force=True)

    @pytest.mark.asyncio
    async def test_run_collection_disabled_collectors(
        self,
        test_config: Config,
        mock_repos: list[dict],
    ) -> None:
        """Test collection with some collectors disabled.

        Args:
            test_config: Test configuration.
            mock_repos: Mock repository list.
        """
        # Disable some collectors
        test_config.collection.enable.pulls = False
        test_config.collection.enable.issues = False
        test_config.collection.enable.commits = False

        with (
            patch("gh_year_end.collect.orchestrator.discover_repos") as mock_discover,
            patch("gh_year_end.collect.orchestrator.collect_repo_metadata") as mock_repo_meta,
            patch("gh_year_end.collect.orchestrator.collect_pulls") as mock_pulls,
            patch("gh_year_end.collect.orchestrator.collect_issues") as mock_issues,
            patch("gh_year_end.collect.orchestrator.collect_reviews") as mock_reviews,
            patch("gh_year_end.collect.orchestrator.collect_issue_comments") as mock_issue_comments,
            patch(
                "gh_year_end.collect.orchestrator.collect_review_comments"
            ) as mock_review_comments,
            patch("gh_year_end.collect.orchestrator.collect_commits") as mock_commits,
            patch(
                "gh_year_end.collect.orchestrator._extract_issue_numbers_from_raw"
            ) as mock_extract_issues,
            patch(
                "gh_year_end.collect.orchestrator._extract_pr_numbers_from_raw"
            ) as mock_extract_prs,
            patch("gh_year_end.collect.orchestrator.GitHubClient") as mock_client,
            patch("gh_year_end.collect.orchestrator.RestClient") as _mock_rest_client,
            patch("gh_year_end.collect.orchestrator.GraphQLClient") as _mock_graphql_client,
            patch("gh_year_end.collect.orchestrator.AdaptiveRateLimiter") as mock_rate_limiter,
            patch("gh_year_end.collect.orchestrator.AsyncJSONLWriter") as _mock_writer,
            patch("os.getenv") as mock_getenv,
            tempfile.TemporaryDirectory() as tmpdir,
        ):
            # Setup config with temp dir
            test_config.storage.root = Path(tmpdir)

            # Setup mocks
            mock_getenv.return_value = "ghp_" + "a" * 36
            mock_discover.return_value = mock_repos

            # Setup collector return values
            mock_repo_meta.return_value = {"repos_processed": 2}
            mock_reviews.return_value = {"reviews_collected": 8}
            mock_issue_comments.return_value = {"comments_collected": 15}
            mock_review_comments.return_value = {"comments_collected": 12}

            # Setup extraction mocks
            mock_extract_issues.return_value = {}
            mock_extract_prs.return_value = {}

            # Setup rate limiter mock
            mock_rate_limiter_instance = MagicMock()
            mock_rate_limiter_instance.get_samples.return_value = []
            mock_rate_limiter.return_value = mock_rate_limiter_instance

            # Setup client mock
            mock_client_instance = AsyncMock()
            mock_client_instance.close = AsyncMock()
            mock_client.return_value = mock_client_instance

            # Run collection
            stats = await run_collection(test_config, force=True)

            # Verify disabled collectors were NOT called
            mock_pulls.assert_not_called()
            mock_issues.assert_not_called()
            mock_commits.assert_not_called()

            # Verify enabled collectors were called
            mock_repo_meta.assert_called_once()
            mock_reviews.assert_called_once()
            mock_issue_comments.assert_called_once()
            mock_review_comments.assert_called_once()

            # Verify stats show skipped collectors
            assert stats["pulls"]["skipped"] is True
            assert stats["issues"]["skipped"] is True
            assert stats["commits"]["skipped"] is True

            # Verify stats show collected collectors
            assert stats["repos"]["repos_processed"] == 2
            assert stats["reviews"]["reviews_collected"] == 8
            assert stats["comments"]["total_comments"] == 27

    @pytest.mark.asyncio
    async def test_run_collection_force_flag(
        self,
        test_config: Config,
        mock_repos: list[dict],
    ) -> None:
        """Test that force flag re-collects existing data.

        Args:
            test_config: Test configuration.
            mock_repos: Mock repository list.
        """
        with (
            patch("gh_year_end.collect.orchestrator.discover_repos") as mock_discover,
            patch("gh_year_end.collect.orchestrator.collect_repo_metadata") as mock_repo_meta,
            patch("gh_year_end.collect.orchestrator.collect_pulls") as mock_pulls,
            patch("gh_year_end.collect.orchestrator.collect_issues") as mock_issues,
            patch("gh_year_end.collect.orchestrator.collect_reviews") as mock_reviews,
            patch("gh_year_end.collect.orchestrator.collect_issue_comments") as mock_issue_comments,
            patch(
                "gh_year_end.collect.orchestrator.collect_review_comments"
            ) as mock_review_comments,
            patch("gh_year_end.collect.orchestrator.collect_commits") as mock_commits,
            patch(
                "gh_year_end.collect.orchestrator._extract_issue_numbers_from_raw"
            ) as mock_extract_issues,
            patch(
                "gh_year_end.collect.orchestrator._extract_pr_numbers_from_raw"
            ) as mock_extract_prs,
            patch("gh_year_end.collect.orchestrator.GitHubClient") as mock_client,
            patch("gh_year_end.collect.orchestrator.RestClient") as _mock_rest_client,
            patch("gh_year_end.collect.orchestrator.GraphQLClient") as _mock_graphql_client,
            patch("gh_year_end.collect.orchestrator.AdaptiveRateLimiter") as mock_rate_limiter,
            patch("gh_year_end.collect.orchestrator.AsyncJSONLWriter") as _mock_writer,
            patch("os.getenv") as mock_getenv,
            tempfile.TemporaryDirectory() as tmpdir,
        ):
            # Setup config with temp dir
            test_config.storage.root = Path(tmpdir)
            paths = PathManager(test_config)
            paths.ensure_directories()

            # Create existing manifest
            manifest_data = {
                "collection_date": "2024-01-01T00:00:00",
                "config": {"target": "test-org", "year": 2024},
                "stats": {"discovery": {"repos_discovered": 1}},
            }
            with paths.manifest_path.open("w") as f:
                import json

                json.dump(manifest_data, f)

            # Setup mocks
            mock_getenv.return_value = "ghp_" + "a" * 36
            mock_discover.return_value = mock_repos

            # Setup collector return values
            mock_repo_meta.return_value = {"repos_processed": 2}
            mock_pulls.return_value = {"pulls_collected": 10}
            mock_issues.return_value = {"issues_collected": 5}
            mock_reviews.return_value = {"reviews_collected": 8}
            mock_issue_comments.return_value = {"comments_collected": 15}
            mock_review_comments.return_value = {"comments_collected": 12}
            mock_commits.return_value = {"commits_collected": 50}

            # Setup extraction mocks
            mock_extract_issues.return_value = {}
            mock_extract_prs.return_value = {}

            # Setup rate limiter mock
            mock_rate_limiter_instance = MagicMock()
            mock_rate_limiter_instance.get_samples.return_value = []
            mock_rate_limiter.return_value = mock_rate_limiter_instance

            # Setup client mock
            mock_client_instance = AsyncMock()
            mock_client_instance.close = AsyncMock()
            mock_client.return_value = mock_client_instance

            # Run collection with force=True
            stats = await run_collection(test_config, force=True)

            # Verify discovery was called (data re-collected)
            mock_discover.assert_called_once()

            # Verify new stats were generated
            assert stats["discovery"]["repos_discovered"] == 2


class TestExtractIssueNumbers:
    """Tests for _extract_issue_numbers_from_raw function."""

    @pytest.mark.asyncio
    async def test_extract_issue_numbers_success(
        self,
        test_config: Config,
    ) -> None:
        """Test extracting issue numbers from JSONL files.

        Args:
            test_config: Test configuration.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup config with temp dir
            test_config.storage.root = Path(tmpdir)
            paths = PathManager(test_config)
            paths.ensure_directories()

            # Create mock repos
            repos = [
                {"full_name": "test-org/repo1"},
                {"full_name": "test-org/repo2"},
            ]

            # Create mock issue JSONL files
            import json

            issue_file1 = paths.issues_raw_path("test-org/repo1")
            issue_file1.parent.mkdir(parents=True, exist_ok=True)
            with issue_file1.open("w") as f:
                f.write(json.dumps({"data": {"number": 1, "title": "Issue 1"}}) + "\n")
                f.write(json.dumps({"data": {"number": 2, "title": "Issue 2"}}) + "\n")
                f.write(json.dumps({"data": {"number": 3, "title": "Issue 3"}}) + "\n")

            issue_file2 = paths.issues_raw_path("test-org/repo2")
            issue_file2.parent.mkdir(parents=True, exist_ok=True)
            with issue_file2.open("w") as f:
                f.write(json.dumps({"data": {"number": 10, "title": "Issue 10"}}) + "\n")

            # Extract issue numbers
            result = await _extract_issue_numbers_from_raw(repos, paths)

            # Verify results
            assert "test-org/repo1" in result
            assert "test-org/repo2" in result
            assert result["test-org/repo1"] == [1, 2, 3]
            assert result["test-org/repo2"] == [10]

    @pytest.mark.asyncio
    async def test_extract_issue_numbers_no_files(
        self,
        test_config: Config,
    ) -> None:
        """Test extracting issue numbers with no JSONL files.

        Args:
            test_config: Test configuration.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup config with temp dir
            test_config.storage.root = Path(tmpdir)
            paths = PathManager(test_config)
            paths.ensure_directories()

            # Create mock repos
            repos = [
                {"full_name": "test-org/repo1"},
            ]

            # Extract issue numbers (no files exist)
            result = await _extract_issue_numbers_from_raw(repos, paths)

            # Verify empty result
            assert result == {}


class TestExtractPRNumbers:
    """Tests for _extract_pr_numbers_from_raw function."""

    @pytest.mark.asyncio
    async def test_extract_pr_numbers_success(
        self,
        test_config: Config,
    ) -> None:
        """Test extracting PR numbers from JSONL files.

        Args:
            test_config: Test configuration.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup config with temp dir
            test_config.storage.root = Path(tmpdir)
            paths = PathManager(test_config)
            paths.ensure_directories()

            # Create mock repos
            repos = [
                {"full_name": "test-org/repo1"},
            ]

            # Create mock PR JSONL file
            import json

            pr_file = paths.pulls_raw_path("test-org/repo1")
            pr_file.parent.mkdir(parents=True, exist_ok=True)
            with pr_file.open("w") as f:
                f.write(json.dumps({"data": {"number": 100, "title": "PR 100"}}) + "\n")
                f.write(json.dumps({"data": {"number": 101, "title": "PR 101"}}) + "\n")

            # Extract PR numbers
            result = await _extract_pr_numbers_from_raw(repos, paths)

            # Verify results
            assert "test-org/repo1" in result
            assert result["test-org/repo1"] == [100, 101]

    @pytest.mark.asyncio
    async def test_extract_pr_numbers_duplicate_numbers(
        self,
        test_config: Config,
    ) -> None:
        """Test extracting PR numbers with duplicates.

        Args:
            test_config: Test configuration.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup config with temp dir
            test_config.storage.root = Path(tmpdir)
            paths = PathManager(test_config)
            paths.ensure_directories()

            # Create mock repos
            repos = [
                {"full_name": "test-org/repo1"},
            ]

            # Create mock PR JSONL file with duplicates
            import json

            pr_file = paths.pulls_raw_path("test-org/repo1")
            pr_file.parent.mkdir(parents=True, exist_ok=True)
            with pr_file.open("w") as f:
                f.write(json.dumps({"data": {"number": 100, "title": "PR 100"}}) + "\n")
                f.write(json.dumps({"data": {"number": 100, "title": "PR 100 again"}}) + "\n")
                f.write(json.dumps({"data": {"number": 101, "title": "PR 101"}}) + "\n")

            # Extract PR numbers
            result = await _extract_pr_numbers_from_raw(repos, paths)

            # Verify duplicates are removed
            assert "test-org/repo1" in result
            assert result["test-org/repo1"] == [100, 101]
