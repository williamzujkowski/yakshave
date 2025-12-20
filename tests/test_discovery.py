"""Tests for repository discovery module."""

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from gh_year_end.collect.discovery import (
    DiscoveryError,
    _apply_filters,
    _extract_metadata,
    _fetch_repos,
    _quick_scan_discovery,
    _write_raw_repos,
    discover_repos,
)
from gh_year_end.collect.filters import FilterChain
from gh_year_end.config import Config, DiscoveryConfig
from gh_year_end.github.http import GitHubClient, GitHubResponse
from gh_year_end.storage.paths import PathManager


class TestApplyFilters:
    """Tests for _apply_filters function."""

    def test_apply_filters_no_filters(self) -> None:
        """Test _apply_filters with all filters disabled."""
        repos = [
            {"name": "repo1", "fork": False, "archived": False, "private": False},
            {"name": "repo2", "fork": True, "archived": False, "private": False},
            {"name": "repo3", "fork": False, "archived": True, "private": True},
        ]

        config = DiscoveryConfig(
            include_forks=True,
            include_archived=True,
            visibility="all",
        )

        filter_chain = FilterChain(config)
        filtered, stats = _apply_filters(repos, filter_chain)
        assert len(filtered) == 3
        assert stats["total_discovered"] == 3
        assert stats["passed_filters"] == 3

    def test_apply_filters_exclude_forks(self) -> None:
        """Test _apply_filters excludes forks."""
        repos = [
            {"name": "repo1", "fork": False, "archived": False, "private": False},
            {"name": "repo2", "fork": True, "archived": False, "private": False},
            {"name": "repo3", "fork": True, "archived": False, "private": False},
        ]

        config = DiscoveryConfig(
            include_forks=False,
            include_archived=True,
            visibility="all",
        )

        filter_chain = FilterChain(config)
        filtered, stats = _apply_filters(repos, filter_chain)
        assert len(filtered) == 1
        assert filtered[0]["name"] == "repo1"
        assert stats["total_rejected"] == 2
        assert stats["rejected_by_filter"]["fork"] == 2

    def test_apply_filters_exclude_archived(self) -> None:
        """Test _apply_filters excludes archived repositories."""
        repos = [
            {"name": "repo1", "fork": False, "archived": False, "private": False},
            {"name": "repo2", "fork": False, "archived": True, "private": False},
            {"name": "repo3", "fork": False, "archived": True, "private": False},
        ]

        config = DiscoveryConfig(
            include_forks=True,
            include_archived=False,
            visibility="all",
        )

        filter_chain = FilterChain(config)
        filtered, stats = _apply_filters(repos, filter_chain)
        assert len(filtered) == 1
        assert filtered[0]["name"] == "repo1"
        assert stats["rejected_by_filter"]["archive"] == 2

    def test_apply_filters_public_only(self) -> None:
        """Test _apply_filters with visibility=public."""
        repos = [
            {"name": "repo1", "fork": False, "archived": False, "private": False},
            {"name": "repo2", "fork": False, "archived": False, "private": True},
            {"name": "repo3", "fork": False, "archived": False, "visibility": "public"},
        ]

        config = DiscoveryConfig(
            include_forks=True,
            include_archived=True,
            visibility="public",
        )

        filter_chain = FilterChain(config)
        filtered, stats = _apply_filters(repos, filter_chain)
        assert len(filtered) == 2
        assert all(r["name"] in ["repo1", "repo3"] for r in filtered)
        assert stats["rejected_by_filter"]["visibility"] == 1

    def test_apply_filters_private_only(self) -> None:
        """Test _apply_filters with visibility=private."""
        repos = [
            {"name": "repo1", "fork": False, "archived": False, "private": False},
            {"name": "repo2", "fork": False, "archived": False, "private": True},
            {"name": "repo3", "fork": False, "archived": False, "visibility": "private"},
        ]

        config = DiscoveryConfig(
            include_forks=True,
            include_archived=True,
            visibility="private",
        )

        filter_chain = FilterChain(config)
        filtered, stats = _apply_filters(repos, filter_chain)
        assert len(filtered) == 2
        assert all(r["name"] in ["repo2", "repo3"] for r in filtered)
        assert stats["rejected_by_filter"]["visibility"] == 1

    def test_apply_filters_visibility_field_fallback(self) -> None:
        """Test _apply_filters falls back to private field for visibility."""
        repos = [
            {"name": "repo1", "private": False},  # No visibility field
            {"name": "repo2", "private": True},  # No visibility field
        ]

        config = DiscoveryConfig(
            include_forks=True,
            include_archived=True,
            visibility="public",
        )

        filter_chain = FilterChain(config)
        filtered, stats = _apply_filters(repos, filter_chain)
        assert len(filtered) == 1
        assert filtered[0]["name"] == "repo1"
        assert stats["rejected_by_filter"]["visibility"] == 1

    def test_apply_filters_combined(self) -> None:
        """Test _apply_filters with multiple filters active."""
        repos = [
            {"name": "repo1", "fork": False, "archived": False, "private": False},
            {"name": "repo2", "fork": True, "archived": False, "private": False},
            {"name": "repo3", "fork": False, "archived": True, "private": False},
            {"name": "repo4", "fork": False, "archived": False, "private": True},
        ]

        config = DiscoveryConfig(
            include_forks=False,
            include_archived=False,
            visibility="public",
        )

        filter_chain = FilterChain(config)
        filtered, stats = _apply_filters(repos, filter_chain)
        assert len(filtered) == 1
        assert filtered[0]["name"] == "repo1"
        assert stats["total_rejected"] == 3

    def test_apply_filters_empty_list(self) -> None:
        """Test _apply_filters with empty repository list."""
        repos: list[dict[str, Any]] = []

        config = DiscoveryConfig(
            include_forks=False,
            include_archived=False,
            visibility="all",
        )

        filter_chain = FilterChain(config)
        filtered, stats = _apply_filters(repos, filter_chain)
        assert len(filtered) == 0
        assert stats["total_discovered"] == 0
        assert stats["passed_filters"] == 0

    def test_apply_filters_missing_fork_field(self) -> None:
        """Test _apply_filters handles missing fork field."""
        repos = [
            {"name": "repo1", "archived": False, "private": False},  # No fork field
        ]

        config = DiscoveryConfig(
            include_forks=False,
            include_archived=True,
            visibility="all",
        )

        filter_chain = FilterChain(config)
        filtered, stats = _apply_filters(repos, filter_chain)
        # Should include repo since fork field is missing (defaults to False)
        assert len(filtered) == 1
        assert stats["total_rejected"] == 0

    def test_apply_filters_missing_archived_field(self) -> None:
        """Test _apply_filters handles missing archived field."""
        repos = [
            {"name": "repo1", "fork": False, "private": False},  # No archived field
        ]

        config = DiscoveryConfig(
            include_forks=True,
            include_archived=False,
            visibility="all",
        )

        filter_chain = FilterChain(config)
        filtered, stats = _apply_filters(repos, filter_chain)
        # Should include repo since archived field is missing (defaults to False)
        assert len(filtered) == 1
        assert stats["total_rejected"] == 0


class TestExtractMetadata:
    """Tests for _extract_metadata function."""

    def test_extract_metadata_all_fields(self) -> None:
        """Test _extract_metadata extracts all fields correctly."""
        repos = [
            {
                "id": 123,
                "name": "test-repo",
                "full_name": "owner/test-repo",
                "description": "A test repository",
                "default_branch": "main",
                "fork": False,
                "archived": False,
                "visibility": "public",
                "created_at": "2023-01-01T00:00:00Z",
                "updated_at": "2023-06-01T00:00:00Z",
                "pushed_at": "2023-06-15T00:00:00Z",
                "language": "Python",
                "stargazers_count": 100,
                "forks_count": 10,
                "open_issues_count": 5,
                "size": 1024,
            }
        ]

        metadata = _extract_metadata(repos)

        assert len(metadata) == 1
        meta = metadata[0]
        assert meta["id"] == 123
        assert meta["name"] == "test-repo"
        assert meta["full_name"] == "owner/test-repo"
        assert meta["description"] == "A test repository"
        assert meta["default_branch"] == "main"
        assert meta["is_fork"] is False
        assert meta["is_archived"] is False
        assert meta["visibility"] == "public"
        assert meta["language"] == "Python"
        assert meta["stargazers_count"] == 100

    def test_extract_metadata_missing_optional_fields(self) -> None:
        """Test _extract_metadata handles missing optional fields gracefully."""
        repos = [
            {
                "id": 123,
                "name": "test-repo",
                "full_name": "owner/test-repo",
                # Missing description, language, etc.
            }
        ]

        metadata = _extract_metadata(repos)

        assert len(metadata) == 1
        meta = metadata[0]
        assert meta["id"] == 123
        assert meta["description"] is None
        assert meta["default_branch"] == "main"  # Default
        assert meta["language"] is None
        assert meta["stargazers_count"] == 0  # Default
        assert meta["forks_count"] == 0  # Default
        assert meta["open_issues_count"] == 0  # Default
        assert meta["size"] == 0  # Default

    def test_extract_metadata_visibility_fallback(self) -> None:
        """Test _extract_metadata falls back to private field for visibility."""
        repos = [
            {
                "id": 123,
                "name": "public-repo",
                "full_name": "owner/public-repo",
                "private": False,
                # No visibility field
            },
            {
                "id": 456,
                "name": "private-repo",
                "full_name": "owner/private-repo",
                "private": True,
                # No visibility field
            },
        ]

        metadata = _extract_metadata(repos)

        assert len(metadata) == 2
        assert metadata[0]["visibility"] == "public"
        assert metadata[1]["visibility"] == "private"

    def test_extract_metadata_visibility_field_preferred(self) -> None:
        """Test _extract_metadata prefers visibility field over private."""
        repos = [
            {
                "id": 123,
                "name": "test-repo",
                "full_name": "owner/test-repo",
                "visibility": "public",
                "private": True,  # Should be ignored in favor of visibility
            }
        ]

        metadata = _extract_metadata(repos)

        assert len(metadata) == 1
        assert metadata[0]["visibility"] == "public"

    def test_extract_metadata_missing_required_fields(self) -> None:
        """Test _extract_metadata skips repos with missing required fields."""
        repos = [
            {
                "id": 123,
                "name": "valid-repo",
                "full_name": "owner/valid-repo",
            },
            {
                # Missing id
                "name": "invalid-repo",
                "full_name": "owner/invalid-repo",
            },
            {
                "id": 456,
                # Missing name
                "full_name": "owner/another-invalid",
            },
        ]

        metadata = _extract_metadata(repos)

        # Should only include the valid repo
        assert len(metadata) == 1
        assert metadata[0]["id"] == 123

    def test_extract_metadata_multiple_repos(self) -> None:
        """Test _extract_metadata processes multiple repositories."""
        repos = [
            {
                "id": i,
                "name": f"repo{i}",
                "full_name": f"owner/repo{i}",
            }
            for i in range(5)
        ]

        metadata = _extract_metadata(repos)

        assert len(metadata) == 5
        for i, meta in enumerate(metadata):
            assert meta["id"] == i
            assert meta["name"] == f"repo{i}"

    def test_extract_metadata_empty_list(self) -> None:
        """Test _extract_metadata with empty repository list."""
        repos: list[dict[str, Any]] = []

        metadata = _extract_metadata(repos)

        assert len(metadata) == 0

    def test_extract_metadata_fork_field_mapping(self) -> None:
        """Test _extract_metadata correctly maps fork to is_fork."""
        repos = [
            {
                "id": 123,
                "name": "test-repo",
                "full_name": "owner/test-repo",
                "fork": True,
            }
        ]

        metadata = _extract_metadata(repos)

        assert len(metadata) == 1
        assert metadata[0]["is_fork"] is True
        assert "fork" not in metadata[0]  # Should use is_fork, not fork

    def test_extract_metadata_archived_field_mapping(self) -> None:
        """Test _extract_metadata correctly maps archived to is_archived."""
        repos = [
            {
                "id": 123,
                "name": "test-repo",
                "full_name": "owner/test-repo",
                "archived": True,
            }
        ]

        metadata = _extract_metadata(repos)

        assert len(metadata) == 1
        assert metadata[0]["is_archived"] is True
        assert "archived" not in metadata[0]  # Should use is_archived, not archived

    def test_extract_metadata_default_values(self) -> None:
        """Test _extract_metadata applies correct default values."""
        repos = [
            {
                "id": 123,
                "name": "minimal-repo",
                "full_name": "owner/minimal-repo",
                # All other fields missing
            }
        ]

        metadata = _extract_metadata(repos)

        assert len(metadata) == 1
        meta = metadata[0]
        assert meta["description"] is None
        assert meta["default_branch"] == "main"
        assert meta["is_fork"] is False
        assert meta["is_archived"] is False
        assert meta["language"] is None
        assert meta["stargazers_count"] == 0
        assert meta["forks_count"] == 0
        assert meta["open_issues_count"] == 0
        assert meta["size"] == 0

    def test_extract_metadata_preserves_none_values(self) -> None:
        """Test _extract_metadata preserves explicit None values."""
        repos = [
            {
                "id": 123,
                "name": "test-repo",
                "full_name": "owner/test-repo",
                "description": None,
                "language": None,
            }
        ]

        metadata = _extract_metadata(repos)

        assert len(metadata) == 1
        meta = metadata[0]
        assert meta["description"] is None
        assert meta["language"] is None


class TestFetchRepos:
    """Tests for _fetch_repos function."""

    @pytest.mark.asyncio
    async def test_fetch_repos_org_single_page(self) -> None:
        """Test _fetch_repos for organization with single page."""
        mock_client = AsyncMock(spec=GitHubClient)
        repos_data = [
            {"id": 1, "name": "repo1", "full_name": "org/repo1"},
            {"id": 2, "name": "repo2", "full_name": "org/repo2"},
        ]

        # First call returns repos, second returns empty list
        mock_client.get.side_effect = [
            GitHubResponse(
                status_code=200,
                data=repos_data,
                headers=MagicMock(),
                url="/orgs/test-org/repos",
            ),
            GitHubResponse(
                status_code=200,
                data=[],
                headers=MagicMock(),
                url="/orgs/test-org/repos",
            ),
        ]

        repos = await _fetch_repos(mock_client, "org", "test-org")

        assert len(repos) == 2
        assert repos[0]["name"] == "repo1"
        assert repos[1]["name"] == "repo2"

        # Verify API calls
        assert mock_client.get.call_count == 2
        first_call = mock_client.get.call_args_list[0]
        assert first_call[0][0] == "/orgs/test-org/repos"
        assert first_call[1]["params"]["page"] == 1
        assert first_call[1]["params"]["per_page"] == 100

    @pytest.mark.asyncio
    async def test_fetch_repos_user_single_page(self) -> None:
        """Test _fetch_repos for user with single page."""
        mock_client = AsyncMock(spec=GitHubClient)
        repos_data = [{"id": 1, "name": "repo1", "full_name": "user/repo1"}]

        mock_client.get.side_effect = [
            GitHubResponse(
                status_code=200,
                data=repos_data,
                headers=MagicMock(),
                url="/users/test-user/repos",
            ),
            GitHubResponse(
                status_code=200,
                data=[],
                headers=MagicMock(),
                url="/users/test-user/repos",
            ),
        ]

        repos = await _fetch_repos(mock_client, "user", "test-user")

        assert len(repos) == 1
        assert repos[0]["name"] == "repo1"

        # Verify correct endpoint used
        first_call = mock_client.get.call_args_list[0]
        assert first_call[0][0] == "/users/test-user/repos"

    @pytest.mark.asyncio
    async def test_fetch_repos_multiple_pages(self) -> None:
        """Test _fetch_repos with pagination across multiple pages."""
        mock_client = AsyncMock(spec=GitHubClient)

        # Page 1
        page1_data = [
            {"id": i, "name": f"repo{i}", "full_name": f"org/repo{i}"} for i in range(100)
        ]
        # Page 2
        page2_data = [
            {"id": i, "name": f"repo{i}", "full_name": f"org/repo{i}"} for i in range(100, 150)
        ]

        mock_client.get.side_effect = [
            GitHubResponse(
                status_code=200, data=page1_data, headers=MagicMock(), url="/orgs/org/repos"
            ),
            GitHubResponse(
                status_code=200, data=page2_data, headers=MagicMock(), url="/orgs/org/repos"
            ),
            GitHubResponse(status_code=200, data=[], headers=MagicMock(), url="/orgs/org/repos"),
        ]

        repos = await _fetch_repos(mock_client, "org", "org")

        assert len(repos) == 150
        assert repos[0]["id"] == 0
        assert repos[99]["id"] == 99
        assert repos[100]["id"] == 100
        assert repos[149]["id"] == 149

        # Verify pagination parameters
        assert mock_client.get.call_count == 3
        assert mock_client.get.call_args_list[0][1]["params"]["page"] == 1
        assert mock_client.get.call_args_list[1][1]["params"]["page"] == 2
        assert mock_client.get.call_args_list[2][1]["params"]["page"] == 3

    @pytest.mark.asyncio
    async def test_fetch_repos_empty_result(self) -> None:
        """Test _fetch_repos with empty result."""
        mock_client = AsyncMock(spec=GitHubClient)

        mock_client.get.return_value = GitHubResponse(
            status_code=200,
            data=[],
            headers=MagicMock(),
            url="/orgs/empty-org/repos",
        )

        repos = await _fetch_repos(mock_client, "org", "empty-org")

        assert len(repos) == 0
        assert mock_client.get.call_count == 1

    @pytest.mark.asyncio
    async def test_fetch_repos_api_error(self) -> None:
        """Test _fetch_repos raises DiscoveryError on API error."""
        mock_client = AsyncMock(spec=GitHubClient)

        mock_client.get.return_value = GitHubResponse(
            status_code=404,
            data={"message": "Not Found"},
            headers=MagicMock(),
            url="/orgs/missing-org/repos",
        )

        with pytest.raises(DiscoveryError, match="API error 404"):
            await _fetch_repos(mock_client, "org", "missing-org")

    @pytest.mark.asyncio
    async def test_fetch_repos_exception(self) -> None:
        """Test _fetch_repos raises DiscoveryError on exception."""
        mock_client = AsyncMock(spec=GitHubClient)

        mock_client.get.side_effect = Exception("Network error")

        with pytest.raises(DiscoveryError, match="Failed to fetch repositories"):
            await _fetch_repos(mock_client, "org", "test-org")

    @pytest.mark.asyncio
    async def test_fetch_repos_invalid_response_type(self) -> None:
        """Test _fetch_repos raises DiscoveryError on invalid response type."""
        mock_client = AsyncMock(spec=GitHubClient)

        # Return dict instead of list
        mock_client.get.return_value = GitHubResponse(
            status_code=200,
            data={"invalid": "response"},
            headers=MagicMock(),
            url="/orgs/test-org/repos",
        )

        with pytest.raises(DiscoveryError, match="Expected list response"):
            await _fetch_repos(mock_client, "org", "test-org")


class TestQuickScanDiscovery:
    """Tests for _quick_scan_discovery function."""

    @pytest.mark.asyncio
    async def test_quick_scan_single_page(self) -> None:
        """Test quick scan discovery with single page."""
        mock_client = AsyncMock(spec=GitHubClient)
        config = DiscoveryConfig(include_forks=False, visibility="public")
        filter_chain = FilterChain(config)

        search_results = {
            "total_count": 2,
            "items": [
                {"id": 1, "name": "repo1", "full_name": "org/repo1"},
                {"id": 2, "name": "repo2", "full_name": "org/repo2"},
            ],
        }

        mock_client.get.return_value = GitHubResponse(
            status_code=200,
            data=search_results,
            headers=MagicMock(),
            url="/search/repositories",
        )

        repos = await _quick_scan_discovery(mock_client, "org", "test-org", filter_chain)

        assert len(repos) == 2
        assert repos[0]["name"] == "repo1"

        # Verify search endpoint called
        mock_client.get.assert_called_once()
        call_args = mock_client.get.call_args
        assert call_args[0][0] == "/search/repositories"
        assert "q" in call_args[1]["params"]
        assert call_args[1]["params"]["per_page"] == 100

    @pytest.mark.asyncio
    async def test_quick_scan_multiple_pages(self) -> None:
        """Test quick scan discovery with multiple pages."""
        mock_client = AsyncMock(spec=GitHubClient)
        config = DiscoveryConfig(include_forks=False)
        filter_chain = FilterChain(config)

        # Page 1: 100 results
        page1_items = [
            {"id": i, "name": f"repo{i}", "full_name": f"org/repo{i}"} for i in range(100)
        ]
        # Page 2: 50 results
        page2_items = [
            {"id": i, "name": f"repo{i}", "full_name": f"org/repo{i}"} for i in range(100, 150)
        ]

        mock_client.get.side_effect = [
            GitHubResponse(
                status_code=200,
                data={"total_count": 150, "items": page1_items},
                headers=MagicMock(),
            ),
            GitHubResponse(
                status_code=200,
                data={"total_count": 150, "items": page2_items},
                headers=MagicMock(),
            ),
        ]

        repos = await _quick_scan_discovery(mock_client, "org", "test-org", filter_chain)

        assert len(repos) == 150
        assert mock_client.get.call_count == 2

    @pytest.mark.asyncio
    async def test_quick_scan_max_results_limit(self) -> None:
        """Test quick scan respects 1000 result limit."""
        mock_client = AsyncMock(spec=GitHubClient)
        config = DiscoveryConfig()
        filter_chain = FilterChain(config)

        # Return 100 items per page, total_count = 2000
        items = [{"id": i, "name": f"repo{i}", "full_name": f"org/repo{i}"} for i in range(100)]

        # Simulate 10 pages (max allowed)
        responses = [
            GitHubResponse(
                status_code=200,
                data={"total_count": 2000, "items": items},
                headers=MagicMock(),
            )
            for _ in range(10)
        ]
        # Add one more that would break the 1000 limit
        responses.append(
            GitHubResponse(
                status_code=200,
                data={"total_count": 2000, "items": items},
                headers=MagicMock(),
            )
        )

        mock_client.get.side_effect = responses

        repos = await _quick_scan_discovery(mock_client, "org", "test-org", filter_chain)

        # Should stop at 1000 items (10 pages * 100 items)
        assert len(repos) == 1000
        assert mock_client.get.call_count == 10

    @pytest.mark.asyncio
    async def test_quick_scan_empty_results(self) -> None:
        """Test quick scan with no results."""
        mock_client = AsyncMock(spec=GitHubClient)
        config = DiscoveryConfig()
        filter_chain = FilterChain(config)

        mock_client.get.return_value = GitHubResponse(
            status_code=200,
            data={"total_count": 0, "items": []},
            headers=MagicMock(),
        )

        repos = await _quick_scan_discovery(mock_client, "org", "test-org", filter_chain)

        assert len(repos) == 0
        assert mock_client.get.call_count == 1

    @pytest.mark.asyncio
    async def test_quick_scan_fallback_on_api_error(self) -> None:
        """Test quick scan falls back to thorough discovery on API error."""
        mock_client = AsyncMock(spec=GitHubClient)
        config = DiscoveryConfig()
        filter_chain = FilterChain(config)

        # Search API fails
        search_response = GitHubResponse(
            status_code=422,
            data={"message": "Validation failed"},
            headers=MagicMock(),
        )

        # Fallback to list API succeeds
        list_response = GitHubResponse(
            status_code=200,
            data=[{"id": 1, "name": "repo1", "full_name": "org/repo1"}],
            headers=MagicMock(),
        )

        mock_client.get.side_effect = [
            search_response,  # Search fails
            list_response,  # List succeeds
            GitHubResponse(status_code=200, data=[], headers=MagicMock()),  # End of list
        ]

        repos = await _quick_scan_discovery(mock_client, "org", "test-org", filter_chain)

        assert len(repos) == 1
        assert repos[0]["name"] == "repo1"

    @pytest.mark.asyncio
    async def test_quick_scan_fallback_on_invalid_response(self) -> None:
        """Test quick scan falls back on invalid response format."""
        mock_client = AsyncMock(spec=GitHubClient)
        config = DiscoveryConfig()
        filter_chain = FilterChain(config)

        # Search API returns invalid format
        search_response = GitHubResponse(
            status_code=200,
            data={"invalid": "format"},  # Missing "items" key
            headers=MagicMock(),
        )

        # Fallback to list API
        list_response = GitHubResponse(
            status_code=200,
            data=[{"id": 1, "name": "repo1", "full_name": "org/repo1"}],
            headers=MagicMock(),
        )

        mock_client.get.side_effect = [
            search_response,
            list_response,
            GitHubResponse(status_code=200, data=[], headers=MagicMock()),
        ]

        repos = await _quick_scan_discovery(mock_client, "org", "test-org", filter_chain)

        assert len(repos) == 1

    @pytest.mark.asyncio
    async def test_quick_scan_fallback_on_exception(self) -> None:
        """Test quick scan falls back on exception."""
        mock_client = AsyncMock(spec=GitHubClient)
        config = DiscoveryConfig()
        filter_chain = FilterChain(config)

        # First call raises exception, subsequent calls succeed
        list_response = GitHubResponse(
            status_code=200,
            data=[{"id": 1, "name": "repo1", "full_name": "org/repo1"}],
            headers=MagicMock(),
        )

        mock_client.get.side_effect = [
            Exception("Network error"),  # Search fails
            list_response,  # List succeeds
            GitHubResponse(status_code=200, data=[], headers=MagicMock()),
        ]

        repos = await _quick_scan_discovery(mock_client, "org", "test-org", filter_chain)

        assert len(repos) == 1

    @pytest.mark.asyncio
    async def test_quick_scan_user_mode(self) -> None:
        """Test quick scan with user mode instead of org."""
        mock_client = AsyncMock(spec=GitHubClient)
        config = DiscoveryConfig()
        filter_chain = FilterChain(config)

        mock_client.get.return_value = GitHubResponse(
            status_code=200,
            data={
                "total_count": 1,
                "items": [{"id": 1, "name": "repo1", "full_name": "user/repo1"}],
            },
            headers=MagicMock(),
        )

        repos = await _quick_scan_discovery(mock_client, "user", "test-user", filter_chain)

        assert len(repos) == 1

        # Verify query includes user: instead of org:
        call_args = mock_client.get.call_args
        query = call_args[1]["params"]["q"]
        assert "user:test-user" in query


class TestWriteRawRepos:
    """Tests for _write_raw_repos function."""

    @pytest.mark.asyncio
    async def test_write_raw_repos(self, tmp_path: Path) -> None:
        """Test writing raw repos to JSONL storage."""
        repos = [
            {"id": 1, "name": "repo1", "full_name": "org/repo1"},
            {"id": 2, "name": "repo2", "full_name": "org/repo2"},
        ]

        mock_client = Mock(spec=GitHubClient)

        # Create minimal config and paths
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test-org"},
                    "auth": {"token_env": "GITHUB_TOKEN"},
                    "windows": {
                        "year": 2024,
                        "since": "2024-01-01T00:00:00Z",
                        "until": "2025-01-01T00:00:00Z",
                    },
                },
                "storage": {"root": str(tmp_path / "data")},
            }
        )
        paths = PathManager(config)
        paths.ensure_directories()

        filter_stats = {
            "total_discovered": 3,
            "passed_filters": 2,
            "total_rejected": 1,
            "rejected_by_filter": {"fork": 1},
        }

        await _write_raw_repos(repos, mock_client, paths, filter_stats)

        # Verify JSONL file created
        assert paths.repos_raw_path.exists()

        # Verify manifest created
        manifest_path = paths.repos_raw_path.parent / "manifest.json"
        assert manifest_path.exists()

        manifest = json.loads(manifest_path.read_text())
        assert manifest["repos_discovered"] == 2
        assert manifest["filter_stats"]["total_discovered"] == 3
        assert manifest["filter_stats"]["total_rejected"] == 1

    @pytest.mark.asyncio
    async def test_write_raw_repos_empty_list(self, tmp_path: Path) -> None:
        """Test writing empty repo list."""
        repos: list[dict[str, Any]] = []

        mock_client = Mock(spec=GitHubClient)

        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test-org"},
                    "auth": {"token_env": "GITHUB_TOKEN"},
                    "windows": {
                        "year": 2024,
                        "since": "2024-01-01T00:00:00Z",
                        "until": "2025-01-01T00:00:00Z",
                    },
                },
                "storage": {"root": str(tmp_path / "data")},
            }
        )
        paths = PathManager(config)
        paths.ensure_directories()

        filter_stats = {
            "total_discovered": 0,
            "passed_filters": 0,
            "total_rejected": 0,
            "rejected_by_filter": {},
        }

        await _write_raw_repos(repos, mock_client, paths, filter_stats)

        # Verify files created even with no repos
        assert paths.repos_raw_path.exists()
        manifest_path = paths.repos_raw_path.parent / "manifest.json"
        assert manifest_path.exists()

        manifest = json.loads(manifest_path.read_text())
        assert manifest["repos_discovered"] == 0


class TestDiscoverRepos:
    """Tests for main discover_repos function."""

    @pytest.mark.asyncio
    async def test_discover_repos_thorough_mode(self, tmp_path: Path) -> None:
        """Test discover_repos with thorough discovery mode."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test-org"},
                    "auth": {"token_env": "GITHUB_TOKEN"},
                    "discovery": {
                        "quick_scan": {"enabled": False},
                        "include_forks": False,
                    },
                    "windows": {
                        "year": 2024,
                        "since": "2024-01-01T00:00:00Z",
                        "until": "2025-01-01T00:00:00Z",
                    },
                },
                "storage": {"root": str(tmp_path / "data")},
            }
        )

        mock_client = AsyncMock(spec=GitHubClient)
        repos_data = [
            {"id": 1, "name": "repo1", "full_name": "org/repo1", "fork": False},
            {"id": 2, "name": "repo2", "full_name": "org/repo2", "fork": False},
        ]

        mock_client.get.side_effect = [
            GitHubResponse(status_code=200, data=repos_data, headers=MagicMock()),
            GitHubResponse(status_code=200, data=[], headers=MagicMock()),
        ]

        paths = PathManager(config)
        paths.ensure_directories()

        metadata = await discover_repos(config, mock_client, paths)

        assert len(metadata) == 2
        assert metadata[0]["name"] == "repo1"
        assert metadata[1]["name"] == "repo2"

        # Verify raw data written
        assert paths.repos_raw_path.exists()

    @pytest.mark.asyncio
    async def test_discover_repos_quick_scan_mode(self, tmp_path: Path) -> None:
        """Test discover_repos with quick scan mode."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test-org"},
                    "auth": {"token_env": "GITHUB_TOKEN"},
                    "discovery": {
                        "quick_scan": {"enabled": True},
                        "include_forks": False,
                    },
                    "windows": {
                        "year": 2024,
                        "since": "2024-01-01T00:00:00Z",
                        "until": "2025-01-01T00:00:00Z",
                    },
                },
                "storage": {"root": str(tmp_path / "data")},
            }
        )

        mock_client = AsyncMock(spec=GitHubClient)

        search_results = {
            "total_count": 1,
            "items": [{"id": 1, "name": "repo1", "full_name": "org/repo1", "fork": False}],
        }

        mock_client.get.return_value = GitHubResponse(
            status_code=200,
            data=search_results,
            headers=MagicMock(),
        )

        paths = PathManager(config)
        paths.ensure_directories()

        metadata = await discover_repos(config, mock_client, paths)

        assert len(metadata) == 1
        assert metadata[0]["name"] == "repo1"

    @pytest.mark.asyncio
    async def test_discover_repos_with_filters(self, tmp_path: Path) -> None:
        """Test discover_repos applies filters correctly."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test-org"},
                    "auth": {"token_env": "GITHUB_TOKEN"},
                    "discovery": {
                        "quick_scan": {"enabled": False},
                        "include_forks": False,
                        "include_archived": False,
                    },
                    "windows": {
                        "year": 2024,
                        "since": "2024-01-01T00:00:00Z",
                        "until": "2025-01-01T00:00:00Z",
                    },
                },
                "storage": {"root": str(tmp_path / "data")},
            }
        )

        mock_client = AsyncMock(spec=GitHubClient)
        repos_data = [
            {"id": 1, "name": "repo1", "full_name": "org/repo1", "fork": False, "archived": False},
            {"id": 2, "name": "repo2", "full_name": "org/repo2", "fork": True, "archived": False},
            {"id": 3, "name": "repo3", "full_name": "org/repo3", "fork": False, "archived": True},
        ]

        mock_client.get.side_effect = [
            GitHubResponse(status_code=200, data=repos_data, headers=MagicMock()),
            GitHubResponse(status_code=200, data=[], headers=MagicMock()),
        ]

        paths = PathManager(config)
        paths.ensure_directories()

        metadata = await discover_repos(config, mock_client, paths)

        # Only repo1 should pass filters
        assert len(metadata) == 1
        assert metadata[0]["name"] == "repo1"

    @pytest.mark.asyncio
    async def test_discover_repos_user_mode(self, tmp_path: Path) -> None:
        """Test discover_repos with user mode."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "user", "name": "test-user"},
                    "auth": {"token_env": "GITHUB_TOKEN"},
                    "discovery": {"quick_scan": {"enabled": False}},
                    "windows": {
                        "year": 2024,
                        "since": "2024-01-01T00:00:00Z",
                        "until": "2025-01-01T00:00:00Z",
                    },
                },
                "storage": {"root": str(tmp_path / "data")},
            }
        )

        mock_client = AsyncMock(spec=GitHubClient)
        repos_data = [{"id": 1, "name": "repo1", "full_name": "user/repo1"}]

        mock_client.get.side_effect = [
            GitHubResponse(status_code=200, data=repos_data, headers=MagicMock()),
            GitHubResponse(status_code=200, data=[], headers=MagicMock()),
        ]

        paths = PathManager(config)
        paths.ensure_directories()

        metadata = await discover_repos(config, mock_client, paths)

        assert len(metadata) == 1

        # Verify user endpoint was called
        assert mock_client.get.call_args_list[0][0][0] == "/users/test-user/repos"

    @pytest.mark.asyncio
    async def test_discover_repos_empty_results(self, tmp_path: Path) -> None:
        """Test discover_repos with no repositories found."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "empty-org"},
                    "auth": {"token_env": "GITHUB_TOKEN"},
                    "discovery": {"quick_scan": {"enabled": False}},
                    "windows": {
                        "year": 2024,
                        "since": "2024-01-01T00:00:00Z",
                        "until": "2025-01-01T00:00:00Z",
                    },
                },
                "storage": {"root": str(tmp_path / "data")},
            }
        )

        mock_client = AsyncMock(spec=GitHubClient)
        mock_client.get.return_value = GitHubResponse(
            status_code=200,
            data=[],
            headers=MagicMock(),
        )

        paths = PathManager(config)
        paths.ensure_directories()

        metadata = await discover_repos(config, mock_client, paths)

        assert len(metadata) == 0

        # Verify manifest still written
        manifest_path = paths.repos_raw_path.parent / "manifest.json"
        assert manifest_path.exists()
