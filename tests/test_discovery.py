"""Tests for repository discovery module."""

from typing import Any

from gh_year_end.collect.discovery import (
    _apply_filters,
    _extract_metadata,
)
from gh_year_end.collect.filters import FilterChain
from gh_year_end.config import DiscoveryConfig


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
