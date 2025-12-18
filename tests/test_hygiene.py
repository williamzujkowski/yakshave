"""Tests for repository hygiene collection module."""

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from gh_year_end.collect.hygiene import collect_repo_hygiene
from gh_year_end.config import CollectionConfig, Config, HygieneConfig
from gh_year_end.github.rest import RestClient
from gh_year_end.storage.paths import PathManager


@pytest.fixture
def mock_config() -> Config:
    """Create a mock configuration."""
    config = MagicMock(spec=Config)
    config.collection = MagicMock(spec=CollectionConfig)
    config.collection.hygiene = MagicMock(spec=HygieneConfig)
    config.collection.hygiene.paths = [
        "SECURITY.md",
        "README.md",
        "LICENSE",
        "CONTRIBUTING.md",
    ]
    config.collection.hygiene.workflow_prefixes = [".github/workflows/"]
    return config


@pytest.fixture
def mock_rest_client() -> RestClient:
    """Create a mock REST client."""
    return MagicMock(spec=RestClient)


@pytest.fixture
def mock_paths(tmp_path: Path) -> PathManager:
    """Create a mock PathManager."""
    paths = MagicMock(spec=PathManager)
    paths.repo_tree_raw_path = (
        lambda name: tmp_path / "repo_tree" / f"{name.replace('/', '__')}.jsonl"
    )
    return paths


@pytest.fixture
def sample_repos() -> list[dict[str, Any]]:
    """Create sample repository data."""
    return [
        {
            "full_name": "org/repo1",
            "default_branch": "main",
        },
        {
            "full_name": "org/repo2",
            "default_branch": "master",
        },
        {
            "full_name": "org/empty-repo",
            "default_branch": None,  # Empty repo
        },
    ]


@pytest.fixture
def sample_tree_data() -> dict[str, Any]:
    """Create sample tree data from GitHub API."""
    return {
        "sha": "abc123",
        "tree": [
            {"path": "README.md", "type": "blob", "sha": "readme123", "size": 1024},
            {"path": "LICENSE", "type": "blob", "sha": "license123", "size": 512},
            {"path": ".github/workflows/ci.yml", "type": "blob", "sha": "ci123", "size": 256},
            {"path": ".github/workflows/release.yml", "type": "blob", "sha": "rel123", "size": 128},
            {"path": "src/main.py", "type": "blob", "sha": "main123", "size": 2048},
        ],
        "truncated": False,
    }


class TestCollectRepoHygiene:
    """Tests for collect_repo_hygiene function."""

    @pytest.mark.asyncio
    async def test_collect_repo_hygiene_success(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        mock_config: Config,
        sample_tree_data: dict[str, Any],
    ) -> None:
        """Test successful hygiene collection."""
        # Setup mock REST client
        mock_rest_client.get_repository_tree = AsyncMock(return_value=sample_tree_data)

        # Call collect_repo_hygiene
        stats = await collect_repo_hygiene(
            repos=sample_repos[:1],  # Only process first repo
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=mock_config,
        )

        # Verify results
        assert stats["repos_processed"] == 1
        assert stats["repos_skipped"] == 0
        assert stats["repos_errored"] == 0
        assert stats["files_checked"] > 0

        # Verify REST client was called
        mock_rest_client.get_repository_tree.assert_called_once_with(
            owner="org",
            repo="repo1",
            tree_sha="main",
            recursive=True,
        )

    @pytest.mark.asyncio
    async def test_collect_repo_hygiene_empty_repo(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        mock_config: Config,
    ) -> None:
        """Test hygiene collection skips repos with no default branch."""
        # Setup mock REST client
        mock_rest_client.get_repository_tree = AsyncMock()

        # Call with empty repo
        stats = await collect_repo_hygiene(
            repos=[sample_repos[2]],  # Empty repo
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=mock_config,
        )

        # Verify results
        assert stats["repos_processed"] == 0
        assert stats["repos_skipped"] == 1
        assert stats["repos_errored"] == 0

        # Verify REST client was not called
        mock_rest_client.get_repository_tree.assert_not_called()

    @pytest.mark.asyncio
    async def test_collect_repo_hygiene_tree_not_found(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        mock_config: Config,
    ) -> None:
        """Test hygiene collection handles 404 (tree not found)."""
        # Setup mock REST client to return None (404)
        mock_rest_client.get_repository_tree = AsyncMock(return_value=None)

        # Call collect_repo_hygiene
        stats = await collect_repo_hygiene(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=mock_config,
        )

        # Verify results
        assert stats["repos_processed"] == 0
        assert stats["repos_skipped"] == 1
        assert stats["repos_errored"] == 0

    @pytest.mark.asyncio
    async def test_collect_repo_hygiene_no_config(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        sample_tree_data: dict[str, Any],
    ) -> None:
        """Test hygiene collection with no config (should use empty lists)."""
        # Setup mock REST client
        mock_rest_client.get_repository_tree = AsyncMock(return_value=sample_tree_data)

        # Call with no config
        stats = await collect_repo_hygiene(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=None,
        )

        # Should still process but with no checks
        assert stats["repos_processed"] == 1
        assert stats["files_checked"] == 1  # Only workflow check

    @pytest.mark.asyncio
    async def test_collect_repo_hygiene_multiple_repos(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        mock_config: Config,
        sample_tree_data: dict[str, Any],
    ) -> None:
        """Test hygiene collection with multiple repositories."""
        # Setup mock REST client
        mock_rest_client.get_repository_tree = AsyncMock(return_value=sample_tree_data)

        # Call with two valid repos
        stats = await collect_repo_hygiene(
            repos=sample_repos[:2],
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=mock_config,
        )

        # Verify results
        assert stats["repos_processed"] == 2
        assert stats["repos_skipped"] == 0
        assert stats["repos_errored"] == 0
        assert mock_rest_client.get_repository_tree.call_count == 2

    @pytest.mark.asyncio
    async def test_collect_repo_hygiene_error_handling(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        mock_config: Config,
    ) -> None:
        """Test hygiene collection handles errors gracefully.

        When an exception occurs before any file checks (early failure),
        it's treated as a skip rather than an error. This is intentional
        behavior to handle inaccessible/empty repos gracefully.
        """
        # Setup mock REST client to raise an exception
        mock_rest_client.get_repository_tree = AsyncMock(side_effect=Exception("API error"))

        # Call collect_repo_hygiene
        stats = await collect_repo_hygiene(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=mock_config,
        )

        # Early failures (before file checks) are treated as skips
        assert stats["repos_processed"] == 0
        assert stats["repos_skipped"] == 1
        assert stats["repos_errored"] == 0


class TestTreeParsing:
    """Tests for tree data parsing and file presence checks."""

    @pytest.mark.asyncio
    async def test_file_presence_detection(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        mock_config: Config,
        sample_tree_data: dict[str, Any],
        tmp_path: Path,
    ) -> None:
        """Test that file presence is correctly detected from tree."""
        # Setup mock REST client
        mock_rest_client.get_repository_tree = AsyncMock(return_value=sample_tree_data)

        # Ensure output directory exists
        output_dir = tmp_path / "repo_tree"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Call collect_repo_hygiene
        stats = await collect_repo_hygiene(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=mock_config,
        )

        # Verify collection succeeded
        assert stats["repos_processed"] == 1

        # Read output file and verify file presence records
        output_path = mock_paths.repo_tree_raw_path(sample_repos[0]["full_name"])
        assert output_path.exists()

        # Parse JSONL and check for presence records
        import json

        presence_records = []
        with output_path.open() as f:
            for line in f:
                record = json.loads(line)
                if record.get("source") == "derived" and record.get("endpoint") == "file_presence":
                    presence_records.append(record["data"])

        # Should have records for all configured hygiene paths
        assert len(presence_records) == len(mock_config.collection.hygiene.paths)

        # Verify README.md was found
        readme_record = next(r for r in presence_records if r["path"] == "README.md")
        assert readme_record["exists"] is True
        assert readme_record["sha"] == "readme123"
        assert readme_record["size"] == 1024

        # Verify SECURITY.md was not found
        security_record = next(r for r in presence_records if r["path"] == "SECURITY.md")
        assert security_record["exists"] is False
        assert security_record["sha"] is None

    @pytest.mark.asyncio
    async def test_workflow_detection(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        mock_config: Config,
        sample_tree_data: dict[str, Any],
        tmp_path: Path,
    ) -> None:
        """Test that CI workflows are correctly detected."""
        # Setup mock REST client
        mock_rest_client.get_repository_tree = AsyncMock(return_value=sample_tree_data)

        # Ensure output directory exists
        output_dir = tmp_path / "repo_tree"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Call collect_repo_hygiene
        stats = await collect_repo_hygiene(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=mock_config,
        )

        # Verify collection succeeded
        assert stats["repos_processed"] == 1

        # Read output file and verify workflow records
        output_path = mock_paths.repo_tree_raw_path(sample_repos[0]["full_name"])

        import json

        workflow_records = []
        with output_path.open() as f:
            for line in f:
                record = json.loads(line)
                if (
                    record.get("source") == "derived"
                    and record.get("endpoint") == "workflow_presence"
                ):
                    workflow_records.append(record["data"])

        # Should have one workflow presence record
        assert len(workflow_records) == 1
        workflow_record = workflow_records[0]

        # Verify workflow files were found
        assert workflow_record["workflow_files_found"] == 2
        assert len(workflow_record["workflow_files"]) == 2

        # Verify workflow file paths
        workflow_paths = [wf["path"] for wf in workflow_record["workflow_files"]]
        assert ".github/workflows/ci.yml" in workflow_paths
        assert ".github/workflows/release.yml" in workflow_paths
