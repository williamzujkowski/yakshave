"""Integration tests for gh-year-end data collection.

These tests require a valid GITHUB_TOKEN and make real API calls.
They use the project's own repository for controlled test data.
"""

import json
import os
import shutil
import tempfile
from pathlib import Path

import pytest

from gh_year_end.config import Config
from gh_year_end.github.auth import GitHubAuth
from gh_year_end.github.http import GitHubClient
from gh_year_end.storage.manifest import Manifest
from gh_year_end.storage.paths import PathManager


@pytest.fixture
def github_token() -> str:
    """Get GitHub token from environment or skip test.

    Returns:
        GitHub token string.

    Raises:
        pytest.skip: If GITHUB_TOKEN is not set.
    """
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        pytest.skip("GITHUB_TOKEN not set - skipping integration test")
    return token


@pytest.fixture
def temp_data_dir() -> Path:
    """Create temporary directory for test data.

    Yields:
        Path to temporary directory.

    Cleanup:
        Removes temporary directory after test.
    """
    tmpdir = Path(tempfile.mkdtemp(prefix="gh_year_end_test_"))
    yield tmpdir
    # Cleanup
    if tmpdir.exists():
        shutil.rmtree(tmpdir)


@pytest.fixture
def integration_config(temp_data_dir: Path) -> Config:
    """Create configuration for integration test.

    Args:
        temp_data_dir: Temporary directory for data storage.

    Returns:
        Config instance configured for testing.
    """
    config = Config.model_validate(
        {
            "github": {
                "target": {
                    "mode": "user",
                    "name": "williamzujkowski",  # Test against owner's repos
                },
                "discovery": {
                    "include_forks": False,
                    "include_archived": False,
                    "visibility": "public",
                },
                "windows": {
                    "year": 2024,
                    "since": "2024-01-01T00:00:00Z",
                    "until": "2025-01-01T00:00:00Z",
                },
            },
            "rate_limit": {
                "strategy": "adaptive",
                "max_concurrency": 2,  # Low concurrency for tests
            },
            "identity": {
                "humans_only": False,  # Don't filter bots in tests
            },
            "collection": {
                "enable": {
                    "pulls": False,  # Disable most collectors for speed
                    "issues": False,
                    "reviews": False,
                    "comments": False,
                    "commits": False,
                    "hygiene": False,
                }
            },
            "storage": {
                "root": str(temp_data_dir),
            },
            "report": {
                "title": "Integration Test",
                "output_dir": str(temp_data_dir / "site"),
            },
        }
    )
    return config


@pytest.mark.integration
class TestRepositoryDiscovery:
    """Integration tests for repository discovery."""

    @pytest.mark.asyncio
    async def test_discover_repos_real_api(
        self,
        github_token: str,
        integration_config: Config,
    ) -> None:
        """Test repository discovery with real GitHub API.

        This test:
        - Makes real API calls to GitHub
        - Discovers repositories from the test user
        - Writes JSONL files to temporary directory
        - Verifies data structure and content
        - Cleans up after itself

        Args:
            github_token: GitHub authentication token.
            integration_config: Test configuration.
        """
        from gh_year_end.collect.discovery import discover_repos

        # Initialize client and paths
        auth = GitHubAuth(token=github_token)
        async with GitHubClient(auth=auth) as client:
            paths = PathManager(integration_config)
            paths.ensure_directories()

            # Discover repositories
            repos = await discover_repos(
                config=integration_config,
                client=client,
                paths=paths,
            )

            # Verify we got repositories
            assert isinstance(repos, list)
            assert len(repos) > 0, "Should discover at least one repository"

            # Verify repository metadata structure
            for repo in repos:
                assert "id" in repo
                assert "name" in repo
                assert "full_name" in repo
                assert "default_branch" in repo
                assert isinstance(repo["id"], int)
                assert isinstance(repo["name"], str)
                assert isinstance(repo["full_name"], str)

            # Verify JSONL file was created
            repos_jsonl = paths.repos_raw_path
            assert repos_jsonl.exists(), "repos.jsonl should exist"

            # Verify JSONL structure
            records = []
            with repos_jsonl.open() as f:
                for line in f:
                    record = json.loads(line)
                    records.append(record)

                    # Verify envelope structure
                    assert "timestamp" in record
                    assert "source" in record
                    assert "endpoint" in record
                    assert "request_id" in record
                    assert "page" in record
                    assert "data" in record

                    # Verify data content
                    data = record["data"]
                    assert "id" in data
                    assert "name" in data
                    assert "full_name" in data

            # Verify we have records
            assert len(records) > 0, "Should have at least one JSONL record"

    @pytest.mark.asyncio
    async def test_discover_repos_with_manifest(
        self,
        github_token: str,
        integration_config: Config,
    ) -> None:
        """Test repository discovery with manifest tracking.

        Args:
            github_token: GitHub authentication token.
            integration_config: Test configuration.
        """
        from gh_year_end.collect.discovery import discover_repos

        # Initialize client and paths
        auth = GitHubAuth(token=github_token)
        async with GitHubClient(auth=auth) as client:
            paths = PathManager(integration_config)
            paths.ensure_directories()

            # Create manifest
            manifest = Manifest(
                target_mode=integration_config.github.target.mode,
                target_name=integration_config.github.target.name,
                year=integration_config.github.windows.year,
            )

            # Discover repositories
            repos = await discover_repos(
                config=integration_config,
                client=client,
                paths=paths,
            )

            # Track repos in manifest
            for repo in repos:
                manifest.add_repo(repo["full_name"])

            # Record discovery stats
            manifest.record_endpoint(
                "discovery",
                records=len(repos),
                requests=1,  # Simplified for test
            )

            # Finish and save manifest
            manifest.finish()
            manifest.save(paths.manifest_path)

            # Verify manifest file exists
            assert paths.manifest_path.exists()

            # Load and verify manifest
            loaded = Manifest.load(paths.manifest_path)
            assert loaded.target_mode == integration_config.github.target.mode
            assert loaded.target_name == integration_config.github.target.name
            assert loaded.year == integration_config.github.windows.year
            assert len(loaded.repos_processed) == len(repos)
            assert loaded.finished_at is not None

            # Verify endpoint stats
            assert "discovery" in loaded.endpoint_stats
            stats = loaded.endpoint_stats["discovery"]
            assert stats.records_fetched == len(repos)


@pytest.mark.integration
class TestJSONLDataStructure:
    """Integration tests for JSONL data structure validation."""

    @pytest.mark.asyncio
    async def test_jsonl_envelope_structure(
        self,
        github_token: str,
        integration_config: Config,
    ) -> None:
        """Test that all JSONL files follow the envelope structure.

        Args:
            github_token: GitHub authentication token.
            integration_config: Test configuration.
        """
        from gh_year_end.collect.discovery import discover_repos

        # Initialize and collect data
        auth = GitHubAuth(token=github_token)
        async with GitHubClient(auth=auth) as client:
            paths = PathManager(integration_config)
            paths.ensure_directories()

            await discover_repos(
                config=integration_config,
                client=client,
                paths=paths,
            )

            # Verify JSONL structure
            repos_jsonl = paths.repos_raw_path
            with repos_jsonl.open() as f:
                for line_num, line in enumerate(f, 1):
                    record = json.loads(line)

                    # Verify required envelope fields
                    assert "timestamp" in record, f"Line {line_num}: missing timestamp"
                    assert "source" in record, f"Line {line_num}: missing source"
                    assert "endpoint" in record, f"Line {line_num}: missing endpoint"
                    assert "request_id" in record, f"Line {line_num}: missing request_id"
                    assert "page" in record, f"Line {line_num}: missing page"
                    assert "data" in record, f"Line {line_num}: missing data"

                    # Verify field types
                    assert isinstance(record["timestamp"], str)
                    assert record["source"] in ["github_rest", "github_graphql"]
                    assert isinstance(record["endpoint"], str)
                    assert isinstance(record["request_id"], str)
                    assert isinstance(record["page"], int)
                    assert isinstance(record["data"], dict)

                    # Verify timestamp format (ISO 8601)
                    from datetime import datetime

                    datetime.fromisoformat(record["timestamp"])

    @pytest.mark.asyncio
    async def test_jsonl_deterministic_ordering(
        self,
        github_token: str,
        integration_config: Config,
    ) -> None:
        """Test that JSONL records maintain deterministic ordering.

        Args:
            github_token: GitHub authentication token.
            integration_config: Test configuration.
        """
        from gh_year_end.collect.discovery import discover_repos

        # Initialize and collect data twice
        auth = GitHubAuth(token=github_token)

        # First collection
        async with GitHubClient(auth=auth) as client:
            paths = PathManager(integration_config)
            paths.ensure_directories()

            await discover_repos(
                config=integration_config,
                client=client,
                paths=paths,
            )

        # Read first collection JSONL
        repos_jsonl = paths.repos_raw_path
        with repos_jsonl.open() as f:
            records1 = [json.loads(line)["data"]["id"] for line in f]

        # Second collection (cleanup and re-run)
        shutil.rmtree(paths.raw_root)
        paths.ensure_directories()

        async with GitHubClient(auth=auth) as client:
            await discover_repos(
                config=integration_config,
                client=client,
                paths=paths,
            )

        # Read second collection JSONL
        with repos_jsonl.open() as f:
            records2 = [json.loads(line)["data"]["id"] for line in f]

        # Verify same repositories in same order
        assert records1 == records2, "Repository IDs should be in same order"


@pytest.mark.integration
class TestErrorHandling:
    """Integration tests for error handling and recovery."""

    @pytest.mark.asyncio
    async def test_invalid_token_error(
        self,
        integration_config: Config,  # noqa: ARG002
    ) -> None:
        """Test that invalid token produces clear error.

        Args:
            integration_config: Test configuration.
        """
        from gh_year_end.github.auth import AuthenticationError

        # Test with invalid token
        with pytest.raises(AuthenticationError):
            GitHubAuth(token="invalid_token_format")

    @pytest.mark.asyncio
    async def test_rate_limit_handling(
        self,
        github_token: str,
        integration_config: Config,
    ) -> None:
        """Test that rate limit information is tracked.

        Args:
            github_token: GitHub authentication token.
            integration_config: Test configuration.
        """
        from gh_year_end.collect.discovery import discover_repos

        # Initialize and collect data
        auth = GitHubAuth(token=github_token)
        async with GitHubClient(auth=auth) as client:
            paths = PathManager(integration_config)
            paths.ensure_directories()

            await discover_repos(
                config=integration_config,
                client=client,
                paths=paths,
            )

            # Verify rate limit state was tracked
            assert client.rate_limit_state is not None
            assert client.rate_limit_state.requests_made > 0


@pytest.mark.integration
class TestDataCleanup:
    """Integration tests for data cleanup."""

    @pytest.mark.asyncio
    async def test_cleanup_removes_all_data(
        self,
        github_token: str,
        integration_config: Config,
    ) -> None:
        """Test that cleanup removes all generated data.

        Args:
            github_token: GitHub authentication token.
            integration_config: Test configuration.
        """
        from gh_year_end.collect.discovery import discover_repos

        # Initialize and collect data
        auth = GitHubAuth(token=github_token)
        async with GitHubClient(auth=auth) as client:
            paths = PathManager(integration_config)
            paths.ensure_directories()

            await discover_repos(
                config=integration_config,
                client=client,
                paths=paths,
            )

            # Verify data exists
            assert paths.raw_root.exists()
            assert paths.repos_raw_path.exists()

            # Cleanup
            shutil.rmtree(paths.raw_root)

            # Verify data removed
            assert not paths.repos_raw_path.exists()
