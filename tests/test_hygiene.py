"""Tests for repository hygiene collection module."""

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gh_year_end.collect.hygiene import (
    HygieneCollectionError,
    _get_branch_protection,
    _get_default_branch,
    _get_security_features,
    _parse_repo_name,
    _select_repos_for_collection,
    _sort_repos_by_metric,
    collect_branch_protection,
    collect_repo_hygiene,
    collect_security_features,
)
from gh_year_end.config import (
    BranchProtectionConfig,
    CollectionConfig,
    Config,
    HygieneConfig,
    SecurityFeaturesConfig,
)
from gh_year_end.github.rest import RestClient
from gh_year_end.storage.checkpoint import CheckpointManager
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


class TestSecurityFeatures:
    """Tests for security features collection."""

    @pytest.fixture
    def security_config(self) -> Config:
        """Create config with security features enabled."""
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.security_features = MagicMock(spec=SecurityFeaturesConfig)
        config.collection.hygiene.security_features.best_effort = True
        return config

    @pytest.fixture
    def security_disabled_config(self) -> Config:
        """Create config with security features disabled."""
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.security_features = MagicMock(spec=SecurityFeaturesConfig)
        config.collection.hygiene.security_features.best_effort = False
        return config

    @pytest.mark.asyncio
    async def test_collect_security_features_disabled(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        security_disabled_config: Config,
    ) -> None:
        """Test security features collection when disabled."""
        stats = await collect_security_features(
            repos=sample_repos,
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=security_disabled_config,
            checkpoint=None,
        )

        assert stats["repos_processed"] == 0
        assert stats["repos_skipped"] == len(sample_repos)
        assert stats["repos_total"] == len(sample_repos)

    @pytest.mark.asyncio
    async def test_collect_security_features_all_enabled(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        security_config: Config,
        tmp_path: Path,
    ) -> None:
        """Test security features collection with all features enabled."""
        # Mock REST client responses
        mock_rest_client.check_vulnerability_alerts = AsyncMock(return_value=True)
        mock_rest_client.get_repo_security_analysis = AsyncMock(
            return_value={
                "security_and_analysis": {
                    "dependabot_security_updates": {"status": "enabled"},
                    "secret_scanning": {"status": "enabled"},
                    "secret_scanning_push_protection": {"status": "enabled"},
                }
            }
        )

        # Mock paths
        mock_paths.security_features_raw_path = (
            lambda name: tmp_path / "security" / f"{name.replace('/', '__')}.jsonl"
        )

        # Ensure directory exists
        (tmp_path / "security").mkdir(parents=True, exist_ok=True)

        stats = await collect_security_features(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=security_config,
            checkpoint=None,
        )

        assert stats["repos_processed"] == 1
        assert stats["repos_with_all_features"] == 1
        assert stats["repos_with_partial_features"] == 0
        assert stats["repos_with_no_access"] == 0

    @pytest.mark.asyncio
    async def test_collect_security_features_no_access(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        security_config: Config,
        tmp_path: Path,
    ) -> None:
        """Test security features collection with no access."""
        # Mock REST client to raise exceptions (permission denied)
        mock_rest_client.check_vulnerability_alerts = AsyncMock(
            side_effect=Exception("403 Forbidden")
        )
        mock_rest_client.get_repo_security_analysis = AsyncMock(
            side_effect=Exception("403 Forbidden")
        )

        # Mock paths
        mock_paths.security_features_raw_path = (
            lambda name: tmp_path / "security" / f"{name.replace('/', '__')}.jsonl"
        )

        # Ensure directory exists
        (tmp_path / "security").mkdir(parents=True, exist_ok=True)

        stats = await collect_security_features(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=security_config,
            checkpoint=None,
        )

        assert stats["repos_processed"] == 1
        assert stats["repos_with_no_access"] == 1
        assert stats["repos_with_all_features"] == 0

    @pytest.mark.asyncio
    async def test_collect_security_features_partial_access(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        security_config: Config,
        tmp_path: Path,
    ) -> None:
        """Test security features collection with partial access."""
        # Mock REST client with partial access
        mock_rest_client.check_vulnerability_alerts = AsyncMock(return_value=True)
        mock_rest_client.get_repo_security_analysis = AsyncMock(
            side_effect=Exception("403 Forbidden")
        )

        # Mock paths
        mock_paths.security_features_raw_path = (
            lambda name: tmp_path / "security" / f"{name.replace('/', '__')}.jsonl"
        )

        # Ensure directory exists
        (tmp_path / "security").mkdir(parents=True, exist_ok=True)

        stats = await collect_security_features(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=security_config,
            checkpoint=None,
        )

        assert stats["repos_processed"] == 1
        assert stats["repos_with_partial_features"] == 1

    @pytest.mark.asyncio
    async def test_get_security_features_all_enabled(
        self,
        mock_rest_client: RestClient,
    ) -> None:
        """Test _get_security_features with all features enabled."""
        # Mock REST client
        mock_rest_client.check_vulnerability_alerts = AsyncMock(return_value=True)
        mock_rest_client.get_repo_security_analysis = AsyncMock(
            return_value={
                "security_and_analysis": {
                    "dependabot_security_updates": {"status": "enabled"},
                    "secret_scanning": {"status": "enabled"},
                    "secret_scanning_push_protection": {"status": "enabled"},
                }
            }
        )

        features = await _get_security_features("org", "repo", mock_rest_client)

        assert features["repo"] == "org/repo"
        assert features["dependabot_alerts_enabled"] is True
        assert features["dependabot_security_updates_enabled"] is True
        assert features["secret_scanning_enabled"] is True
        assert features["secret_scanning_push_protection_enabled"] is True
        assert features["error"] is None

    @pytest.mark.asyncio
    async def test_get_security_features_all_disabled(
        self,
        mock_rest_client: RestClient,
    ) -> None:
        """Test _get_security_features with all features disabled."""
        # Mock REST client
        mock_rest_client.check_vulnerability_alerts = AsyncMock(
            side_effect=Exception("403 Forbidden")
        )
        mock_rest_client.get_repo_security_analysis = AsyncMock(
            side_effect=Exception("403 Forbidden")
        )

        features = await _get_security_features("org", "repo", mock_rest_client)

        assert features["repo"] == "org/repo"
        assert features["dependabot_alerts_enabled"] is None
        assert features["dependabot_security_updates_enabled"] is None
        assert features["secret_scanning_enabled"] is None
        assert features["secret_scanning_push_protection_enabled"] is None
        assert features["error"] == "403: Security features not accessible"


class TestBranchProtection:
    """Tests for branch protection collection."""

    @pytest.fixture
    def bp_skip_config(self) -> Config:
        """Create config with branch protection in skip mode."""
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.branch_protection = MagicMock(spec=BranchProtectionConfig)
        config.collection.hygiene.branch_protection.mode = "skip"
        return config

    @pytest.fixture
    def bp_best_effort_config(self) -> Config:
        """Create config with branch protection in best_effort mode."""
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.branch_protection = MagicMock(spec=BranchProtectionConfig)
        config.collection.hygiene.branch_protection.mode = "best_effort"
        return config

    @pytest.fixture
    def bp_sample_config(self) -> Config:
        """Create config with branch protection in sample mode."""
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.branch_protection = MagicMock(spec=BranchProtectionConfig)
        config.collection.hygiene.branch_protection.mode = "sample"
        config.collection.hygiene.branch_protection.sample_count = 2
        config.collection.hygiene.branch_protection.sample_top_repos_by = "prs_merged"
        return config

    @pytest.mark.asyncio
    async def test_collect_branch_protection_skip_mode(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        bp_skip_config: Config,
    ) -> None:
        """Test branch protection collection in skip mode."""
        stats = await collect_branch_protection(
            repos=sample_repos,
            rest_client=mock_rest_client,
            path_manager=mock_paths,
            config=bp_skip_config,
            checkpoint=None,
        )

        assert stats["mode"] == "skip"
        assert stats["repos_skipped"] == len(sample_repos)
        assert stats["repos_processed"] == 0

    @pytest.mark.asyncio
    async def test_collect_branch_protection_best_effort(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        bp_best_effort_config: Config,
        tmp_path: Path,
    ) -> None:
        """Test branch protection collection in best_effort mode."""
        # Mock REST client
        mock_rest_client.get_branch_protection = AsyncMock(
            return_value=(
                {
                    "required_status_checks": {"strict": True},
                    "enforce_admins": {"enabled": True},
                },
                200,
            )
        )

        # Mock paths
        mock_paths.branch_protection_raw_path = (
            lambda name: tmp_path / "bp" / f"{name.replace('/', '__')}.jsonl"
        )

        # Ensure directory exists
        (tmp_path / "bp").mkdir(parents=True, exist_ok=True)

        stats = await collect_branch_protection(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            path_manager=mock_paths,
            config=bp_best_effort_config,
            checkpoint=None,
        )

        assert stats["mode"] == "best_effort"
        assert stats["repos_processed"] == 1
        assert stats["protection_enabled"] == 1

    @pytest.mark.asyncio
    async def test_collect_branch_protection_sample_mode(
        self,
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        bp_sample_config: Config,
        tmp_path: Path,
    ) -> None:
        """Test branch protection collection in sample mode."""
        # Create repos with different metrics
        repos = [
            {
                "full_name": "org/repo1",
                "default_branch": "main",
                "mergedPullRequests": {"totalCount": 100},
            },
            {
                "full_name": "org/repo2",
                "default_branch": "main",
                "mergedPullRequests": {"totalCount": 50},
            },
            {
                "full_name": "org/repo3",
                "default_branch": "main",
                "mergedPullRequests": {"totalCount": 150},
            },
        ]

        # Mock REST client
        mock_rest_client.get_branch_protection = AsyncMock(
            return_value=({"required_status_checks": {"strict": True}}, 200)
        )

        # Mock paths
        mock_paths.branch_protection_raw_path = (
            lambda name: tmp_path / "bp" / f"{name.replace('/', '__')}.jsonl"
        )

        # Ensure directory exists
        (tmp_path / "bp").mkdir(parents=True, exist_ok=True)

        stats = await collect_branch_protection(
            repos=repos,
            rest_client=mock_rest_client,
            path_manager=mock_paths,
            config=bp_sample_config,
            checkpoint=None,
        )

        # Should only process top 2 repos by prs_merged
        assert stats["mode"] == "sample"
        assert stats["repos_selected"] == 2
        assert stats["repos_processed"] == 2
        assert stats["repos_skipped"] == 1

    @pytest.mark.asyncio
    async def test_get_branch_protection_enabled(
        self,
        mock_rest_client: RestClient,
    ) -> None:
        """Test _get_branch_protection with protection enabled."""
        mock_rest_client.get_branch_protection = AsyncMock(
            return_value=(
                {
                    "required_status_checks": {"strict": True, "contexts": ["ci"]},
                    "enforce_admins": {"enabled": True},
                    "required_pull_request_reviews": {
                        "required_approving_review_count": 2,
                        "dismiss_stale_reviews": True,
                        "require_code_owner_reviews": True,
                        "require_last_push_approval": False,
                    },
                    "restrictions": None,
                    "allow_force_pushes": {"enabled": False},
                    "allow_deletions": {"enabled": False},
                    "required_linear_history": {"enabled": True},
                    "required_conversation_resolution": {"enabled": True},
                },
                200,
            )
        )

        result = await _get_branch_protection("org", "repo", "main", mock_rest_client)

        assert result["repo"] == "org/repo"
        assert result["branch"] == "main"
        assert result["protection_enabled"] is True
        assert result["enforce_admins"] is True
        assert result["allow_force_pushes"] is False
        assert result["required_linear_history"] is True
        assert result["required_reviews"]["required_approving_review_count"] == 2

    @pytest.mark.asyncio
    async def test_get_branch_protection_disabled(
        self,
        mock_rest_client: RestClient,
    ) -> None:
        """Test _get_branch_protection with protection disabled (404)."""
        mock_rest_client.get_branch_protection = AsyncMock(return_value=(None, 404))

        result = await _get_branch_protection("org", "repo", "main", mock_rest_client)

        assert result["repo"] == "org/repo"
        assert result["branch"] == "main"
        assert result["protection_enabled"] is False
        assert result["error"] is None

    @pytest.mark.asyncio
    async def test_get_branch_protection_forbidden(
        self,
        mock_rest_client: RestClient,
    ) -> None:
        """Test _get_branch_protection with permission denied (403)."""
        mock_rest_client.get_branch_protection = AsyncMock(return_value=(None, 403))

        result = await _get_branch_protection("org", "repo", "main", mock_rest_client)

        assert result["repo"] == "org/repo"
        assert result["branch"] == "main"
        assert result["protection_enabled"] is None
        assert result["error"] == "403: Resource not accessible by integration"


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_parse_repo_name_valid(self) -> None:
        """Test _parse_repo_name with valid input."""
        owner, name = _parse_repo_name("org/repo")
        assert owner == "org"
        assert name == "repo"

    def test_parse_repo_name_invalid(self) -> None:
        """Test _parse_repo_name with invalid input."""
        with pytest.raises(ValueError, match="Invalid repository name format"):
            _parse_repo_name("invalid-name")

        with pytest.raises(ValueError, match="Invalid repository name format"):
            _parse_repo_name("too/many/slashes")

    def test_get_default_branch_graphql_format(self) -> None:
        """Test _get_default_branch with GraphQL format."""
        repo = {"defaultBranchRef": {"name": "develop"}}
        assert _get_default_branch(repo) == "develop"

    def test_get_default_branch_rest_format(self) -> None:
        """Test _get_default_branch with REST format."""
        repo = {"default_branch": "master"}
        assert _get_default_branch(repo) == "master"

    def test_get_default_branch_fallback(self) -> None:
        """Test _get_default_branch with no branch info (fallback to main)."""
        repo = {}
        assert _get_default_branch(repo) == "main"

    def test_get_default_branch_null_value(self) -> None:
        """Test _get_default_branch with null value."""
        repo = {"defaultBranchRef": None}
        assert _get_default_branch(repo) == "main"

        repo = {"default_branch": None}
        assert _get_default_branch(repo) == "main"

    def test_sort_repos_by_metric_prs_merged(self) -> None:
        """Test _sort_repos_by_metric sorting by prs_merged."""
        repos = [
            {"full_name": "org/repo1", "mergedPullRequests": {"totalCount": 50}},
            {"full_name": "org/repo2", "mergedPullRequests": {"totalCount": 150}},
            {"full_name": "org/repo3", "mergedPullRequests": {"totalCount": 100}},
        ]

        sorted_repos = _sort_repos_by_metric(repos, "prs_merged")

        assert sorted_repos[0]["full_name"] == "org/repo2"  # 150
        assert sorted_repos[1]["full_name"] == "org/repo3"  # 100
        assert sorted_repos[2]["full_name"] == "org/repo1"  # 50

    def test_sort_repos_by_metric_stars(self) -> None:
        """Test _sort_repos_by_metric sorting by stars."""
        repos = [
            {"full_name": "org/repo1", "stargazerCount": 10},
            {"full_name": "org/repo2", "stargazerCount": 100},
            {"full_name": "org/repo3", "stargazerCount": 50},
        ]

        sorted_repos = _sort_repos_by_metric(repos, "stars")

        assert sorted_repos[0]["full_name"] == "org/repo2"  # 100
        assert sorted_repos[1]["full_name"] == "org/repo3"  # 50
        assert sorted_repos[2]["full_name"] == "org/repo1"  # 10

    def test_sort_repos_by_metric_missing_values(self) -> None:
        """Test _sort_repos_by_metric with missing values."""
        repos = [
            {"full_name": "org/repo1", "stargazerCount": 100},
            {"full_name": "org/repo2"},  # Missing stargazerCount
            {"full_name": "org/repo3", "stargazerCount": 50},
        ]

        sorted_repos = _sort_repos_by_metric(repos, "stars")

        assert sorted_repos[0]["full_name"] == "org/repo1"  # 100
        assert sorted_repos[1]["full_name"] == "org/repo3"  # 50
        assert sorted_repos[2]["full_name"] == "org/repo2"  # 0 (missing)

    def test_sort_repos_by_metric_unknown_metric(self) -> None:
        """Test _sort_repos_by_metric with unknown metric (defaults to stars)."""
        repos = [
            {"full_name": "org/repo1", "stargazerCount": 10},
            {"full_name": "org/repo2", "stargazerCount": 100},
        ]

        sorted_repos = _sort_repos_by_metric(repos, "unknown_metric")

        assert sorted_repos[0]["full_name"] == "org/repo2"  # 100
        assert sorted_repos[1]["full_name"] == "org/repo1"  # 10

    def test_select_repos_for_collection_skip(self) -> None:
        """Test _select_repos_for_collection with skip mode."""
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.branch_protection = MagicMock(spec=BranchProtectionConfig)
        config.collection.hygiene.branch_protection.mode = "skip"

        repos = [{"full_name": "org/repo1"}, {"full_name": "org/repo2"}]
        selected = _select_repos_for_collection(repos, config)

        assert len(selected) == 0

    def test_select_repos_for_collection_best_effort(self) -> None:
        """Test _select_repos_for_collection with best_effort mode."""
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.branch_protection = MagicMock(spec=BranchProtectionConfig)
        config.collection.hygiene.branch_protection.mode = "best_effort"

        repos = [{"full_name": "org/repo1"}, {"full_name": "org/repo2"}]
        selected = _select_repos_for_collection(repos, config)

        assert len(selected) == 2
        assert selected == repos

    def test_select_repos_for_collection_sample(self) -> None:
        """Test _select_repos_for_collection with sample mode."""
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.branch_protection = MagicMock(spec=BranchProtectionConfig)
        config.collection.hygiene.branch_protection.mode = "sample"
        config.collection.hygiene.branch_protection.sample_count = 2
        config.collection.hygiene.branch_protection.sample_top_repos_by = "prs_merged"

        repos = [
            {"full_name": "org/repo1", "mergedPullRequests": {"totalCount": 50}},
            {"full_name": "org/repo2", "mergedPullRequests": {"totalCount": 150}},
            {"full_name": "org/repo3", "mergedPullRequests": {"totalCount": 100}},
        ]

        selected = _select_repos_for_collection(repos, config)

        assert len(selected) == 2
        assert selected[0]["full_name"] == "org/repo2"  # Top repo
        assert selected[1]["full_name"] == "org/repo3"  # Second repo


class TestEdgeCases:
    """Tests for edge cases and error scenarios."""

    @pytest.mark.asyncio
    async def test_collect_repo_hygiene_missing_full_name(
        self,
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        mock_config: Config,
    ) -> None:
        """Test hygiene collection with repo missing full_name field."""
        repos = [{"default_branch": "main"}]  # Missing full_name

        stats = await collect_repo_hygiene(
            repos=repos,
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=mock_config,
        )

        # Should skip repo with missing full_name
        assert stats["repos_skipped"] == 1
        assert stats["repos_processed"] == 0

    @pytest.mark.asyncio
    async def test_collect_security_features_with_checkpoint(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        tmp_path: Path,
    ) -> None:
        """Test security features collection with checkpoint."""
        # Create config
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.security_features = MagicMock(spec=SecurityFeaturesConfig)
        config.collection.hygiene.security_features.best_effort = True

        # Mock checkpoint
        checkpoint = MagicMock(spec=CheckpointManager)
        checkpoint.is_repo_endpoint_complete = MagicMock(return_value=True)

        stats = await collect_security_features(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=config,
            checkpoint=checkpoint,
        )

        # Should resume from checkpoint and skip already complete repos
        assert stats["repos_resumed"] == 1
        assert stats["repos_processed"] == 0

    @pytest.mark.asyncio
    async def test_collect_security_features_error_with_checkpoint(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        tmp_path: Path,
    ) -> None:
        """Test security features collection error handling with checkpoint."""
        # Create config
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.security_features = MagicMock(spec=SecurityFeaturesConfig)
        config.collection.hygiene.security_features.best_effort = True

        # Mock checkpoint
        checkpoint = MagicMock(spec=CheckpointManager)
        checkpoint.is_repo_endpoint_complete = MagicMock(return_value=False)
        checkpoint.mark_repo_endpoint_in_progress = MagicMock()
        checkpoint.mark_repo_endpoint_failed = MagicMock()

        # Mock REST client - these don't raise exceptions in _get_security_features
        # because they're caught internally
        mock_rest_client.check_vulnerability_alerts = AsyncMock(return_value=None)
        mock_rest_client.get_repo_security_analysis = AsyncMock(return_value=None)

        # Mock paths to raise an exception when trying to write (simulating disk error)
        def raise_on_path(name: str) -> Path:
            raise Exception("Disk write error")

        mock_paths.security_features_raw_path = raise_on_path

        stats = await collect_security_features(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            paths=mock_paths,
            config=config,
            checkpoint=checkpoint,
        )

        # Should track errors
        assert len(stats["errors"]) == 1
        assert "Disk write error" in stats["errors"][0]["error"]
        checkpoint.mark_repo_endpoint_failed.assert_called_once()

    @pytest.mark.asyncio
    async def test_collect_branch_protection_with_checkpoint(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
    ) -> None:
        """Test branch protection collection with checkpoint."""
        # Create config
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.branch_protection = MagicMock(spec=BranchProtectionConfig)
        config.collection.hygiene.branch_protection.mode = "best_effort"

        # Mock checkpoint
        checkpoint = MagicMock(spec=CheckpointManager)
        checkpoint.is_repo_endpoint_complete = MagicMock(return_value=True)

        stats = await collect_branch_protection(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            path_manager=mock_paths,
            config=config,
            checkpoint=checkpoint,
        )

        # Should resume from checkpoint
        assert stats["repos_resumed"] == 1
        assert stats["repos_processed"] == 0

    @pytest.mark.asyncio
    async def test_collect_branch_protection_error_with_checkpoint(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        tmp_path: Path,
    ) -> None:
        """Test branch protection collection error handling with checkpoint."""
        # Create config
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.branch_protection = MagicMock(spec=BranchProtectionConfig)
        config.collection.hygiene.branch_protection.mode = "best_effort"

        # Mock checkpoint
        checkpoint = MagicMock(spec=CheckpointManager)
        checkpoint.is_repo_endpoint_complete = MagicMock(return_value=False)
        checkpoint.mark_repo_endpoint_in_progress = MagicMock()
        checkpoint.mark_repo_endpoint_failed = MagicMock()

        # Mock REST client to raise exception
        mock_rest_client.get_branch_protection = AsyncMock(
            side_effect=Exception("Network error")
        )

        # Mock paths
        mock_paths.branch_protection_raw_path = (
            lambda name: tmp_path / "bp" / f"{name.replace('/', '__')}.jsonl"
        )

        stats = await collect_branch_protection(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            path_manager=mock_paths,
            config=config,
            checkpoint=checkpoint,
        )

        # Should track errors
        assert len(stats["errors"]) == 1
        checkpoint.mark_repo_endpoint_failed.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_branch_protection_unexpected_status_code(
        self,
        mock_rest_client: RestClient,
    ) -> None:
        """Test _get_branch_protection with unexpected status code."""
        mock_rest_client.get_branch_protection = AsyncMock(return_value=(None, 500))

        result = await _get_branch_protection("org", "repo", "main", mock_rest_client)

        assert result["repo"] == "org/repo"
        assert result["branch"] == "main"
        assert result["protection_enabled"] is None
        assert "Unexpected status code: 500" in result["error"]

    def test_sort_repos_by_metric_exception_handling(self) -> None:
        """Test _sort_repos_by_metric handles exceptions gracefully."""
        # Create repos with data that could cause sorting issues
        repos = [
            {"full_name": "org/repo1", "stargazerCount": "not_a_number"},
            {"full_name": "org/repo2", "stargazerCount": 100},
        ]

        # Should not raise exception, returns original order
        sorted_repos = _sort_repos_by_metric(repos, "stars")

        # Should handle the invalid value gracefully
        assert len(sorted_repos) == 2

    def test_select_repos_for_collection_unknown_mode(self) -> None:
        """Test _select_repos_for_collection with unknown mode."""
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.branch_protection = MagicMock(spec=BranchProtectionConfig)
        config.collection.hygiene.branch_protection.mode = "unknown_mode"

        repos = [{"full_name": "org/repo1"}, {"full_name": "org/repo2"}]
        selected = _select_repos_for_collection(repos, config)

        # Should default to skip (empty list)
        assert len(selected) == 0

    @pytest.mark.asyncio
    async def test_collect_branch_protection_permission_denied(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        tmp_path: Path,
    ) -> None:
        """Test branch protection collection tracking permission denied."""
        # Create config
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.branch_protection = MagicMock(spec=BranchProtectionConfig)
        config.collection.hygiene.branch_protection.mode = "best_effort"

        # Mock REST client to return 403
        mock_rest_client.get_branch_protection = AsyncMock(return_value=(None, 403))

        # Mock paths
        mock_paths.branch_protection_raw_path = (
            lambda name: tmp_path / "bp" / f"{name.replace('/', '__')}.jsonl"
        )

        # Ensure directory exists
        (tmp_path / "bp").mkdir(parents=True, exist_ok=True)

        stats = await collect_branch_protection(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            path_manager=mock_paths,
            config=config,
            checkpoint=None,
        )

        assert stats["repos_processed"] == 1
        assert stats["permission_denied"] == 1

    @pytest.mark.asyncio
    async def test_collect_branch_protection_disabled_404(
        self,
        sample_repos: list[dict[str, Any]],
        mock_rest_client: RestClient,
        mock_paths: PathManager,
        tmp_path: Path,
    ) -> None:
        """Test branch protection collection tracking disabled protection (404)."""
        # Create config
        config = MagicMock(spec=Config)
        config.collection = MagicMock(spec=CollectionConfig)
        config.collection.hygiene = MagicMock(spec=HygieneConfig)
        config.collection.hygiene.branch_protection = MagicMock(spec=BranchProtectionConfig)
        config.collection.hygiene.branch_protection.mode = "best_effort"

        # Mock REST client to return 404
        mock_rest_client.get_branch_protection = AsyncMock(return_value=(None, 404))

        # Mock paths
        mock_paths.branch_protection_raw_path = (
            lambda name: tmp_path / "bp" / f"{name.replace('/', '__')}.jsonl"
        )

        # Ensure directory exists
        (tmp_path / "bp").mkdir(parents=True, exist_ok=True)

        stats = await collect_branch_protection(
            repos=sample_repos[:1],
            rest_client=mock_rest_client,
            path_manager=mock_paths,
            config=config,
            checkpoint=None,
        )

        assert stats["repos_processed"] == 1
        assert stats["protection_disabled"] == 1
