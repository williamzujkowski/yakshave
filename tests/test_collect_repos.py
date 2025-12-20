"""Tests for repository metadata collector module."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from gh_year_end.collect.repos import (
    _check_file_presence,
    _check_workflows_presence,
    _fetch_branch_protection,
    _fetch_repo_metadata,
    _parse_repo_name,
    collect_repo_metadata,
)
from gh_year_end.config import Config
from gh_year_end.github.graphql import GraphQLError


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
                    "hygiene": True,
                },
                "hygiene": {
                    "paths": [
                        "README.md",
                        "LICENSE",
                        "SECURITY.md",
                    ],
                    "workflow_prefixes": [".github/workflows/"],
                    "branch_protection": {
                        "mode": "sample",
                        "sample_top_repos_by": "prs_merged",
                        "sample_count": 5,
                    },
                    "security_features": {
                        "best_effort": True,
                    },
                },
            },
            "storage": {
                "root": str(tmp_path / "data"),
            },
        }
    )


@pytest.fixture
def sample_repos() -> list[dict]:
    """Sample repository data."""
    return [
        {
            "full_name": "test-org/repo1",
            "name": "repo1",
            "description": "Test repository 1",
        },
        {
            "full_name": "test-org/repo2",
            "name": "repo2",
            "description": "Test repository 2",
        },
    ]


@pytest.fixture
def sample_repo_metadata() -> dict:
    """Sample repository metadata from GraphQL."""
    return {
        "id": "MDEwOlJlcG9zaXRvcnk=",
        "databaseId": 123456,
        "name": "repo1",
        "nameWithOwner": "test-org/repo1",
        "description": "Test repository",
        "createdAt": "2020-01-01T00:00:00Z",
        "updatedAt": "2024-06-15T10:00:00Z",
        "pushedAt": "2024-06-14T15:30:00Z",
        "url": "https://github.com/test-org/repo1",
        "homepageUrl": "https://example.com",
        "isPrivate": False,
        "isFork": False,
        "isArchived": False,
        "isDisabled": False,
        "isLocked": False,
        "isMirror": False,
        "isTemplate": False,
        "hasIssuesEnabled": True,
        "hasProjectsEnabled": True,
        "hasWikiEnabled": False,
        "hasDiscussionsEnabled": False,
        "stargazerCount": 100,
        "forkCount": 25,
        "watchers": {"totalCount": 50},
        "diskUsageKb": 1024,
        "defaultBranchRef": {
            "name": "main",
            "id": "MDM6UmVm",
        },
        "primaryLanguage": {
            "name": "Python",
            "color": "#3572A5",
        },
        "languages": {
            "totalSize": 50000,
            "totalCount": 3,
            "edges": [
                {"size": 40000, "node": {"name": "Python", "color": "#3572A5"}},
                {"size": 8000, "node": {"name": "JavaScript", "color": "#f1e05a"}},
                {"size": 2000, "node": {"name": "HTML", "color": "#e34c26"}},
            ],
        },
        "repositoryTopics": {
            "totalCount": 2,
            "nodes": [
                {"topic": {"name": "python"}},
                {"topic": {"name": "testing"}},
            ],
        },
        "licenseInfo": {
            "name": "MIT License",
            "spdxId": "MIT",
            "url": "https://api.github.com/licenses/mit",
            "key": "mit",
        },
        "codeOfConduct": None,
        "fundingLinks": [],
        "securityPolicyUrl": None,
        "owner": {
            "__typename": "Organization",
            "login": "test-org",
            "name": "Test Organization",
            "email": "test@example.com",
        },
        "collaborators": {"totalCount": 5},
        "issues": {"totalCount": 100},
        "closedIssues": {"totalCount": 80},
        "openIssues": {"totalCount": 20},
        "pullRequests": {"totalCount": 50},
        "mergedPullRequests": {"totalCount": 40},
        "openPullRequests": {"totalCount": 5},
        "closedPullRequests": {"totalCount": 5},
        "releases": {"totalCount": 10},
        "deployments": {"totalCount": 25},
        "vulnerabilityAlerts": {"totalCount": 0},
        "hasVulnerabilityAlertsEnabled": True,
        "dependencyGraphManifests": {"totalCount": 2},
    }


@pytest.fixture
def mock_graphql_client():
    """Create mock GraphQL client."""
    client = AsyncMock()
    return client


@pytest.fixture
def mock_writer():
    """Create mock JSONL writer."""
    writer = AsyncMock()
    return writer


@pytest.fixture
def mock_rate_limiter():
    """Create mock rate limiter."""
    limiter = AsyncMock()
    return limiter


class TestParseRepoName:
    """Tests for _parse_repo_name function."""

    def test_parse_valid_repo_name(self):
        """Test parsing valid repository name."""
        owner, name = _parse_repo_name("test-org/repo1")

        assert owner == "test-org"
        assert name == "repo1"

    def test_parse_repo_name_with_hyphen(self):
        """Test parsing repository name with hyphens."""
        owner, name = _parse_repo_name("my-org/my-awesome-repo")

        assert owner == "my-org"
        assert name == "my-awesome-repo"

    def test_parse_repo_name_with_dots(self):
        """Test parsing repository name with dots."""
        owner, name = _parse_repo_name("company.org/project.v2")

        assert owner == "company.org"
        assert name == "project.v2"

    def test_parse_invalid_repo_name_no_slash(self):
        """Test parsing invalid repository name without slash."""
        with pytest.raises(ValueError, match="Invalid repository name format"):
            _parse_repo_name("invalid-repo-name")

    def test_parse_invalid_repo_name_multiple_slashes(self):
        """Test parsing invalid repository name with multiple slashes."""
        with pytest.raises(ValueError, match="Invalid repository name format"):
            _parse_repo_name("org/sub/repo")

    def test_parse_empty_repo_name(self):
        """Test parsing empty repository name."""
        with pytest.raises(ValueError, match="Invalid repository name format"):
            _parse_repo_name("")


class TestFetchRepoMetadata:
    """Tests for _fetch_repo_metadata function."""

    @pytest.mark.asyncio
    async def test_fetch_repo_metadata_success(
        self, mock_graphql_client, mock_rate_limiter, sample_repo_metadata
    ):
        """Test successful repository metadata fetch."""
        mock_graphql_client.execute.return_value = {"repository": sample_repo_metadata}

        result = await _fetch_repo_metadata(
            owner="test-org",
            name="repo1",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
        )

        assert result == sample_repo_metadata
        mock_graphql_client.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetch_repo_metadata_graphql_error(self, mock_graphql_client, mock_rate_limiter):
        """Test handling GraphQL error during metadata fetch."""
        mock_graphql_client.execute.side_effect = GraphQLError(
            [{"message": "Repository not found"}]
        )

        result = await _fetch_repo_metadata(
            owner="test-org",
            name="nonexistent",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
        )

        assert result == {}

    @pytest.mark.asyncio
    async def test_fetch_repo_metadata_unexpected_error(
        self, mock_graphql_client, mock_rate_limiter
    ):
        """Test handling unexpected error during metadata fetch."""
        mock_graphql_client.execute.side_effect = Exception("Network error")

        result = await _fetch_repo_metadata(
            owner="test-org",
            name="repo1",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
        )

        assert result == {}

    @pytest.mark.asyncio
    async def test_fetch_repo_metadata_empty_response(self, mock_graphql_client, mock_rate_limiter):
        """Test handling empty repository response."""
        mock_graphql_client.execute.return_value = {}

        result = await _fetch_repo_metadata(
            owner="test-org",
            name="repo1",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
        )

        assert result == {}


class TestFetchBranchProtection:
    """Tests for _fetch_branch_protection function."""

    @pytest.mark.asyncio
    async def test_fetch_branch_protection_success(
        self, mock_graphql_client, mock_rate_limiter, sample_config
    ):
        """Test successful branch protection fetch."""
        protection_rule = {
            "id": "BPR_kwDOABCD",
            "pattern": "main",
            "requiresApprovingReviews": True,
            "requiredApprovingReviewCount": 2,
            "requiresStatusChecks": True,
            "requiresStrictStatusChecks": True,
            "requiresCommitSignatures": False,
            "isAdminEnforced": True,
        }

        mock_graphql_client.execute.return_value = {
            "repository": {"branchProtectionRule": {"nodes": [protection_rule]}}
        }

        result = await _fetch_branch_protection(
            owner="test-org",
            name="repo1",
            branch="main",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert result == protection_rule

    @pytest.mark.asyncio
    async def test_fetch_branch_protection_no_rules(
        self, mock_graphql_client, mock_rate_limiter, sample_config
    ):
        """Test branch protection with no rules."""
        mock_graphql_client.execute.return_value = {
            "repository": {"branchProtectionRule": {"nodes": []}}
        }

        result = await _fetch_branch_protection(
            owner="test-org",
            name="repo1",
            branch="main",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_branch_protection_skip_mode(
        self, mock_graphql_client, mock_rate_limiter, sample_config
    ):
        """Test branch protection with skip mode."""
        # Modify config to skip branch protection
        sample_config.collection.hygiene.branch_protection.mode = "skip"

        result = await _fetch_branch_protection(
            owner="test-org",
            name="repo1",
            branch="main",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert result is None
        mock_graphql_client.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_fetch_branch_protection_graphql_error(
        self, mock_graphql_client, mock_rate_limiter, sample_config
    ):
        """Test handling GraphQL error during branch protection fetch."""
        mock_graphql_client.execute.side_effect = GraphQLError(
            [{"message": "Insufficient permissions"}]
        )

        result = await _fetch_branch_protection(
            owner="test-org",
            name="repo1",
            branch="main",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_branch_protection_unexpected_error(
        self, mock_graphql_client, mock_rate_limiter, sample_config
    ):
        """Test handling unexpected error during branch protection fetch."""
        mock_graphql_client.execute.side_effect = Exception("Network error")

        result = await _fetch_branch_protection(
            owner="test-org",
            name="repo1",
            branch="main",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert result is None


class TestCheckFilePresence:
    """Tests for _check_file_presence function."""

    @pytest.mark.asyncio
    async def test_check_file_presence_all_files_exist(
        self, mock_graphql_client, mock_rate_limiter, sample_config
    ):
        """Test checking file presence when all files exist."""

        # Mock file presence responses
        def mock_execute(query, variables):
            if "FILE_PRESENCE_QUERY" in query or "expression" in variables:
                return {
                    "repository": {
                        "object": {
                            "id": "blob123",
                            "byteSize": 1024,
                        }
                    }
                }
            # Workflows query
            return {
                "repository": {
                    "defaultBranchRef": {
                        "target": {
                            "tree": {
                                "entries": [
                                    {
                                        "path": ".github/workflows/ci.yml",
                                        "type": "blob",
                                        "name": "ci.yml",
                                    }
                                ]
                            }
                        }
                    }
                }
            }

        mock_graphql_client.execute.side_effect = mock_execute

        result = await _check_file_presence(
            owner="test-org",
            name="repo1",
            default_branch="main",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert result["README.md"] is True
        assert result["LICENSE"] is True
        assert result["SECURITY.md"] is True
        assert result["_has_workflows"] is True

    @pytest.mark.asyncio
    async def test_check_file_presence_no_files_exist(
        self, mock_graphql_client, mock_rate_limiter, sample_config
    ):
        """Test checking file presence when no files exist."""

        # Mock empty responses
        def mock_execute(query, variables):
            if "expression" in variables:
                return {"repository": {"object": None}}
            # Workflows query
            return {"repository": {"defaultBranchRef": {"target": {"tree": {"entries": []}}}}}

        mock_graphql_client.execute.side_effect = mock_execute

        result = await _check_file_presence(
            owner="test-org",
            name="repo1",
            default_branch="main",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert result["README.md"] is False
        assert result["LICENSE"] is False
        assert result["SECURITY.md"] is False
        assert result["_has_workflows"] is False

    @pytest.mark.asyncio
    async def test_check_file_presence_graphql_error(
        self, mock_graphql_client, mock_rate_limiter, sample_config
    ):
        """Test handling GraphQL error during file presence check."""
        mock_graphql_client.execute.side_effect = GraphQLError([{"message": "Not found"}])

        result = await _check_file_presence(
            owner="test-org",
            name="repo1",
            default_branch="main",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        # All files should be marked as not present
        assert result["README.md"] is False
        assert result["LICENSE"] is False
        assert result["SECURITY.md"] is False
        assert result["_has_workflows"] is False

    @pytest.mark.asyncio
    async def test_check_file_presence_unexpected_error(
        self, mock_graphql_client, mock_rate_limiter, sample_config
    ):
        """Test handling unexpected error during file presence check."""
        mock_graphql_client.execute.side_effect = Exception("Network error")

        result = await _check_file_presence(
            owner="test-org",
            name="repo1",
            default_branch="main",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        # All files should be marked as not present
        assert result["README.md"] is False
        assert result["LICENSE"] is False
        assert result["SECURITY.md"] is False
        assert result["_has_workflows"] is False


class TestCheckWorkflowsPresence:
    """Tests for _check_workflows_presence function."""

    @pytest.mark.asyncio
    async def test_check_workflows_presence_has_workflows(
        self, mock_graphql_client, mock_rate_limiter, sample_config
    ):
        """Test checking workflows when they exist."""
        mock_graphql_client.execute.return_value = {
            "repository": {
                "defaultBranchRef": {
                    "target": {
                        "tree": {
                            "entries": [
                                {
                                    "path": ".github/workflows/ci.yml",
                                    "type": "blob",
                                    "name": "ci.yml",
                                },
                                {
                                    "path": ".github/workflows/release.yml",
                                    "type": "blob",
                                    "name": "release.yml",
                                },
                            ]
                        }
                    }
                }
            }
        }

        result = await _check_workflows_presence(
            owner="test-org",
            name="repo1",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert result is True

    @pytest.mark.asyncio
    async def test_check_workflows_presence_no_workflows(
        self, mock_graphql_client, mock_rate_limiter, sample_config
    ):
        """Test checking workflows when none exist."""
        mock_graphql_client.execute.return_value = {
            "repository": {
                "defaultBranchRef": {
                    "target": {
                        "tree": {
                            "entries": [
                                {
                                    "path": "README.md",
                                    "type": "blob",
                                    "name": "README.md",
                                },
                            ]
                        }
                    }
                }
            }
        }

        result = await _check_workflows_presence(
            owner="test-org",
            name="repo1",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_check_workflows_presence_has_directory_but_no_files(
        self, mock_graphql_client, mock_rate_limiter, sample_config
    ):
        """Test checking workflows when directory exists but has no workflow files."""
        mock_graphql_client.execute.return_value = {
            "repository": {
                "defaultBranchRef": {
                    "target": {
                        "tree": {
                            "entries": [
                                {
                                    "path": ".github/workflows",
                                    "type": "tree",
                                    "name": "workflows",
                                },
                            ]
                        }
                    }
                }
            }
        }

        result = await _check_workflows_presence(
            owner="test-org",
            name="repo1",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_check_workflows_presence_graphql_error(
        self, mock_graphql_client, mock_rate_limiter, sample_config
    ):
        """Test handling GraphQL error during workflows check."""
        mock_graphql_client.execute.side_effect = GraphQLError([{"message": "Not found"}])

        result = await _check_workflows_presence(
            owner="test-org",
            name="repo1",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_check_workflows_presence_unexpected_error(
        self, mock_graphql_client, mock_rate_limiter, sample_config
    ):
        """Test handling unexpected error during workflows check."""
        mock_graphql_client.execute.side_effect = Exception("Network error")

        result = await _check_workflows_presence(
            owner="test-org",
            name="repo1",
            graphql_client=mock_graphql_client,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert result is False


class TestCollectRepoMetadata:
    """Tests for collect_repo_metadata function."""

    @pytest.mark.asyncio
    async def test_collect_repo_metadata_success(
        self,
        sample_repos,
        sample_repo_metadata,
        mock_graphql_client,
        mock_writer,
        mock_rate_limiter,
        sample_config,
    ):
        """Test successful repository metadata collection."""

        # Mock successful responses
        def mock_execute(query, variables):
            if "branchProtectionRule" in query:
                return {"repository": {"branchProtectionRule": {"nodes": []}}}
            if "expression" in variables:
                return {"repository": {"object": {"id": "blob123"}}}
            if "defaultBranchRef" in query and "tree" in query:
                return {
                    "repository": {
                        "defaultBranchRef": {
                            "target": {
                                "tree": {
                                    "entries": [
                                        {
                                            "path": ".github/workflows/ci.yml",
                                            "type": "blob",
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            # Default: repo metadata
            return {"repository": sample_repo_metadata}

        mock_graphql_client.execute.side_effect = mock_execute

        stats = await collect_repo_metadata(
            repos=sample_repos[:1],  # Test with one repo
            graphql_client=mock_graphql_client,
            writer=mock_writer,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert stats["repos_total"] == 1
        assert stats["repos_processed"] == 1
        assert stats["repos_failed"] == 0
        assert stats["branch_protection_failed"] == 1  # No protection rules
        mock_writer.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_collect_repo_metadata_multiple_repos(
        self,
        sample_repos,
        sample_repo_metadata,
        mock_graphql_client,
        mock_writer,
        mock_rate_limiter,
        sample_config,
    ):
        """Test collecting metadata for multiple repositories."""

        # Mock successful responses for all repos
        def mock_execute(query, variables):
            if "branchProtectionRule" in query:
                return {"repository": {"branchProtectionRule": {"nodes": []}}}
            if "expression" in variables:
                return {"repository": {"object": {"id": "blob123"}}}
            if "defaultBranchRef" in query and "tree" in query:
                return {"repository": {"defaultBranchRef": {"target": {"tree": {"entries": []}}}}}
            return {"repository": sample_repo_metadata}

        mock_graphql_client.execute.side_effect = mock_execute

        stats = await collect_repo_metadata(
            repos=sample_repos,
            graphql_client=mock_graphql_client,
            writer=mock_writer,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert stats["repos_total"] == 2
        assert stats["repos_processed"] == 2
        assert stats["repos_failed"] == 0
        assert mock_writer.write.call_count == 2

    @pytest.mark.asyncio
    async def test_collect_repo_metadata_with_branch_protection(
        self,
        sample_repos,
        sample_repo_metadata,
        mock_graphql_client,
        mock_writer,
        mock_rate_limiter,
        sample_config,
    ):
        """Test collecting metadata with branch protection."""
        protection_rule = {
            "id": "BPR_kwDOABCD",
            "pattern": "main",
            "requiresApprovingReviews": True,
        }

        def mock_execute(query, variables):
            if "branchProtectionRule" in query:
                return {"repository": {"branchProtectionRule": {"nodes": [protection_rule]}}}
            if "expression" in variables:
                return {"repository": {"object": {"id": "blob123"}}}
            if "defaultBranchRef" in query and "tree" in query:
                return {"repository": {"defaultBranchRef": {"target": {"tree": {"entries": []}}}}}
            return {"repository": sample_repo_metadata}

        mock_graphql_client.execute.side_effect = mock_execute

        stats = await collect_repo_metadata(
            repos=sample_repos[:1],
            graphql_client=mock_graphql_client,
            writer=mock_writer,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert stats["repos_processed"] == 1
        assert stats["branch_protection_accessible"] == 1
        assert stats["branch_protection_failed"] == 0

    @pytest.mark.asyncio
    async def test_collect_repo_metadata_empty_metadata_response(
        self,
        sample_repos,
        mock_graphql_client,
        mock_writer,
        mock_rate_limiter,
        sample_config,
    ):
        """Test handling empty metadata response."""
        mock_graphql_client.execute.return_value = {}

        stats = await collect_repo_metadata(
            repos=sample_repos[:1],
            graphql_client=mock_graphql_client,
            writer=mock_writer,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert stats["repos_total"] == 1
        assert stats["repos_processed"] == 0
        assert stats["repos_failed"] == 1
        assert len(stats["errors"]) == 1
        assert stats["errors"][0]["error"] == "Empty metadata response"

    @pytest.mark.asyncio
    async def test_collect_repo_metadata_invalid_repo_name(
        self,
        mock_graphql_client,
        mock_writer,
        mock_rate_limiter,
        sample_config,
    ):
        """Test handling invalid repository name."""
        invalid_repos = [{"full_name": "invalid-name-no-slash", "name": "invalid"}]

        stats = await collect_repo_metadata(
            repos=invalid_repos,
            graphql_client=mock_graphql_client,
            writer=mock_writer,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert stats["repos_total"] == 1
        assert stats["repos_processed"] == 0
        assert stats["repos_failed"] == 1
        assert len(stats["errors"]) == 1
        assert "Invalid repository name format" in stats["errors"][0]["error"]

    @pytest.mark.asyncio
    async def test_collect_repo_metadata_graphql_error(
        self,
        sample_repos,
        mock_graphql_client,
        mock_writer,
        mock_rate_limiter,
        sample_config,
    ):
        """Test handling GraphQL error during collection."""
        mock_graphql_client.execute.side_effect = GraphQLError([{"message": "API error"}])

        stats = await collect_repo_metadata(
            repos=sample_repos[:1],
            graphql_client=mock_graphql_client,
            writer=mock_writer,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert stats["repos_total"] == 1
        assert stats["repos_processed"] == 0
        assert stats["repos_failed"] == 1

    @pytest.mark.asyncio
    async def test_collect_repo_metadata_hygiene_disabled(
        self,
        sample_repos,
        sample_repo_metadata,
        mock_graphql_client,
        mock_writer,
        mock_rate_limiter,
        sample_config,
    ):
        """Test collection with hygiene disabled."""
        # Disable hygiene collection
        sample_config.collection.enable.hygiene = False

        mock_graphql_client.execute.return_value = {"repository": sample_repo_metadata}

        stats = await collect_repo_metadata(
            repos=sample_repos[:1],
            graphql_client=mock_graphql_client,
            writer=mock_writer,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert stats["repos_processed"] == 1
        # Writer should be called, but metadata should not include file presence
        mock_writer.write.assert_called_once()
        written_data = mock_writer.write.call_args[1]["data"]
        assert "filePresence" not in written_data

    @pytest.mark.asyncio
    async def test_collect_repo_metadata_empty_repos_list(
        self,
        mock_graphql_client,
        mock_writer,
        mock_rate_limiter,
        sample_config,
    ):
        """Test collection with empty repositories list."""
        stats = await collect_repo_metadata(
            repos=[],
            graphql_client=mock_graphql_client,
            writer=mock_writer,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert stats["repos_total"] == 0
        assert stats["repos_processed"] == 0
        assert stats["repos_failed"] == 0
        mock_writer.write.assert_not_called()

    @pytest.mark.asyncio
    async def test_collect_repo_metadata_partial_failure(
        self,
        sample_repos,
        sample_repo_metadata,
        mock_graphql_client,
        mock_writer,
        mock_rate_limiter,
        sample_config,
    ):
        """Test collection with partial failures."""
        # First repo succeeds, second fails
        call_count = 0

        def mock_execute(query, variables):
            nonlocal call_count
            call_count += 1
            # First repo's metadata query fails
            if call_count == 1:
                raise GraphQLError([{"message": "First repo failed"}])
            # Second repo succeeds
            if "branchProtectionRule" in query:
                return {"repository": {"branchProtectionRule": {"nodes": []}}}
            if "expression" in variables:
                return {"repository": {"object": {"id": "blob123"}}}
            if "defaultBranchRef" in query and "tree" in query:
                return {"repository": {"defaultBranchRef": {"target": {"tree": {"entries": []}}}}}
            return {"repository": sample_repo_metadata}

        mock_graphql_client.execute.side_effect = mock_execute

        stats = await collect_repo_metadata(
            repos=sample_repos,
            graphql_client=mock_graphql_client,
            writer=mock_writer,
            rate_limiter=mock_rate_limiter,
            config=sample_config,
        )

        assert stats["repos_total"] == 2
        assert stats["repos_processed"] == 1
        assert stats["repos_failed"] == 1
        assert len(stats["errors"]) == 1
        assert mock_writer.write.call_count == 1
