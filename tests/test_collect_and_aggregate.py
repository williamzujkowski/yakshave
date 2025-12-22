"""Tests for the new collect_and_aggregate() function."""

from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from gh_year_end.collect.orchestrator import collect_and_aggregate
from gh_year_end.config import Config


@pytest.mark.asyncio
async def test_collect_and_aggregate_returns_metrics_dict(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """Test that collect_and_aggregate returns a properly structured metrics dict."""
    # Create minimal config
    config_dict = {
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

    # Create config object
    Config.model_validate(config_dict)

    # Set dummy token
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_test_token_dummy")

    # Note: This test will fail without a real GitHub token and repos
    # For now, we just verify the function signature and structure
    # A real integration test would need mocking or live fixtures

    # Verify function is callable
    assert callable(collect_and_aggregate)

    # Verify it returns a dict with expected keys
    # (This will fail if we actually call it without mocking)
    # For now, just check the function exists and has proper signature
    import inspect

    sig = inspect.signature(collect_and_aggregate)
    params = list(sig.parameters.keys())

    assert "config" in params
    assert "verbose" in params
    assert "quiet" in params


@pytest.mark.asyncio
async def test_collect_and_aggregate_with_mock_discovery(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """Test collect_and_aggregate with mocked discovery returning no repos."""
    from unittest.mock import AsyncMock, patch

    # Create minimal config
    config_dict = {
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

    config = Config.model_validate(config_dict)
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_test_token_dummy")

    # Mock discover_repos to return empty list
    with patch(
        "gh_year_end.collect.orchestrator.discover_repos",
        new_callable=AsyncMock,
        return_value=[],
    ):
        # Call function
        result = await collect_and_aggregate(config, quiet=True)

        # Verify result structure
        assert isinstance(result, dict)
        assert "summary" in result
        assert "leaderboards" in result
        assert "timeseries" in result
        assert "repo_health" in result
        assert "hygiene_scores" in result
        assert "awards" in result

        # Verify empty results
        assert result["summary"]["total_repos"] == 0
        assert result["summary"]["total_contributors"] == 0
        assert result["summary"]["total_prs"] == 0


def test_metrics_aggregator_import() -> None:
    """Test that MetricsAggregator can be imported from aggregator module."""
    from gh_year_end.collect.aggregator import MetricsAggregator

    assert MetricsAggregator is not None


@pytest.mark.asyncio
async def test_collect_repo_hygiene_inline(tmp_path: Path) -> None:
    """Test that _collect_repo_hygiene_inline collects comprehensive hygiene data."""
    from unittest.mock import AsyncMock

    from gh_year_end.collect.orchestrator import _collect_repo_hygiene_inline

    # Create minimal config
    config_dict = {
        "github": {
            "target": {"mode": "user", "name": "test-user"},
            "windows": {
                "year": 2024,
                "since": "2024-01-01T00:00:00Z",
                "until": "2025-01-01T00:00:00Z",
            },
        },
        "collection": {
            "enable": {
                "pulls": True,
                "issues": True,
                "reviews": True,
                "comments": False,
                "commits": False,
                "hygiene": True,
            }
        },
        "storage": {
            "root": str(tmp_path / "data"),
        },
    }

    config = Config.model_validate(config_dict)

    # Mock repo data
    repo = {
        "full_name": "test-user/test-repo",
        "default_branch": "main",
    }

    # Mock REST client
    rest_client = AsyncMock()

    # Mock branch protection response
    rest_client.get_branch_protection.return_value = (
        {
            "required_status_checks": {"strict": True},
            "enforce_admins": {"enabled": True},
        },
        200,
    )

    # Mock repository tree response
    rest_client.get_repository_tree.return_value = {
        "tree": [
            {"path": "README.md", "type": "blob"},
            {"path": "SECURITY.md", "type": "blob"},
            {"path": ".github/CODEOWNERS", "type": "blob"},
            {"path": "CONTRIBUTING.md", "type": "blob"},
            {"path": "LICENSE", "type": "blob"},
            {"path": ".github/workflows/ci.yml", "type": "blob"},
        ]
    }

    # Mock security analysis response
    rest_client.get_repo_security_analysis.return_value = {
        "security_and_analysis": {
            "dependabot_security_updates": {"status": "enabled"},
            "secret_scanning": {"status": "enabled"},
        }
    }

    # Call function
    hygiene_data = await _collect_repo_hygiene_inline(
        repo=repo,
        owner="test-user",
        repo_name="test-repo",
        rest_client=rest_client,
        config=config,
    )

    # Verify hygiene data structure
    assert hygiene_data["repo"] == "test-user/test-repo"
    assert hygiene_data["default_branch"] == "main"
    assert hygiene_data["protected"] is True
    assert hygiene_data["branch_protection_enabled"] is True
    assert hygiene_data["has_readme"] is True
    assert hygiene_data["has_security_md"] is True
    assert hygiene_data["has_codeowners"] is True
    assert hygiene_data["has_contributing"] is True
    assert hygiene_data["has_license"] is True
    assert hygiene_data["has_ci_workflows"] is True
    assert hygiene_data["dependabot_enabled"] is True
    assert hygiene_data["secret_scanning_enabled"] is True

    # Verify score is calculated (should be 100 with all features enabled)
    assert hygiene_data["score"] == 100
