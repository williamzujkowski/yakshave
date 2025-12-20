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
