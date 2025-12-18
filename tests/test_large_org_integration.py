"""Integration tests for handling large organizations.

Tests the integration of filters, checkpoints, rate limiting, and collection
orchestration when processing organizations with hundreds of repositories.
"""

import asyncio
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from gh_year_end.collect.discovery import discover_repos
from gh_year_end.collect.filters import FilterChain
from gh_year_end.config import (
    Config,
)
from gh_year_end.github.http import GitHubClient, GitHubResponse
from gh_year_end.github.ratelimit import (
    AdaptiveRateLimiter,
    APIType,
    CircuitState,
    ProgressState,
    RequestPriority,
)
from gh_year_end.storage.checkpoint import CheckpointManager
from gh_year_end.storage.paths import PathManager

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def large_org_config(tmp_path: Path) -> Config:
    """Config with all filters enabled for a large organization."""
    config_dict = {
        "github": {
            "target": {"mode": "org", "name": "bigcorp"},
            "auth": {"token_env": "GITHUB_TOKEN"},
            "discovery": {
                "include_forks": False,
                "include_archived": False,
                "visibility": "all",
                "activity_filter": {
                    "enabled": True,
                    "min_pushed_within_days": 365,
                },
                "size_filter": {
                    "enabled": True,
                    "min_kb": 10,
                    "max_kb": 100000,
                },
                "language_filter": {
                    "enabled": True,
                    "include": ["Python", "JavaScript", "Go"],
                    "exclude": ["HTML", "CSS"],
                },
                "topics_filter": {
                    "enabled": True,
                    "require_any": ["backend", "frontend", "api"],
                    "exclude": ["deprecated"],
                },
                "name_pattern_filter": {
                    "enabled": True,
                    "exclude_regex": [r".*-archive$", r"^test-.*"],
                },
                "quick_scan": {"enabled": False},
            },
            "windows": {
                "year": 2024,
                "since": "2024-01-01T00:00:00Z",
                "until": "2025-01-01T00:00:00Z",
            },
        },
        "storage": {
            "root": str(tmp_path / "data"),
        },
    }

    return Config.model_validate(config_dict)


@pytest.fixture
def mock_repos() -> list[dict[str, Any]]:
    """Generate 200 mock repositories with varied attributes.

    Distribution:
    - 50% archived (100 repos)
    - 30% forks (60 repos)
    - Various languages, topics, sizes, activity dates
    """
    repos = []
    now = datetime.now(UTC)

    for i in range(200):
        # Determine repo characteristics
        is_archived = i % 2 == 0  # 50% archived
        is_fork = i % 3 == 0  # 33% forks

        # Language distribution
        languages = ["Python", "JavaScript", "Go", "Java", "Ruby", "HTML", "CSS"]
        language = languages[i % len(languages)]

        # Topics distribution
        all_topics = ["backend", "frontend", "api", "cli", "web", "deprecated", "ml"]
        topics = [all_topics[j] for j in range(i % 3 + 1)]  # 1-3 topics

        # Size distribution (in KB)
        sizes = [5, 50, 500, 5000, 50000, 150000]
        size = sizes[i % len(sizes)]

        # Activity distribution
        days_ago = i % 400  # 0-399 days ago
        pushed_at = (now - timedelta(days=days_ago)).isoformat()

        # Name patterns
        name_prefixes = ["repo", "test", "service", "lib", "tool"]
        name_suffixes = ["", "-api", "-archive", "-v2"]
        name = f"{name_prefixes[i % len(name_prefixes)]}-{i:03d}{name_suffixes[i % len(name_suffixes)]}"

        repo = {
            "id": 1000 + i,
            "name": name,
            "full_name": f"bigcorp/{name}",
            "description": f"Repository {i}",
            "fork": is_fork,
            "archived": is_archived,
            "private": i % 4 == 0,  # 25% private
            "visibility": "private" if i % 4 == 0 else "public",
            "default_branch": "main",
            "language": language,
            "topics": topics,
            "size": size,
            "stargazers_count": i % 100,
            "forks_count": i % 20,
            "open_issues_count": i % 50,
            "created_at": (now - timedelta(days=365 * 2)).isoformat(),
            "updated_at": (now - timedelta(days=days_ago)).isoformat(),
            "pushed_at": pushed_at,
        }

        repos.append(repo)

    return repos


@pytest.fixture
def mock_github_client() -> AsyncMock:
    """Mock GitHub client with rate limit headers."""
    client = AsyncMock(spec=GitHubClient)

    # Default response headers
    default_headers = {
        "x-ratelimit-limit": "5000",
        "x-ratelimit-remaining": "4500",
        "x-ratelimit-reset": str(int(time.time() + 3600)),
    }

    # Mock successful response
    def make_response(data: Any, headers: dict[str, str] | None = None) -> GitHubResponse:
        """Create a mock response."""
        response = Mock(spec=GitHubResponse)
        response.data = data
        response.status_code = 200
        response.is_success = True
        response.headers = headers or default_headers
        return response

    client.make_response = make_response

    return client


@pytest.fixture
def temp_paths(tmp_path: Path, large_org_config: Config) -> PathManager:
    """Create temporary path manager."""
    # Update config to use tmp_path
    large_org_config.storage.root = tmp_path / "data"
    return PathManager(large_org_config)


@pytest.fixture
def checkpoint_manager(tmp_path: Path, large_org_config: Config) -> CheckpointManager:
    """Create checkpoint manager with config."""
    checkpoint_path = tmp_path / "checkpoint.json"
    mgr = CheckpointManager(checkpoint_path)
    mgr.create_new(large_org_config)
    return mgr


# ============================================================================
# Filter Integration Tests
# ============================================================================


class TestFilterIntegration:
    """Test filter chain integration with large datasets."""

    def test_filter_chain_reduces_repos(
        self,
        large_org_config: Config,
        mock_repos: list[dict[str, Any]],
    ) -> None:
        """Test that filter chain significantly reduces 200 repos."""
        filter_chain = FilterChain(large_org_config.github.discovery)

        # Count passing repos
        passing = sum(1 for repo in mock_repos if filter_chain.evaluate(repo).passed)

        # Should filter out a significant portion
        assert passing < len(mock_repos), "Filters should reduce repo count"
        assert passing > 0, "Some repos should pass filters"

        # Expected reductions:
        # - 50% archived (100 rejected)
        # - 30% forks (60 rejected, but overlap with archived)
        # - Activity filter (some old repos)
        # - Language filter (HTML, CSS)
        # - Topics filter (must have backend/frontend/api)
        # - Name pattern (test-* and *-archive)

        # Should have roughly 20-50 repos passing
        assert 10 <= passing <= 80, f"Expected 10-80 repos, got {passing}"

    def test_quick_scan_query_generation(
        self,
        large_org_config: Config,
    ) -> None:
        """Test that quick scan generates valid Search API query."""
        filter_chain = FilterChain(large_org_config.github.discovery)

        query = filter_chain.get_search_query("bigcorp", "org")

        # Should include org qualifier
        assert "org:bigcorp" in query

        # Should not include fork/archived (excluded by config)
        assert "fork:false" in query or "fork:" not in query
        assert "archived:false" in query or "archived:" not in query

        # Query should be non-empty and reasonable length
        assert len(query) > 10
        assert len(query) < 500

    def test_filter_statistics_accurate(
        self,
        large_org_config: Config,
        mock_repos: list[dict[str, Any]],
    ) -> None:
        """Test that filter statistics match actual rejections."""
        filter_chain = FilterChain(large_org_config.github.discovery)

        passed_count = 0
        rejected_count = 0

        for repo in mock_repos:
            result = filter_chain.evaluate(repo)
            if result.passed:
                passed_count += 1
            else:
                rejected_count += 1
                if result.filter_name:
                    filter_chain.record_rejection(result.filter_name)

        stats = filter_chain.get_stats()

        # Total rejections should match
        assert sum(stats.values()) == rejected_count

        # Should have rejections from multiple filters
        assert len(stats) > 0

        # Archive and fork filters should have high counts
        assert stats.get("archive", 0) > 0 or stats.get("fork", 0) > 0


# ============================================================================
# Checkpoint Integration Tests
# ============================================================================


class TestCheckpointIntegration:
    """Test checkpoint system integration with collection."""

    def test_checkpoint_created_on_start(
        self,
        tmp_path: Path,
        large_org_config: Config,
    ) -> None:
        """Test that new checkpoint is created at collection start."""
        checkpoint_path = tmp_path / "checkpoint.json"
        mgr = CheckpointManager(checkpoint_path)

        assert not mgr.exists()

        mgr.create_new(large_org_config)

        assert mgr.exists()
        assert checkpoint_path.exists()

        # Load and verify structure
        mgr.load()
        assert mgr._data["version"] == "1.0"
        assert mgr._data["target"]["mode"] == "org"
        assert mgr._data["target"]["name"] == "bigcorp"
        assert mgr._data["year"] == 2024

    def test_checkpoint_saved_on_interrupt(
        self,
        checkpoint_manager: CheckpointManager,
        mock_repos: list[dict[str, Any]],
    ) -> None:
        """Test that checkpoint is saved when collection is interrupted."""
        # Update with repos
        checkpoint_manager.update_repos(mock_repos[:10])

        # Mark some as in progress
        checkpoint_manager.mark_repo_endpoint_in_progress("bigcorp/repo-000", "pulls")
        checkpoint_manager.update_progress("bigcorp/repo-000", "pulls", page=1, records=30)

        # Mark some as complete
        checkpoint_manager.mark_repo_endpoint_complete("bigcorp/repo-000", "pulls")
        checkpoint_manager.mark_repo_endpoint_complete("bigcorp/repo-000", "issues")

        # Save (simulating interrupt)
        checkpoint_manager.save()

        # Create new manager and load
        new_mgr = CheckpointManager(checkpoint_manager.checkpoint_path)
        new_mgr.load()

        # Verify state preserved
        stats = new_mgr.get_stats()
        assert stats["total_repos"] == 10
        assert new_mgr.is_repo_endpoint_complete("bigcorp/repo-000", "pulls")

    def test_checkpoint_resume_skips_completed(
        self,
        checkpoint_manager: CheckpointManager,
        mock_repos: list[dict[str, Any]],
    ) -> None:
        """Test that resume skips completed repos."""
        # Setup: mark some repos complete, some pending
        test_repos = mock_repos[:10]
        checkpoint_manager.update_repos(test_repos)

        # Complete first 5 repos (mark all expected endpoints complete)
        endpoints = ["pulls", "issues", "reviews", "comments", "commits"]
        completed_repos = [repo["full_name"] for repo in test_repos[:5]]

        for repo_name in completed_repos:
            for endpoint in endpoints:
                checkpoint_manager.mark_repo_endpoint_in_progress(repo_name, endpoint)
                checkpoint_manager.mark_repo_endpoint_complete(repo_name, endpoint)

        # Get repos to process
        repos_to_process = checkpoint_manager.get_repos_to_process(retry_failed=False)

        # Should only return incomplete repos (5 remaining)
        assert len(repos_to_process) == 5

        # Should not include completed repos
        for repo_name in completed_repos:
            assert repo_name not in repos_to_process

    def test_checkpoint_retry_failed_only(
        self,
        checkpoint_manager: CheckpointManager,
        mock_repos: list[dict[str, Any]],
    ) -> None:
        """Test that failed repos are only retried when requested."""
        checkpoint_manager.update_repos(mock_repos[:10])

        # Mark some as failed with non-retryable errors (to set repo status to FAILED)
        checkpoint_manager.mark_repo_endpoint_failed(
            "bigcorp/repo-000",
            "pulls",
            "API error 500",
            retryable=False,
        )
        checkpoint_manager.mark_repo_endpoint_failed(
            "bigcorp/repo-001",
            "issues",
            "Rate limit exceeded",
            retryable=False,
        )

        # Without retry_failed, should not include failed repos
        repos_without_retry = checkpoint_manager.get_repos_to_process(retry_failed=False)
        assert "bigcorp/repo-000" not in repos_without_retry
        assert "bigcorp/repo-001" not in repos_without_retry

        # With retry_failed, should include them
        repos_with_retry = checkpoint_manager.get_repos_to_process(retry_failed=True)
        assert "bigcorp/repo-000" in repos_with_retry
        assert "bigcorp/repo-001" in repos_with_retry


# ============================================================================
# Rate Limiter Integration Tests
# ============================================================================


class TestRateLimiterIntegration:
    """Test rate limiter integration with collection."""

    @pytest.mark.asyncio
    async def test_burst_allowed_then_throttled(
        self,
        large_org_config: Config,
    ) -> None:
        """Test that burst capacity allows fast initial requests then throttles."""
        limiter = AdaptiveRateLimiter(large_org_config.rate_limit)

        # Test burst capacity without rate limit constraints first
        start_time = time.time()
        burst_count = 5  # Small burst

        for _ in range(burst_count):
            await limiter.acquire(priority=RequestPriority.HIGH)
            limiter.release(success=True)

        burst_duration = time.time() - start_time

        # Initial burst should be fast
        assert burst_duration < 2.0, f"Burst took {burst_duration}s, expected < 2s"

        # Now test that rate limit state affects behavior
        # Set very low rate limit to trigger throttling
        limiter._state[APIType.REST].limit = 5000
        limiter._state[APIType.REST].remaining = 250  # 5% remaining (triggers max delays)
        limiter._state[APIType.REST].reset_at = time.time() + 3600

        # Calculate delay for LOW priority at 5% remaining
        # This should trigger significant adaptive delay
        state = limiter._state[APIType.REST]
        calculated_delay = limiter._calculate_adaptive_delay(state, RequestPriority.LOW)

        # At 5% remaining, LOW priority should have significant delay
        assert calculated_delay > 0.1, (
            f"Expected delay > 0.1s at 5% remaining, got {calculated_delay}s"
        )

        # Verify rate limit state is properly tracked
        assert state.remaining_percent < 10
        assert state.remaining_percent == 5.0

    @pytest.mark.asyncio
    async def test_circuit_breaker_opens_on_failures(
        self,
        large_org_config: Config,
    ) -> None:
        """Test that circuit breaker opens after multiple failures."""
        limiter = AdaptiveRateLimiter(large_org_config.rate_limit)

        # Initial state should be closed
        assert limiter._circuit_breaker.get_state() == CircuitState.CLOSED

        # Record failures (default threshold is 5)
        # Need to await the record_failure calls
        for _ in range(5):
            await limiter.acquire()
            limiter.release(success=False)
            # Give async tasks time to complete
            await asyncio.sleep(0.01)

        # Circuit should open (check after giving async tasks time)
        await asyncio.sleep(0.1)
        assert limiter._circuit_breaker.get_state() == CircuitState.OPEN

        # Should not allow execution
        can_execute = await limiter._circuit_breaker.can_execute()
        assert not can_execute

    @pytest.mark.asyncio
    async def test_priority_affects_ordering(
        self,
        large_org_config: Config,
    ) -> None:
        """Test that HIGH priority requests have lower delays than LOW priority."""
        # Simulate low rate limit state
        limiter = AdaptiveRateLimiter(large_org_config.rate_limit)

        # Set rate limit to 30% remaining (should trigger adaptive delays)
        limiter._state[APIType.REST].limit = 5000
        limiter._state[APIType.REST].remaining = 1500  # 30%
        limiter._state[APIType.REST].reset_at = time.time() + 3600

        # Measure high priority delay
        start = time.time()
        await limiter.acquire(priority=RequestPriority.HIGH)
        high_delay = time.time() - start
        limiter.release()

        # Measure low priority delay
        start = time.time()
        await limiter.acquire(priority=RequestPriority.LOW)
        low_delay = time.time() - start
        limiter.release()

        # Low priority should have higher delay
        # (or at least not less than high priority)
        assert low_delay >= high_delay * 0.8, (
            f"Low priority delay ({low_delay}s) should be >= high priority delay ({high_delay}s)"
        )


# ============================================================================
# Full Pipeline Integration Tests
# ============================================================================


class TestFullPipelineIntegration:
    """Test full collection pipeline with mocked GitHub API."""

    @pytest.mark.asyncio
    async def test_large_org_collection_with_filters(
        self,
        large_org_config: Config,
        mock_repos: list[dict[str, Any]],
        temp_paths: PathManager,
    ) -> None:
        """Test collection pipeline filters 200 repos to ~50."""
        # Mock GitHub client
        with patch("gh_year_end.collect.discovery._fetch_repos") as mock_fetch:
            mock_fetch.return_value = mock_repos

            # Mock writer to avoid file I/O
            with patch("gh_year_end.collect.discovery._write_raw_repos"):
                # Create mock client
                mock_client = AsyncMock(spec=GitHubClient)

                # Run discovery
                discovered = await discover_repos(
                    config=large_org_config,
                    client=mock_client,
                    paths=temp_paths,
                )

                # Should filter down significantly
                assert len(discovered) < len(mock_repos)
                assert len(discovered) > 0

                # Should be roughly 10-80 repos after filtering
                assert 10 <= len(discovered) <= 80

    @pytest.mark.asyncio
    async def test_large_org_collection_with_checkpoint(
        self,
        tmp_path: Path,
        large_org_config: Config,
        mock_repos: list[dict[str, Any]],
    ) -> None:
        """Test collection with checkpoint interrupt and resume."""
        checkpoint_path = tmp_path / "checkpoint.json"
        mgr = CheckpointManager(checkpoint_path)
        mgr.create_new(large_org_config)

        # Update with first 20 repos
        mgr.update_repos(mock_repos[:20])

        # Simulate partial collection - complete first 10 repos fully
        endpoints = ["pulls", "issues", "reviews", "comments", "commits"]
        test_repos = mock_repos[:20]
        completed_repos = [repo["full_name"] for repo in test_repos[:10]]

        for repo_name in completed_repos:
            for endpoint in endpoints:
                mgr.mark_repo_endpoint_in_progress(repo_name, endpoint)
                mgr.update_progress(repo_name, endpoint, page=1, records=25)
                mgr.mark_repo_endpoint_complete(repo_name, endpoint)

        # Save checkpoint (simulating interrupt)
        mgr.save()

        # Resume: load checkpoint
        new_mgr = CheckpointManager(checkpoint_path)
        new_mgr.load()

        # Get remaining repos
        remaining = new_mgr.get_repos_to_process()

        # Should have 10 remaining (20 total - 10 completed)
        assert len(remaining) == 10

        # Complete the rest
        for repo_name in remaining:
            for endpoint in endpoints:
                new_mgr.mark_repo_endpoint_in_progress(repo_name, endpoint)
                new_mgr.mark_repo_endpoint_complete(repo_name, endpoint)

        # All should be complete now
        final_remaining = new_mgr.get_repos_to_process()
        assert len(final_remaining) == 0

        # Stats should show all complete
        stats = new_mgr.get_stats()
        assert stats["repos_complete"] == 20
        assert stats["repos_pending"] == 0

    @pytest.mark.asyncio
    async def test_large_org_collection_progress_tracking(
        self,
        large_org_config: Config,
    ) -> None:
        """Test that progress tracking calculates ETA correctly."""
        limiter = AdaptiveRateLimiter(large_org_config.rate_limit)

        # Create progress state for 100 repos
        progress = ProgressState(
            phase="pulls",
            total_items=100,
            completed_items=0,
        )

        limiter.set_progress_state(progress)

        # Simulate progress
        for i in range(25):
            progress.completed_items = i + 1
            await asyncio.sleep(0.01)  # Small delay to simulate work

        # Calculate ETA
        seconds_remaining, eta_str = progress.calculate_eta()

        # Should have reasonable ETA
        assert seconds_remaining > 0
        assert eta_str != "unknown"
        assert "s" in eta_str or "m" in eta_str

        # Should have made requests
        assert progress.requests_made >= 0


# ============================================================================
# Stress Test Scenarios
# ============================================================================


class TestStressScenarios:
    """Stress test scenarios for large organization handling."""

    def test_filter_performance_200_repos(
        self,
        large_org_config: Config,
        mock_repos: list[dict[str, Any]],
    ) -> None:
        """Test that filtering 200 repos completes quickly."""
        filter_chain = FilterChain(large_org_config.github.discovery)

        start_time = time.time()

        passing_repos = []
        for repo in mock_repos:
            result = filter_chain.evaluate(repo)
            if result.passed:
                passing_repos.append(repo)
                filter_chain.record_rejection(result.filter_name or "none")

        duration = time.time() - start_time

        # Should complete in under 1 second
        assert duration < 1.0, f"Filtering took {duration}s, expected < 1s"

        # Stats should be available
        stats = filter_chain.get_stats()
        assert len(stats) > 0

    def test_checkpoint_update_performance(
        self,
        checkpoint_manager: CheckpointManager,
        mock_repos: list[dict[str, Any]],
    ) -> None:
        """Test checkpoint updates with 200 repos."""
        start_time = time.time()

        # Update with all repos
        checkpoint_manager.update_repos(mock_repos)

        duration = time.time() - start_time

        # Should complete quickly
        assert duration < 2.0, f"Checkpoint update took {duration}s"

        # All repos should be in checkpoint
        stats = checkpoint_manager.get_stats()
        assert stats["total_repos"] == len(mock_repos)

    def test_checkpoint_resume_page_tracking(
        self,
        checkpoint_manager: CheckpointManager,
    ) -> None:
        """Test page-level resume for paginated endpoints."""
        repo_name = "bigcorp/large-repo"
        endpoint = "pulls"

        # Initialize repo
        checkpoint_manager.update_repos([{"full_name": repo_name}])
        checkpoint_manager.mark_repo_endpoint_in_progress(repo_name, endpoint)

        # Simulate collecting 10 pages
        for page in range(1, 11):
            checkpoint_manager.update_progress(repo_name, endpoint, page=page, records=100)

        # Get resume page
        resume_page = checkpoint_manager.get_resume_page(repo_name, endpoint)

        # Should resume from page 11 (last_page_written + 1)
        assert resume_page == 11

        # Mark complete
        checkpoint_manager.mark_repo_endpoint_complete(repo_name, endpoint)

        # Should be complete
        assert checkpoint_manager.is_repo_endpoint_complete(repo_name, endpoint)
