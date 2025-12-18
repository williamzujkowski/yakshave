"""Tests for GitHub HTTP client module."""

from datetime import UTC, datetime

import httpx

from gh_year_end.github.http import (
    GitHubResponse,
    HTTPRateLimitState,
    RateLimitInfo,
)


class TestRateLimitInfo:
    """Tests for RateLimitInfo model."""

    def test_from_headers_valid(self) -> None:
        """Test RateLimitInfo.from_headers with valid headers."""
        headers = httpx.Headers(
            {
                "x-ratelimit-limit": "5000",
                "x-ratelimit-remaining": "4999",
                "x-ratelimit-reset": "1234567890",
                "x-ratelimit-used": "1",
                "x-ratelimit-resource": "core",
            }
        )

        info = RateLimitInfo.from_headers(headers)

        assert info is not None
        assert info.limit == 5000
        assert info.remaining == 4999
        assert info.used == 1
        assert info.resource == "core"
        assert info.reset == datetime.fromtimestamp(1234567890, tz=UTC)

    def test_from_headers_missing_headers(self) -> None:
        """Test RateLimitInfo.from_headers returns None for missing headers."""
        headers = httpx.Headers({})
        info = RateLimitInfo.from_headers(headers)
        assert info is None

    def test_from_headers_partial_headers(self) -> None:
        """Test RateLimitInfo.from_headers with partial headers."""
        headers = httpx.Headers(
            {
                "x-ratelimit-limit": "5000",
                "x-ratelimit-remaining": "4999",
                "x-ratelimit-reset": "1234567890",
            }
        )

        info = RateLimitInfo.from_headers(headers)

        assert info is not None
        assert info.limit == 5000
        assert info.remaining == 4999
        assert info.used == 0  # Default when missing
        assert info.resource == "core"  # Default when missing

    def test_from_headers_defaults(self) -> None:
        """Test RateLimitInfo.from_headers handles missing optional fields."""
        headers = httpx.Headers(
            {
                "x-ratelimit-limit": "5000",
                # Missing other fields
            }
        )

        info = RateLimitInfo.from_headers(headers)

        assert info is not None
        assert info.limit == 5000
        assert info.remaining == 0
        assert info.used == 0
        assert info.resource == "core"


class TestGitHubResponse:
    """Tests for GitHubResponse dataclass."""

    def test_is_success_200(self) -> None:
        """Test is_success returns True for 200 status code."""
        response = GitHubResponse(
            status_code=200,
            data={},
            headers=httpx.Headers({}),
        )
        assert response.is_success is True

    def test_is_success_201(self) -> None:
        """Test is_success returns True for 201 status code."""
        response = GitHubResponse(
            status_code=201,
            data={},
            headers=httpx.Headers({}),
        )
        assert response.is_success is True

    def test_is_success_299(self) -> None:
        """Test is_success returns True for 299 status code."""
        response = GitHubResponse(
            status_code=299,
            data={},
            headers=httpx.Headers({}),
        )
        assert response.is_success is True

    def test_is_success_false_4xx(self) -> None:
        """Test is_success returns False for 4xx status codes."""
        response = GitHubResponse(
            status_code=404,
            data={},
            headers=httpx.Headers({}),
        )
        assert response.is_success is False

    def test_is_success_false_5xx(self) -> None:
        """Test is_success returns False for 5xx status codes."""
        response = GitHubResponse(
            status_code=500,
            data={},
            headers=httpx.Headers({}),
        )
        assert response.is_success is False

    def test_is_rate_limited_429(self) -> None:
        """Test is_rate_limited returns True for 429 status code."""
        response = GitHubResponse(
            status_code=429,
            data={},
            headers=httpx.Headers({}),
        )
        assert response.is_rate_limited is True

    def test_is_rate_limited_403_with_zero_remaining(self) -> None:
        """Test is_rate_limited returns True for 403 with zero remaining."""
        rate_limit = RateLimitInfo(
            limit=5000,
            remaining=0,
            reset=datetime.now(UTC),
            used=5000,
        )
        response = GitHubResponse(
            status_code=403,
            data={},
            headers=httpx.Headers({}),
            rate_limit=rate_limit,
        )
        assert response.is_rate_limited is True

    def test_is_rate_limited_403_with_remaining(self) -> None:
        """Test is_rate_limited returns False for 403 with remaining requests."""
        rate_limit = RateLimitInfo(
            limit=5000,
            remaining=100,
            reset=datetime.now(UTC),
            used=4900,
        )
        response = GitHubResponse(
            status_code=403,
            data={},
            headers=httpx.Headers({}),
            rate_limit=rate_limit,
        )
        assert response.is_rate_limited is False

    def test_is_rate_limited_403_without_rate_limit_info(self) -> None:
        """Test is_rate_limited returns False for 403 without rate limit info."""
        response = GitHubResponse(
            status_code=403,
            data={},
            headers=httpx.Headers({}),
            rate_limit=None,
        )
        assert response.is_rate_limited is False

    def test_is_rate_limited_false_200(self) -> None:
        """Test is_rate_limited returns False for successful response."""
        response = GitHubResponse(
            status_code=200,
            data={},
            headers=httpx.Headers({}),
        )
        assert response.is_rate_limited is False


class TestHTTPRateLimitState:
    """Tests for HTTPRateLimitState tracking."""

    def test_initial_state(self) -> None:
        """Test initial state of HTTPRateLimitState."""
        state = HTTPRateLimitState()

        assert state.last_rate_limit is None
        assert state.requests_made == 0
        assert state.rate_limit_hits == 0
        assert isinstance(state.last_check, datetime)

    def test_update_increments_requests_made(self) -> None:
        """Test update increments requests_made counter."""
        state = HTTPRateLimitState()

        state.update(None)
        assert state.requests_made == 1

        state.update(None)
        assert state.requests_made == 2

    def test_update_with_rate_limit_info(self) -> None:
        """Test update stores rate limit info."""
        state = HTTPRateLimitState()
        rate_limit = RateLimitInfo(
            limit=5000,
            remaining=4999,
            reset=datetime.now(UTC),
            used=1,
        )

        state.update(rate_limit)

        assert state.last_rate_limit == rate_limit
        assert state.requests_made == 1
        assert state.rate_limit_hits == 0

    def test_update_increments_rate_limit_hits(self) -> None:
        """Test update increments rate_limit_hits when remaining is zero."""
        state = HTTPRateLimitState()
        rate_limit = RateLimitInfo(
            limit=5000,
            remaining=0,
            reset=datetime.now(UTC),
            used=5000,
        )

        state.update(rate_limit)

        assert state.rate_limit_hits == 1
        assert state.requests_made == 1

    def test_update_multiple_rate_limit_hits(self) -> None:
        """Test update increments rate_limit_hits on multiple zero remaining updates."""
        state = HTTPRateLimitState()

        for _ in range(3):
            rate_limit = RateLimitInfo(
                limit=5000,
                remaining=0,
                reset=datetime.now(UTC),
                used=5000,
            )
            state.update(rate_limit)

        assert state.rate_limit_hits == 3
        assert state.requests_made == 3

    def test_update_last_check_timestamp(self) -> None:
        """Test update updates last_check timestamp."""
        state = HTTPRateLimitState()
        initial_check = state.last_check

        import time

        time.sleep(0.01)  # Small delay to ensure different timestamp

        rate_limit = RateLimitInfo(
            limit=5000,
            remaining=4999,
            reset=datetime.now(UTC),
            used=1,
        )
        state.update(rate_limit)

        assert state.last_check > initial_check

    def test_update_without_rate_limit_info(self) -> None:
        """Test update without rate limit info only increments counter."""
        state = HTTPRateLimitState()

        state.update(None)

        assert state.last_rate_limit is None
        assert state.requests_made == 1
        assert state.rate_limit_hits == 0
