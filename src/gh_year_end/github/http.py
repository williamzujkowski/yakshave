"""GitHub HTTP client with rate limit handling.

Async HTTP client for GitHub API with automatic rate limit handling,
retry logic, and request/response tracking.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Optional

import httpx
from pydantic import BaseModel

from gh_year_end import __version__
from gh_year_end.github.auth import GitHubAuth

logger = logging.getLogger(__name__)


class RateLimitInfo(BaseModel):
    """GitHub API rate limit information from response headers."""

    limit: int
    remaining: int
    reset: datetime
    used: int
    resource: str = "core"

    @classmethod
    def from_headers(cls, headers: httpx.Headers) -> Optional["RateLimitInfo"]:
        """Extract rate limit info from response headers.

        Args:
            headers: HTTP response headers.

        Returns:
            RateLimitInfo if headers present, None otherwise.
        """
        if "x-ratelimit-limit" not in headers:
            return None

        reset_timestamp = int(headers.get("x-ratelimit-reset", "0"))
        reset_dt = datetime.fromtimestamp(reset_timestamp, tz=UTC)

        return cls(
            limit=int(headers.get("x-ratelimit-limit", "0")),
            remaining=int(headers.get("x-ratelimit-remaining", "0")),
            reset=reset_dt,
            used=int(headers.get("x-ratelimit-used", "0")),
            resource=headers.get("x-ratelimit-resource", "core"),
        )


@dataclass
class GitHubResponse:
    """GitHub API response with parsed data and metadata."""

    status_code: int
    data: Any
    headers: httpx.Headers
    rate_limit: RateLimitInfo | None = None
    url: str = ""
    retry_after: int | None = None

    @property
    def is_success(self) -> bool:
        """Check if response was successful (2xx status code)."""
        return 200 <= self.status_code < 300

    @property
    def is_rate_limited(self) -> bool:
        """Check if response indicates rate limiting (429 or 403 with rate limit)."""
        return self.status_code == 429 or (
            self.status_code == 403
            and self.rate_limit is not None
            and self.rate_limit.remaining == 0
        )


@dataclass
class HTTPRateLimitState:
    """Tracks rate limit state across HTTP requests."""

    last_rate_limit: RateLimitInfo | None = None
    last_check: datetime = field(default_factory=lambda: datetime.now(UTC))
    requests_made: int = 0
    rate_limit_hits: int = 0

    def update(self, rate_limit: RateLimitInfo | None) -> None:
        """Update state with new rate limit info.

        Args:
            rate_limit: Latest rate limit info from response.
        """
        self.requests_made += 1
        if rate_limit:
            self.last_rate_limit = rate_limit
            self.last_check = datetime.now(UTC)

            if rate_limit.remaining == 0:
                self.rate_limit_hits += 1
                logger.warning(
                    "Rate limit reached. Limit: %d, Reset: %s",
                    rate_limit.limit,
                    rate_limit.reset.isoformat(),
                )


class GitHubHTTPError(Exception):
    """Base exception for GitHub HTTP errors."""


class RateLimitExceeded(GitHubHTTPError):
    """Raised when rate limit is exceeded."""

    def __init__(self, reset_at: datetime, retry_after: int | None = None) -> None:
        self.reset_at = reset_at
        self.retry_after = retry_after
        super().__init__(f"Rate limit exceeded. Resets at {reset_at.isoformat()}")


class GitHubClient:
    """Async HTTP client for GitHub API with rate limit handling.

    Features:
    - Automatic authentication
    - Retry logic with exponential backoff
    - Rate limit detection and handling
    - Request/response logging
    - Configurable timeouts and retries
    """

    BASE_URL = "https://api.github.com"
    DEFAULT_TIMEOUT = 30.0
    DEFAULT_MAX_RETRIES = 3
    INITIAL_BACKOFF = 1.0
    BACKOFF_MULTIPLIER = 2.0

    def __init__(
        self,
        auth: GitHubAuth | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_url: str = BASE_URL,
    ) -> None:
        """Initialize GitHub HTTP client.

        Args:
            auth: GitHubAuth instance. If None, creates from environment.
            timeout: Request timeout in seconds.
            max_retries: Maximum number of retries for failed requests.
            base_url: Base URL for GitHub API.
        """
        self._auth = auth or GitHubAuth()
        self._timeout = timeout
        self._max_retries = max_retries
        self._base_url = base_url.rstrip("/")

        self._client: httpx.AsyncClient | None = None
        self._rate_limit_state = HTTPRateLimitState()

    @property
    def rate_limit_state(self) -> HTTPRateLimitState:
        """Get current rate limit state.

        Returns:
            Current rate limit tracking state.
        """
        return self._rate_limit_state

    def _get_headers(self) -> dict[str, str]:
        """Get headers for API requests.

        Returns:
            Dictionary of HTTP headers.
        """
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": f"gh-year-end/{__version__}",
        }
        headers.update(self._auth.get_authorization_header())
        return headers

    async def _ensure_client(self) -> httpx.AsyncClient:
        """Ensure async client is initialized.

        Returns:
            Active httpx.AsyncClient instance.
        """
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=self._timeout,
                headers=self._get_headers(),
                follow_redirects=True,
            )
        return self._client

    async def _handle_rate_limit(self, response: httpx.Response, retry_count: int) -> int | None:
        """Handle rate limit response.

        Args:
            response: HTTP response that may indicate rate limiting.
            retry_count: Current retry attempt number.

        Returns:
            Seconds to wait before retry, or None if shouldn't retry.

        Raises:
            RateLimitExceeded: If max retries exceeded.
        """
        # Check for retry-after header (secondary rate limits)
        retry_after = response.headers.get("retry-after")
        if retry_after:
            wait_seconds = int(retry_after)
            logger.warning("Secondary rate limit hit. Retry after %d seconds", wait_seconds)
            return wait_seconds

        # Check for primary rate limit
        rate_limit = RateLimitInfo.from_headers(response.headers)
        if rate_limit and rate_limit.remaining == 0:
            now = datetime.now(UTC)
            wait_seconds = int((rate_limit.reset - now).total_seconds()) + 1

            if retry_count >= self._max_retries:
                raise RateLimitExceeded(
                    reset_at=rate_limit.reset,
                    retry_after=wait_seconds if retry_after else None,
                )

            logger.warning(
                "Primary rate limit exhausted. Waiting %d seconds until %s",
                wait_seconds,
                rate_limit.reset.isoformat(),
            )
            return wait_seconds

        return None

    async def _retry_request(
        self,
        method: str,
        path: str,
        retry_count: int,
        **kwargs: Any,
    ) -> httpx.Response:
        """Retry a request with exponential backoff.

        Args:
            method: HTTP method.
            path: API path.
            retry_count: Current retry attempt.
            **kwargs: Additional arguments for request.

        Returns:
            HTTP response.

        Raises:
            GitHubHTTPError: If max retries exceeded.
        """
        if retry_count >= self._max_retries:
            raise GitHubHTTPError(f"Max retries ({self._max_retries}) exceeded for {method} {path}")

        wait_seconds = self.INITIAL_BACKOFF * (self.BACKOFF_MULTIPLIER**retry_count)
        logger.debug(
            "Retry %d/%d for %s %s after %.1fs",
            retry_count + 1,
            self._max_retries,
            method,
            path,
            wait_seconds,
        )
        await asyncio.sleep(wait_seconds)

        return await self._do_request(method, path, retry_count + 1, **kwargs)

    async def _do_request(
        self,
        method: str,
        path: str,
        retry_count: int = 0,
        **kwargs: Any,
    ) -> httpx.Response:
        """Execute HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.).
            path: API path (without base URL).
            retry_count: Current retry attempt number.
            **kwargs: Additional arguments passed to httpx.

        Returns:
            HTTP response.

        Raises:
            GitHubHTTPError: On request failure after retries.
            RateLimitExceeded: If rate limit exceeded.
        """
        client = await self._ensure_client()

        logger.debug("%s %s (attempt %d)", method, path, retry_count + 1)

        try:
            response = await client.request(method, path, **kwargs)

            # Handle rate limiting
            if response.status_code in (429, 403):
                wait_seconds = await self._handle_rate_limit(response, retry_count)
                if wait_seconds:
                    await asyncio.sleep(wait_seconds)
                    return await self._retry_request(method, path, retry_count, **kwargs)

            # Retry on server errors (5xx)
            if 500 <= response.status_code < 600:
                logger.warning("Server error %d for %s %s", response.status_code, method, path)
                return await self._retry_request(method, path, retry_count, **kwargs)

            # Raise for other client errors (4xx except 403/404, which are often expected)
            # 403 = permission denied, 404 = not found - both handled by caller
            if 400 <= response.status_code < 500 and response.status_code not in (403, 404):
                logger.error(
                    "Client error %d for %s %s: %s",
                    response.status_code,
                    method,
                    path,
                    response.text,
                )
                response.raise_for_status()

            return response

        except httpx.TimeoutException as e:
            logger.warning("Timeout for %s %s", method, path)
            if retry_count < self._max_retries:
                return await self._retry_request(method, path, retry_count, **kwargs)
            raise GitHubHTTPError(f"Request timeout: {e}") from e

        except httpx.NetworkError as e:
            logger.warning("Network error for %s %s: %s", method, path, e)
            if retry_count < self._max_retries:
                return await self._retry_request(method, path, retry_count, **kwargs)
            raise GitHubHTTPError(f"Network error: {e}") from e

    async def request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> GitHubResponse:
        """Make an HTTP request to GitHub API.

        Args:
            method: HTTP method (GET, POST, etc.).
            path: API path (e.g., "/user" or "repos/owner/repo").
            **kwargs: Additional arguments passed to httpx (params, json, etc.).

        Returns:
            GitHubResponse with parsed data and metadata.

        Raises:
            GitHubHTTPError: On request failure.
            RateLimitExceeded: If rate limit exceeded.
        """
        response = await self._do_request(method, path, **kwargs)

        # Extract rate limit info
        rate_limit = RateLimitInfo.from_headers(response.headers)
        self._rate_limit_state.update(rate_limit)

        # Extract retry-after if present
        retry_after = None
        if "retry-after" in response.headers:
            retry_after = int(response.headers["retry-after"])

        # Parse JSON response
        data = None
        if response.content:
            try:
                data = response.json()
            except Exception as e:
                logger.warning("Failed to parse JSON response: %s", e)
                data = response.text

        return GitHubResponse(
            status_code=response.status_code,
            data=data,
            headers=response.headers,
            rate_limit=rate_limit,
            url=str(response.url),
            retry_after=retry_after,
        )

    async def get(self, path: str, **kwargs: Any) -> GitHubResponse:
        """Make a GET request.

        Args:
            path: API path.
            **kwargs: Additional arguments (params, etc.).

        Returns:
            GitHubResponse with parsed data.
        """
        return await self.request("GET", path, **kwargs)

    async def post(self, path: str, **kwargs: Any) -> GitHubResponse:
        """Make a POST request.

        Args:
            path: API path.
            **kwargs: Additional arguments (json, data, etc.).

        Returns:
            GitHubResponse with parsed data.
        """
        return await self.request("POST", path, **kwargs)

    async def close(self) -> None:
        """Close the HTTP client and cleanup resources."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "GitHubClient":
        """Async context manager entry."""
        await self._ensure_client()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()
