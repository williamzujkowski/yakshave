"""Adaptive rate limiter for GitHub API requests.

Implements intelligent throttling based on rate limit headers and configurable strategies
to avoid hitting GitHub's primary and secondary rate limits.
"""

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from gh_year_end.config import RateLimitConfig

logger = logging.getLogger(__name__)


class APIType(str, Enum):
    """GitHub API type for separate rate limit tracking."""

    REST = "rest"
    GRAPHQL = "graphql"


@dataclass
class RateLimitState:
    """Current state of rate limits for a specific API type."""

    api_type: APIType
    limit: int = 5000  # Default GitHub limit
    remaining: int = 5000
    reset_at: float = 0.0  # Unix timestamp
    last_updated: float = field(default_factory=time.time)

    @property
    def remaining_percent(self) -> float:
        """Calculate percentage of requests remaining."""
        if self.limit == 0:
            return 0.0
        return (self.remaining / self.limit) * 100

    @property
    def seconds_until_reset(self) -> float:
        """Calculate seconds until rate limit reset."""
        return max(0.0, self.reset_at - time.time())

    def is_exhausted(self) -> bool:
        """Check if rate limit is exhausted."""
        return self.remaining <= 0


@dataclass
class RateLimitSample:
    """Rate limit state sample for analytics."""

    timestamp: str
    api_type: str
    limit: int
    remaining: int
    remaining_percent: float
    reset_at: str
    seconds_until_reset: float

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSONL storage."""
        return {
            "timestamp": self.timestamp,
            "api_type": self.api_type,
            "limit": self.limit,
            "remaining": self.remaining,
            "remaining_percent": self.remaining_percent,
            "reset_at": self.reset_at,
            "seconds_until_reset": self.seconds_until_reset,
        }


class AdaptiveRateLimiter:
    """Adaptive rate limiter with GitHub-specific throttling strategies."""

    def __init__(self, config: RateLimitConfig) -> None:
        """Initialize rate limiter.

        Args:
            config: Rate limit configuration.
        """
        self.config = config
        self._semaphore = asyncio.Semaphore(config.max_concurrency)
        self._lock = asyncio.Lock()

        # Separate state tracking for REST and GraphQL
        self._state: dict[APIType, RateLimitState] = {
            APIType.REST: RateLimitState(api_type=APIType.REST),
            APIType.GRAPHQL: RateLimitState(api_type=APIType.GRAPHQL),
        }

        # Secondary rate limit tracking (requests per minute)
        self._request_timestamps: deque[float] = deque(maxlen=200)

        # Sample collection
        self._samples: list[RateLimitSample] = []
        self._requests_since_sample = 0

        logger.info(
            "Rate limiter initialized: strategy=%s, max_concurrency=%d",
            config.strategy,
            config.max_concurrency,
        )

    async def acquire(self, api_type: APIType = APIType.REST) -> None:
        """Acquire permission to make a request.

        Blocks until it's safe to proceed based on rate limit state.

        Args:
            api_type: Type of API being called (REST or GraphQL).
        """
        # Acquire semaphore slot for concurrency control
        await self._semaphore.acquire()

        try:
            # Check and apply throttling
            await self._apply_throttling(api_type)

            # Track request timing for secondary limit protection
            async with self._lock:
                self._request_timestamps.append(time.time())

        except Exception:
            # Release semaphore if we fail
            self._semaphore.release()
            raise

    def release(self) -> None:
        """Release the acquired semaphore slot."""
        self._semaphore.release()

    async def _apply_throttling(self, api_type: APIType) -> None:
        """Apply adaptive throttling based on current rate limit state.

        Args:
            api_type: Type of API being called.
        """
        async with self._lock:
            state = self._state[api_type]

            # Check if we hit the rate limit
            if state.is_exhausted():
                wait_time = state.seconds_until_reset
                logger.warning(
                    "Rate limit exhausted for %s API. Sleeping %.1f seconds until reset.",
                    api_type.value,
                    wait_time,
                )
                await asyncio.sleep(wait_time + 1)  # Add 1 second buffer
                return

            # Apply secondary rate limit protection
            await self._enforce_secondary_limit()

            # Apply adaptive delay based on strategy
            if self.config.strategy == "adaptive":
                delay = self._calculate_adaptive_delay(state)
                if delay > 0:
                    logger.debug(
                        "Adaptive throttling: %.2fs delay (%.1f%% remaining)",
                        delay,
                        state.remaining_percent,
                    )
                    await asyncio.sleep(delay)

    def _calculate_adaptive_delay(self, state: RateLimitState) -> float:
        """Calculate adaptive delay based on remaining rate limit percentage.

        Args:
            state: Current rate limit state.

        Returns:
            Delay in seconds.
        """
        remaining_pct = state.remaining_percent

        # > 50%: full speed (no delay)
        if remaining_pct > 50:
            return 0.0

        # 20-50%: slow down (linear scaling)
        if remaining_pct > 20:
            # Scale from 0 to min_sleep_seconds as we go from 50% to 20%
            factor = (50 - remaining_pct) / 30  # 0.0 to 1.0
            return self.config.min_sleep_seconds * factor

        # < 20%: significant delay (exponential scaling)
        if remaining_pct > 0:
            # Scale from min to max sleep as we go from 20% to 0%
            factor = (20 - remaining_pct) / 20  # 0.0 to 1.0
            delay_range = self.config.max_sleep_seconds - self.config.min_sleep_seconds
            return self.config.min_sleep_seconds + (delay_range * (factor**2))

        # Exhausted
        return self.config.max_sleep_seconds

    async def _enforce_secondary_limit(self) -> None:
        """Enforce secondary rate limit (requests per minute).

        GitHub has undocumented secondary limits around ~100 req/min.
        We stay conservative and enforce a slightly lower limit.
        """
        now = time.time()
        minute_ago = now - 60

        # Count requests in last minute
        recent_requests = sum(1 for ts in self._request_timestamps if ts > minute_ago)

        # If we're approaching the limit, slow down
        max_per_minute = 90  # Conservative limit
        if recent_requests >= max_per_minute:
            # Find oldest request in the window
            oldest_in_window = min(
                (ts for ts in self._request_timestamps if ts > minute_ago),
                default=now,
            )
            sleep_time = max(0.0, 60 - (now - oldest_in_window) + 0.5)

            if sleep_time > 0:
                logger.debug(
                    "Secondary rate limit protection: %d requests/min, sleeping %.1fs",
                    recent_requests,
                    sleep_time,
                )
                await asyncio.sleep(sleep_time)

    def update(self, headers: dict[str, str], api_type: APIType = APIType.REST) -> None:
        """Update rate limit state from response headers.

        Args:
            headers: HTTP response headers (case-insensitive).
            api_type: Type of API that was called.
        """
        # Normalize header keys to lowercase
        normalized = {k.lower(): v for k, v in headers.items()}

        # Check for retry-after header (takes priority)
        if "retry-after" in normalized:
            retry_after = int(normalized["retry-after"])
            logger.warning("Retry-After header present: %d seconds", retry_after)
            # We'll handle this in acquire() by checking state
            # Set remaining to 0 to trigger wait
            state = self._state[api_type]
            state.remaining = 0
            state.reset_at = time.time() + retry_after
            state.last_updated = time.time()
            return

        # Extract rate limit headers
        limit = normalized.get("x-ratelimit-limit")
        remaining = normalized.get("x-ratelimit-remaining")
        reset = normalized.get("x-ratelimit-reset")

        if limit and remaining and reset:
            state = self._state[api_type]
            state.limit = int(limit)
            state.remaining = int(remaining)
            state.reset_at = float(reset)
            state.last_updated = time.time()

            logger.debug(
                "Rate limit updated: %s API: %d/%d (%.1f%% remaining)",
                api_type.value,
                state.remaining,
                state.limit,
                state.remaining_percent,
            )

            # Increment sample counter and record if needed
            self._requests_since_sample += 1
            if (
                self._requests_since_sample
                >= self.config.sample_rate_limit_endpoint_every_n_requests
            ):
                self.record_sample(api_type)
                self._requests_since_sample = 0

    def record_sample(self, api_type: APIType = APIType.REST) -> RateLimitSample:
        """Record current rate limit state as a sample.

        Args:
            api_type: Type of API to sample.

        Returns:
            Recorded sample.
        """
        state = self._state[api_type]

        sample = RateLimitSample(
            timestamp=datetime.now(UTC).isoformat(),
            api_type=api_type.value,
            limit=state.limit,
            remaining=state.remaining,
            remaining_percent=round(state.remaining_percent, 2),
            reset_at=datetime.fromtimestamp(state.reset_at).isoformat() + "Z"
            if state.reset_at > 0
            else "",
            seconds_until_reset=round(state.seconds_until_reset, 2),
        )

        self._samples.append(sample)

        logger.debug(
            "Rate limit sample recorded: %s API: %d/%d (%.1f%%)",
            api_type.value,
            state.remaining,
            state.limit,
            state.remaining_percent,
        )

        return sample

    def get_samples(self) -> list[dict[str, Any]]:
        """Get all recorded samples as dictionaries.

        Returns:
            List of sample dictionaries for JSONL storage.
        """
        return [sample.to_dict() for sample in self._samples]

    def clear_samples(self) -> None:
        """Clear all recorded samples."""
        self._samples.clear()

    def get_state(self, api_type: APIType = APIType.REST) -> RateLimitState:
        """Get current rate limit state for an API type.

        Args:
            api_type: Type of API to query.

        Returns:
            Current rate limit state.
        """
        return self._state[api_type]

    async def __aenter__(self) -> "AdaptiveRateLimiter":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        # Nothing to clean up
        pass
