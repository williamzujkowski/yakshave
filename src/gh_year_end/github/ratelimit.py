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
from enum import Enum, IntEnum
from typing import Any

from gh_year_end.config import RateLimitConfig

logger = logging.getLogger(__name__)


class APIType(str, Enum):
    """GitHub API type for separate rate limit tracking."""

    REST = "rest"
    GRAPHQL = "graphql"


class RequestPriority(IntEnum):
    """Priority levels for API requests."""

    CRITICAL = 0  # Rate limit checks
    HIGH = 1  # Discovery
    MEDIUM = 2  # PRs, issues
    LOW = 3  # Comments, commits


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failures exceeded threshold
    HALF_OPEN = "half_open"  # Testing if service recovered


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


@dataclass
class ProgressState:
    """Track progress of a collection phase."""

    phase: str
    total_items: int
    completed_items: int = 0
    start_time: float = field(default_factory=time.time)
    requests_made: int = 0

    def calculate_eta(self) -> tuple[float, str]:
        """Calculate estimated time to completion.

        Returns:
            Tuple of (seconds_remaining, formatted_eta_string).
        """
        if self.completed_items == 0:
            return (0.0, "unknown")

        elapsed = time.time() - self.start_time
        rate = self.completed_items / elapsed
        remaining_items = self.total_items - self.completed_items

        if rate <= 0:
            return (0.0, "unknown")

        seconds_remaining = remaining_items / rate
        minutes = int(seconds_remaining // 60)
        seconds = int(seconds_remaining % 60)

        eta_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"

        return (seconds_remaining, eta_str)


class TokenBucket:
    """Token bucket rate limiter for burst control."""

    def __init__(self, capacity: int, fill_rate: float) -> None:
        """Initialize token bucket.

        Args:
            capacity: Maximum number of tokens (burst capacity).
            fill_rate: Tokens added per second (sustained rate).
        """
        self.capacity = capacity
        self.fill_rate = fill_rate
        self.tokens = float(capacity)
        self._last_refill = time.time()
        self._lock = asyncio.Lock()

    async def try_acquire(self, count: int = 1) -> bool:
        """Try to acquire tokens from the bucket.

        Args:
            count: Number of tokens to acquire.

        Returns:
            True if tokens were acquired, False otherwise.
        """
        async with self._lock:
            await self.refill()

            if self.tokens >= count:
                self.tokens -= count
                return True
            return False

    async def refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill

        # Add tokens based on fill rate
        new_tokens = elapsed * self.fill_rate
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self._last_refill = now


class CircuitBreaker:
    """Circuit breaker for API failure protection."""

    def __init__(
        self,
        failure_threshold: int = 5,
        success_threshold: int = 3,
        timeout_seconds: float = 60.0,
    ) -> None:
        """Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening circuit.
            success_threshold: Number of successes to close circuit from half-open.
            timeout_seconds: Seconds to wait before attempting half-open.
        """
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.timeout_seconds = timeout_seconds

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0.0
        self._lock = asyncio.Lock()

    async def record_success(self) -> None:
        """Record a successful request."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    logger.info("Circuit breaker closing after %d successes", self._success_count)
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0

    async def record_failure(self) -> None:
        """Record a failed request."""
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.CLOSED:
                if self._failure_count >= self.failure_threshold:
                    logger.warning("Circuit breaker opening after %d failures", self._failure_count)
                    self._state = CircuitState.OPEN
            elif self._state == CircuitState.HALF_OPEN:
                logger.warning("Circuit breaker re-opening after failure in half-open state")
                self._state = CircuitState.OPEN
                self._success_count = 0

    async def can_execute(self) -> bool:
        """Check if requests can be executed.

        Returns:
            True if circuit allows execution, False otherwise.
        """
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                # Check if timeout has elapsed
                if time.time() - self._last_failure_time >= self.timeout_seconds:
                    logger.info("Circuit breaker transitioning to half-open")
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                    return True
                return False

            # HALF_OPEN state
            return True

    def get_state(self) -> CircuitState:
        """Get current circuit state.

        Returns:
            Current circuit state.
        """
        return self._state


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

        # Token bucket for burst control
        self._token_bucket = TokenBucket(
            capacity=config.burst.capacity,
            fill_rate=config.burst.sustained_rate,
        )

        # Circuit breaker for failure protection
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            success_threshold=3,
            timeout_seconds=60.0,
        )

        # Secondary rate limit tracking (requests per minute)
        self._request_timestamps: deque[float] = deque(maxlen=200)
        self._secondary_backoff_multiplier = 1.0

        # Progress tracking
        self._progress_state: ProgressState | None = None

        # Sample collection
        self._samples: list[RateLimitSample] = []
        self._requests_since_sample = 0

        logger.info(
            "Rate limiter initialized: strategy=%s, max_concurrency=%d, burst_capacity=%d",
            config.strategy,
            config.max_concurrency,
            config.burst.capacity,
        )

    def set_progress_state(self, progress_state: ProgressState) -> None:
        """Set progress tracking state.

        Args:
            progress_state: Progress state for current phase.
        """
        self._progress_state = progress_state

    def get_progress_state(self) -> ProgressState | None:
        """Get current progress state.

        Returns:
            Current progress state or None if not set.
        """
        return self._progress_state

    async def acquire(
        self,
        api_type: APIType = APIType.REST,
        priority: RequestPriority = RequestPriority.MEDIUM,
    ) -> None:
        """Acquire permission to make a request.

        Blocks until it's safe to proceed based on rate limit state.

        Args:
            api_type: Type of API being called (REST or GraphQL).
            priority: Priority level for this request.
        """
        # Check circuit breaker
        if not await self._circuit_breaker.can_execute():
            logger.warning("Circuit breaker open, blocking request")
            # Wait for circuit to potentially close
            await asyncio.sleep(5.0)
            if not await self._circuit_breaker.can_execute():
                msg = "Circuit breaker open, cannot execute requests"
                raise RuntimeError(msg)

        # Acquire semaphore slot for concurrency control
        await self._semaphore.acquire()

        try:
            # Try to acquire token from bucket
            max_retries = 10
            for _attempt in range(max_retries):
                if await self._token_bucket.try_acquire():
                    break
                # Wait and refill tokens
                await asyncio.sleep(0.1)
            else:
                logger.debug("Token bucket exhausted, proceeding with caution")

            # Check and apply throttling
            await self._apply_throttling(api_type, priority)

            # Track request timing for secondary limit protection
            async with self._lock:
                self._request_timestamps.append(time.time())
                if self._progress_state:
                    self._progress_state.requests_made += 1

        except Exception:
            # Release semaphore if we fail
            self._semaphore.release()
            raise

    def release(self, success: bool = True) -> None:
        """Release the acquired semaphore slot.

        Args:
            success: Whether the request was successful.
        """
        self._semaphore.release()

        # Update circuit breaker - fire and forget
        # We don't need to await or store these tasks as they're simple state updates
        if success:
            _ = asyncio.create_task(self._circuit_breaker.record_success())  # noqa: RUF006
        else:
            _ = asyncio.create_task(self._circuit_breaker.record_failure())  # noqa: RUF006

    async def _apply_throttling(self, api_type: APIType, priority: RequestPriority) -> None:
        """Apply adaptive throttling based on current rate limit state.

        Args:
            api_type: Type of API being called.
            priority: Priority level for this request.
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

            # Apply adaptive delay based on strategy and priority
            if self.config.strategy == "adaptive":
                delay = self._calculate_adaptive_delay(state, priority)
                if delay > 0:
                    logger.debug(
                        "Adaptive throttling: %.2fs delay (%.1f%% remaining, priority=%s)",
                        delay,
                        state.remaining_percent,
                        priority.name,
                    )
                    await asyncio.sleep(delay)

    def _calculate_adaptive_delay(self, state: RateLimitState, priority: RequestPriority) -> float:
        """Calculate adaptive delay based on remaining rate limit percentage and priority.

        Args:
            state: Current rate limit state.
            priority: Request priority level.

        Returns:
            Delay in seconds.
        """
        remaining_pct = state.remaining_percent

        # Priority multiplier (CRITICAL gets 0.5x, LOW gets 1.5x)
        priority_multiplier: float = 1.0 + (float(priority.value) * 0.25) - 0.5

        # > 50%: full speed (no delay) - optimized threshold
        if remaining_pct > 50:
            return 0.0

        # 25-50%: minimal delay for LOW priority only
        if remaining_pct > 25:
            if priority == RequestPriority.LOW:
                factor = (50 - remaining_pct) / 25  # 0.0 to 1.0
                return self.config.min_sleep_seconds * factor * 0.5
            return 0.0

        # 10-25%: moderate delay (linear scaling)
        if remaining_pct > 10:
            factor = (25 - remaining_pct) / 15  # 0.0 to 1.0
            base_delay = self.config.min_sleep_seconds * factor
            return base_delay * priority_multiplier

        # 5-10%: significant delay (exponential scaling)
        if remaining_pct > 5:
            factor = (10 - remaining_pct) / 5  # 0.0 to 1.0
            delay_range = self.config.max_sleep_seconds - self.config.min_sleep_seconds
            base_delay = self.config.min_sleep_seconds + (delay_range * (factor**1.5))
            return float(base_delay * priority_multiplier)

        # < 5%: critical delay (exponential scaling)
        if remaining_pct > 0:
            factor = (5 - remaining_pct) / 5  # 0.0 to 1.0
            base_delay = self.config.max_sleep_seconds * (factor**2)
            return base_delay * priority_multiplier

        # Exhausted
        return self.config.max_sleep_seconds

    async def _enforce_secondary_limit(self) -> None:
        """Enforce secondary rate limit (requests per minute).

        GitHub has undocumented secondary limits around ~100 req/min.
        We stay conservative and enforce a slightly lower limit with adaptive backoff.
        """
        now = time.time()
        window_seconds = self.config.secondary.detection_window_seconds
        window_start = now - window_seconds

        # Count requests in the detection window
        recent_requests = sum(1 for ts in self._request_timestamps if ts > window_start)

        # Calculate rate per minute
        requests_per_minute = (recent_requests / window_seconds) * 60

        # If we're approaching the limit, slow down with adaptive backoff
        max_per_minute = self.config.secondary.max_requests_per_minute

        threshold = self.config.secondary.threshold
        if requests_per_minute >= max_per_minute * threshold:
            # Find oldest request in the window
            oldest_in_window = min(
                (ts for ts in self._request_timestamps if ts > window_start),
                default=now,
            )
            base_sleep_time = max(0.0, window_seconds - (now - oldest_in_window) + 0.5)

            # Apply backoff multiplier if we've hit this repeatedly
            sleep_time = base_sleep_time * self._secondary_backoff_multiplier

            if sleep_time > 0:
                logger.warning(
                    "Secondary rate limit protection: %.1f req/min (limit: %d), "
                    "sleeping %.1fs (backoff: %.1fx)",
                    requests_per_minute,
                    max_per_minute,
                    sleep_time,
                    self._secondary_backoff_multiplier,
                )
                # Increase backoff for next time
                max_backoff = self.config.secondary.max_backoff_multiplier
                self._secondary_backoff_multiplier = min(
                    self._secondary_backoff_multiplier * self.config.secondary.backoff_multiplier,
                    max_backoff,
                )
                await asyncio.sleep(sleep_time)
        else:
            # Reset backoff if we're under the limit
            if self._secondary_backoff_multiplier > 1.0:
                self._secondary_backoff_multiplier = max(
                    1.0, self._secondary_backoff_multiplier * 0.9
                )

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
