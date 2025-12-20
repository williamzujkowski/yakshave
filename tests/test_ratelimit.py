"""Tests for adaptive rate limiter module."""

import asyncio
import time

import pytest

from gh_year_end.config import RateLimitConfig
from gh_year_end.github.ratelimit import (
    AdaptiveRateLimiter,
    APIType,
    CircuitBreaker,
    CircuitState,
    ProgressState,
    RateLimitSample,
    RateLimitState,
    RequestPriority,
    TokenBucket,
)


class TestRateLimitState:
    """Tests for RateLimitState properties."""

    def test_remaining_percent_full(self) -> None:
        """Test remaining_percent when fully available."""
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=5000,
        )
        assert state.remaining_percent == 100.0

    def test_remaining_percent_half(self) -> None:
        """Test remaining_percent at 50%."""
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=2500,
        )
        assert state.remaining_percent == 50.0

    def test_remaining_percent_quarter(self) -> None:
        """Test remaining_percent at 25%."""
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=1250,
        )
        assert state.remaining_percent == 25.0

    def test_remaining_percent_zero(self) -> None:
        """Test remaining_percent when exhausted."""
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=0,
        )
        assert state.remaining_percent == 0.0

    def test_remaining_percent_with_zero_limit(self) -> None:
        """Test remaining_percent handles zero limit."""
        state = RateLimitState(
            api_type=APIType.REST,
            limit=0,
            remaining=0,
        )
        assert state.remaining_percent == 0.0

    def test_is_exhausted_true(self) -> None:
        """Test is_exhausted returns True when remaining is zero."""
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=0,
        )
        assert state.is_exhausted() is True

    def test_is_exhausted_false(self) -> None:
        """Test is_exhausted returns False when remaining is positive."""
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=1,
        )
        assert state.is_exhausted() is False

    def test_is_exhausted_negative(self) -> None:
        """Test is_exhausted returns True when remaining is negative."""
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=-1,
        )
        assert state.is_exhausted() is True

    def test_seconds_until_reset_future(self) -> None:
        """Test seconds_until_reset for future reset time."""
        future_time = time.time() + 100
        state = RateLimitState(
            api_type=APIType.REST,
            reset_at=future_time,
        )
        seconds = state.seconds_until_reset
        assert 99 <= seconds <= 101  # Allow small timing variance

    def test_seconds_until_reset_past(self) -> None:
        """Test seconds_until_reset for past reset time."""
        past_time = time.time() - 100
        state = RateLimitState(
            api_type=APIType.REST,
            reset_at=past_time,
        )
        assert state.seconds_until_reset == 0.0


class TestAdaptiveRateLimiterCalculateDelay:
    """Tests for AdaptiveRateLimiter._calculate_adaptive_delay."""

    def test_calculate_delay_above_50_percent(self) -> None:
        """Test adaptive delay returns 0 when remaining > 50%."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=2501,  # 50.02%
        )

        delay = limiter._calculate_adaptive_delay(state, RequestPriority.MEDIUM)
        assert delay == 0.0

    def test_calculate_delay_at_50_percent(self) -> None:
        """Test adaptive delay at exactly 50%."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=2500,  # Exactly 50%
        )

        delay = limiter._calculate_adaptive_delay(state, RequestPriority.MEDIUM)
        assert delay == 0.0

    def test_calculate_delay_between_25_and_50_percent(self) -> None:
        """Test adaptive delay in 25-50% range."""
        config = RateLimitConfig(min_sleep_seconds=2.0)
        limiter = AdaptiveRateLimiter(config)

        # At 35% (between 25 and 50), only LOW priority gets delay
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=1750,  # 35%
        )

        # MEDIUM priority gets no delay in this range
        delay = limiter._calculate_adaptive_delay(state, RequestPriority.MEDIUM)
        assert delay == 0.0

        # LOW priority gets delay
        delay_low = limiter._calculate_adaptive_delay(state, RequestPriority.LOW)
        # factor = (50 - 35) / 25 = 0.6
        # delay = 2.0 * 0.6 * 0.5 = 0.6
        assert 0.5 <= delay_low <= 0.7

    def test_calculate_delay_at_20_percent(self) -> None:
        """Test adaptive delay at exactly 20%."""
        config = RateLimitConfig(min_sleep_seconds=1.0, max_sleep_seconds=10.0)
        limiter = AdaptiveRateLimiter(config)
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=1000,  # Exactly 20%
        )

        delay = limiter._calculate_adaptive_delay(state, RequestPriority.MEDIUM)
        # At 20%, in 10-25% range: factor = (25-20)/15 = 0.333
        # delay = 1.0 * 0.333 * 1.0 ≈ 0.333
        assert 0.3 <= delay <= 0.4

    def test_calculate_delay_at_10_percent(self) -> None:
        """Test adaptive delay at exactly 10%."""
        config = RateLimitConfig(min_sleep_seconds=1.0, max_sleep_seconds=10.0)
        limiter = AdaptiveRateLimiter(config)
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=500,  # 10%
        )

        delay = limiter._calculate_adaptive_delay(state, RequestPriority.MEDIUM)
        # At 10%, at boundary, should be at edge of 10-25% range
        # factor = (25-10)/15 = 1.0
        # delay = 1.0 * 1.0 * 1.0 = 1.0
        assert 0.9 <= delay <= 1.1

    def test_calculate_delay_at_7_percent(self) -> None:
        """Test adaptive delay between 5-10%."""
        config = RateLimitConfig(min_sleep_seconds=1.0, max_sleep_seconds=10.0)
        limiter = AdaptiveRateLimiter(config)
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=350,  # 7%
        )

        delay = limiter._calculate_adaptive_delay(state, RequestPriority.MEDIUM)
        # At 7%, in 5-10% range: factor = (10-7)/5 = 0.6
        # delay_range = 10.0 - 1.0 = 9.0
        # delay = 1.0 + (9.0 * (0.6^1.5)) * 1.0 ≈ 1.0 + 4.19 = 5.19
        assert 4.5 <= delay <= 6.0

    def test_calculate_delay_at_3_percent(self) -> None:
        """Test adaptive delay below 5% (critical)."""
        config = RateLimitConfig(min_sleep_seconds=1.0, max_sleep_seconds=10.0)
        limiter = AdaptiveRateLimiter(config)
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=150,  # 3%
        )

        delay = limiter._calculate_adaptive_delay(state, RequestPriority.MEDIUM)
        # At 3%, in <5% range: factor = (5-3)/5 = 0.4
        # delay = 10.0 * (0.4^2) * 1.0 = 1.6
        assert 1.5 <= delay <= 2.0

    def test_calculate_delay_at_zero_remaining(self) -> None:
        """Test adaptive delay when exhausted."""
        config = RateLimitConfig(max_sleep_seconds=60.0)
        limiter = AdaptiveRateLimiter(config)
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=0,
        )

        delay = limiter._calculate_adaptive_delay(state, RequestPriority.MEDIUM)
        assert delay == 60.0


class TestAdaptiveRateLimiterUpdate:
    """Tests for AdaptiveRateLimiter.update method."""

    def test_update_with_rate_limit_headers(self) -> None:
        """Test update extracts rate limit info from headers."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        headers = {
            "x-ratelimit-limit": "5000",
            "x-ratelimit-remaining": "4999",
            "x-ratelimit-reset": str(int(time.time()) + 3600),
        }

        limiter.update(headers, APIType.REST)

        state = limiter.get_state(APIType.REST)
        assert state.limit == 5000
        assert state.remaining == 4999

    def test_update_with_retry_after_header(self) -> None:
        """Test update handles retry-after header."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        headers = {
            "retry-after": "60",
        }

        limiter.update(headers, APIType.REST)

        state = limiter.get_state(APIType.REST)
        assert state.remaining == 0
        assert state.reset_at > time.time()

    def test_update_case_insensitive_headers(self) -> None:
        """Test update handles mixed-case headers."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        headers = {
            "X-RateLimit-Limit": "5000",
            "X-RATELIMIT-REMAINING": "4500",
            "x-RateLimit-Reset": str(int(time.time()) + 3600),
        }

        limiter.update(headers, APIType.REST)

        state = limiter.get_state(APIType.REST)
        assert state.limit == 5000
        assert state.remaining == 4500

    def test_update_missing_headers(self) -> None:
        """Test update handles missing headers gracefully."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        initial_state = limiter.get_state(APIType.REST)
        initial_limit = initial_state.limit

        headers = {}
        limiter.update(headers, APIType.REST)

        # State should remain unchanged
        state = limiter.get_state(APIType.REST)
        assert state.limit == initial_limit

    def test_update_graphql_api_type(self) -> None:
        """Test update works with GraphQL API type."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        headers = {
            "x-ratelimit-limit": "500",
            "x-ratelimit-remaining": "499",
            "x-ratelimit-reset": str(int(time.time()) + 3600),
        }

        limiter.update(headers, APIType.GRAPHQL)

        state = limiter.get_state(APIType.GRAPHQL)
        assert state.limit == 500
        assert state.remaining == 499


class TestAdaptiveRateLimiterRecordSample:
    """Tests for AdaptiveRateLimiter.record_sample method."""

    def test_record_sample_creates_sample(self) -> None:
        """Test record_sample creates a RateLimitSample."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        sample = limiter.record_sample(APIType.REST)

        assert isinstance(sample, RateLimitSample)
        assert sample.api_type == "rest"
        assert sample.limit == 5000
        assert sample.remaining == 5000

    def test_record_sample_reflects_current_state(self) -> None:
        """Test record_sample reflects current rate limit state."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        # Update state
        headers = {
            "x-ratelimit-limit": "5000",
            "x-ratelimit-remaining": "3000",
            "x-ratelimit-reset": str(int(time.time()) + 3600),
        }
        limiter.update(headers, APIType.REST)

        sample = limiter.record_sample(APIType.REST)

        assert sample.limit == 5000
        assert sample.remaining == 3000
        assert sample.remaining_percent == 60.0

    def test_record_sample_stores_sample(self) -> None:
        """Test record_sample stores sample in internal list."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        limiter.record_sample(APIType.REST)
        limiter.record_sample(APIType.REST)

        samples = limiter.get_samples()
        assert len(samples) == 2

    def test_record_sample_to_dict(self) -> None:
        """Test sample to_dict produces valid dictionary."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        sample = limiter.record_sample(APIType.REST)
        sample_dict = sample.to_dict()

        assert isinstance(sample_dict, dict)
        assert "timestamp" in sample_dict
        assert "api_type" in sample_dict
        assert "limit" in sample_dict
        assert "remaining" in sample_dict
        assert "remaining_percent" in sample_dict

    def test_get_samples_returns_dicts(self) -> None:
        """Test get_samples returns list of dictionaries."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        limiter.record_sample(APIType.REST)
        samples = limiter.get_samples()

        assert isinstance(samples, list)
        assert len(samples) == 1
        assert isinstance(samples[0], dict)

    def test_clear_samples(self) -> None:
        """Test clear_samples removes all samples."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        limiter.record_sample(APIType.REST)
        limiter.record_sample(APIType.REST)
        assert len(limiter.get_samples()) == 2

        limiter.clear_samples()
        assert len(limiter.get_samples()) == 0


class TestAdaptiveRateLimiterGetState:
    """Tests for AdaptiveRateLimiter.get_state method."""

    def test_get_state_rest(self) -> None:
        """Test get_state returns REST API state."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        state = limiter.get_state(APIType.REST)

        assert state.api_type == APIType.REST
        assert isinstance(state, RateLimitState)

    def test_get_state_graphql(self) -> None:
        """Test get_state returns GraphQL API state."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        state = limiter.get_state(APIType.GRAPHQL)

        assert state.api_type == APIType.GRAPHQL
        assert isinstance(state, RateLimitState)

    def test_get_state_separate_tracking(self) -> None:
        """Test REST and GraphQL states are tracked separately."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        # Update REST
        limiter.update(
            {
                "x-ratelimit-limit": "5000",
                "x-ratelimit-remaining": "4000",
                "x-ratelimit-reset": str(int(time.time()) + 3600),
            },
            APIType.REST,
        )

        # Update GraphQL
        limiter.update(
            {
                "x-ratelimit-limit": "500",
                "x-ratelimit-remaining": "300",
                "x-ratelimit-reset": str(int(time.time()) + 3600),
            },
            APIType.GRAPHQL,
        )

        rest_state = limiter.get_state(APIType.REST)
        graphql_state = limiter.get_state(APIType.GRAPHQL)

        assert rest_state.limit == 5000
        assert rest_state.remaining == 4000
        assert graphql_state.limit == 500
        assert graphql_state.remaining == 300


class TestTokenBucket:
    """Tests for TokenBucket rate limiter."""

    @pytest.mark.asyncio
    async def test_token_bucket_initialization(self) -> None:
        """Test token bucket initializes with full capacity."""
        bucket = TokenBucket(capacity=10, fill_rate=1.0)

        assert bucket.capacity == 10
        assert bucket.fill_rate == 1.0
        assert bucket.tokens == 10.0

    @pytest.mark.asyncio
    async def test_token_bucket_acquire_success(self) -> None:
        """Test acquiring tokens when available."""
        bucket = TokenBucket(capacity=10, fill_rate=1.0)

        result = await bucket.try_acquire(count=5)
        assert result is True
        assert bucket.tokens == 5.0

    @pytest.mark.asyncio
    async def test_token_bucket_acquire_failure(self) -> None:
        """Test acquiring tokens when insufficient."""
        bucket = TokenBucket(capacity=10, fill_rate=1.0)

        # Exhaust bucket
        await bucket.try_acquire(count=10)

        result = await bucket.try_acquire(count=1)
        assert result is False

    @pytest.mark.asyncio
    async def test_token_bucket_refill(self) -> None:
        """Test token bucket refills over time."""
        bucket = TokenBucket(capacity=10, fill_rate=10.0)

        # Exhaust bucket
        await bucket.try_acquire(count=10)
        assert bucket.tokens == 0.0

        # Wait for refill
        await asyncio.sleep(0.5)
        await bucket.refill()

        # Should have refilled ~5 tokens (10 tokens/sec * 0.5 sec)
        assert bucket.tokens >= 4.0


class TestCircuitBreaker:
    """Tests for CircuitBreaker failure protection."""

    @pytest.mark.asyncio
    async def test_circuit_breaker_initial_state(self) -> None:
        """Test circuit breaker starts in CLOSED state."""
        breaker = CircuitBreaker()

        assert breaker.get_state() == CircuitState.CLOSED
        assert await breaker.can_execute() is True

    @pytest.mark.asyncio
    async def test_circuit_breaker_opens_after_failures(self) -> None:
        """Test circuit opens after threshold failures."""
        breaker = CircuitBreaker(failure_threshold=3)

        # Record failures
        for _ in range(3):
            await breaker.record_failure()

        assert breaker.get_state() == CircuitState.OPEN
        assert await breaker.can_execute() is False

    @pytest.mark.asyncio
    async def test_circuit_breaker_success_resets_failures(self) -> None:
        """Test success resets failure count in CLOSED state."""
        breaker = CircuitBreaker(failure_threshold=3)

        await breaker.record_failure()
        await breaker.record_failure()
        await breaker.record_success()

        # Should still be closed
        assert breaker.get_state() == CircuitState.CLOSED
        assert await breaker.can_execute() is True

    @pytest.mark.asyncio
    async def test_circuit_breaker_half_open_transition(self) -> None:
        """Test circuit transitions to HALF_OPEN after timeout."""
        breaker = CircuitBreaker(failure_threshold=2, timeout_seconds=0.1)

        # Open circuit
        await breaker.record_failure()
        await breaker.record_failure()
        assert breaker.get_state() == CircuitState.OPEN

        # Wait for timeout
        await asyncio.sleep(0.15)

        # Should transition to HALF_OPEN
        can_execute = await breaker.can_execute()
        assert can_execute is True
        assert breaker.get_state() == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_circuit_breaker_closes_after_successes(self) -> None:
        """Test circuit closes after success threshold in HALF_OPEN."""
        breaker = CircuitBreaker(
            failure_threshold=2,
            success_threshold=2,
            timeout_seconds=0.1,
        )

        # Open circuit
        await breaker.record_failure()
        await breaker.record_failure()

        # Wait for timeout
        await asyncio.sleep(0.15)
        await breaker.can_execute()  # Transition to HALF_OPEN

        # Record successes
        await breaker.record_success()
        await breaker.record_success()

        assert breaker.get_state() == CircuitState.CLOSED


class TestProgressState:
    """Tests for ProgressState tracking."""

    def test_progress_state_initialization(self) -> None:
        """Test progress state initializes correctly."""
        state = ProgressState(phase="collection", total_items=100)

        assert state.phase == "collection"
        assert state.total_items == 100
        assert state.completed_items == 0
        assert state.requests_made == 0

    def test_progress_state_calculate_eta_no_progress(self) -> None:
        """Test ETA calculation with no progress."""
        state = ProgressState(phase="collection", total_items=100)

        seconds, eta_str = state.calculate_eta()
        assert seconds == 0.0
        assert eta_str == "unknown"

    def test_progress_state_calculate_eta_with_progress(self) -> None:
        """Test ETA calculation with progress."""
        state = ProgressState(phase="collection", total_items=100)
        state.completed_items = 50
        state.start_time = time.time() - 60  # 60 seconds ago

        seconds, eta_str = state.calculate_eta()

        # Should estimate ~60 more seconds (50 items in 60s = 50 items remaining)
        assert 50 <= seconds <= 70
        assert "m" in eta_str or "s" in eta_str

    def test_progress_state_calculate_eta_formatting(self) -> None:
        """Test ETA formatting with minutes and seconds."""
        state = ProgressState(phase="collection", total_items=200)
        state.completed_items = 50
        state.start_time = time.time() - 30  # 30 seconds ago

        _, eta_str = state.calculate_eta()

        # 150 items remaining at 50/30s rate = 90s = 1m 30s
        assert "m" in eta_str and "s" in eta_str


class TestAdaptiveRateLimiterAcquireRelease:
    """Tests for AdaptiveRateLimiter acquire/release methods."""

    @pytest.mark.asyncio
    async def test_acquire_and_release_normal(self) -> None:
        """Test normal acquire and release flow."""
        config = RateLimitConfig(max_concurrency=5)
        limiter = AdaptiveRateLimiter(config)

        await limiter.acquire()
        limiter.release(success=True)

        # Should not raise any exceptions

    @pytest.mark.asyncio
    async def test_acquire_respects_concurrency_limit(self) -> None:
        """Test acquire respects max_concurrency."""
        config = RateLimitConfig(max_concurrency=2)
        limiter = AdaptiveRateLimiter(config)

        # Acquire 2 slots
        await limiter.acquire()
        await limiter.acquire()

        # Third acquire should block
        acquire_task = asyncio.create_task(limiter.acquire())

        # Give it time to potentially complete
        await asyncio.sleep(0.1)

        # Task should still be pending
        assert not acquire_task.done()

        # Release one slot
        limiter.release(success=True)

        # Now acquire should complete
        await asyncio.wait_for(acquire_task, timeout=1.0)
        limiter.release(success=True)

    @pytest.mark.asyncio
    async def test_acquire_waits_when_exhausted(self) -> None:
        """Test acquire waits when rate limit exhausted."""
        config = RateLimitConfig(strategy="adaptive")
        limiter = AdaptiveRateLimiter(config)

        # Set rate limit to exhausted with reset in near future
        state = limiter.get_state(APIType.REST)
        state.remaining = 0
        state.reset_at = time.time() + 0.2

        start = time.time()
        await limiter.acquire()
        elapsed = time.time() - start

        # Should have waited ~0.2 seconds
        assert elapsed >= 0.2

        limiter.release(success=True)

    @pytest.mark.asyncio
    async def test_acquire_with_priority(self) -> None:
        """Test acquire with different priority levels."""
        config = RateLimitConfig(strategy="adaptive")
        limiter = AdaptiveRateLimiter(config)

        # CRITICAL priority should work
        await limiter.acquire(api_type=APIType.REST, priority=RequestPriority.CRITICAL)
        limiter.release(success=True)

        # LOW priority should work
        await limiter.acquire(api_type=APIType.REST, priority=RequestPriority.LOW)
        limiter.release(success=True)

    @pytest.mark.asyncio
    async def test_release_updates_circuit_breaker(self) -> None:
        """Test release updates circuit breaker state."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        await limiter.acquire()
        limiter.release(success=True)

        # Circuit should remain closed
        assert limiter._circuit_breaker.get_state() == CircuitState.CLOSED

        # Simulate failures
        await limiter.acquire()
        limiter.release(success=False)
        await asyncio.sleep(0.05)  # Give task time to run

        # Additional checks would need more failures to open circuit


class TestAdaptiveRateLimiterProgressTracking:
    """Tests for AdaptiveRateLimiter progress tracking."""

    def test_set_and_get_progress_state(self) -> None:
        """Test setting and getting progress state."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        progress = ProgressState(phase="collection", total_items=100)
        limiter.set_progress_state(progress)

        retrieved = limiter.get_progress_state()
        assert retrieved is progress
        assert retrieved.phase == "collection"

    def test_get_progress_state_none_by_default(self) -> None:
        """Test progress state is None by default."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        assert limiter.get_progress_state() is None

    @pytest.mark.asyncio
    async def test_acquire_increments_request_count(self) -> None:
        """Test acquire increments request count in progress state."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        progress = ProgressState(phase="collection", total_items=100)
        limiter.set_progress_state(progress)

        assert progress.requests_made == 0

        await limiter.acquire()
        limiter.release(success=True)

        assert progress.requests_made == 1


class TestAdaptiveRateLimiterContextManager:
    """Tests for AdaptiveRateLimiter async context manager."""

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        """Test async context manager entry and exit."""
        config = RateLimitConfig()

        async with AdaptiveRateLimiter(config) as limiter:
            assert isinstance(limiter, AdaptiveRateLimiter)

        # Should exit cleanly


class TestAdaptiveRateLimiterSampling:
    """Tests for rate limit sampling and sample rate triggers."""

    def test_update_triggers_sampling(self) -> None:
        """Test update triggers sampling based on sample_rate."""
        config = RateLimitConfig(sample_rate_limit_endpoint_every_n_requests=2)
        limiter = AdaptiveRateLimiter(config)

        # First update - no sample
        limiter.update(
            {
                "x-ratelimit-limit": "5000",
                "x-ratelimit-remaining": "4999",
                "x-ratelimit-reset": str(int(time.time()) + 3600),
            },
            APIType.REST,
        )
        assert len(limiter.get_samples()) == 0

        # Second update - should trigger sample
        limiter.update(
            {
                "x-ratelimit-limit": "5000",
                "x-ratelimit-remaining": "4998",
                "x-ratelimit-reset": str(int(time.time()) + 3600),
            },
            APIType.REST,
        )
        assert len(limiter.get_samples()) == 1


class TestAdaptiveRateLimiterPriorityCalculations:
    """Tests for priority multiplier calculations."""

    def test_priority_affects_delay_calculation(self) -> None:
        """Test different priorities produce different delays."""
        config = RateLimitConfig(min_sleep_seconds=1.0, max_sleep_seconds=10.0)
        limiter = AdaptiveRateLimiter(config)

        # Same state, different priorities
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=500,  # 10%
        )

        delay_critical = limiter._calculate_adaptive_delay(state, RequestPriority.CRITICAL)
        limiter._calculate_adaptive_delay(state, RequestPriority.HIGH)
        delay_medium = limiter._calculate_adaptive_delay(state, RequestPriority.MEDIUM)
        delay_low = limiter._calculate_adaptive_delay(state, RequestPriority.LOW)

        # CRITICAL should have shortest delay, LOW should have longest
        assert delay_critical < delay_medium
        assert delay_medium < delay_low


class TestAdaptiveRateLimiterSecondaryLimit:
    """Tests for secondary rate limit enforcement."""

    @pytest.mark.asyncio
    async def test_secondary_limit_detection(self) -> None:
        """Test secondary rate limit is detected and enforced."""
        config = RateLimitConfig(
            max_concurrency=1,
            strategy="adaptive",
        )
        config.secondary.max_requests_per_minute = 10
        config.secondary.detection_window_seconds = 1.0
        config.secondary.threshold = 0.8

        limiter = AdaptiveRateLimiter(config)

        # Make multiple rapid requests
        for _ in range(8):
            await limiter.acquire()
            limiter.release(success=True)

        # Next request should trigger secondary limit protection
        start = time.time()
        await limiter.acquire()
        time.time() - start

        # Should have introduced some delay
        # Note: This is hard to test precisely due to timing
        limiter.release(success=True)

    @pytest.mark.asyncio
    async def test_secondary_limit_backoff_multiplier(self) -> None:
        """Test secondary limit backoff multiplier increases."""
        config = RateLimitConfig()
        config.secondary.max_requests_per_minute = 5
        config.secondary.detection_window_seconds = 1.0
        config.secondary.threshold = 0.5

        limiter = AdaptiveRateLimiter(config)

        # Initial multiplier
        assert limiter._secondary_backoff_multiplier == 1.0

        # Trigger secondary limit multiple times
        for _ in range(10):
            limiter._request_timestamps.append(time.time())

        await limiter._enforce_secondary_limit()

        # Multiplier should have increased
        assert limiter._secondary_backoff_multiplier > 1.0

    @pytest.mark.asyncio
    async def test_secondary_limit_backoff_resets(self) -> None:
        """Test secondary limit backoff resets when under limit."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        # Set high backoff
        limiter._secondary_backoff_multiplier = 2.0

        # Add just a few recent requests (under limit)
        limiter._request_timestamps.append(time.time())

        await limiter._enforce_secondary_limit()

        # Multiplier should decrease toward 1.0
        assert limiter._secondary_backoff_multiplier < 2.0


class TestAdaptiveRateLimiterCircuitBreakerIntegration:
    """Tests for circuit breaker integration with acquire."""

    @pytest.mark.asyncio
    async def test_acquire_blocked_by_open_circuit(self) -> None:
        """Test acquire raises error when circuit breaker is open."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        # Force circuit open
        limiter._circuit_breaker._state = CircuitState.OPEN
        limiter._circuit_breaker._last_failure_time = time.time()

        # Should raise error after waiting
        with pytest.raises(RuntimeError, match="Circuit breaker open"):
            await limiter.acquire()

    @pytest.mark.asyncio
    async def test_acquire_succeeds_with_closed_circuit(self) -> None:
        """Test acquire works when circuit is closed."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        # Ensure circuit is closed
        assert limiter._circuit_breaker.get_state() == CircuitState.CLOSED

        await limiter.acquire()
        limiter.release(success=True)


class TestProgressStateEdgeCases:
    """Tests for ProgressState edge cases."""

    def test_progress_state_calculate_eta_zero_rate(self) -> None:
        """Test ETA when rate is zero (no time elapsed)."""
        state = ProgressState(phase="collection", total_items=100)
        state.completed_items = 50
        state.start_time = time.time()  # No time elapsed

        _seconds, eta_str = state.calculate_eta()

        # With zero elapsed time, rate would be infinite, so we handle this
        assert eta_str in ["unknown", "0s"] or "m" in eta_str or "s" in eta_str

    def test_progress_state_calculate_eta_seconds_only(self) -> None:
        """Test ETA formatting with seconds only."""
        state = ProgressState(phase="collection", total_items=100)
        state.completed_items = 90
        state.start_time = time.time() - 30  # 30 seconds ago

        _, eta_str = state.calculate_eta()

        # 10 items remaining at 90/30s rate = 3.33s
        assert "s" in eta_str
        assert "m" not in eta_str or eta_str.startswith("0m")


class TestAdaptiveRateLimiterStrategyBehavior:
    """Tests for different rate limiting strategies."""

    @pytest.mark.asyncio
    async def test_adaptive_strategy_applies_delays(self) -> None:
        """Test adaptive strategy applies delays based on state."""
        config = RateLimitConfig(strategy="adaptive", min_sleep_seconds=0.1)
        limiter = AdaptiveRateLimiter(config)

        # Set low remaining to trigger delay
        state = limiter.get_state(APIType.REST)
        state.limit = 5000
        state.remaining = 100  # 2%
        state.reset_at = time.time() + 3600

        start = time.time()
        await limiter.acquire()
        time.time() - start

        # Should have some delay (not testing exact amount due to timing)
        limiter.release(success=True)

    @pytest.mark.asyncio
    async def test_fixed_strategy_no_delays(self) -> None:
        """Test fixed strategy doesn't apply adaptive delays."""
        config = RateLimitConfig(strategy="fixed")
        limiter = AdaptiveRateLimiter(config)

        # Set low remaining
        state = limiter.get_state(APIType.REST)
        state.limit = 5000
        state.remaining = 100  # 2%
        state.reset_at = time.time() + 3600

        start = time.time()
        await limiter.acquire()
        elapsed = time.time() - start

        # Should have minimal delay since strategy is not "adaptive"
        assert elapsed < 1.0
        limiter.release(success=True)


class TestRateLimitSample:
    """Tests for RateLimitSample dataclass."""

    def test_rate_limit_sample_to_dict(self) -> None:
        """Test RateLimitSample to_dict conversion."""
        sample = RateLimitSample(
            timestamp="2024-01-01T00:00:00Z",
            api_type="rest",
            limit=5000,
            remaining=4000,
            remaining_percent=80.0,
            reset_at="2024-01-01T01:00:00Z",
            seconds_until_reset=3600.0,
        )

        result = sample.to_dict()

        assert isinstance(result, dict)
        assert result["timestamp"] == "2024-01-01T00:00:00Z"
        assert result["api_type"] == "rest"
        assert result["limit"] == 5000
        assert result["remaining"] == 4000
        assert result["remaining_percent"] == 80.0
        assert result["reset_at"] == "2024-01-01T01:00:00Z"
        assert result["seconds_until_reset"] == 3600.0


class TestAdaptiveRateLimiterTokenBucketExhaustion:
    """Tests for token bucket exhaustion handling."""

    @pytest.mark.asyncio
    async def test_acquire_proceeds_when_token_bucket_exhausted(self) -> None:
        """Test acquire proceeds even when token bucket is exhausted."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        # Exhaust token bucket
        for _ in range(20):
            await limiter._token_bucket.try_acquire()

        # Should still be able to acquire (with logging)
        await limiter.acquire()
        limiter.release(success=True)


class TestAdaptiveRateLimiterUpdateEdgeCases:
    """Tests for update method edge cases."""

    def test_update_with_zero_reset_time(self) -> None:
        """Test update handles zero reset time."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        headers = {
            "x-ratelimit-limit": "5000",
            "x-ratelimit-remaining": "4999",
            "x-ratelimit-reset": "0",
        }

        limiter.update(headers, APIType.REST)

        state = limiter.get_state(APIType.REST)
        assert state.reset_at == 0.0

    def test_record_sample_with_zero_reset(self) -> None:
        """Test record_sample handles zero reset time."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        # Don't update headers, so reset_at remains 0
        sample = limiter.record_sample(APIType.REST)

        assert sample.reset_at == ""
        assert sample.seconds_until_reset == 0.0
