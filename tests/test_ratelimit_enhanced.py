"""Comprehensive tests for enhanced rate limiting system."""

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from gh_year_end.config import BurstConfig, RateLimitConfig, SecondaryLimitConfig
from gh_year_end.github.ratelimit import (
    AdaptiveRateLimiter,
    APIType,
    CircuitBreaker,
    CircuitState,
    ProgressState,
    RequestPriority,
    TokenBucket,
)


class TestTokenBucket:
    """Tests for TokenBucket burst control."""

    @pytest.mark.asyncio
    async def test_initial_state_full_capacity(self) -> None:
        """Test token bucket starts at full capacity."""
        bucket = TokenBucket(capacity=30, fill_rate=10.0)
        assert bucket.tokens == 30.0
        assert bucket.capacity == 30
        assert bucket.fill_rate == 10.0

    @pytest.mark.asyncio
    async def test_acquire_reduces_tokens(self) -> None:
        """Test acquiring tokens reduces available count."""
        bucket = TokenBucket(capacity=30, fill_rate=10.0)

        result = await bucket.try_acquire(5)
        assert result is True
        assert bucket.tokens == 25.0

    @pytest.mark.asyncio
    async def test_refill_adds_tokens_over_time(self) -> None:
        """Test tokens refill based on elapsed time and fill rate."""
        bucket = TokenBucket(capacity=30, fill_rate=10.0)

        # Acquire some tokens
        await bucket.try_acquire(20)
        assert bucket.tokens == 10.0

        # Mock time passage (1 second = 10 tokens at rate 10.0)
        with patch("gh_year_end.github.ratelimit.time") as mock_time_module:
            initial_time = 1000.0
            mock_time_module.time.return_value = initial_time
            bucket._last_refill = initial_time

            # Advance time by 1 second
            mock_time_module.time.return_value = initial_time + 1.0
            await bucket.refill()

            # Should have gained 10 tokens (1 second * 10.0 fill_rate)
            assert bucket.tokens == 20.0

    @pytest.mark.asyncio
    async def test_cannot_acquire_more_than_available(self) -> None:
        """Test cannot acquire more tokens than available."""
        bucket = TokenBucket(capacity=30, fill_rate=10.0)

        # Try to acquire more than capacity
        result = await bucket.try_acquire(50)
        assert result is False
        assert bucket.tokens == 30.0  # Tokens unchanged

    @pytest.mark.asyncio
    async def test_rate_limiting_after_burst_exhausted(self) -> None:
        """Test rate limiting behavior after burst is exhausted."""
        bucket = TokenBucket(capacity=10, fill_rate=5.0)

        # Exhaust the bucket
        result1 = await bucket.try_acquire(10)
        assert result1 is True
        assert bucket.tokens < 0.01  # Nearly zero (may have tiny refill)

        # Try to acquire immediately
        result2 = await bucket.try_acquire(1)
        assert result2 is False

    @pytest.mark.asyncio
    async def test_recovery_over_time(self) -> None:
        """Test bucket recovers tokens over time."""
        bucket = TokenBucket(capacity=20, fill_rate=10.0)

        # Exhaust most tokens
        await bucket.try_acquire(18)
        assert bucket.tokens == 2.0

        # Mock time passage for recovery
        with patch("gh_year_end.github.ratelimit.time") as mock_time_module:
            initial_time = 1000.0
            mock_time_module.time.return_value = initial_time
            bucket._last_refill = initial_time

            # Advance time by 0.5 seconds (should gain 5 tokens)
            mock_time_module.time.return_value = initial_time + 0.5
            await bucket.refill()

            assert bucket.tokens == 7.0

            # Advance another 1.5 seconds (should gain 15 tokens, capped at capacity)
            mock_time_module.time.return_value = initial_time + 2.0
            await bucket.refill()

            assert bucket.tokens == 20.0  # Capped at capacity

    @pytest.mark.asyncio
    async def test_refill_respects_capacity_limit(self) -> None:
        """Test refill does not exceed capacity."""
        bucket = TokenBucket(capacity=30, fill_rate=10.0)

        # Start with full capacity
        assert bucket.tokens == 30.0

        # Mock time passage - try to overfill
        with patch("gh_year_end.github.ratelimit.time") as mock_time_module:
            initial_time = 1000.0
            mock_time_module.time.return_value = initial_time
            bucket._last_refill = initial_time

            # Advance time by 10 seconds (would add 100 tokens)
            mock_time_module.time.return_value = initial_time + 10.0
            await bucket.refill()

            # Should be capped at capacity
            assert bucket.tokens == 30.0


class TestCircuitBreaker:
    """Tests for CircuitBreaker failure protection."""

    @pytest.mark.asyncio
    async def test_initial_state_is_closed(self) -> None:
        """Test circuit breaker starts in CLOSED state."""
        breaker = CircuitBreaker(
            failure_threshold=5,
            success_threshold=3,
            timeout_seconds=60.0,
        )
        assert breaker.get_state() == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_record_success_in_closed_state(self) -> None:
        """Test recording success in CLOSED state resets failure count."""
        breaker = CircuitBreaker(failure_threshold=5)

        # Record some failures
        breaker._failure_count = 3

        # Record success should reset failures
        await breaker.record_success()
        assert breaker._failure_count == 0
        assert breaker.get_state() == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_record_failure_increments_count(self) -> None:
        """Test recording failure increments failure count."""
        breaker = CircuitBreaker(failure_threshold=5)

        await breaker.record_failure()
        assert breaker._failure_count == 1

        await breaker.record_failure()
        assert breaker._failure_count == 2

    @pytest.mark.asyncio
    async def test_opens_after_failure_threshold_exceeded(self) -> None:
        """Test circuit opens after failure threshold is exceeded."""
        breaker = CircuitBreaker(failure_threshold=3)

        # Record failures up to threshold
        for _ in range(3):
            await breaker.record_failure()

        assert breaker.get_state() == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_open_state_rejects_requests(self) -> None:
        """Test OPEN state rejects requests."""
        breaker = CircuitBreaker(failure_threshold=2, timeout_seconds=60.0)

        # Open the circuit
        await breaker.record_failure()
        await breaker.record_failure()
        assert breaker.get_state() == CircuitState.OPEN

        # Should reject execution
        can_execute = await breaker.can_execute()
        assert can_execute is False

    @pytest.mark.asyncio
    async def test_transitions_to_half_open_after_timeout(self) -> None:
        """Test circuit transitions to HALF_OPEN after timeout."""
        breaker = CircuitBreaker(failure_threshold=2, timeout_seconds=1.0)

        # Open the circuit
        with patch("gh_year_end.github.ratelimit.time") as mock_time_module:
            initial_time = 1000.0
            mock_time_module.time.return_value = initial_time

            await breaker.record_failure()
            await breaker.record_failure()
            assert breaker.get_state() == CircuitState.OPEN

            # Advance time past timeout
            mock_time_module.time.return_value = initial_time + 2.0

            # Check execution should transition to HALF_OPEN
            can_execute = await breaker.can_execute()
            assert can_execute is True
            assert breaker.get_state() == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_to_closed_after_success_threshold(self) -> None:
        """Test HALF_OPEN -> CLOSED after success threshold."""
        breaker = CircuitBreaker(
            failure_threshold=2,
            success_threshold=3,
            timeout_seconds=1.0,
        )

        # Open the circuit
        with patch("gh_year_end.github.ratelimit.time") as mock_time_module:
            initial_time = 1000.0
            mock_time_module.time.return_value = initial_time

            await breaker.record_failure()
            await breaker.record_failure()

            # Transition to HALF_OPEN
            mock_time_module.time.return_value = initial_time + 2.0
            await breaker.can_execute()
            assert breaker.get_state() == CircuitState.HALF_OPEN

            # Record successes to close circuit
            await breaker.record_success()
            assert breaker.get_state() == CircuitState.HALF_OPEN

            await breaker.record_success()
            assert breaker.get_state() == CircuitState.HALF_OPEN

            await breaker.record_success()
            assert breaker.get_state() == CircuitState.CLOSED
            assert breaker._failure_count == 0
            assert breaker._success_count == 0

    @pytest.mark.asyncio
    async def test_half_open_to_open_on_failure(self) -> None:
        """Test HALF_OPEN -> OPEN on failure."""
        breaker = CircuitBreaker(
            failure_threshold=2,
            success_threshold=3,
            timeout_seconds=1.0,
        )

        # Open and transition to HALF_OPEN
        with patch("gh_year_end.github.ratelimit.time") as mock_time_module:
            initial_time = 1000.0
            mock_time_module.time.return_value = initial_time

            await breaker.record_failure()
            await breaker.record_failure()

            mock_time_module.time.return_value = initial_time + 2.0
            await breaker.can_execute()
            assert breaker.get_state() == CircuitState.HALF_OPEN

            # Record a failure in HALF_OPEN
            await breaker.record_failure()

            # Should re-open
            assert breaker.get_state() == CircuitState.OPEN
            assert breaker._success_count == 0


class TestRequestPriority:
    """Tests for RequestPriority ordering."""

    def test_priority_ordering(self) -> None:
        """Test priority ordering (CRITICAL < HIGH < MEDIUM < LOW)."""
        assert RequestPriority.CRITICAL < RequestPriority.HIGH
        assert RequestPriority.HIGH < RequestPriority.MEDIUM
        assert RequestPriority.MEDIUM < RequestPriority.LOW

    def test_priority_values(self) -> None:
        """Test priority numeric values."""
        assert RequestPriority.CRITICAL.value == 0
        assert RequestPriority.HIGH.value == 1
        assert RequestPriority.MEDIUM.value == 2
        assert RequestPriority.LOW.value == 3

    def test_priority_affects_delay_calculation(self) -> None:
        """Test priority affects delay calculation."""
        config = RateLimitConfig(min_sleep_seconds=1.0)
        limiter = AdaptiveRateLimiter(config)

        # Create state at 35% (should have moderate delay)
        from gh_year_end.github.ratelimit import RateLimitState

        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=1750,  # 35%
        )

        # Calculate delays for different priorities
        delay_critical = limiter._calculate_adaptive_delay(state, RequestPriority.CRITICAL)
        delay_high = limiter._calculate_adaptive_delay(state, RequestPriority.HIGH)
        delay_medium = limiter._calculate_adaptive_delay(state, RequestPriority.MEDIUM)
        delay_low = limiter._calculate_adaptive_delay(state, RequestPriority.LOW)

        # Lower priority should have higher delay
        assert delay_critical <= delay_high <= delay_medium <= delay_low


class TestProgressState:
    """Tests for ProgressState tracking."""

    def test_calculate_eta_with_no_progress(self) -> None:
        """Test calculate_eta with no completed items."""
        progress = ProgressState(
            phase="test",
            total_items=100,
            completed_items=0,
        )

        seconds, eta_str = progress.calculate_eta()
        assert seconds == 0.0
        assert eta_str == "unknown"

    def test_calculate_eta_with_partial_progress(self) -> None:
        """Test calculate_eta with partial completion."""
        with patch("time.time") as mock_time:
            initial_time = 1000.0
            mock_time.return_value = initial_time

            progress = ProgressState(
                phase="test",
                total_items=100,
                completed_items=0,
                start_time=initial_time,
            )

            # Simulate progress after 10 seconds (completed 20 items)
            mock_time.return_value = initial_time + 10.0
            progress.completed_items = 20

            seconds, eta_str = progress.calculate_eta()

            # Rate = 20 items / 10 seconds = 2 items/sec
            # Remaining = 80 items
            # ETA = 80 / 2 = 40 seconds
            assert 39.0 <= seconds <= 41.0
            # Format is "40s" not "0m 40s" per implementation
            assert eta_str == "40s"

    def test_calculate_eta_with_zero_rate(self) -> None:
        """Test calculate_eta handles zero elapsed time."""
        with patch("time.time") as mock_time:
            initial_time = 1000.0
            mock_time.return_value = initial_time

            progress = ProgressState(
                phase="test",
                total_items=100,
                completed_items=10,
                start_time=initial_time,
            )

            # Time hasn't advanced yet - will cause ZeroDivisionError in current implementation
            # This test documents the current behavior (could be improved in future)
            # For now, we expect it to raise
            with pytest.raises(ZeroDivisionError):
                progress.calculate_eta()

    def test_format_duration_seconds(self) -> None:
        """Test format_duration for seconds only."""
        progress = ProgressState(
            phase="test",
            total_items=100,
            completed_items=50,
            start_time=time.time() - 25.0,
        )

        _, eta_str = progress.calculate_eta()
        # Should be around 25 seconds
        assert "s" in eta_str

    def test_format_duration_minutes(self) -> None:
        """Test format_duration for minutes and seconds."""
        with patch("time.time") as mock_time:
            initial_time = 1000.0
            mock_time.return_value = initial_time

            progress = ProgressState(
                phase="test",
                total_items=1000,
                completed_items=0,
                start_time=initial_time,
            )

            # Simulate slow progress (10 items in 100 seconds)
            mock_time.return_value = initial_time + 100.0
            progress.completed_items = 10

            _, eta_str = progress.calculate_eta()

            # Rate = 10 / 100 = 0.1 items/sec
            # Remaining = 990 items
            # ETA = 990 / 0.1 = 9900 seconds = 165 minutes
            assert "m" in eta_str

    def test_format_duration_hours(self) -> None:
        """Test format_duration handles hours correctly."""
        with patch("time.time") as mock_time:
            initial_time = 1000.0
            mock_time.return_value = initial_time

            progress = ProgressState(
                phase="test",
                total_items=10000,
                completed_items=0,
                start_time=initial_time,
            )

            # Simulate very slow progress (1 item in 100 seconds)
            mock_time.return_value = initial_time + 100.0
            progress.completed_items = 1

            seconds, _ = progress.calculate_eta()

            # Should be many hours
            hours = seconds / 3600
            assert hours > 200  # Very long ETA


class TestAdaptiveRateLimiterEnhanced:
    """Enhanced tests for AdaptiveRateLimiter integration."""

    @pytest.mark.asyncio
    async def test_token_bucket_integration(self) -> None:
        """Test token bucket is integrated into rate limiter."""
        config = RateLimitConfig(
            burst=BurstConfig(capacity=10, sustained_rate=5.0),
        )
        limiter = AdaptiveRateLimiter(config)

        # Token bucket should be initialized
        assert limiter._token_bucket.capacity == 10
        assert limiter._token_bucket.fill_rate == 5.0

    @pytest.mark.asyncio
    async def test_circuit_breaker_integration(self) -> None:
        """Test circuit breaker is integrated into rate limiter."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        # Circuit breaker should be initialized
        assert limiter._circuit_breaker.get_state() == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_priority_based_delays(self) -> None:
        """Test priority affects actual delays."""
        config = RateLimitConfig(
            strategy="adaptive",
            min_sleep_seconds=0.1,
        )
        limiter = AdaptiveRateLimiter(config)

        # Set state to 35% (moderate usage)
        headers = {
            "x-ratelimit-limit": "5000",
            "x-ratelimit-remaining": "1750",
            "x-ratelimit-reset": str(int(time.time()) + 3600),
        }
        limiter.update(headers, APIType.REST)

        # Mock sleep to track delays
        sleep_calls = []

        async def mock_sleep(duration: float) -> None:
            sleep_calls.append(duration)

        with patch("asyncio.sleep", side_effect=mock_sleep):
            # High priority request
            await limiter.acquire(api_type=APIType.REST, priority=RequestPriority.HIGH)
            limiter.release()
            high_delay = sum(sleep_calls)
            sleep_calls.clear()

            # Low priority request
            await limiter.acquire(api_type=APIType.REST, priority=RequestPriority.LOW)
            limiter.release()
            low_delay = sum(sleep_calls)

            # Low priority should have higher delay
            assert low_delay >= high_delay

    @pytest.mark.asyncio
    async def test_progress_state_tracking(self) -> None:
        """Test progress state is tracked correctly."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        # Set progress state
        progress = ProgressState(
            phase="testing",
            total_items=100,
            completed_items=0,
        )
        limiter.set_progress_state(progress)

        assert limiter.get_progress_state() == progress

        # Acquire should increment request count
        await limiter.acquire()
        limiter.release()

        assert progress.requests_made == 1

    @pytest.mark.asyncio
    async def test_enhanced_adaptive_delay_calculation(self) -> None:
        """Test enhanced adaptive delay calculation with all factors."""
        config = RateLimitConfig(
            strategy="adaptive",
            min_sleep_seconds=1.0,
            max_sleep_seconds=10.0,
        )
        limiter = AdaptiveRateLimiter(config)

        from gh_year_end.github.ratelimit import RateLimitState

        # Test different remaining percentages
        # Calculations:
        # - 90%: >75% => 0.0
        # - 60% LOW: (75-60)/25 * 1.0 * 0.5 = 0.3
        # - 35% MEDIUM: (50-35)/25 * 1.0 * 1.0 = 0.6
        # - 15% MEDIUM: factor=(25-15)/15=0.667, delay=1.0+(9.0*0.667^1.5)*1.0 = 1.0+4.89 = 5.89
        # - 5% MEDIUM: factor=(10-5)/10=0.5, delay=10.0*(0.5^2)*1.0 = 2.5
        test_cases = [
            (90, RequestPriority.MEDIUM, 0.0),  # >75%: no delay
            (60, RequestPriority.LOW, (0.0, 1.0)),  # >50%: minimal for LOW only
            (35, RequestPriority.MEDIUM, (0.0, 1.0)),  # 25-50%: moderate
            (15, RequestPriority.MEDIUM, (1.0, 10.0)),  # 10-25%: significant
            (5, RequestPriority.MEDIUM, (2.0, 3.0)),  # <10%: critical (2.5)
        ]

        for remaining_pct, priority, expected in test_cases:
            remaining = int(5000 * remaining_pct / 100)
            state = RateLimitState(
                api_type=APIType.REST,
                limit=5000,
                remaining=remaining,
            )

            delay = limiter._calculate_adaptive_delay(state, priority)

            if isinstance(expected, tuple):
                assert expected[0] <= delay <= expected[1], (
                    f"Delay {delay} not in range {expected} for {remaining_pct}%"
                )
            else:
                assert delay == expected, f"Expected {expected}, got {delay} for {remaining_pct}%"

    @pytest.mark.asyncio
    async def test_circuit_breaker_blocks_requests(self) -> None:
        """Test circuit breaker blocks requests when open."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        # Open the circuit
        for _ in range(5):
            await limiter._circuit_breaker.record_failure()

        assert limiter._circuit_breaker.get_state() == CircuitState.OPEN

        # Acquire should raise after timeout
        with pytest.raises(RuntimeError, match="Circuit breaker open"):
            await limiter.acquire()

    @pytest.mark.asyncio
    async def test_release_updates_circuit_breaker_on_success(self) -> None:
        """Test release updates circuit breaker on success."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        # Mock the circuit breaker
        limiter._circuit_breaker.record_success = AsyncMock()

        # Release with success
        limiter.release(success=True)

        # Wait for async task to complete
        await asyncio.sleep(0.1)

        # Should have recorded success
        limiter._circuit_breaker.record_success.assert_called_once()

    @pytest.mark.asyncio
    async def test_release_updates_circuit_breaker_on_failure(self) -> None:
        """Test release updates circuit breaker on failure."""
        config = RateLimitConfig()
        limiter = AdaptiveRateLimiter(config)

        # Mock the circuit breaker
        limiter._circuit_breaker.record_failure = AsyncMock()

        # Release with failure
        limiter.release(success=False)

        # Wait for async task to complete
        await asyncio.sleep(0.1)

        # Should have recorded failure
        limiter._circuit_breaker.record_failure.assert_called_once()

    @pytest.mark.asyncio
    async def test_token_bucket_exhaustion_handling(self) -> None:
        """Test handling of token bucket exhaustion."""
        config = RateLimitConfig(
            burst=BurstConfig(capacity=5, sustained_rate=1.0),
        )
        limiter = AdaptiveRateLimiter(config)

        # Exhaust token bucket
        for _ in range(5):
            result = await limiter._token_bucket.try_acquire()
            assert result is True

        # Next acquire should handle exhaustion gracefully
        result = await limiter._token_bucket.try_acquire()
        assert result is False


class TestConfigModels:
    """Tests for configuration models."""

    def test_burst_config_defaults(self) -> None:
        """Test BurstConfig has correct defaults."""
        config = BurstConfig()
        assert config.capacity == 30
        assert config.sustained_rate == 10.0
        assert config.recovery_rate == 2.0

    def test_burst_config_validation(self) -> None:
        """Test BurstConfig validates constraints."""
        # Valid config
        config = BurstConfig(capacity=50, sustained_rate=20.0)
        assert config.capacity == 50
        assert config.sustained_rate == 20.0

        # Invalid capacity
        with pytest.raises(ValidationError):
            BurstConfig(capacity=0)

        # Invalid sustained_rate
        with pytest.raises(ValidationError):
            BurstConfig(sustained_rate=0.0)

    def test_secondary_limit_config_defaults(self) -> None:
        """Test SecondaryLimitConfig has correct defaults."""
        config = SecondaryLimitConfig()
        assert config.max_requests_per_minute == 90
        assert config.detection_window_seconds == 60
        assert config.backoff_multiplier == 1.5

    def test_secondary_limit_config_validation(self) -> None:
        """Test SecondaryLimitConfig validates constraints."""
        # Valid config
        config = SecondaryLimitConfig(
            max_requests_per_minute=100,
            detection_window_seconds=30,
            backoff_multiplier=2.0,
        )
        assert config.max_requests_per_minute == 100
        assert config.detection_window_seconds == 30
        assert config.backoff_multiplier == 2.0

        # Invalid max_requests_per_minute
        with pytest.raises(ValidationError):
            SecondaryLimitConfig(max_requests_per_minute=0)

        # Invalid backoff_multiplier
        with pytest.raises(ValidationError):
            SecondaryLimitConfig(backoff_multiplier=0.5)

    def test_ratelimit_config_integration(self) -> None:
        """Test RateLimitConfig integrates burst and secondary configs."""
        config = RateLimitConfig(
            strategy="adaptive",
            max_concurrency=8,
            burst=BurstConfig(capacity=20, sustained_rate=5.0),
            secondary=SecondaryLimitConfig(max_requests_per_minute=80),
        )

        assert config.strategy == "adaptive"
        assert config.max_concurrency == 8
        assert config.burst.capacity == 20
        assert config.burst.sustained_rate == 5.0
        assert config.secondary.max_requests_per_minute == 80

    def test_ratelimit_config_with_defaults(self) -> None:
        """Test RateLimitConfig uses default nested configs."""
        config = RateLimitConfig()

        # Should have default burst config
        assert config.burst.capacity == 30
        assert config.burst.sustained_rate == 10.0

        # Should have default secondary config
        assert config.secondary.max_requests_per_minute == 90
        assert config.secondary.detection_window_seconds == 60
