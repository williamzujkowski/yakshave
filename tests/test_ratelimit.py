"""Tests for adaptive rate limiter module."""

import time

from gh_year_end.config import RateLimitConfig
from gh_year_end.github.ratelimit import (
    AdaptiveRateLimiter,
    APIType,
    RateLimitSample,
    RateLimitState,
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

        delay = limiter._calculate_adaptive_delay(state)
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

        delay = limiter._calculate_adaptive_delay(state)
        assert delay == 0.0

    def test_calculate_delay_between_20_and_50_percent(self) -> None:
        """Test adaptive delay in 20-50% range (linear scaling)."""
        config = RateLimitConfig(min_sleep_seconds=2.0)
        limiter = AdaptiveRateLimiter(config)

        # At 35% (midpoint between 20 and 50), factor should be 0.5
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=1750,  # 35%
        )

        delay = limiter._calculate_adaptive_delay(state)
        # factor = (50 - 35) / 30 = 0.5
        # delay = 2.0 * 0.5 = 1.0
        assert 0.9 <= delay <= 1.1

    def test_calculate_delay_at_20_percent(self) -> None:
        """Test adaptive delay at exactly 20%."""
        config = RateLimitConfig(min_sleep_seconds=1.0)
        limiter = AdaptiveRateLimiter(config)
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=1000,  # Exactly 20%
        )

        delay = limiter._calculate_adaptive_delay(state)
        # At 20%, factor is 1.0, so delay = min_sleep_seconds
        assert delay == 1.0

    def test_calculate_delay_below_20_percent(self) -> None:
        """Test adaptive delay below 20% (exponential scaling)."""
        config = RateLimitConfig(min_sleep_seconds=1.0, max_sleep_seconds=10.0)
        limiter = AdaptiveRateLimiter(config)
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=500,  # 10%
        )

        delay = limiter._calculate_adaptive_delay(state)
        # factor = (20 - 10) / 20 = 0.5
        # delay_range = 10.0 - 1.0 = 9.0
        # delay = 1.0 + (9.0 * 0.5^2) = 1.0 + 2.25 = 3.25
        assert 3.0 <= delay <= 3.5

    def test_calculate_delay_at_5_percent(self) -> None:
        """Test adaptive delay at very low percentage."""
        config = RateLimitConfig(min_sleep_seconds=1.0, max_sleep_seconds=10.0)
        limiter = AdaptiveRateLimiter(config)
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=250,  # 5%
        )

        delay = limiter._calculate_adaptive_delay(state)
        # factor = (20 - 5) / 20 = 0.75
        # delay = 1.0 + (9.0 * 0.75^2) = 1.0 + 5.0625 = 6.0625
        assert 5.5 <= delay <= 6.5

    def test_calculate_delay_at_zero_remaining(self) -> None:
        """Test adaptive delay when exhausted."""
        config = RateLimitConfig(max_sleep_seconds=60.0)
        limiter = AdaptiveRateLimiter(config)
        state = RateLimitState(
            api_type=APIType.REST,
            limit=5000,
            remaining=0,
        )

        delay = limiter._calculate_adaptive_delay(state)
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
