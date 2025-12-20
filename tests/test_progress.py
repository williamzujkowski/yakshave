"""Tests for progress tracking and display."""

import time
from collections import deque
from datetime import datetime
from unittest.mock import MagicMock, Mock

from gh_year_end.collect.progress import ProgressStats, ProgressTracker


class TestProgressStats:
    """Tests for ProgressStats dataclass."""

    def test_default_initialization(self) -> None:
        """Test that ProgressStats initializes with correct defaults."""
        stats = ProgressStats()

        assert stats.total_repos == 0
        assert stats.completed_repos == 0
        assert stats.skipped_repos == 0
        assert stats.failed_repos == 0
        assert stats.current_repo == ""
        assert stats.current_phase == ""
        assert stats.phase_start_time == 0.0
        assert stats.collection_start_time == 0.0
        assert stats.total_requests == 0
        assert stats.rate_limit_remaining == 5000
        assert stats.rate_limit_reset is None
        assert stats.rate_limit_wait_seconds == 0.0
        assert isinstance(stats.request_times, deque)
        assert len(stats.request_times) == 0
        assert stats.request_times.maxlen == 100
        assert isinstance(stats.items_collected, dict)
        assert len(stats.items_collected) == 0

    def test_custom_initialization(self) -> None:
        """Test ProgressStats with custom values."""
        now = time.time()
        reset = datetime.now()

        stats = ProgressStats(
            total_repos=10,
            completed_repos=5,
            skipped_repos=2,
            failed_repos=1,
            current_repo="owner/repo",
            current_phase="pulls",
            phase_start_time=now,
            collection_start_time=now - 100,
            total_requests=50,
            rate_limit_remaining=4000,
            rate_limit_reset=reset,
            rate_limit_wait_seconds=10.5,
        )

        assert stats.total_repos == 10
        assert stats.completed_repos == 5
        assert stats.skipped_repos == 2
        assert stats.failed_repos == 1
        assert stats.current_repo == "owner/repo"
        assert stats.current_phase == "pulls"
        assert stats.phase_start_time == now
        assert stats.collection_start_time == now - 100
        assert stats.total_requests == 50
        assert stats.rate_limit_remaining == 4000
        assert stats.rate_limit_reset == reset
        assert stats.rate_limit_wait_seconds == 10.5

    def test_request_times_maxlen(self) -> None:
        """Test that request_times deque respects maxlen of 100."""
        stats = ProgressStats()

        # Add 150 items
        for i in range(150):
            stats.request_times.append(float(i))

        # Should only keep last 100
        assert len(stats.request_times) == 100
        assert stats.request_times[0] == 50.0  # First item should be 50
        assert stats.request_times[-1] == 149.0  # Last item should be 149


class TestProgressTracker:
    """Tests for ProgressTracker class."""

    def test_initialization_defaults(self) -> None:
        """Test ProgressTracker initialization with defaults."""
        tracker = ProgressTracker()

        assert tracker.stats.total_repos == 0
        assert tracker.verbose is False
        assert tracker.quiet is False
        assert tracker.rate_limiter is None
        assert tracker._live is None
        assert tracker._progress is None
        assert tracker._overall_task is None
        assert tracker._phase_task is None
        assert len(tracker._completed_phases) == 0
        assert tracker.console is not None

    def test_initialization_with_params(self) -> None:
        """Test ProgressTracker initialization with custom params."""
        mock_limiter = Mock()
        tracker = ProgressTracker(
            total_repos=10,
            verbose=True,
            quiet=True,
            rate_limiter=mock_limiter,
        )

        assert tracker.stats.total_repos == 10
        assert tracker.verbose is True
        assert tracker.quiet is True
        assert tracker.rate_limiter is mock_limiter

    def test_phases_list(self) -> None:
        """Test that PHASES class variable contains expected phases."""
        expected_phases = [
            "discovery",
            "repo_metadata",
            "pulls",
            "issues",
            "reviews",
            "comments",
            "commits",
            "branch_protection",
            "security_features",
        ]

        assert expected_phases == ProgressTracker.PHASES

    def test_start_quiet_mode(self) -> None:
        """Test that start() does nothing in quiet mode."""
        tracker = ProgressTracker(quiet=True)
        tracker.start()

        assert tracker._live is None
        assert tracker._progress is None
        assert tracker._overall_task is None

    def test_start_normal_mode(self) -> None:
        """Test that start() initializes display in normal mode."""
        tracker = ProgressTracker(total_repos=5)
        tracker.start()

        try:
            assert tracker._progress is not None
            assert tracker._live is not None
            assert tracker._overall_task is not None
        finally:
            tracker.stop()

    def test_stop(self) -> None:
        """Test that stop() cleans up display."""
        tracker = ProgressTracker(total_repos=5)
        tracker.start()

        assert tracker._live is not None
        assert tracker._progress is not None

        tracker.stop()

        assert tracker._live is None
        assert tracker._progress is None

    def test_stop_without_start(self) -> None:
        """Test that stop() is safe to call without start()."""
        tracker = ProgressTracker()
        tracker.stop()  # Should not raise

        assert tracker._live is None
        assert tracker._progress is None

    def test_context_manager(self) -> None:
        """Test ProgressTracker as context manager."""
        tracker = ProgressTracker(total_repos=5)

        with tracker as t:
            assert t is tracker
            assert t._live is not None
            assert t._progress is not None

        # After exiting context
        assert tracker._live is None
        assert tracker._progress is None

    def test_set_phase(self) -> None:
        """Test setting current phase."""
        tracker = ProgressTracker(total_repos=5, quiet=True)
        start_time = time.time()

        tracker.set_phase("pulls")

        assert tracker.stats.current_phase == "pulls"
        assert tracker.stats.phase_start_time >= start_time

    def test_set_phase_updates_display(self) -> None:
        """Test that set_phase updates display when active."""
        tracker = ProgressTracker(total_repos=5)
        tracker.start()

        try:
            tracker.set_phase("issues")

            assert tracker.stats.current_phase == "issues"
            assert tracker._progress is not None
        finally:
            tracker.stop()

    def test_mark_phase_complete(self) -> None:
        """Test marking a phase as complete."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        tracker.mark_phase_complete("pulls")

        assert "pulls" in tracker._completed_phases
        assert len(tracker._completed_phases) == 1

        tracker.mark_phase_complete("issues")

        assert "issues" in tracker._completed_phases
        assert len(tracker._completed_phases) == 2

    def test_mark_phase_complete_idempotent(self) -> None:
        """Test that marking same phase complete multiple times works."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        tracker.mark_phase_complete("pulls")
        tracker.mark_phase_complete("pulls")

        assert "pulls" in tracker._completed_phases
        assert len(tracker._completed_phases) == 1

    def test_set_repo(self) -> None:
        """Test setting current repository."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        tracker.set_repo("owner/repo-name")

        assert tracker.stats.current_repo == "owner/repo-name"

    def test_mark_repo_complete(self) -> None:
        """Test marking repo as complete."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        assert tracker.stats.completed_repos == 0

        tracker.mark_repo_complete()
        assert tracker.stats.completed_repos == 1

        tracker.mark_repo_complete()
        assert tracker.stats.completed_repos == 2

    def test_mark_repo_skipped(self) -> None:
        """Test marking repo as skipped."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        assert tracker.stats.skipped_repos == 0

        tracker.mark_repo_skipped()
        assert tracker.stats.skipped_repos == 1

        tracker.mark_repo_skipped()
        assert tracker.stats.skipped_repos == 2

    def test_mark_repo_failed(self) -> None:
        """Test marking repo as failed."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        assert tracker.stats.failed_repos == 0

        tracker.mark_repo_failed()
        assert tracker.stats.failed_repos == 1

        tracker.mark_repo_failed()
        assert tracker.stats.failed_repos == 2

    def test_record_request(self) -> None:
        """Test recording request durations."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        assert tracker.stats.total_requests == 0
        assert len(tracker.stats.request_times) == 0

        tracker.record_request(0.5)

        assert tracker.stats.total_requests == 1
        assert len(tracker.stats.request_times) == 1
        assert tracker.stats.request_times[0] == 0.5

        tracker.record_request(1.2)

        assert tracker.stats.total_requests == 2
        assert len(tracker.stats.request_times) == 2
        assert tracker.stats.request_times[1] == 1.2

    def test_record_request_respects_maxlen(self) -> None:
        """Test that recording requests respects deque maxlen."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        # Record 150 requests
        for i in range(150):
            tracker.record_request(float(i))

        assert tracker.stats.total_requests == 150
        assert len(tracker.stats.request_times) == 100  # maxlen
        assert tracker.stats.request_times[0] == 50.0
        assert tracker.stats.request_times[-1] == 149.0

    def test_record_rate_limit_wait(self) -> None:
        """Test recording rate limit wait time."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        assert tracker.stats.rate_limit_wait_seconds == 0.0

        tracker.record_rate_limit_wait(10.5)
        assert tracker.stats.rate_limit_wait_seconds == 10.5

        tracker.record_rate_limit_wait(5.2)
        assert tracker.stats.rate_limit_wait_seconds == 15.7

    def test_update_items_collected(self) -> None:
        """Test updating item collection counts."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        assert len(tracker.stats.items_collected) == 0

        tracker.update_items_collected("pulls", 10)

        assert tracker.stats.items_collected["pulls"] == 10

        tracker.update_items_collected("pulls", 5)

        assert tracker.stats.items_collected["pulls"] == 15

        tracker.update_items_collected("issues", 20)

        assert tracker.stats.items_collected["issues"] == 20
        assert tracker.stats.items_collected["pulls"] == 15

    def test_set_total_repos(self) -> None:
        """Test setting total repos count."""
        tracker = ProgressTracker(quiet=True)

        assert tracker.stats.total_repos == 0

        tracker.set_total_repos(25)

        assert tracker.stats.total_repos == 25

    def test_get_summary_minimal(self) -> None:
        """Test get_summary with minimal data."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        summary = tracker.get_summary()

        assert "elapsed_seconds" in summary
        assert summary["elapsed_seconds"] >= 0
        assert summary["total_repos"] == 5
        assert summary["completed_repos"] == 0
        assert summary["skipped_repos"] == 0
        assert summary["failed_repos"] == 0
        assert summary["total_requests"] == 0
        assert summary["rate_limit_wait_seconds"] == 0.0
        assert summary["items_collected"] == {}
        assert summary["avg_request_time"] == 0.0

    def test_get_summary_with_data(self) -> None:
        """Test get_summary with actual data."""
        tracker = ProgressTracker(total_repos=10, quiet=True)

        # Simulate activity
        tracker.mark_repo_complete()
        tracker.mark_repo_complete()
        tracker.mark_repo_skipped()
        tracker.mark_repo_failed()
        tracker.record_request(0.5)
        tracker.record_request(1.5)
        tracker.record_request(1.0)
        tracker.record_rate_limit_wait(10.0)
        tracker.update_items_collected("pulls", 50)
        tracker.update_items_collected("issues", 30)

        summary = tracker.get_summary()

        assert summary["total_repos"] == 10
        assert summary["completed_repos"] == 2
        assert summary["skipped_repos"] == 1
        assert summary["failed_repos"] == 1
        assert summary["total_requests"] == 3
        assert summary["rate_limit_wait_seconds"] == 10.0
        assert summary["items_collected"]["pulls"] == 50
        assert summary["items_collected"]["issues"] == 30
        assert summary["avg_request_time"] == 1.0  # (0.5 + 1.5 + 1.0) / 3

    def test_calculate_eta_no_data(self) -> None:
        """Test ETA calculation with no request data."""
        tracker = ProgressTracker(total_repos=10, quiet=True)

        eta = tracker._calculate_eta()

        assert eta == ""

    def test_calculate_eta_completed(self) -> None:
        """Test ETA when all repos are complete."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        # Mark all repos as complete or skipped
        tracker.mark_repo_complete()
        tracker.mark_repo_complete()
        tracker.mark_repo_complete()
        tracker.mark_repo_skipped()
        tracker.mark_repo_skipped()

        # Add some request data
        tracker.record_request(1.0)

        eta = tracker._calculate_eta()

        assert eta == "Almost done!"

    def test_calculate_eta_seconds(self) -> None:
        """Test ETA calculation returning seconds."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        # Record fast requests
        for _ in range(10):
            tracker.record_request(0.1)  # 0.1s per request

        # Complete 1 repo, 4 remaining
        tracker.mark_repo_complete()

        eta = tracker._calculate_eta()

        # Should show seconds (< 60s)
        assert eta.endswith("s")
        assert "m" not in eta
        assert "h" not in eta

    def test_calculate_eta_minutes(self) -> None:
        """Test ETA calculation returning minutes."""
        tracker = ProgressTracker(total_repos=10, quiet=True)

        # Record requests
        for _ in range(10):
            tracker.record_request(1.0)  # 1s per request

        # Complete 1 repo, 9 remaining
        # With 1s per request and ~10 requests per repo = ~90s = ~1m
        tracker.mark_repo_complete()

        eta = tracker._calculate_eta()

        # Should show minutes or seconds
        assert "m" in eta or "s" in eta

    def test_calculate_eta_hours(self) -> None:
        """Test ETA calculation returning hours."""
        tracker = ProgressTracker(total_repos=100, quiet=True)

        # Record slow requests
        for _ in range(10):
            tracker.record_request(10.0)  # 10s per request

        # Complete 1 repo, 99 remaining
        # With 10s per request and ~10 requests per repo = ~9900s = ~2h 45m
        tracker.mark_repo_complete()

        eta = tracker._calculate_eta()

        # Should show hours and minutes
        assert "h" in eta
        assert "m" in eta

    def test_calculate_eta_with_rate_limit_wait(self) -> None:
        """Test that ETA includes rate limit wait time."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        # Record requests
        for _ in range(10):
            tracker.record_request(1.0)

        # Add significant rate limit wait
        tracker.record_rate_limit_wait(100.0)

        # Complete 1 repo, 4 remaining
        tracker.mark_repo_complete()

        eta = tracker._calculate_eta()

        # ETA should include the wait time (should push it to minutes)
        assert eta != ""
        assert "m" in eta or "h" in eta

    def test_estimate_requests_per_repo_no_completed(self) -> None:
        """Test request estimation with no completed repos."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        estimate = tracker._estimate_requests_per_repo()

        assert estimate == 10.0  # Default estimate

    def test_estimate_requests_per_repo_with_data(self) -> None:
        """Test request estimation with completed repos."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        # Simulate 50 requests for 2 completed repos
        tracker.stats.total_requests = 50
        tracker.mark_repo_complete()
        tracker.mark_repo_complete()

        estimate = tracker._estimate_requests_per_repo()

        assert estimate == 25.0  # 50 requests / 2 repos

    def test_estimate_requests_per_repo_single_repo(self) -> None:
        """Test request estimation with single completed repo."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        tracker.stats.total_requests = 15
        tracker.mark_repo_complete()

        estimate = tracker._estimate_requests_per_repo()

        assert estimate == 15.0  # 15 requests / 1 repo

    def test_get_rate_limit_status_no_limiter(self) -> None:
        """Test rate limit status with no limiter."""
        tracker = ProgressTracker(total_repos=5, quiet=True)

        status = tracker._get_rate_limit_status()

        assert status == ""

    def test_get_rate_limit_status_with_limiter(self) -> None:
        """Test rate limit status with active limiter."""
        mock_state = MagicMock()
        mock_state.remaining = 3000
        mock_state.limit = 5000
        mock_state.reset_at = time.time() + 3600  # 1 hour from now

        mock_limiter = MagicMock()
        mock_limiter.get_state.return_value = mock_state

        tracker = ProgressTracker(
            total_repos=5,
            quiet=True,
            rate_limiter=mock_limiter,
        )

        status = tracker._get_rate_limit_status()

        assert "Rate Limit:" in status
        assert "3000" in status
        assert "5000" in status
        assert "resets" in status

    def test_get_rate_limit_status_colors(self) -> None:
        """Test rate limit status color coding."""
        # Test green (high remaining)
        mock_state = MagicMock()
        mock_state.remaining = 3000
        mock_state.limit = 5000
        mock_state.reset_at = time.time() + 3600

        mock_limiter = MagicMock()
        mock_limiter.get_state.return_value = mock_state

        tracker = ProgressTracker(quiet=True, rate_limiter=mock_limiter)
        status = tracker._get_rate_limit_status()
        assert "[green]" in status

        # Test yellow (medium remaining)
        mock_state.remaining = 300
        status = tracker._get_rate_limit_status()
        assert "[yellow]" in status

        # Test red (low remaining)
        mock_state.remaining = 50
        status = tracker._get_rate_limit_status()
        assert "[red]" in status

    def test_get_rate_limit_status_unknown_reset(self) -> None:
        """Test rate limit status with unknown reset time."""
        mock_state = MagicMock()
        mock_state.remaining = 3000
        mock_state.limit = 5000
        mock_state.reset_at = 0  # Unknown

        mock_limiter = MagicMock()
        mock_limiter.get_state.return_value = mock_state

        tracker = ProgressTracker(quiet=True, rate_limiter=mock_limiter)

        status = tracker._get_rate_limit_status()

        assert "unknown" in status

    def test_update_display_quiet_mode(self) -> None:
        """Test that update_display is safe in quiet mode."""
        tracker = ProgressTracker(quiet=True)

        # Should not raise even though _live is None
        tracker.update_display()

        assert True  # No exception raised

    def test_update_display_active(self) -> None:
        """Test update_display with active display."""
        tracker = ProgressTracker(total_repos=5)
        tracker.start()

        try:
            # Should update without error
            tracker.update_display()
            assert True
        finally:
            tracker.stop()

    def test_create_display_minimal(self) -> None:
        """Test display creation with minimal data."""
        tracker = ProgressTracker(total_repos=5)
        tracker.start()

        try:
            table = tracker._create_display()

            assert table is not None
        finally:
            tracker.stop()

    def test_create_display_with_all_data(self) -> None:
        """Test display creation with all data populated."""
        mock_state = MagicMock()
        mock_state.remaining = 3000
        mock_state.limit = 5000
        mock_state.reset_at = time.time() + 3600

        mock_limiter = MagicMock()
        mock_limiter.get_state.return_value = mock_state

        tracker = ProgressTracker(total_repos=5, rate_limiter=mock_limiter)
        tracker.start()

        try:
            tracker.set_phase("pulls")
            tracker.set_repo("owner/repo")
            tracker.mark_repo_complete()
            tracker.record_request(1.0)
            tracker.update_items_collected("pulls", 10)

            table = tracker._create_display()

            assert table is not None
        finally:
            tracker.stop()

    def test_full_workflow(self) -> None:
        """Test complete workflow with all operations."""
        tracker = ProgressTracker(total_repos=3, quiet=True)

        # Set up tracking
        tracker.set_total_repos(3)

        # Process first repo
        tracker.set_phase("pulls")
        tracker.set_repo("owner/repo1")
        tracker.record_request(0.5)
        tracker.record_request(0.6)
        tracker.update_items_collected("pulls", 5)
        tracker.mark_repo_complete()

        # Process second repo
        tracker.set_repo("owner/repo2")
        tracker.record_request(0.7)
        tracker.update_items_collected("pulls", 3)
        tracker.mark_repo_skipped()

        # Process third repo (fails)
        tracker.set_repo("owner/repo3")
        tracker.record_request(0.8)
        tracker.mark_repo_failed()

        # Complete phase
        tracker.mark_phase_complete("pulls")

        # Verify final state
        summary = tracker.get_summary()
        assert summary["total_repos"] == 3
        assert summary["completed_repos"] == 1
        assert summary["skipped_repos"] == 1
        assert summary["failed_repos"] == 1
        assert summary["total_requests"] == 4
        assert summary["items_collected"]["pulls"] == 8
        assert "pulls" in tracker._completed_phases
