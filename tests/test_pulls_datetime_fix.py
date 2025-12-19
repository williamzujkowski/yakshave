"""Test datetime comparison fix in pulls collector.

This test verifies that timezone-aware datetimes are properly compared
without the "can't compare offset-naive and offset-aware datetimes" error.
"""

from datetime import UTC, datetime

from gh_year_end.collect.pulls import _all_prs_before_date, _filter_prs_by_date


def test_filter_prs_by_date_timezone_aware():
    """Test PR filtering with timezone-aware datetimes."""
    # Create timezone-aware config datetimes (as parsed from YAML)
    since = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    until = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)

    # Sample PRs with GitHub API format timestamps
    prs = [
        {
            "number": 1,
            "updated_at": "2025-01-15T10:30:00Z",  # In range
        },
        {
            "number": 2,
            "updated_at": "2024-12-15T10:30:00Z",  # Before range
        },
        {
            "number": 3,
            "updated_at": "2025-06-01T12:00:00Z",  # In range
        },
        {
            "number": 4,
            "updated_at": "2026-01-15T10:30:00Z",  # After range
        },
    ]

    # Filter PRs - should not raise timezone comparison error
    filtered = _filter_prs_by_date(prs, since, until)

    # Verify correct PRs are included
    assert len(filtered) == 2
    assert filtered[0]["number"] == 1
    assert filtered[1]["number"] == 3


def test_all_prs_before_date_timezone_aware():
    """Test early termination check with timezone-aware datetimes."""
    # Create timezone-aware config datetime
    since = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)

    # All PRs before date
    prs_before = [
        {"number": 1, "updated_at": "2024-12-15T10:30:00Z"},
        {"number": 2, "updated_at": "2024-11-01T08:00:00Z"},
    ]

    # Should not raise timezone comparison error
    assert _all_prs_before_date(prs_before, since) is True

    # Some PRs after date
    prs_mixed = [
        {"number": 1, "updated_at": "2025-06-15T10:30:00Z"},
        {"number": 2, "updated_at": "2024-11-01T08:00:00Z"},
    ]

    # Should not raise timezone comparison error
    assert _all_prs_before_date(prs_mixed, since) is False


def test_filter_prs_handles_missing_updated_at():
    """Test that PRs without updated_at are skipped gracefully."""
    since = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    until = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)

    prs = [
        {"number": 1, "updated_at": "2025-01-15T10:30:00Z"},
        {"number": 2},  # Missing updated_at
        {"number": 3, "updated_at": None},  # Null updated_at
    ]

    filtered = _filter_prs_by_date(prs, since, until)

    # Only PR #1 should be included
    assert len(filtered) == 1
    assert filtered[0]["number"] == 1
