"""Tests for report contributors module."""

import pytest

from gh_year_end.report.contributors import (
    get_engineers_list,
    populate_activity_timelines,
)


class TestGetEngineersList:
    """Tests for get_engineers_list function."""

    def test_get_engineers_from_nested_format(self):
        """Test extracting engineers from nested leaderboard format."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": {
                    "org": [
                        {
                            "user_id": "alice",
                            "login": "alice",
                            "avatar_url": "https://example.com/alice.jpg",
                            "value": 42,
                        }
                    ]
                }
            }
        }

        result = get_engineers_list(leaderboards_data)

        assert len(result) == 1
        assert result[0]["user_id"] == "alice"
        assert result[0]["login"] == "alice"
        assert result[0]["avatar_url"] == "https://example.com/alice.jpg"
        assert result[0]["prs_merged"] == 42
        assert result[0]["rank"] == 1

    def test_get_engineers_from_flat_format(self):
        """Test extracting engineers from flat leaderboard format."""
        leaderboards_data = {
            "prs_opened": [
                {"user": "bob", "login": "bob", "avatar_url": "https://example.com/bob.jpg", "count": 30}
            ]
        }

        result = get_engineers_list(leaderboards_data)

        assert len(result) == 1
        assert result[0]["user_id"] == "bob"
        assert result[0]["login"] == "bob"
        assert result[0]["prs_opened"] == 30

    def test_merge_multiple_metrics(self):
        """Test merging data from multiple metrics."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": [
                    {"user": "alice", "value": 42, "avatar_url": "https://example.com/alice.jpg"}
                ],
                "reviews_submitted": [{"user": "alice", "value": 100}],
                "issues_opened": [{"user": "alice", "value": 15}],
            }
        }

        result = get_engineers_list(leaderboards_data)

        assert len(result) == 1
        engineer = result[0]
        assert engineer["prs_merged"] == 42
        assert engineer["reviews_submitted"] == 100
        assert engineer["issues_opened"] == 15

    def test_merge_multiple_contributors(self):
        """Test merging data from multiple contributors."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": [
                    {"user": "alice", "value": 42},
                    {"user": "bob", "value": 30},
                ],
                "reviews_submitted": [
                    {"user": "bob", "value": 50},
                    {"user": "charlie", "value": 25},
                ],
            }
        }

        result = get_engineers_list(leaderboards_data)

        assert len(result) == 3
        # Check that all users are present
        user_ids = {eng["user_id"] for eng in result}
        assert user_ids == {"alice", "bob", "charlie"}

    def test_calculate_contributions_total(self):
        """Test calculating total contributions for each engineer."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": [{"user": "alice", "value": 10}],
                "prs_opened": [{"user": "alice", "value": 15}],
                "reviews_submitted": [{"user": "alice", "value": 20}],
                "issues_opened": [{"user": "alice", "value": 5}],
                "issues_closed": [{"user": "alice", "value": 8}],
                "comments_total": [{"user": "alice", "value": 12}],
            }
        }

        result = get_engineers_list(leaderboards_data)

        assert result[0]["contributions_total"] == 70  # 10+15+20+5+8+12

    def test_sorted_by_contributions(self):
        """Test that engineers are sorted by total contributions."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": [
                    {"user": "alice", "value": 100},
                    {"user": "bob", "value": 50},
                    {"user": "charlie", "value": 200},
                ]
            }
        }

        result = get_engineers_list(leaderboards_data)

        assert len(result) == 3
        assert result[0]["user_id"] == "charlie"
        assert result[1]["user_id"] == "alice"
        assert result[2]["user_id"] == "bob"

    def test_assign_ranks(self):
        """Test that ranks are assigned based on sorted order."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": [
                    {"user": "alice", "value": 100},
                    {"user": "bob", "value": 200},
                    {"user": "charlie", "value": 50},
                ]
            }
        }

        result = get_engineers_list(leaderboards_data)

        assert result[0]["rank"] == 1
        assert result[0]["user_id"] == "bob"
        assert result[1]["rank"] == 2
        assert result[1]["user_id"] == "alice"
        assert result[2]["rank"] == 3
        assert result[2]["user_id"] == "charlie"

    def test_initialize_all_metrics(self):
        """Test that all metrics are initialized to 0."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": [{"user": "alice", "value": 10}]
            }
        }

        result = get_engineers_list(leaderboards_data)

        engineer = result[0]
        assert engineer["prs_merged"] == 10
        assert engineer["prs_opened"] == 0
        assert engineer["reviews_submitted"] == 0
        assert engineer["approvals"] == 0
        assert engineer["changes_requested"] == 0
        assert engineer["issues_opened"] == 0
        assert engineer["issues_closed"] == 0
        assert engineer["comments_total"] == 0
        assert engineer["review_comments_total"] == 0

    def test_activity_timeline_initialized(self):
        """Test that activity_timeline is initialized as empty list."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": [{"user": "alice", "value": 10}]
            }
        }

        result = get_engineers_list(leaderboards_data)

        assert result[0]["activity_timeline"] == []

    def test_populate_timelines_when_provided(self):
        """Test that activity timelines are populated when timeseries data provided."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": [{"user": "alice", "value": 10}]
            }
        }
        timeseries_data = {
            "weekly": {
                "prs_merged": [
                    {"period": "2025-W01", "user": "alice", "count": 5},
                    {"period": "2025-W02", "user": "alice", "count": 10},
                ]
            }
        }

        result = get_engineers_list(leaderboards_data, timeseries_data)

        assert len(result[0]["activity_timeline"]) == 2
        assert result[0]["activity_timeline"] == [5, 10]

    def test_empty_leaderboards(self):
        """Test handling empty leaderboards data."""
        result = get_engineers_list({})

        assert result == []

    def test_missing_user_id(self):
        """Test skipping entries without user_id or user key."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": [
                    {"user": "alice", "value": 10},
                    {"value": 20},  # Missing user_id
                    {"user": "bob", "value": 30},
                ]
            }
        }

        result = get_engineers_list(leaderboards_data)

        # Should only include alice and bob
        assert len(result) == 2
        user_ids = {eng["user_id"] for eng in result}
        assert user_ids == {"alice", "bob"}

    def test_handle_both_value_and_count_keys(self):
        """Test handling both value and count field names."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": [{"user": "alice", "value": 10}],
                "prs_opened": [{"user": "alice", "count": 15}],
            }
        }

        result = get_engineers_list(leaderboards_data)

        assert result[0]["prs_merged"] == 10
        assert result[0]["prs_opened"] == 15

    def test_keep_latest_avatar_url(self):
        """Test that avatar_url is kept when encountered."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": [
                    {"user": "alice", "value": 10, "avatar_url": "https://example.com/alice.jpg"}
                ],
                "prs_opened": [
                    {"user": "alice", "value": 15}  # No avatar_url
                ],
            }
        }

        result = get_engineers_list(leaderboards_data)

        # Avatar URL should be preserved from first metric
        assert result[0]["avatar_url"] == "https://example.com/alice.jpg"


class TestPopulateActivityTimelines:
    """Tests for populate_activity_timelines function."""

    def test_populate_single_user(self):
        """Test populating activity timeline for a single user."""
        contributors = [
            {
                "user_id": "alice",
                "login": "alice",
                "activity_timeline": [],
            }
        ]
        timeseries_data = {
            "weekly": {
                "prs_merged": [
                    {"period": "2025-W01", "user": "alice", "count": 5},
                    {"period": "2025-W02", "user": "alice", "count": 10},
                ]
            }
        }

        populate_activity_timelines(contributors, timeseries_data)

        assert contributors[0]["activity_timeline"] == [5, 10]

    def test_populate_multiple_users(self):
        """Test populating timelines for multiple users."""
        contributors = [
            {"user_id": "alice", "login": "alice", "activity_timeline": []},
            {"user_id": "bob", "login": "bob", "activity_timeline": []},
        ]
        timeseries_data = {
            "weekly": {
                "prs_merged": [
                    {"period": "2025-W01", "user": "alice", "count": 5},
                    {"period": "2025-W01", "user": "bob", "count": 3},
                ]
            }
        }

        populate_activity_timelines(contributors, timeseries_data)

        assert contributors[0]["activity_timeline"] == [5]
        assert contributors[1]["activity_timeline"] == [3]

    def test_aggregate_multiple_metrics(self):
        """Test aggregating multiple metric types."""
        contributors = [
            {"user_id": "alice", "login": "alice", "activity_timeline": []}
        ]
        timeseries_data = {
            "weekly": {
                "prs_merged": [{"period": "2025-W01", "user": "alice", "count": 5}],
                "prs_opened": [{"period": "2025-W01", "user": "alice", "count": 3}],
                "reviews_submitted": [{"period": "2025-W01", "user": "alice", "count": 2}],
            }
        }

        populate_activity_timelines(contributors, timeseries_data)

        # Should sum all metric types: 5 + 3 + 2 = 10
        assert contributors[0]["activity_timeline"] == [10]

    def test_aggregate_across_weeks(self):
        """Test aggregating activity across multiple weeks."""
        contributors = [
            {"user_id": "alice", "login": "alice", "activity_timeline": []}
        ]
        timeseries_data = {
            "weekly": {
                "prs_merged": [
                    {"period": "2025-W01", "user": "alice", "count": 5},
                    {"period": "2025-W02", "user": "alice", "count": 8},
                ],
                "reviews_submitted": [
                    {"period": "2025-W01", "user": "alice", "count": 2},
                    {"period": "2025-W02", "user": "alice", "count": 4},
                ],
            }
        }

        populate_activity_timelines(contributors, timeseries_data)

        # W01: 5+2=7, W02: 8+4=12
        assert contributors[0]["activity_timeline"] == [7, 12]

    def test_sorted_by_period(self):
        """Test that timeline is sorted chronologically."""
        contributors = [
            {"user_id": "alice", "login": "alice", "activity_timeline": []}
        ]
        timeseries_data = {
            "weekly": {
                "prs_merged": [
                    {"period": "2025-W10", "user": "alice", "count": 10},
                    {"period": "2025-W01", "user": "alice", "count": 5},
                    {"period": "2025-W05", "user": "alice", "count": 8},
                ]
            }
        }

        populate_activity_timelines(contributors, timeseries_data)

        # Should be sorted: W01, W05, W10
        assert contributors[0]["activity_timeline"] == [5, 8, 10]

    def test_user_with_no_activity(self):
        """Test handling user with no activity data."""
        contributors = [
            {"user_id": "alice", "login": "alice", "activity_timeline": []},
            {"user_id": "bob", "login": "bob", "activity_timeline": []},
        ]
        timeseries_data = {
            "weekly": {
                "prs_merged": [
                    {"period": "2025-W01", "user": "alice", "count": 5},
                ]
            }
        }

        populate_activity_timelines(contributors, timeseries_data)

        # Alice should have activity, Bob should have empty timeline
        assert contributors[0]["activity_timeline"] == [5]
        assert contributors[1]["activity_timeline"] == []

    def test_empty_timeseries(self):
        """Test handling empty timeseries data."""
        contributors = [
            {"user_id": "alice", "login": "alice", "activity_timeline": []}
        ]
        timeseries_data = {"weekly": {}}

        populate_activity_timelines(contributors, timeseries_data)

        assert contributors[0]["activity_timeline"] == []

    def test_missing_weekly_key(self):
        """Test handling missing weekly key."""
        contributors = [
            {"user_id": "alice", "login": "alice", "activity_timeline": []}
        ]
        timeseries_data = {}

        populate_activity_timelines(contributors, timeseries_data)

        assert contributors[0]["activity_timeline"] == []

    def test_match_by_login_fallback(self):
        """Test matching users by login when user_id not in timeseries."""
        contributors = [
            {"user_id": "123", "login": "alice", "activity_timeline": []}
        ]
        timeseries_data = {
            "weekly": {
                "prs_merged": [
                    {"period": "2025-W01", "user": "alice", "count": 5},
                ]
            }
        }

        populate_activity_timelines(contributors, timeseries_data)

        # Should match by login
        assert contributors[0]["activity_timeline"] == [5]

    def test_skip_entries_with_missing_fields(self):
        """Test skipping entries with missing required fields."""
        contributors = [
            {"user_id": "alice", "login": "alice", "activity_timeline": []}
        ]
        timeseries_data = {
            "weekly": {
                "prs_merged": [
                    {"period": "2025-W01", "count": 5},  # Missing user
                    {"user": "alice", "count": 10},  # Missing period
                    {"period": "2025-W02", "user": "alice", "count": 15},  # Valid
                ]
            }
        }

        populate_activity_timelines(contributors, timeseries_data)

        # Should only include the valid entry
        assert contributors[0]["activity_timeline"] == [15]

    def test_all_metric_types_aggregated(self):
        """Test that all expected metric types are aggregated."""
        contributors = [
            {"user_id": "alice", "login": "alice", "activity_timeline": []}
        ]
        timeseries_data = {
            "weekly": {
                "prs_opened": [{"period": "2025-W01", "user": "alice", "count": 1}],
                "prs_merged": [{"period": "2025-W01", "user": "alice", "count": 2}],
                "reviews_submitted": [{"period": "2025-W01", "user": "alice", "count": 3}],
                "issues_opened": [{"period": "2025-W01", "user": "alice", "count": 4}],
                "issues_closed": [{"period": "2025-W01", "user": "alice", "count": 5}],
                "comments_total": [{"period": "2025-W01", "user": "alice", "count": 6}],
            }
        }

        populate_activity_timelines(contributors, timeseries_data)

        # Sum: 1+2+3+4+5+6 = 21
        assert contributors[0]["activity_timeline"] == [21]

    def test_in_place_modification(self):
        """Test that contributors list is modified in-place."""
        contributors = [
            {"user_id": "alice", "login": "alice", "activity_timeline": []}
        ]
        original_list = contributors
        timeseries_data = {
            "weekly": {
                "prs_merged": [{"period": "2025-W01", "user": "alice", "count": 5}]
            }
        }

        populate_activity_timelines(contributors, timeseries_data)

        # Should be the same object, modified in-place
        assert contributors is original_list
        assert contributors[0]["activity_timeline"] == [5]
