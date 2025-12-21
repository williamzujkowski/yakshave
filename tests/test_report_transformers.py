"""Tests for report transformers module."""

from gh_year_end.report.transformers import (
    calculate_fun_facts,
    calculate_highlights,
    transform_activity_timeline,
    transform_awards_data,
    transform_leaderboards,
)


class TestTransformAwardsData:
    """Tests for transform_awards_data function."""

    def test_transform_single_award(self):
        """Test transforming a single award."""
        awards_data = {
            "top_pr_author": {
                "user": "alice",
                "count": 42,
                "avatar_url": "https://example.com/alice.jpg",
            }
        }

        result = transform_awards_data(awards_data)

        assert "individual" in result
        assert len(result["individual"]) == 1
        award = result["individual"][0]
        assert award["award_key"] == "top_pr_author"
        assert award["title"] == "Top PR Author"
        assert award["description"] == "Most pull requests opened"
        assert award["winner_name"] == "alice"
        assert award["winner_avatar_url"] == "https://example.com/alice.jpg"
        assert award["supporting_stats"] == "42 PRs opened"

    def test_transform_multiple_awards(self):
        """Test transforming multiple awards."""
        awards_data = {
            "top_pr_author": {
                "user": "alice",
                "count": 42,
                "avatar_url": "https://example.com/alice.jpg",
            },
            "top_reviewer": {
                "user": "bob",
                "count": 100,
                "avatar_url": "https://example.com/bob.jpg",
            },
            "top_issue_opener": {
                "user": "charlie",
                "count": 25,
                "avatar_url": "https://example.com/charlie.jpg",
            },
        }

        result = transform_awards_data(awards_data)

        assert len(result["individual"]) == 3
        assert len(result["repository"]) == 0
        assert len(result["risk"]) == 0

        # Verify each award is present
        award_keys = {award["award_key"] for award in result["individual"]}
        assert award_keys == {"top_pr_author", "top_reviewer", "top_issue_opener"}

    def test_transform_unknown_award_ignored(self):
        """Test that unknown award keys are ignored."""
        awards_data = {
            "top_pr_author": {
                "user": "alice",
                "count": 42,
                "avatar_url": "https://example.com/alice.jpg",
            },
            "unknown_award": {
                "user": "bob",
                "count": 10,
                "avatar_url": "https://example.com/bob.jpg",
            },
        }

        result = transform_awards_data(awards_data)

        assert len(result["individual"]) == 1
        assert result["individual"][0]["award_key"] == "top_pr_author"

    def test_transform_empty_awards(self):
        """Test transforming empty awards data."""
        result = transform_awards_data({})

        assert result["individual"] == []
        assert result["repository"] == []
        assert result["risk"] == []

    def test_transform_missing_fields(self):
        """Test handling missing fields in award data."""
        awards_data = {
            "top_pr_author": {
                "user": "alice",
                # Missing count and avatar_url
            }
        }

        result = transform_awards_data(awards_data)

        award = result["individual"][0]
        assert award["winner_name"] == "alice"
        assert award["winner_avatar_url"] == ""
        assert award["supporting_stats"] == "0 PRs opened"


class TestTransformLeaderboards:
    """Tests for transform_leaderboards function."""

    def test_transform_simple_leaderboard(self):
        """Test transforming a simple leaderboard."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": [
                    {"user": "alice", "count": 42, "avatar_url": "https://example.com/alice.jpg"},
                    {"user": "bob", "count": 30, "avatar_url": "https://example.com/bob.jpg"},
                ]
            }
        }

        result = transform_leaderboards(leaderboards_data)

        assert "prs_merged" in result
        assert len(result["prs_merged"]) == 2
        assert result["prs_merged"][0]["login"] == "alice"
        assert result["prs_merged"][0]["value"] == 42
        assert result["prs_merged"][0]["avatar_url"] == "https://example.com/alice.jpg"

    def test_transform_nested_org_format(self):
        """Test transforming leaderboard with nested org structure."""
        leaderboards_data = {
            "leaderboards": {
                "prs_opened": {
                    "org": [
                        {
                            "login": "alice",
                            "value": 50,
                            "avatar_url": "https://example.com/alice.jpg",
                        },
                    ]
                }
            }
        }

        result = transform_leaderboards(leaderboards_data)

        assert len(result["prs_opened"]) == 1
        assert result["prs_opened"][0]["login"] == "alice"
        assert result["prs_opened"][0]["value"] == 50

    def test_transform_flat_format(self):
        """Test transforming flat format (no leaderboards wrapper)."""
        leaderboards_data = {
            "prs_merged": [
                {"user": "alice", "count": 10, "avatar_url": "https://example.com/alice.jpg"},
            ]
        }

        result = transform_leaderboards(leaderboards_data)

        assert len(result["prs_merged"]) == 1
        assert result["prs_merged"][0]["login"] == "alice"
        assert result["prs_merged"][0]["value"] == 10

    def test_transform_overall_leaderboard(self):
        """Test transforming overall leaderboard with extra metrics."""
        leaderboards_data = {
            "leaderboards": {
                "overall": [
                    {
                        "login": "alice",
                        "value": 100,
                        "avatar_url": "https://example.com/alice.jpg",
                        "prs_merged": 42,
                        "reviews_submitted": 30,
                        "issues_closed": 15,
                        "comments_total": 13,
                    }
                ]
            }
        }

        result = transform_leaderboards(leaderboards_data)

        assert len(result["overall"]) == 1
        entry = result["overall"][0]
        assert entry["login"] == "alice"
        assert entry["overall_score"] == 100
        assert entry["prs_merged"] == 42
        assert entry["reviews_submitted"] == 30
        assert entry["issues_closed"] == 15
        assert entry["comments_total"] == 13

    def test_transform_multiple_metrics(self):
        """Test transforming multiple leaderboard metrics."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": [{"user": "alice", "count": 10}],
                "reviews_submitted": [{"user": "bob", "count": 20}],
                "issues_opened": [{"user": "charlie", "count": 5}],
            }
        }

        result = transform_leaderboards(leaderboards_data)

        assert len(result["prs_merged"]) == 1
        assert len(result["reviews_submitted"]) == 1
        assert len(result["issues_opened"]) == 1

    def test_transform_empty_metrics(self):
        """Test transforming with empty metrics."""
        result = transform_leaderboards({})

        # All metric lists should be empty
        assert result["prs_merged"] == []
        assert result["prs_opened"] == []
        assert result["reviews_submitted"] == []

    def test_transform_mixed_field_names(self):
        """Test handling mixed field name formats."""
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": [
                    {"login": "alice", "value": 10},  # login + value
                    {"user": "bob", "count": 20},  # user + count
                ]
            }
        }

        result = transform_leaderboards(leaderboards_data)

        assert len(result["prs_merged"]) == 2
        assert result["prs_merged"][0]["login"] == "alice"
        assert result["prs_merged"][0]["value"] == 10
        assert result["prs_merged"][1]["login"] == "bob"
        assert result["prs_merged"][1]["value"] == 20


class TestTransformActivityTimeline:
    """Tests for transform_activity_timeline function."""

    def test_transform_weekly_prs_merged(self):
        """Test transforming weekly PR merge data."""
        timeseries_data = {
            "weekly": {
                "prs_merged": [
                    {"period": "2025-W01", "user": "alice", "count": 5},
                    {"period": "2025-W01", "user": "bob", "count": 3},
                    {"period": "2025-W02", "user": "alice", "count": 10},
                ]
            }
        }

        result = transform_activity_timeline(timeseries_data)

        assert len(result) == 2
        assert result[0]["date"] == "2024-12-30"  # Monday of week 1, 2025
        assert result[0]["value"] == 8  # 5 + 3
        assert result[1]["date"] == "2025-01-06"  # Monday of week 2, 2025
        assert result[1]["value"] == 10

    def test_transform_single_period(self):
        """Test transforming a single time period."""
        timeseries_data = {
            "weekly": {
                "prs_merged": [
                    {"period": "2025-W10", "user": "alice", "count": 15},
                ]
            }
        }

        result = transform_activity_timeline(timeseries_data)

        assert len(result) == 1
        assert result[0]["date"] == "2025-03-03"
        assert result[0]["value"] == 15

    def test_transform_empty_data(self):
        """Test transforming empty timeseries data."""
        result = transform_activity_timeline({})

        assert result == []

    def test_transform_missing_weekly_key(self):
        """Test handling missing weekly key."""
        timeseries_data = {
            "monthly": {"prs_merged": [{"period": "2025-01", "user": "alice", "count": 100}]}
        }

        result = transform_activity_timeline(timeseries_data)

        assert result == []

    def test_transform_missing_prs_merged(self):
        """Test handling missing prs_merged metric."""
        timeseries_data = {
            "weekly": {"issues_opened": [{"period": "2025-W01", "user": "alice", "count": 5}]}
        }

        result = transform_activity_timeline(timeseries_data)

        assert result == []

    def test_transform_invalid_period_format(self):
        """Test handling invalid period format."""
        timeseries_data = {
            "weekly": {
                "prs_merged": [
                    {"period": "invalid", "user": "alice", "count": 5},
                    {"period": "2025-W02", "user": "bob", "count": 10},
                ]
            }
        }

        result = transform_activity_timeline(timeseries_data)

        # Should skip invalid entry but process valid one
        assert len(result) == 1
        assert result[0]["value"] == 10

    def test_transform_sorted_output(self):
        """Test that output is sorted by date."""
        timeseries_data = {
            "weekly": {
                "prs_merged": [
                    {"period": "2025-W10", "user": "alice", "count": 5},
                    {"period": "2025-W01", "user": "bob", "count": 3},
                    {"period": "2025-W05", "user": "charlie", "count": 8},
                ]
            }
        }

        result = transform_activity_timeline(timeseries_data)

        assert len(result) == 3
        # Verify chronological order
        assert result[0]["date"] < result[1]["date"] < result[2]["date"]


class TestCalculateHighlights:
    """Tests for calculate_highlights function."""

    def test_calculate_most_active_month(self):
        """Test calculating most active month."""
        summary_data = {}
        timeseries_data = {
            "monthly": {
                "prs_merged": [
                    {"period": "2025-01", "user": "alice", "count": 10},
                    {"period": "2025-01", "user": "bob", "count": 5},
                    {"period": "2025-02", "user": "alice", "count": 30},
                ]
            }
        }
        repo_health_list = []

        result = calculate_highlights(summary_data, timeseries_data, repo_health_list)

        assert result["most_active_month"] == "February 2025"
        assert result["most_active_month_prs"] == 30

    def test_calculate_review_coverage(self):
        """Test calculating average review coverage."""
        summary_data = {}
        timeseries_data = {"monthly": {}}
        repo_health_list = [
            {"review_coverage": 80.0},
            {"review_coverage": 90.0},
            {"review_coverage": 70.0},
        ]

        result = calculate_highlights(summary_data, timeseries_data, repo_health_list)

        assert result["review_coverage"] == 80.0

    def test_calculate_avg_review_time_hours(self):
        """Test calculating average review time in hours."""
        summary_data = {}
        timeseries_data = {"monthly": {}}
        repo_health_list = [
            {"median_time_to_first_review": 3600},  # 1 hour
            {"median_time_to_first_review": 7200},  # 2 hours
        ]

        result = calculate_highlights(summary_data, timeseries_data, repo_health_list)

        assert result["avg_review_time"] == "1.5 hours"

    def test_calculate_avg_review_time_minutes(self):
        """Test calculating average review time in minutes."""
        summary_data = {}
        timeseries_data = {"monthly": {}}
        repo_health_list = [
            {"median_time_to_first_review": 1800},  # 30 minutes
        ]

        result = calculate_highlights(summary_data, timeseries_data, repo_health_list)

        assert result["avg_review_time"] == "30 minutes"

    def test_calculate_avg_review_time_days(self):
        """Test calculating average review time in days."""
        summary_data = {}
        timeseries_data = {"monthly": {}}
        repo_health_list = [
            {"median_time_to_first_review": 172800},  # 2 days
        ]

        result = calculate_highlights(summary_data, timeseries_data, repo_health_list)

        assert result["avg_review_time"] == "2.0 days"

    def test_calculate_empty_data(self):
        """Test calculating highlights with empty data."""
        result = calculate_highlights({}, {}, [])

        assert result["most_active_month"] == "N/A"
        assert result["most_active_month_prs"] == 0
        assert result["avg_review_time"] == "N/A"
        assert result["review_coverage"] == 0
        assert result["new_contributors"] == 0

    def test_calculate_with_none_values(self):
        """Test handling None values in repo health data."""
        summary_data = {}
        timeseries_data = {"monthly": {}}
        repo_health_list = [
            {"review_coverage": 80.0, "median_time_to_first_review": 3600},
            {"review_coverage": None, "median_time_to_first_review": None},
        ]

        result = calculate_highlights(summary_data, timeseries_data, repo_health_list)

        # Should only count non-None values
        assert result["review_coverage"] == 80.0
        assert result["avg_review_time"] == "1.0 hours"

    def test_new_contributors_always_zero(self):
        """Test that new contributors is always 0 (not implemented)."""
        summary_data = {}
        timeseries_data = {"monthly": {}}
        repo_health_list = []

        result = calculate_highlights(summary_data, timeseries_data, repo_health_list)

        assert result["new_contributors"] == 0


class TestCalculateFunFacts:
    """Tests for calculate_fun_facts function."""

    def test_calculate_total_comments(self):
        """Test extracting total comments from summary data."""
        summary_data = {"total_comments": 12345}
        timeseries_data = {"weekly": {}}
        leaderboards_data = {}

        result = calculate_fun_facts(summary_data, timeseries_data, leaderboards_data)

        assert result["total_comments"] == 12345

    def test_calculate_busiest_day(self):
        """Test calculating busiest day from weekly data."""
        summary_data = {}
        timeseries_data = {
            "weekly": {
                "prs_opened": [
                    {"period": "2025-W10", "user": "alice", "count": 5},
                    {"period": "2025-W10", "user": "bob", "count": 10},
                    {"period": "2025-W05", "user": "charlie", "count": 8},
                ]
            }
        }
        leaderboards_data = {}

        result = calculate_fun_facts(summary_data, timeseries_data, leaderboards_data)

        assert result["busiest_day"] == "March 03, 2025"  # Monday of W10
        assert result["busiest_day_count"] == 15  # 5 + 10

    def test_calculate_empty_data(self):
        """Test calculating fun facts with empty data."""
        result = calculate_fun_facts({}, {}, {})

        assert result["total_comments"] == 0
        assert result["busiest_day"] is None
        assert result["busiest_day_count"] is None
        assert result["most_active_hour"] is None
        assert result["total_lines_changed"] is None
        assert result["avg_pr_size"] is None
        assert result["most_used_emoji"] is None

    def test_unavailable_metrics_are_none(self):
        """Test that unavailable metrics are None."""
        summary_data = {"total_comments": 100}
        timeseries_data = {"weekly": {}}
        leaderboards_data = {}

        result = calculate_fun_facts(summary_data, timeseries_data, leaderboards_data)

        # These metrics are not implemented yet
        assert result["most_active_hour"] is None
        assert result["total_lines_changed"] is None
        assert result["avg_pr_size"] is None
        assert result["most_used_emoji"] is None

    def test_invalid_period_format(self):
        """Test handling invalid period format in busiest day calculation."""
        summary_data = {}
        timeseries_data = {
            "weekly": {
                "prs_opened": [
                    {"period": "invalid-format", "user": "alice", "count": 100},
                ]
            }
        }
        leaderboards_data = {}

        result = calculate_fun_facts(summary_data, timeseries_data, leaderboards_data)

        # Should handle error gracefully
        assert result["busiest_day"] is None
        assert result["busiest_day_count"] is None

    def test_missing_weekly_data(self):
        """Test handling missing weekly data."""
        summary_data = {"total_comments": 50}
        timeseries_data = {"monthly": {"prs_merged": []}}
        leaderboards_data = {}

        result = calculate_fun_facts(summary_data, timeseries_data, leaderboards_data)

        assert result["total_comments"] == 50
        assert result["busiest_day"] is None
