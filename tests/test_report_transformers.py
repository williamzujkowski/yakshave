"""Tests for report transformers module."""

from gh_year_end.report.transformers import (
    calculate_fun_facts,
    calculate_highlights,
    calculate_insights,
    calculate_risks,
    generate_chart_data,
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
        """Test that new contributors comes from summary data."""
        summary_data = {}
        timeseries_data = {"monthly": {}}
        repo_health_list = []

        result = calculate_highlights(summary_data, timeseries_data, repo_health_list)

        # Should default to 0 when not in summary
        assert result["new_contributors"] == 0

        # Should use value from summary when provided
        summary_data = {"new_contributors": 5}
        result = calculate_highlights(summary_data, timeseries_data, repo_health_list)
        assert result["new_contributors"] == 5


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


class TestGenerateChartData:
    """Tests for generate_chart_data function."""

    def test_generate_all_chart_datasets(self):
        """Test that all chart datasets are generated."""
        timeseries_data = {
            "weekly": {
                "prs_opened": [{"period": "2025-W01", "user": "alice", "count": 10}],
                "prs_merged": [{"period": "2025-W01", "user": "alice", "count": 8}],
                "reviews_submitted": [{"period": "2025-W01", "user": "bob", "count": 5}],
            }
        }
        summary_data = {}
        leaderboards_data = {}

        result = generate_chart_data(timeseries_data, summary_data, leaderboards_data)

        assert "collaboration_data" in result
        assert "velocity_data" in result
        assert "quality_data" in result
        assert "community_data" in result

    def test_collaboration_data_format(self):
        """Test collaboration data has correct format."""
        timeseries_data = {
            "weekly": {
                "reviews_submitted": [
                    {"period": "2025-W01", "user": "alice", "count": 5},
                    {"period": "2025-W01", "user": "bob", "count": 3},
                ],
                "review_comments": [{"period": "2025-W01", "user": "alice", "count": 10}],
                "issue_comments": [{"period": "2025-W01", "user": "bob", "count": 2}],
            }
        }
        summary_data = {}
        leaderboards_data = {}

        result = generate_chart_data(timeseries_data, summary_data, leaderboards_data)

        collaboration = result["collaboration_data"]
        assert len(collaboration) == 1
        assert collaboration[0]["date"] == "2024-12-30"  # Monday of W01, 2025
        assert collaboration[0]["reviews"] == 8  # 5 + 3
        assert collaboration[0]["comments"] == 12  # 10 + 2
        assert collaboration[0]["cross_team"] == 0  # Not implemented

    def test_velocity_data_format(self):
        """Test velocity data has correct format."""
        timeseries_data = {
            "weekly": {
                "prs_opened": [
                    {"period": "2025-W01", "user": "alice", "count": 10},
                    {"period": "2025-W01", "user": "bob", "count": 5},
                ],
                "prs_merged": [{"period": "2025-W01", "user": "alice", "count": 8}],
            }
        }
        summary_data = {}
        leaderboards_data = {}

        result = generate_chart_data(timeseries_data, summary_data, leaderboards_data)

        velocity = result["velocity_data"]
        assert len(velocity) == 1
        assert velocity[0]["date"] == "2024-12-30"
        assert velocity[0]["prs_opened"] == 15  # 10 + 5
        assert velocity[0]["prs_merged"] == 8
        assert velocity[0]["time_to_merge"] == 0  # Not implemented

    def test_quality_data_empty(self):
        """Test quality data returns empty (not implemented)."""
        timeseries_data = {"weekly": {}}
        summary_data = {}
        leaderboards_data = {}

        result = generate_chart_data(timeseries_data, summary_data, leaderboards_data)

        quality = result["quality_data"]
        assert quality == []

    def test_community_data_tracks_unique_contributors(self):
        """Test community data counts unique contributors per week."""
        timeseries_data = {
            "weekly": {
                "prs_opened": [
                    {"period": "2025-W01", "user": "alice", "count": 10},
                    {"period": "2025-W01", "user": "bob", "count": 5},
                ],
                "reviews_submitted": [
                    {"period": "2025-W01", "user": "alice", "count": 3},  # alice again
                    {"period": "2025-W01", "user": "charlie", "count": 2},  # new user
                ],
            }
        }
        summary_data = {}
        leaderboards_data = {}

        result = generate_chart_data(timeseries_data, summary_data, leaderboards_data)

        community = result["community_data"]
        assert len(community) == 1
        assert community[0]["date"] == "2024-12-30"
        assert community[0]["active_contributors"] == 3  # alice, bob, charlie
        assert community[0]["new_contributors"] == 0  # Not implemented

    def test_multiple_weeks_sorted_chronologically(self):
        """Test that chart data is sorted by date."""
        timeseries_data = {
            "weekly": {
                "prs_merged": [
                    {"period": "2025-W10", "user": "alice", "count": 5},
                    {"period": "2025-W01", "user": "alice", "count": 3},
                    {"period": "2025-W05", "user": "alice", "count": 8},
                ]
            }
        }
        summary_data = {}
        leaderboards_data = {}

        result = generate_chart_data(timeseries_data, summary_data, leaderboards_data)

        velocity = result["velocity_data"]
        assert len(velocity) == 3
        # Verify chronological order
        assert velocity[0]["date"] < velocity[1]["date"] < velocity[2]["date"]
        assert velocity[0]["prs_merged"] == 3  # W01
        assert velocity[1]["prs_merged"] == 8  # W05
        assert velocity[2]["prs_merged"] == 5  # W10

    def test_handles_empty_timeseries(self):
        """Test handling empty timeseries data."""
        timeseries_data = {}
        summary_data = {}
        leaderboards_data = {}

        result = generate_chart_data(timeseries_data, summary_data, leaderboards_data)

        assert result["collaboration_data"] == []
        assert result["velocity_data"] == []
        assert result["quality_data"] == []
        assert result["community_data"] == []

    def test_handles_missing_weekly_key(self):
        """Test handling missing weekly key."""
        timeseries_data = {"monthly": {}}
        summary_data = {}
        leaderboards_data = {}

        result = generate_chart_data(timeseries_data, summary_data, leaderboards_data)

        assert result["collaboration_data"] == []
        assert result["velocity_data"] == []
        assert result["quality_data"] == []
        assert result["community_data"] == []

    def test_skips_invalid_periods(self):
        """Test that invalid period formats are skipped."""
        timeseries_data = {
            "weekly": {
                "prs_merged": [
                    {"period": "invalid", "user": "alice", "count": 5},
                    {"period": "2025-W02", "user": "bob", "count": 10},
                ]
            }
        }
        summary_data = {}
        leaderboards_data = {}

        result = generate_chart_data(timeseries_data, summary_data, leaderboards_data)

        velocity = result["velocity_data"]
        assert len(velocity) == 1  # Only valid entry
        assert velocity[0]["prs_merged"] == 10

    def test_aggregates_across_users(self):
        """Test that metrics are aggregated across all users per week."""
        timeseries_data = {
            "weekly": {
                "reviews_submitted": [
                    {"period": "2025-W01", "user": "alice", "count": 5},
                    {"period": "2025-W01", "user": "bob", "count": 3},
                    {"period": "2025-W01", "user": "charlie", "count": 2},
                ]
            }
        }
        summary_data = {}
        leaderboards_data = {}

        result = generate_chart_data(timeseries_data, summary_data, leaderboards_data)

        collaboration = result["collaboration_data"]
        assert len(collaboration) == 1
        assert collaboration[0]["reviews"] == 10  # 5 + 3 + 2

    def test_handles_missing_metrics(self):
        """Test handling when specific metrics are missing."""
        timeseries_data = {
            "weekly": {
                # Only prs_merged, missing others
                "prs_merged": [{"period": "2025-W01", "user": "alice", "count": 5}]
            }
        }
        summary_data = {}
        leaderboards_data = {}

        result = generate_chart_data(timeseries_data, summary_data, leaderboards_data)

        # Collaboration should be empty (no reviews or comments)
        assert result["collaboration_data"] == []

        # Velocity should have prs_merged but not prs_opened
        velocity = result["velocity_data"]
        assert len(velocity) == 1
        assert velocity[0]["prs_merged"] == 5
        assert velocity[0]["prs_opened"] == 0

        # Community should track the user
        community = result["community_data"]
        assert len(community) == 1
        assert community[0]["active_contributors"] == 1


class TestCalculateInsights:
    """Tests for calculate_insights function."""

    def test_calculate_avg_reviewers_per_pr(self):
        """Test calculating average reviewers per PR."""
        summary_data = {
            "total_prs": 100,
            "total_reviews": 250,
        }
        leaderboards_data = {}
        repo_health_list = []
        hygiene_scores_list = []

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        assert result["avg_reviewers_per_pr"] == 2.5

    def test_calculate_avg_reviewers_per_pr_zero_prs(self):
        """Test calculating average reviewers per PR with zero PRs."""
        summary_data = {
            "total_prs": 0,
            "total_reviews": 0,
        }
        leaderboards_data = {}
        repo_health_list = []
        hygiene_scores_list = []

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        assert result["avg_reviewers_per_pr"] == 0

    def test_calculate_review_participation_rate(self):
        """Test calculating review participation rate."""
        summary_data = {
            "total_contributors": 10,
        }
        leaderboards_data = {
            "reviews_submitted": [
                {"user": "alice", "count": 50},
                {"user": "bob", "count": 30},
                {"user": "charlie", "count": 20},
            ]
        }
        repo_health_list = []
        hygiene_scores_list = []

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        # 3 reviewers out of 10 contributors = 30%
        assert result["review_participation_rate"] == 30

    def test_calculate_review_participation_rate_nested(self):
        """Test calculating review participation rate with nested data."""
        summary_data = {
            "total_contributors": 20,
        }
        leaderboards_data = {
            "leaderboards": {
                "reviews_submitted": {
                    "org": [
                        {"user": "alice", "count": 50},
                        {"user": "bob", "count": 30},
                    ]
                }
            }
        }
        repo_health_list = []
        hygiene_scores_list = []

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        # 2 reviewers out of 20 contributors = 10%
        assert result["review_participation_rate"] == 10

    def test_calculate_prs_per_week(self):
        """Test calculating PRs per week."""
        summary_data = {
            "prs_merged": 104,
        }
        leaderboards_data = {}
        repo_health_list = []
        hygiene_scores_list = []

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        # 104 PRs / 52 weeks = 2.0
        assert result["prs_per_week"] == 2.0

    def test_calculate_merge_rate(self):
        """Test calculating merge rate."""
        summary_data = {
            "total_prs": 100,
            "prs_merged": 85,
        }
        leaderboards_data = {}
        repo_health_list = []
        hygiene_scores_list = []

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        assert result["merge_rate"] == 85

    def test_calculate_merge_rate_zero_prs(self):
        """Test calculating merge rate with zero PRs."""
        summary_data = {
            "total_prs": 0,
            "prs_merged": 0,
        }
        leaderboards_data = {}
        repo_health_list = []
        hygiene_scores_list = []

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        assert result["merge_rate"] == 0

    def test_calculate_bus_factor(self):
        """Test calculating bus factor."""
        summary_data = {}
        leaderboards_data = {
            "prs_merged": [
                {"user": "alice", "count": 50},  # 50%
                {"user": "bob", "count": 25},  # 25%
                {"user": "charlie", "count": 15},  # 15%
                {"user": "david", "count": 10},  # 10%
            ]
        }
        repo_health_list = []
        hygiene_scores_list = []

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        # alice alone has 50%, so bus factor = 1
        assert result["bus_factor"] == 1

    def test_calculate_bus_factor_multiple_contributors(self):
        """Test calculating bus factor with multiple contributors needed."""
        summary_data = {}
        leaderboards_data = {
            "prs_merged": [
                {"user": "alice", "count": 30},  # 30%
                {"user": "bob", "count": 25},  # 25%
                {"user": "charlie", "count": 20},  # 20%
                {"user": "david", "count": 15},  # 15%
                {"user": "eve", "count": 10},  # 10%
            ]
        }
        repo_health_list = []
        hygiene_scores_list = []

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        # alice (30%) + bob (25%) = 55%, so bus factor = 2
        assert result["bus_factor"] == 2

    def test_calculate_bus_factor_nested_data(self):
        """Test calculating bus factor with nested leaderboards data."""
        summary_data = {}
        leaderboards_data = {
            "leaderboards": {
                "prs_merged": {
                    "org": [
                        {"user": "alice", "count": 60},
                        {"user": "bob", "count": 40},
                    ]
                }
            }
        }
        repo_health_list = []
        hygiene_scores_list = []

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        # alice has 60/100 = 60%, so bus factor = 1
        assert result["bus_factor"] == 1

    def test_calculate_bus_factor_value_field(self):
        """Test calculating bus factor using 'value' field instead of 'count'."""
        summary_data = {}
        leaderboards_data = {
            "prs_merged": [
                {"user": "alice", "value": 70},
                {"user": "bob", "value": 30},
            ]
        }
        repo_health_list = []
        hygiene_scores_list = []

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        # alice has 70/100 = 70%, so bus factor = 1
        assert result["bus_factor"] == 1

    def test_unavailable_metrics_are_none(self):
        """Test that unavailable metrics return None."""
        summary_data = {}
        leaderboards_data = {}
        repo_health_list = []
        hygiene_scores_list = []

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        # These metrics are not available from current data
        # TODO: cross_team_reviews requires team/org data
        assert result["cross_team_reviews"] == 0
        # TODO: median_pr_size requires PR detail data
        assert result["median_pr_size"] == 0
        # CI/CODEOWNERS/Security metrics calculated from hygiene data (0 when empty)
        assert result["repos_with_ci"] == 0
        assert result["repos_with_codeowners"] == 0
        assert result["repos_with_security_policy"] == 0
        # TODO: contributor_retention requires historical contributor data
        assert result["contributor_retention"] == 0

    def test_new_contributors_always_zero(self):
        """Test that new contributors comes from summary data."""
        summary_data = {}
        leaderboards_data = {}
        repo_health_list = []
        hygiene_scores_list = []

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        # Should default to 0 when not in summary
        assert result["new_contributors"] == 0

        # Should use value from summary when provided
        summary_data = {"new_contributors": 8}
        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )
        assert result["new_contributors"] == 8

    def test_calculate_hygiene_metrics_from_data(self):
        """Test that CI/CODEOWNERS/Security metrics are calculated from hygiene data."""
        summary_data = {}
        leaderboards_data = {}
        repo_health_list = []
        hygiene_scores_list = [
            {
                "repo_id": "repo1",
                "has_ci_workflows": True,
                "has_codeowners": True,
                "has_security_md": True,
            },
            {
                "repo_id": "repo2",
                "has_ci_workflows": False,
                "has_codeowners": True,
                "has_security_md": False,
            },
            {
                "repo_id": "repo3",
                "has_ci_workflows": True,
                "has_codeowners": False,
                "has_security_md": True,
            },
            {
                "repo_id": "repo4",
                "has_ci_workflows": True,
                "has_codeowners": True,
                "has_security_md": True,
            },
        ]

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        # 3 out of 4 repos have CI workflows (75%)
        assert result["repos_with_ci"] == 75
        # 3 out of 4 repos have CODEOWNERS (75%)
        assert result["repos_with_codeowners"] == 75
        # 3 out of 4 repos have security policy (75%)
        assert result["repos_with_security_policy"] == 75

    def test_calculate_empty_data(self):
        """Test calculating insights with empty data."""
        result = calculate_insights({}, {}, [], [])

        # Should not crash and return sensible defaults
        assert result["avg_reviewers_per_pr"] == 0
        assert result["review_participation_rate"] == 0
        assert result["prs_per_week"] == 0
        assert result["merge_rate"] == 0
        assert result["bus_factor"] == 0
        assert result["new_contributors"] == 0

    def test_calculate_with_missing_fields(self):
        """Test calculating insights with missing fields in data."""
        summary_data = {
            "total_prs": 50,
            # Missing total_reviews, prs_merged, total_contributors
        }
        leaderboards_data = {}
        repo_health_list = []
        hygiene_scores_list = []

        result = calculate_insights(
            summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
        )

        # Should handle missing data gracefully
        assert result["avg_reviewers_per_pr"] == 0
        assert result["review_participation_rate"] == 0
        assert result["prs_per_week"] == 0
        assert result["merge_rate"] == 0


class TestCalculateRisks:
    """Tests for calculate_risks function."""

    def test_identify_missing_security_policy(self):
        """Test identifying repos missing SECURITY.md."""
        repo_health = []
        hygiene_scores = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                "has_security_md": False,
            },
            {
                "repo_id": "R_002",
                "repo_full_name": "org/repo2",
                "has_security_md": True,
            },
        ]

        result = calculate_risks(repo_health, hygiene_scores)

        risk = next((r for r in result if r["title"] == "Missing Security Policy"), None)
        assert risk is not None
        assert risk["severity"] == "high"
        assert risk["description"] == "1 repositories are missing SECURITY.md"
        assert risk["repos"] == ["org/repo1"]

    def test_identify_missing_ci_cd(self):
        """Test identifying repos missing CI/CD workflows."""
        repo_health = []
        hygiene_scores = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                "has_ci_workflows": False,
            },
            {
                "repo_id": "R_002",
                "repo_full_name": "org/repo2",
                "has_ci_workflows": True,
            },
            {
                "repo_id": "R_003",
                "repo_full_name": "org/repo3",
                "has_ci_workflows": False,
            },
        ]

        result = calculate_risks(repo_health, hygiene_scores)

        risk = next((r for r in result if r["title"] == "Missing CI/CD"), None)
        assert risk is not None
        assert risk["severity"] == "medium"
        assert len(risk["repos"]) == 2
        assert "org/repo1" in risk["repos"]
        assert "org/repo3" in risk["repos"]

    def test_identify_low_documentation_score(self):
        """Test identifying repos with low hygiene scores."""
        repo_health = []
        hygiene_scores = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                "score": 45,
            },
            {
                "repo_id": "R_002",
                "repo_full_name": "org/repo2",
                "score": 80,
            },
            {
                "repo_id": "R_003",
                "repo_full_name": "org/repo3",
                "score": 55,
            },
        ]

        result = calculate_risks(repo_health, hygiene_scores)

        risk = next((r for r in result if r["title"] == "Low Documentation Score"), None)
        assert risk is not None
        assert risk["severity"] == "medium"
        assert len(risk["repos"]) == 2
        assert "org/repo1" in risk["repos"]
        assert "org/repo3" in risk["repos"]

    def test_identify_missing_codeowners(self):
        """Test identifying repos missing CODEOWNERS."""
        repo_health = []
        hygiene_scores = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                "has_codeowners": False,
            },
            {
                "repo_id": "R_002",
                "repo_full_name": "org/repo2",
                "has_codeowners": True,
            },
        ]

        result = calculate_risks(repo_health, hygiene_scores)

        risk = next((r for r in result if r["title"] == "Missing CODEOWNERS"), None)
        assert risk is not None
        assert risk["severity"] == "low"
        assert risk["repos"] == ["org/repo1"]

    def test_identify_long_review_times(self):
        """Test identifying repos with long review times."""
        repo_health = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                "median_time_to_first_review": 200000,  # > 48 hours
            },
            {
                "repo_id": "R_002",
                "repo_full_name": "org/repo2",
                "median_time_to_first_review": 3600,  # 1 hour
            },
        ]
        hygiene_scores = []

        result = calculate_risks(repo_health, hygiene_scores)

        risk = next((r for r in result if r["title"] == "Long Review Times"), None)
        assert risk is not None
        assert risk["severity"] == "medium"
        assert risk["repos"] == ["org/repo1"]

    def test_identify_low_review_coverage(self):
        """Test identifying repos with low review coverage."""
        repo_health = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                "review_coverage": 30.0,
            },
            {
                "repo_id": "R_002",
                "repo_full_name": "org/repo2",
                "review_coverage": 80.0,
            },
            {
                "repo_id": "R_003",
                "repo_full_name": "org/repo3",
                "review_coverage": 45.0,
            },
        ]
        hygiene_scores = []

        result = calculate_risks(repo_health, hygiene_scores)

        risk = next((r for r in result if r["title"] == "Low Review Coverage"), None)
        assert risk is not None
        assert risk["severity"] == "high"
        assert len(risk["repos"]) == 2
        assert "org/repo1" in risk["repos"]
        assert "org/repo3" in risk["repos"]

    def test_identify_high_stale_prs(self):
        """Test identifying repos with many stale PRs."""
        repo_health = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                "stale_pr_count": 10,
            },
            {
                "repo_id": "R_002",
                "repo_full_name": "org/repo2",
                "stale_pr_count": 2,
            },
        ]
        hygiene_scores = []

        result = calculate_risks(repo_health, hygiene_scores)

        risk = next((r for r in result if r["title"] == "High Stale PR Count"), None)
        assert risk is not None
        assert risk["severity"] == "medium"
        assert risk["repos"] == ["org/repo1"]

    def test_identify_low_contributor_activity(self):
        """Test identifying repos with low contributor activity."""
        repo_health = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                "active_contributors_90d": 1,
            },
            {
                "repo_id": "R_002",
                "repo_full_name": "org/repo2",
                "active_contributors_90d": 5,
            },
        ]
        hygiene_scores = []

        result = calculate_risks(repo_health, hygiene_scores)

        risk = next((r for r in result if r["title"] == "Low Contributor Activity"), None)
        assert risk is not None
        assert risk["severity"] == "medium"
        assert risk["repos"] == ["org/repo1"]

    def test_identify_disabled_security_features(self):
        """Test identifying repos with disabled security features."""
        repo_health = []
        hygiene_scores = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                "dependabot_enabled": False,
                "secret_scanning_enabled": True,
            },
            {
                "repo_id": "R_002",
                "repo_full_name": "org/repo2",
                "dependabot_enabled": True,
                "secret_scanning_enabled": False,
            },
            {
                "repo_id": "R_003",
                "repo_full_name": "org/repo3",
                "dependabot_enabled": True,
                "secret_scanning_enabled": True,
            },
        ]

        result = calculate_risks(repo_health, hygiene_scores)

        risk = next((r for r in result if r["title"] == "Disabled Security Features"), None)
        assert risk is not None
        assert risk["severity"] == "high"
        assert len(risk["repos"]) == 2
        assert "org/repo1" in risk["repos"]
        assert "org/repo2" in risk["repos"]

    def test_identify_no_branch_protection(self):
        """Test identifying repos without branch protection."""
        repo_health = []
        hygiene_scores = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                "branch_protection_enabled": False,
            },
            {
                "repo_id": "R_002",
                "repo_full_name": "org/repo2",
                "branch_protection_enabled": True,
            },
        ]

        result = calculate_risks(repo_health, hygiene_scores)

        risk = next((r for r in result if r["title"] == "No Branch Protection"), None)
        assert risk is not None
        assert risk["severity"] == "high"
        assert risk["repos"] == ["org/repo1"]

    def test_multiple_risks_detected(self):
        """Test that multiple risks can be detected simultaneously."""
        repo_health = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                "review_coverage": 30.0,
                "stale_pr_count": 10,
            }
        ]
        hygiene_scores = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                "has_security_md": False,
                "has_ci_workflows": False,
                "branch_protection_enabled": False,
            }
        ]

        result = calculate_risks(repo_health, hygiene_scores)

        # Should detect at least 5 different risks
        assert len(result) >= 5
        titles = {r["title"] for r in result}
        assert "Missing Security Policy" in titles
        assert "Missing CI/CD" in titles
        assert "No Branch Protection" in titles
        assert "Low Review Coverage" in titles
        assert "High Stale PR Count" in titles

    def test_no_risks_when_all_healthy(self):
        """Test that no risks are returned when all metrics are healthy."""
        repo_health = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                "review_coverage": 90.0,
                "median_time_to_first_review": 3600,
                "stale_pr_count": 0,
                "active_contributors_90d": 5,
            }
        ]
        hygiene_scores = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                "score": 100,
                "has_security_md": True,
                "has_ci_workflows": True,
                "has_codeowners": True,
                "branch_protection_enabled": True,
                "dependabot_enabled": True,
                "secret_scanning_enabled": True,
            }
        ]

        result = calculate_risks(repo_health, hygiene_scores)

        assert result == []

    def test_empty_data(self):
        """Test handling of empty data."""
        result = calculate_risks([], [])

        assert result == []

    def test_repos_are_sorted(self):
        """Test that repos in risk reports are sorted alphabetically."""
        repo_health = []
        hygiene_scores = [
            {"repo_id": "R_003", "repo_full_name": "org/zebra", "has_security_md": False},
            {"repo_id": "R_001", "repo_full_name": "org/alpha", "has_security_md": False},
            {"repo_id": "R_002", "repo_full_name": "org/beta", "has_security_md": False},
        ]

        result = calculate_risks(repo_health, hygiene_scores)

        risk = next((r for r in result if r["title"] == "Missing Security Policy"), None)
        assert risk is not None
        assert risk["repos"] == ["org/alpha", "org/beta", "org/zebra"]

    def test_missing_fields_handled_gracefully(self):
        """Test that missing fields are handled gracefully."""
        repo_health = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                # Missing review_coverage, median_time_to_first_review, etc.
            }
        ]
        hygiene_scores = [
            {
                "repo_id": "R_001",
                "repo_full_name": "org/repo1",
                # Missing has_security_md, has_ci_workflows, etc.
            }
        ]

        # Should not raise exceptions
        result = calculate_risks(repo_health, hygiene_scores)

        # Repos should be flagged for missing features (defaults to False)
        assert len(result) > 0
