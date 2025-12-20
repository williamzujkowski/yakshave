"""Tests for MetricsAggregator."""

from datetime import datetime

import pytest

from gh_year_end.collect.aggregator import BOT_PATTERNS, MetricsAggregator


class TestBotDetection:
    """Test bot detection logic."""

    def test_bot_patterns(self):
        """Test that BOT_PATTERNS contains expected patterns."""
        assert "[bot]" in BOT_PATTERNS
        assert "github-actions" in BOT_PATTERNS
        assert "dependabot" in BOT_PATTERNS

    def test_is_bot_with_type_bot(self):
        """Test detection of GitHub Bot type."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        user = {"login": "some-user", "type": "Bot"}
        assert agg._is_bot(user) is True

    def test_is_bot_with_bot_suffix(self):
        """Test detection of [bot] suffix."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        user = {"login": "renovate[bot]", "type": "User"}
        assert agg._is_bot(user) is True

    def test_is_bot_with_github_actions(self):
        """Test detection of github-actions."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        user = {"login": "github-actions[bot]", "type": "Bot"}
        assert agg._is_bot(user) is True

    def test_is_bot_with_dependabot(self):
        """Test detection of dependabot."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        user = {"login": "dependabot[bot]", "type": "Bot"}
        assert agg._is_bot(user) is True

    def test_is_not_bot_regular_user(self):
        """Test that regular users are not detected as bots."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        user = {"login": "john-doe", "type": "User"}
        assert agg._is_bot(user) is False

    def test_is_bot_with_none_user(self):
        """Test that None user is considered a bot."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        assert agg._is_bot(None) is True

    def test_is_bot_case_insensitive(self):
        """Test that bot detection is case insensitive."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        user = {"login": "GitHub-Actions", "type": "User"}
        assert agg._is_bot(user) is True


class TestUserCaching:
    """Test user caching functionality."""

    def test_cache_user_basic(self):
        """Test caching basic user info."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        user = {
            "login": "alice",
            "avatar_url": "https://example.com/avatar.png",
            "type": "User",
        }
        agg._cache_user(user)

        assert "alice" in agg.users
        assert agg.users["alice"]["login"] == "alice"
        assert agg.users["alice"]["avatar_url"] == "https://example.com/avatar.png"
        assert agg.users["alice"]["is_bot"] is False

    def test_cache_user_bot(self):
        """Test caching bot user."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        user = {"login": "renovate[bot]", "avatar_url": "", "type": "Bot"}
        agg._cache_user(user)

        assert "renovate[bot]" in agg.users
        assert agg.users["renovate[bot]"]["is_bot"] is True

    def test_cache_user_none(self):
        """Test that caching None user is safe."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        agg._cache_user(None)
        assert len(agg.users) == 0

    def test_cache_user_without_login(self):
        """Test that caching user without login is safe."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        user = {"avatar_url": "https://example.com/avatar.png"}
        agg._cache_user(user)
        assert len(agg.users) == 0

    def test_cache_user_idempotent(self):
        """Test that caching same user twice doesn't duplicate."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        user = {"login": "alice", "avatar_url": "url1", "type": "User"}
        agg._cache_user(user)
        agg._cache_user(user)

        assert len(agg.users) == 1
        assert agg.users["alice"]["avatar_url"] == "url1"


class TestTimeBucketing:
    """Test time bucketing for time series."""

    def test_get_week_key(self):
        """Test week key generation."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        dt = datetime(2024, 1, 15)  # Week 3 of 2024
        week_key = agg._get_week_key(dt)
        assert week_key == "2024-W03"

    def test_get_month_key(self):
        """Test month key generation."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        dt = datetime(2024, 1, 15)
        month_key = agg._get_month_key(dt)
        assert month_key == "2024-01"

    def test_get_week_key_year_boundary(self):
        """Test week key at year boundary."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        dt = datetime(2024, 1, 1)  # First day of 2024
        week_key = agg._get_week_key(dt)
        # ISO week date: 2024-01-01 is in week 1 of 2024
        assert week_key == "2024-W01"


class TestRepoTracking:
    """Test repository tracking."""

    def test_add_repo(self):
        """Test adding a repository."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo = {
            "full_name": "owner/repo",
            "name": "repo",
            "description": "Test repo",
            "html_url": "https://github.com/owner/repo",
        }
        agg.add_repo(repo)

        assert "owner/repo" in agg.repos
        assert "owner/repo" in agg.repo_health
        assert agg.repo_health["owner/repo"]["pr_count"] == 0


class TestPRMetrics:
    """Test PR metrics aggregation."""

    def test_add_pr_basic(self):
        """Test adding a basic PR."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        pr = {
            "user": {"login": "alice", "avatar_url": "url", "type": "User"},
            "created_at": "2024-01-15T10:00:00Z",
        }
        agg.add_pr(repo_id, pr)

        assert agg.leaderboards["prs_opened"]["alice"] == 1
        assert agg.repo_health[repo_id]["pr_count"] == 1
        assert "alice" in agg.repo_health[repo_id]["contributors"]
        assert "alice" in agg.users

    def test_add_pr_merged(self):
        """Test adding a merged PR."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        pr = {
            "user": {"login": "alice", "avatar_url": "url", "type": "User"},
            "created_at": "2024-01-15T10:00:00Z",
            "merged_at": "2024-01-16T10:00:00Z",
        }
        agg.add_pr(repo_id, pr)

        assert agg.leaderboards["prs_opened"]["alice"] == 1
        assert agg.leaderboards["prs_merged"]["alice"] == 1

    def test_add_pr_bot_filtered(self):
        """Test that bot PRs are filtered out."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        pr = {
            "user": {"login": "dependabot[bot]", "avatar_url": "url", "type": "Bot"},
            "created_at": "2024-01-15T10:00:00Z",
        }
        agg.add_pr(repo_id, pr)

        assert "dependabot[bot]" not in agg.leaderboards["prs_opened"]
        assert agg.repo_health[repo_id]["pr_count"] == 0

    def test_add_pr_time_series(self):
        """Test PR time series tracking."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        pr = {
            "user": {"login": "alice", "avatar_url": "url", "type": "User"},
            "created_at": "2024-01-15T10:00:00Z",
        }
        agg.add_pr(repo_id, pr)

        # Check weekly counters
        assert "2024-W03" in agg._weekly_counters["prs_opened"]
        assert agg._weekly_counters["prs_opened"]["2024-W03"]["alice"] == 1

        # Check monthly counters
        assert "2024-01" in agg._monthly_counters["prs_opened"]
        assert agg._monthly_counters["prs_opened"]["2024-01"]["alice"] == 1


class TestIssueMetrics:
    """Test issue metrics aggregation."""

    def test_add_issue_basic(self):
        """Test adding a basic issue."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        issue = {
            "user": {"login": "bob", "avatar_url": "url", "type": "User"},
            "created_at": "2024-01-15T10:00:00Z",
            "state": "open",
        }
        agg.add_issue(repo_id, issue)

        assert agg.leaderboards["issues_opened"]["bob"] == 1
        assert agg.repo_health[repo_id]["issue_count"] == 1
        assert "bob" in agg.repo_health[repo_id]["contributors"]

    def test_add_issue_closed(self):
        """Test adding a closed issue."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        issue = {
            "user": {"login": "bob", "avatar_url": "url", "type": "User"},
            "created_at": "2024-01-15T10:00:00Z",
            "state": "closed",
            "closed_at": "2024-01-20T10:00:00Z",
        }
        agg.add_issue(repo_id, issue)

        assert agg.leaderboards["issues_opened"]["bob"] == 1
        assert agg.leaderboards["issues_closed"]["bob"] == 1

    def test_add_issue_skips_pr(self):
        """Test that issues with pull_request key are skipped."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        issue = {
            "user": {"login": "bob", "avatar_url": "url", "type": "User"},
            "created_at": "2024-01-15T10:00:00Z",
            "pull_request": {"url": "https://api.github.com/repos/owner/repo/pulls/1"},
        }
        agg.add_issue(repo_id, issue)

        assert "bob" not in agg.leaderboards["issues_opened"]
        assert agg.repo_health[repo_id]["issue_count"] == 0


class TestReviewMetrics:
    """Test review metrics aggregation."""

    def test_add_review_basic(self):
        """Test adding a basic review."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        review = {
            "user": {"login": "charlie", "avatar_url": "url", "type": "User"},
            "submitted_at": "2024-01-15T10:00:00Z",
            "state": "COMMENTED",
        }
        agg.add_review(repo_id, 1, review)

        assert agg.leaderboards["reviews_submitted"]["charlie"] == 1
        assert agg.repo_health[repo_id]["review_count"] == 1

    def test_add_review_approval(self):
        """Test adding an approval review."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        review = {
            "user": {"login": "charlie", "avatar_url": "url", "type": "User"},
            "submitted_at": "2024-01-15T10:00:00Z",
            "state": "APPROVED",
        }
        agg.add_review(repo_id, 1, review)

        assert agg.leaderboards["reviews_submitted"]["charlie"] == 1
        assert agg.leaderboards["approvals"]["charlie"] == 1

    def test_add_review_changes_requested(self):
        """Test adding a changes requested review."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        review = {
            "user": {"login": "charlie", "avatar_url": "url", "type": "User"},
            "submitted_at": "2024-01-15T10:00:00Z",
            "state": "CHANGES_REQUESTED",
        }
        agg.add_review(repo_id, 1, review)

        assert agg.leaderboards["reviews_submitted"]["charlie"] == 1
        assert agg.leaderboards["changes_requested"]["charlie"] == 1


class TestCommentMetrics:
    """Test comment metrics aggregation."""

    def test_add_comment_issue(self):
        """Test adding an issue comment."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        comment = {
            "user": {"login": "dave", "avatar_url": "url", "type": "User"},
            "created_at": "2024-01-15T10:00:00Z",
        }
        agg.add_comment(repo_id, comment, comment_type="issue")

        assert agg.leaderboards["comments_total"]["dave"] == 1
        assert "dave" not in agg.leaderboards["review_comments_total"]

    def test_add_comment_review(self):
        """Test adding a review comment."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        comment = {
            "user": {"login": "dave", "avatar_url": "url", "type": "User"},
            "created_at": "2024-01-15T10:00:00Z",
        }
        agg.add_comment(repo_id, comment, comment_type="review")

        assert agg.leaderboards["comments_total"]["dave"] == 1
        assert agg.leaderboards["review_comments_total"]["dave"] == 1


class TestHygieneTracking:
    """Test hygiene score tracking."""

    def test_set_hygiene(self):
        """Test setting hygiene data."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"

        hygiene_data = {
            "repo": repo_id,
            "score": 85,
            "checks": {
                "has_readme": True,
                "has_license": True,
                "has_contributing": False,
            },
        }
        agg.set_hygiene(repo_id, hygiene_data)

        assert repo_id in agg.hygiene
        assert agg.hygiene[repo_id]["score"] == 85


class TestExport:
    """Test export functionality."""

    def test_export_structure(self):
        """Test that export returns expected structure."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        export = agg.export()

        assert "summary" in export
        assert "leaderboards" in export
        assert "timeseries" in export
        assert "repo_health" in export
        assert "hygiene_scores" in export
        assert "awards" in export

    def test_export_summary(self):
        """Test summary export."""
        agg = MetricsAggregator(year=2024, target_name="testuser", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        pr = {
            "user": {"login": "alice", "avatar_url": "url", "type": "User"},
            "created_at": "2024-01-15T10:00:00Z",
        }
        agg.add_pr(repo_id, pr)

        export = agg.export()
        summary = export["summary"]

        assert summary["year"] == 2024
        assert summary["target_name"] == "testuser"
        assert summary["total_repos"] == 1
        assert summary["total_prs"] == 1
        assert summary["total_contributors"] == 1

    def test_export_leaderboards(self):
        """Test leaderboards export."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        # Add PRs from multiple users
        for user, count in [("alice", 3), ("bob", 2), ("charlie", 1)]:
            for _ in range(count):
                pr = {
                    "user": {"login": user, "avatar_url": f"{user}.png", "type": "User"},
                    "created_at": "2024-01-15T10:00:00Z",
                }
                agg.add_pr(repo_id, pr)

        export = agg.export()
        leaderboards = export["leaderboards"]

        assert "prs_opened" in leaderboards
        assert len(leaderboards["prs_opened"]) == 3
        # Check sorting
        assert leaderboards["prs_opened"][0]["user"] == "alice"
        assert leaderboards["prs_opened"][0]["count"] == 3
        assert leaderboards["prs_opened"][1]["user"] == "bob"
        assert leaderboards["prs_opened"][1]["count"] == 2

    def test_export_filters_bots_from_leaderboards(self):
        """Test that bots are filtered from exported leaderboards."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        # Add PRs from user and bot
        pr_user = {
            "user": {"login": "alice", "avatar_url": "url", "type": "User"},
            "created_at": "2024-01-15T10:00:00Z",
        }
        pr_bot = {
            "user": {"login": "dependabot[bot]", "avatar_url": "url", "type": "Bot"},
            "created_at": "2024-01-15T10:00:00Z",
        }
        agg.add_pr(repo_id, pr_user)
        agg.add_pr(repo_id, pr_bot)

        export = agg.export()
        leaderboards = export["leaderboards"]

        # Only alice should be in leaderboard
        assert len(leaderboards["prs_opened"]) == 1
        assert leaderboards["prs_opened"][0]["user"] == "alice"

    def test_export_timeseries(self):
        """Test time series export."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        pr = {
            "user": {"login": "alice", "avatar_url": "url", "type": "User"},
            "created_at": "2024-01-15T10:00:00Z",
        }
        agg.add_pr(repo_id, pr)

        export = agg.export()
        timeseries = export["timeseries"]

        assert "weekly" in timeseries
        assert "monthly" in timeseries
        assert "prs_opened" in timeseries["weekly"]
        assert len(timeseries["weekly"]["prs_opened"]) == 1
        assert timeseries["weekly"]["prs_opened"][0]["period"] == "2024-W03"
        assert timeseries["weekly"]["prs_opened"][0]["user"] == "alice"

    def test_export_repo_health(self):
        """Test repo health export."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        pr = {
            "user": {"login": "alice", "avatar_url": "url", "type": "User"},
            "created_at": "2024-01-15T10:00:00Z",
        }
        agg.add_pr(repo_id, pr)

        export = agg.export()
        repo_health = export["repo_health"]

        assert len(repo_health) == 1
        assert repo_health[0]["repo"] == repo_id
        assert repo_health[0]["contributor_count"] == 1
        assert repo_health[0]["pr_count"] == 1

    def test_export_awards(self):
        """Test awards export."""
        agg = MetricsAggregator(year=2024, target_name="test", target_mode="user")
        repo_id = "owner/repo"
        agg.add_repo({"full_name": repo_id, "name": "repo"})

        # Add data to trigger awards
        pr = {
            "user": {"login": "alice", "avatar_url": "url", "type": "User"},
            "created_at": "2024-01-15T10:00:00Z",
        }
        agg.add_pr(repo_id, pr)

        export = agg.export()
        awards = export["awards"]

        assert "top_pr_author" in awards
        assert awards["top_pr_author"]["user"] == "alice"
        assert awards["top_pr_author"]["count"] == 1


class TestIntegration:
    """Integration tests combining multiple operations."""

    def test_full_workflow(self):
        """Test a complete workflow with multiple repos and users."""
        agg = MetricsAggregator(year=2024, target_name="org", target_mode="org")

        # Add repos
        repo1 = {"full_name": "org/repo1", "name": "repo1"}
        repo2 = {"full_name": "org/repo2", "name": "repo2"}
        agg.add_repo(repo1)
        agg.add_repo(repo2)

        # Add PRs
        pr1 = {
            "user": {"login": "alice", "avatar_url": "a.png", "type": "User"},
            "created_at": "2024-01-15T10:00:00Z",
            "merged_at": "2024-01-16T10:00:00Z",
        }
        pr2 = {
            "user": {"login": "bob", "avatar_url": "b.png", "type": "User"},
            "created_at": "2024-02-15T10:00:00Z",
        }
        agg.add_pr("org/repo1", pr1)
        agg.add_pr("org/repo2", pr2)

        # Add issues
        issue = {
            "user": {"login": "alice", "avatar_url": "a.png", "type": "User"},
            "created_at": "2024-01-20T10:00:00Z",
            "state": "closed",
            "closed_at": "2024-01-25T10:00:00Z",
        }
        agg.add_issue("org/repo1", issue)

        # Add reviews
        review = {
            "user": {"login": "charlie", "avatar_url": "c.png", "type": "User"},
            "submitted_at": "2024-01-16T10:00:00Z",
            "state": "APPROVED",
        }
        agg.add_review("org/repo1", 1, review)

        # Export and validate
        export = agg.export()

        assert export["summary"]["total_repos"] == 2
        assert export["summary"]["total_contributors"] == 3
        assert export["summary"]["total_prs"] == 2
        assert export["summary"]["prs_merged"] == 1
        assert export["summary"]["total_issues"] == 1
        assert export["summary"]["issues_closed"] == 1

        assert len(export["leaderboards"]["prs_opened"]) == 2
        assert len(export["repo_health"]) == 2
