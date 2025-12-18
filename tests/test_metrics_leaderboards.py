"""Tests for leaderboard metrics calculator."""

from pathlib import Path

import polars as pl
import pytest

from gh_year_end.config import Config
from gh_year_end.metrics.leaderboards import calculate_leaderboards


@pytest.fixture
def config(tmp_path: Path) -> Config:
    """Create test configuration."""
    return Config.model_validate(
        {
            "github": {
                "target": {"mode": "org", "name": "test-org"},
                "windows": {
                    "year": 2025,
                    "since": "2025-01-01T00:00:00Z",
                    "until": "2026-01-01T00:00:00Z",
                },
            },
            "storage": {"root": str(tmp_path)},
            "identity": {"humans_only": True},
        }
    )


@pytest.fixture
def curated_dir(tmp_path: Path) -> Path:
    """Create curated data directory."""
    curated = tmp_path / "curated" / "year=2025"
    curated.mkdir(parents=True)
    return curated


class TestCalculateLeaderboards:
    """Tests for calculate_leaderboards function."""

    def test_missing_dim_user_raises_error(self, curated_dir: Path, config: Config) -> None:
        """Test that missing dim_user table raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="dim_user table not found"):
            calculate_leaderboards(curated_dir, config)

    def test_empty_tables_returns_empty_leaderboard(
        self, curated_dir: Path, config: Config
    ) -> None:
        """Test that empty fact tables return empty leaderboard."""
        # Create empty dim_user
        dim_user = pl.DataFrame(
            schema={
                "user_id": pl.Utf8,
                "login": pl.Utf8,
                "type": pl.Utf8,
                "profile_url": pl.Utf8,
                "is_bot": pl.Boolean,
                "bot_reason": pl.Utf8,
                "display_name": pl.Utf8,
            }
        )
        dim_user.write_parquet(curated_dir / "dim_user.parquet")

        result = calculate_leaderboards(curated_dir, config)

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0
        assert "year" in result.columns
        assert "metric_key" in result.columns
        assert "scope" in result.columns
        assert "repo_id" in result.columns
        assert "user_id" in result.columns
        assert "value" in result.columns
        assert "rank" in result.columns

    def test_pr_metrics_calculation(self, curated_dir: Path, config: Config) -> None:
        """Test PR metrics calculation (opened, closed, merged)."""
        # Create dim_user with humans only
        dim_user = pl.DataFrame(
            {
                "user_id": ["U_alice", "U_bob", "U_bot"],
                "login": ["alice", "bob", "bot[bot]"],
                "type": ["User", "User", "Bot"],
                "profile_url": ["", "", ""],
                "is_bot": [False, False, True],
                "bot_reason": [None, None, "Pattern match: .*\\[bot\\]$"],
                "display_name": [None, None, None],
            }
        )
        dim_user.write_parquet(curated_dir / "dim_user.parquet")

        # Create fact_pull_request
        fact_pr = pl.DataFrame(
            {
                "pr_id": ["PR_1", "PR_2", "PR_3", "PR_4", "PR_5"],
                "repo_id": ["R_1", "R_1", "R_2", "R_1", "R_1"],
                "number": [1, 2, 3, 4, 5],
                "author_user_id": ["U_alice", "U_bob", "U_alice", "U_bot", "U_alice"],
                "created_at": [
                    "2025-01-01T10:00:00Z",
                    "2025-01-02T10:00:00Z",
                    "2025-01-03T10:00:00Z",
                    "2025-01-04T10:00:00Z",
                    "2025-01-05T10:00:00Z",
                ],
                "updated_at": ["2025-01-01T10:00:00Z"] * 5,
                "closed_at": [
                    None,
                    "2025-01-03T10:00:00Z",
                    "2025-01-04T10:00:00Z",
                    None,
                    "2025-01-06T10:00:00Z",
                ],
                "merged_at": [
                    None,
                    "2025-01-03T10:00:00Z",
                    None,
                    None,
                    "2025-01-06T10:00:00Z",
                ],
                "state": ["open", "merged", "closed", "open", "merged"],
                "is_draft": [False] * 5,
                "labels": [""] * 5,
                "milestone": [None] * 5,
                "additions": [10] * 5,
                "deletions": [5] * 5,
                "changed_files": [2] * 5,
                "title_len": [10] * 5,
                "body_len": [100] * 5,
            }
        )
        fact_pr = fact_pr.with_columns(
            [
                pl.col("created_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"),
                pl.col("updated_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"),
                pl.col("closed_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"),
                pl.col("merged_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"),
            ]
        )
        fact_pr.write_parquet(curated_dir / "fact_pull_request.parquet")

        result = calculate_leaderboards(curated_dir, config)

        # Filter bot (U_bot) should be excluded due to humans_only=True
        # Alice: 3 PRs opened (PR_1, PR_3, PR_5), 1 merged (PR_5), 2 closed (PR_3, PR_5)
        # Bob: 1 PR opened (PR_2), 1 merged (PR_2), 1 closed (PR_2)

        # Check prs_opened org-wide
        prs_opened_org = result.filter(
            (pl.col("metric_key") == "prs_opened") & (pl.col("scope") == "org")
        )
        assert len(prs_opened_org) == 2
        alice_opened = prs_opened_org.filter(pl.col("user_id") == "U_alice")
        assert alice_opened["value"].item() == 3
        assert alice_opened["rank"].item() == 1
        bob_opened = prs_opened_org.filter(pl.col("user_id") == "U_bob")
        assert bob_opened["value"].item() == 1
        assert bob_opened["rank"].item() == 2

        # Check prs_merged org-wide
        prs_merged_org = result.filter(
            (pl.col("metric_key") == "prs_merged") & (pl.col("scope") == "org")
        )
        assert len(prs_merged_org) == 2
        alice_merged = prs_merged_org.filter(pl.col("user_id") == "U_alice")
        assert alice_merged["value"].item() == 1
        bob_merged = prs_merged_org.filter(pl.col("user_id") == "U_bob")
        assert bob_merged["value"].item() == 1
        # Both should have rank 1 (tied)

        # Check repo-scoped metrics
        prs_opened_repo = result.filter(
            (pl.col("metric_key") == "prs_opened") & (pl.col("scope") == "repo")
        )
        # Alice has 2 in R_1 and 1 in R_2, Bob has 1 in R_1
        assert len(prs_opened_repo) == 3

    def test_issue_metrics_calculation(self, curated_dir: Path, config: Config) -> None:
        """Test issue metrics calculation (opened, closed)."""
        # Create dim_user
        dim_user = pl.DataFrame(
            {
                "user_id": ["U_alice", "U_bob"],
                "login": ["alice", "bob"],
                "type": ["User", "User"],
                "profile_url": ["", ""],
                "is_bot": [False, False],
                "bot_reason": [None, None],
                "display_name": [None, None],
            }
        )
        dim_user.write_parquet(curated_dir / "dim_user.parquet")

        # Create fact_issue
        fact_issue = pl.DataFrame(
            {
                "issue_id": ["I_1", "I_2", "I_3"],
                "repo_id": ["R_1", "R_1", "R_2"],
                "number": [1, 2, 3],
                "author_user_id": ["U_alice", "U_bob", "U_alice"],
                "created_at": [
                    "2025-01-01T10:00:00Z",
                    "2025-01-02T10:00:00Z",
                    "2025-01-03T10:00:00Z",
                ],
                "updated_at": ["2025-01-01T10:00:00Z"] * 3,
                "closed_at": [None, "2025-01-04T10:00:00Z", "2025-01-05T10:00:00Z"],
                "state": ["open", "closed", "closed"],
                "labels": [""] * 3,
                "title_len": [10] * 3,
                "body_len": [100] * 3,
            }
        )
        fact_issue = fact_issue.with_columns(
            [
                pl.col("created_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"),
                pl.col("updated_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"),
                pl.col("closed_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"),
            ]
        )
        fact_issue.write_parquet(curated_dir / "fact_issue.parquet")

        result = calculate_leaderboards(curated_dir, config)

        # Check issues_opened org-wide
        issues_opened_org = result.filter(
            (pl.col("metric_key") == "issues_opened") & (pl.col("scope") == "org")
        )
        assert len(issues_opened_org) == 2
        alice_opened = issues_opened_org.filter(pl.col("user_id") == "U_alice")
        assert alice_opened["value"].item() == 2

        # Check issues_closed org-wide
        issues_closed_org = result.filter(
            (pl.col("metric_key") == "issues_closed") & (pl.col("scope") == "org")
        )
        assert len(issues_closed_org) == 2

    def test_review_metrics_calculation(self, curated_dir: Path, config: Config) -> None:
        """Test review metrics calculation (submitted, approvals, changes_requested)."""
        # Create dim_user
        dim_user = pl.DataFrame(
            {
                "user_id": ["U_alice", "U_bob"],
                "login": ["alice", "bob"],
                "type": ["User", "User"],
                "profile_url": ["", ""],
                "is_bot": [False, False],
                "bot_reason": [None, None],
                "display_name": [None, None],
            }
        )
        dim_user.write_parquet(curated_dir / "dim_user.parquet")

        # Create fact_review
        fact_review = pl.DataFrame(
            {
                "review_id": ["R_1", "R_2", "R_3", "R_4"],
                "repo_id": ["R_1", "R_1", "R_2", "R_1"],
                "pr_id": [None] * 4,
                "reviewer_user_id": ["U_alice", "U_bob", "U_alice", "U_alice"],
                "submitted_at": [
                    "2025-01-01T10:00:00Z",
                    "2025-01-02T10:00:00Z",
                    "2025-01-03T10:00:00Z",
                    "2025-01-04T10:00:00Z",
                ],
                "state": ["APPROVED", "APPROVED", "CHANGES_REQUESTED", "COMMENTED"],
                "body_len": [100] * 4,
            }
        )
        fact_review = fact_review.with_columns(
            pl.col("submitted_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ")
        )
        fact_review.write_parquet(curated_dir / "fact_review.parquet")

        result = calculate_leaderboards(curated_dir, config)

        # Check reviews_submitted org-wide
        reviews_submitted_org = result.filter(
            (pl.col("metric_key") == "reviews_submitted") & (pl.col("scope") == "org")
        )
        assert len(reviews_submitted_org) == 2
        alice_reviews = reviews_submitted_org.filter(pl.col("user_id") == "U_alice")
        assert alice_reviews["value"].item() == 3

        # Check approvals org-wide
        approvals_org = result.filter(
            (pl.col("metric_key") == "approvals") & (pl.col("scope") == "org")
        )
        assert len(approvals_org) == 2
        alice_approvals = approvals_org.filter(pl.col("user_id") == "U_alice")
        assert alice_approvals["value"].item() == 1
        bob_approvals = approvals_org.filter(pl.col("user_id") == "U_bob")
        assert bob_approvals["value"].item() == 1

        # Check changes_requested org-wide
        changes_org = result.filter(
            (pl.col("metric_key") == "changes_requested") & (pl.col("scope") == "org")
        )
        assert len(changes_org) == 1
        alice_changes = changes_org.filter(pl.col("user_id") == "U_alice")
        assert alice_changes["value"].item() == 1

    def test_comment_metrics_calculation(self, curated_dir: Path, config: Config) -> None:
        """Test comment metrics calculation (comments_total, review_comments_total)."""
        # Create dim_user
        dim_user = pl.DataFrame(
            {
                "user_id": ["U_alice", "U_bob"],
                "login": ["alice", "bob"],
                "type": ["User", "User"],
                "profile_url": ["", ""],
                "is_bot": [False, False],
                "bot_reason": [None, None],
                "display_name": [None, None],
            }
        )
        dim_user.write_parquet(curated_dir / "dim_user.parquet")

        # Create fact_issue_comment
        fact_issue_comment = pl.DataFrame(
            {
                "comment_id": ["IC_1", "IC_2"],
                "repo_id": ["R_1", "R_1"],
                "parent_type": ["issue", "pr"],
                "parent_id": ["I_1", "PR_1"],
                "author_user_id": ["U_alice", "U_bob"],
                "created_at": ["2025-01-01T10:00:00Z", "2025-01-02T10:00:00Z"],
                "body_len": [100, 200],
                "year": [2025, 2025],
            }
        )
        fact_issue_comment = fact_issue_comment.with_columns(
            pl.col("created_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ")
        )
        fact_issue_comment.write_parquet(curated_dir / "fact_issue_comment.parquet")

        # Create fact_review_comment
        fact_review_comment = pl.DataFrame(
            {
                "comment_id": ["RC_1", "RC_2", "RC_3"],
                "repo_id": ["R_1", "R_2", "R_1"],
                "pr_id": ["PR_1", "PR_2", "PR_1"],
                "author_user_id": ["U_alice", "U_alice", "U_bob"],
                "created_at": [
                    "2025-01-01T10:00:00Z",
                    "2025-01-02T10:00:00Z",
                    "2025-01-03T10:00:00Z",
                ],
                "path": ["file.py"] * 3,
                "line": [10, 20, 30],
                "body_len": [50, 75, 100],
                "year": [2025, 2025, 2025],
            }
        )
        fact_review_comment = fact_review_comment.with_columns(
            pl.col("created_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ")
        )
        fact_review_comment.write_parquet(curated_dir / "fact_review_comment.parquet")

        result = calculate_leaderboards(curated_dir, config)

        # Check review_comments_total org-wide
        review_comments_org = result.filter(
            (pl.col("metric_key") == "review_comments_total") & (pl.col("scope") == "org")
        )
        assert len(review_comments_org) == 2
        alice_review_comments = review_comments_org.filter(pl.col("user_id") == "U_alice")
        assert alice_review_comments["value"].item() == 2

        # Check comments_total org-wide (issue + review comments)
        comments_total_org = result.filter(
            (pl.col("metric_key") == "comments_total") & (pl.col("scope") == "org")
        )
        assert len(comments_total_org) == 2
        alice_total = comments_total_org.filter(pl.col("user_id") == "U_alice")
        assert alice_total["value"].item() == 3  # 1 issue + 2 review
        bob_total = comments_total_org.filter(pl.col("user_id") == "U_bob")
        assert bob_total["value"].item() == 2  # 1 issue + 1 review

    def test_humans_only_filtering(self, curated_dir: Path, config: Config) -> None:
        """Test that humans_only config filters out bots."""
        # Create dim_user with bots
        dim_user = pl.DataFrame(
            {
                "user_id": ["U_alice", "U_bot"],
                "login": ["alice", "bot[bot]"],
                "type": ["User", "Bot"],
                "profile_url": ["", ""],
                "is_bot": [False, True],
                "bot_reason": [None, "Pattern match: .*\\[bot\\]$"],
                "display_name": [None, None],
            }
        )
        dim_user.write_parquet(curated_dir / "dim_user.parquet")

        # Create fact_pull_request with both human and bot
        fact_pr = pl.DataFrame(
            {
                "pr_id": ["PR_1", "PR_2"],
                "repo_id": ["R_1", "R_1"],
                "number": [1, 2],
                "author_user_id": ["U_alice", "U_bot"],
                "created_at": ["2025-01-01T10:00:00Z", "2025-01-02T10:00:00Z"],
                "updated_at": ["2025-01-01T10:00:00Z", "2025-01-02T10:00:00Z"],
                "closed_at": [None, None],
                "merged_at": [None, None],
                "state": ["open", "open"],
                "is_draft": [False, False],
                "labels": ["", ""],
                "milestone": [None, None],
                "additions": [10, 20],
                "deletions": [5, 10],
                "changed_files": [2, 3],
                "title_len": [10, 15],
                "body_len": [100, 200],
            }
        )
        fact_pr = fact_pr.with_columns(
            [
                pl.col("created_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"),
                pl.col("updated_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"),
            ]
        )
        fact_pr.write_parquet(curated_dir / "fact_pull_request.parquet")

        result = calculate_leaderboards(curated_dir, config)

        # Only alice should appear (bot filtered)
        prs_opened_org = result.filter(
            (pl.col("metric_key") == "prs_opened") & (pl.col("scope") == "org")
        )
        assert len(prs_opened_org) == 1
        assert prs_opened_org["user_id"].item() == "U_alice"

    def test_dense_ranking(self, curated_dir: Path, config: Config) -> None:
        """Test that dense ranking works correctly (ties get same rank)."""
        # Create dim_user
        dim_user = pl.DataFrame(
            {
                "user_id": ["U_alice", "U_bob", "U_charlie"],
                "login": ["alice", "bob", "charlie"],
                "type": ["User", "User", "User"],
                "profile_url": ["", "", ""],
                "is_bot": [False, False, False],
                "bot_reason": [None, None, None],
                "display_name": [None, None, None],
            }
        )
        dim_user.write_parquet(curated_dir / "dim_user.parquet")

        # Create fact_pull_request with tied values
        fact_pr = pl.DataFrame(
            {
                "pr_id": ["PR_1", "PR_2", "PR_3", "PR_4"],
                "repo_id": ["R_1"] * 4,
                "number": [1, 2, 3, 4],
                "author_user_id": ["U_alice", "U_alice", "U_bob", "U_charlie"],
                "created_at": [
                    "2025-01-01T10:00:00Z",
                    "2025-01-02T10:00:00Z",
                    "2025-01-03T10:00:00Z",
                    "2025-01-04T10:00:00Z",
                ],
                "updated_at": ["2025-01-01T10:00:00Z"] * 4,
                "closed_at": [None] * 4,
                "merged_at": [None] * 4,
                "state": ["open"] * 4,
                "is_draft": [False] * 4,
                "labels": [""] * 4,
                "milestone": [None] * 4,
                "additions": [10] * 4,
                "deletions": [5] * 4,
                "changed_files": [2] * 4,
                "title_len": [10] * 4,
                "body_len": [100] * 4,
            }
        )
        fact_pr = fact_pr.with_columns(
            [
                pl.col("created_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"),
                pl.col("updated_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"),
            ]
        )
        fact_pr.write_parquet(curated_dir / "fact_pull_request.parquet")

        result = calculate_leaderboards(curated_dir, config)

        # Alice: 2 PRs (rank 1)
        # Bob: 1 PR (rank 2, tied with Charlie)
        # Charlie: 1 PR (rank 2, tied with Bob)
        prs_opened_org = result.filter(
            (pl.col("metric_key") == "prs_opened") & (pl.col("scope") == "org")
        ).sort("user_id")

        assert len(prs_opened_org) == 3
        ranks = prs_opened_org["rank"].to_list()
        values = prs_opened_org["value"].to_list()

        # Alice should be rank 1 with value 2
        alice_idx = prs_opened_org["user_id"].to_list().index("U_alice")
        assert ranks[alice_idx] == 1
        assert values[alice_idx] == 2

        # Bob and Charlie should both be rank 2 with value 1
        bob_idx = prs_opened_org["user_id"].to_list().index("U_bob")
        charlie_idx = prs_opened_org["user_id"].to_list().index("U_charlie")
        assert ranks[bob_idx] == 2
        assert ranks[charlie_idx] == 2
        assert values[bob_idx] == 1
        assert values[charlie_idx] == 1

    def test_deterministic_ordering(self, curated_dir: Path, config: Config) -> None:
        """Test that output is deterministically sorted."""
        # Create minimal dim_user
        dim_user = pl.DataFrame(
            {
                "user_id": ["U_alice"],
                "login": ["alice"],
                "type": ["User"],
                "profile_url": [""],
                "is_bot": [False],
                "bot_reason": [None],
                "display_name": [None],
            }
        )
        dim_user.write_parquet(curated_dir / "dim_user.parquet")

        # Create minimal fact tables
        fact_pr = pl.DataFrame(
            {
                "pr_id": ["PR_1"],
                "repo_id": ["R_1"],
                "number": [1],
                "author_user_id": ["U_alice"],
                "created_at": ["2025-01-01T10:00:00Z"],
                "updated_at": ["2025-01-01T10:00:00Z"],
                "closed_at": [None],
                "merged_at": [None],
                "state": ["open"],
                "is_draft": [False],
                "labels": [""],
                "milestone": [None],
                "additions": [10],
                "deletions": [5],
                "changed_files": [2],
                "title_len": [10],
                "body_len": [100],
            }
        )
        fact_pr = fact_pr.with_columns(
            [
                pl.col("created_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"),
                pl.col("updated_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"),
            ]
        )
        fact_pr.write_parquet(curated_dir / "fact_pull_request.parquet")

        # Run twice and compare
        result1 = calculate_leaderboards(curated_dir, config)
        result2 = calculate_leaderboards(curated_dir, config)

        # Should be identical
        assert result1.equals(result2)
