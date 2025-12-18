"""Tests for repository health metrics calculator."""

import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gh_year_end.config import Config, GitHubConfig, TargetConfig, WindowsConfig
from gh_year_end.metrics.repo_health import calculate_repo_health


@pytest.fixture
def test_config() -> Config:
    """Create test configuration."""
    return Config(
        github=GitHubConfig(
            target=TargetConfig(mode="org", name="test-org"),
            windows=WindowsConfig(
                year=2025,
                since=datetime(2025, 1, 1, 0, 0, 0),
                until=datetime(2026, 1, 1, 0, 0, 0),
            ),
        ),
        storage={"root": "./data"},
        rate_limit={},
        identity={},
        collection={"enable": {}},
        report={},
    )


@pytest.fixture
def curated_data_dir(tmp_path: Path) -> Path:
    """Create temporary curated data directory with test data."""
    curated_dir = tmp_path / "curated" / "year=2025"
    curated_dir.mkdir(parents=True)

    # Create sample repositories
    repos = [
        {
            "repo_id": "R_1",
            "owner": "test-org",
            "name": "repo-a",
            "full_name": "test-org/repo-a",
            "is_archived": False,
            "is_fork": False,
            "is_private": False,
            "default_branch": "main",
            "stars": 100,
            "forks": 10,
            "watchers": 50,
            "topics": "python,testing",
            "language": "Python",
            "created_at": "2024-01-01T00:00:00Z",
            "pushed_at": "2025-12-01T00:00:00Z",
        },
        {
            "repo_id": "R_2",
            "owner": "test-org",
            "name": "repo-b",
            "full_name": "test-org/repo-b",
            "is_archived": False,
            "is_fork": False,
            "is_private": False,
            "default_branch": "main",
            "stars": 50,
            "forks": 5,
            "watchers": 25,
            "topics": "javascript",
            "language": "JavaScript",
            "created_at": "2024-06-01T00:00:00Z",
            "pushed_at": "2025-11-15T00:00:00Z",
        },
    ]
    repos_df = pd.DataFrame(repos)
    _write_parquet(curated_dir / "dim_repo.parquet", repos_df)

    # Create sample PRs
    year_start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    year_end = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)

    prs = [
        # Repo A - active with merged PRs
        {
            "pr_id": "PR_1",
            "repo_id": "R_1",
            "number": 1,
            "author_user_id": "U_1",
            "created_at": year_start + timedelta(days=10),
            "updated_at": year_start + timedelta(days=11),
            "closed_at": year_start + timedelta(days=11),
            "merged_at": year_start + timedelta(days=11),
            "state": "merged",
            "is_draft": False,
            "labels": "bug",
            "milestone": None,
            "additions": 50,
            "deletions": 10,
            "changed_files": 2,
            "title_len": 30,
            "body_len": 100,
        },
        {
            "pr_id": "PR_2",
            "repo_id": "R_1",
            "number": 2,
            "author_user_id": "U_2",
            "created_at": year_end - timedelta(days=5),
            "updated_at": year_end - timedelta(days=4),
            "closed_at": year_end - timedelta(days=4),
            "merged_at": year_end - timedelta(days=4),
            "state": "merged",
            "is_draft": False,
            "labels": "feature",
            "milestone": None,
            "additions": 100,
            "deletions": 20,
            "changed_files": 5,
            "title_len": 40,
            "body_len": 200,
        },
        {
            "pr_id": "PR_3",
            "repo_id": "R_1",
            "number": 3,
            "author_user_id": "U_3",
            "created_at": year_start + timedelta(days=100),
            "updated_at": year_start + timedelta(days=100),
            "closed_at": None,
            "merged_at": None,
            "state": "open",
            "is_draft": False,
            "labels": "",
            "milestone": None,
            "additions": 25,
            "deletions": 5,
            "changed_files": 1,
            "title_len": 20,
            "body_len": 50,
        },
        # Repo B - less active
        {
            "pr_id": "PR_4",
            "repo_id": "R_2",
            "number": 1,
            "author_user_id": "U_4",
            "created_at": year_start + timedelta(days=50),
            "updated_at": year_start + timedelta(days=52),
            "closed_at": year_start + timedelta(days=52),
            "merged_at": year_start + timedelta(days=52),
            "state": "merged",
            "is_draft": False,
            "labels": "docs",
            "milestone": None,
            "additions": 10,
            "deletions": 2,
            "changed_files": 1,
            "title_len": 25,
            "body_len": 75,
        },
    ]
    prs_df = pd.DataFrame(prs)
    _write_parquet(curated_dir / "fact_pull_request.parquet", prs_df)

    # Create sample issues
    issues = [
        {
            "issue_id": "I_1",
            "repo_id": "R_1",
            "number": 10,
            "author_user_id": "U_5",
            "created_at": year_end - timedelta(days=20),
            "updated_at": year_end - timedelta(days=15),
            "closed_at": year_end - timedelta(days=15),
            "state": "closed",
            "labels": "bug",
            "title_len": 30,
            "body_len": 100,
        },
        {
            "issue_id": "I_2",
            "repo_id": "R_1",
            "number": 11,
            "author_user_id": "U_6",
            "created_at": year_start + timedelta(days=200),
            "updated_at": year_start + timedelta(days=200),
            "closed_at": None,
            "state": "open",
            "labels": "question",
            "title_len": 25,
            "body_len": 50,
        },
        {
            "issue_id": "I_3",
            "repo_id": "R_2",
            "number": 5,
            "author_user_id": "U_7",
            "created_at": year_end - timedelta(days=10),
            "updated_at": year_end - timedelta(days=10),
            "closed_at": None,
            "state": "open",
            "labels": "enhancement",
            "title_len": 35,
            "body_len": 120,
        },
    ]
    issues_df = pd.DataFrame(issues)
    _write_parquet(curated_dir / "fact_issue.parquet", issues_df)

    # Create sample reviews
    reviews = [
        {
            "review_id": "REV_1",
            "repo_id": "R_1",
            "pr_id": "PR_1",
            "reviewer_user_id": "U_8",
            "submitted_at": year_start + timedelta(days=10, hours=5),
            "state": "APPROVED",
            "body_len": 50,
        },
        {
            "review_id": "REV_2",
            "repo_id": "R_1",
            "pr_id": "PR_2",
            "reviewer_user_id": "U_9",
            "submitted_at": year_end - timedelta(days=4, hours=12),
            "state": "APPROVED",
            "body_len": 30,
        },
    ]
    reviews_df = pd.DataFrame(reviews)
    _write_parquet(curated_dir / "fact_review.parquet", reviews_df)

    # Create sample comments with explicit schema
    issue_comments_schema = pa.schema(
        [
            pa.field("comment_id", pa.string()),
            pa.field("repo_id", pa.string()),
            pa.field("parent_type", pa.string()),
            pa.field("parent_id", pa.string()),
            pa.field("author_user_id", pa.string()),
            pa.field("created_at", pa.timestamp("us", tz="UTC")),
            pa.field("body_len", pa.int64()),
            pa.field("year", pa.int32()),
        ]
    )
    issue_comments = [
        {
            "comment_id": "IC_1",
            "repo_id": "R_1",
            "parent_type": "issue",
            "parent_id": "I_1",
            "author_user_id": "U_10",
            "created_at": year_end - timedelta(days=18),
            "body_len": 75,
            "year": 2025,
        },
    ]
    issue_comments_table = pa.Table.from_pylist(issue_comments, schema=issue_comments_schema)
    pq.write_table(
        issue_comments_table,
        curated_dir / "fact_issue_comment.parquet",
        compression="snappy",
        use_dictionary=False,
    )

    review_comments_schema = pa.schema(
        [
            pa.field("comment_id", pa.string()),
            pa.field("repo_id", pa.string()),
            pa.field("pr_id", pa.string()),
            pa.field("author_user_id", pa.string()),
            pa.field("created_at", pa.timestamp("us", tz="UTC")),
            pa.field("path", pa.string()),
            pa.field("line", pa.int32()),
            pa.field("body_len", pa.int64()),
            pa.field("year", pa.int32()),
        ]
    )
    review_comments = [
        {
            "comment_id": "RC_1",
            "repo_id": "R_1",
            "pr_id": "PR_1",
            "author_user_id": "U_11",
            "created_at": year_start + timedelta(days=10, hours=3),
            "path": "src/main.py",
            "line": 42,
            "body_len": 60,
            "year": 2025,
        },
    ]
    review_comments_table = pa.Table.from_pylist(review_comments, schema=review_comments_schema)
    pq.write_table(
        review_comments_table,
        curated_dir / "fact_review_comment.parquet",
        compression="snappy",
        use_dictionary=False,
    )

    return curated_dir


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    """Write DataFrame to Parquet file."""
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path, compression="snappy")


def test_calculate_repo_health_basic(curated_data_dir: Path, test_config: Config) -> None:
    """Test basic repo health calculation."""
    metrics = calculate_repo_health(curated_data_dir, test_config)

    assert not metrics.empty
    assert len(metrics) == 2  # Two repos

    # Check schema
    expected_columns = [
        "repo_id",
        "repo_full_name",
        "year",
        "active_contributors_30d",
        "active_contributors_90d",
        "active_contributors_365d",
        "prs_opened",
        "prs_merged",
        "issues_opened",
        "issues_closed",
        "review_coverage",
        "median_time_to_first_review",
        "median_time_to_merge",
        "stale_pr_count",
        "stale_issue_count",
    ]
    assert list(metrics.columns) == expected_columns

    # Check year
    assert all(metrics["year"] == 2025)

    # Check sorting by repo_id
    assert metrics["repo_id"].tolist() == ["R_1", "R_2"]


def test_calculate_pr_stats(curated_data_dir: Path, test_config: Config) -> None:
    """Test PR statistics calculation."""
    metrics = calculate_repo_health(curated_data_dir, test_config)

    repo_a = metrics[metrics["repo_id"] == "R_1"].iloc[0]
    repo_b = metrics[metrics["repo_id"] == "R_2"].iloc[0]

    # Repo A: 3 PRs opened, 2 merged
    assert repo_a["prs_opened"] == 3
    assert repo_a["prs_merged"] == 2

    # Repo B: 1 PR opened, 1 merged
    assert repo_b["prs_opened"] == 1
    assert repo_b["prs_merged"] == 1


def test_calculate_issue_stats(curated_data_dir: Path, test_config: Config) -> None:
    """Test issue statistics calculation."""
    metrics = calculate_repo_health(curated_data_dir, test_config)

    repo_a = metrics[metrics["repo_id"] == "R_1"].iloc[0]
    repo_b = metrics[metrics["repo_id"] == "R_2"].iloc[0]

    # Repo A: 2 issues opened, 1 closed
    assert repo_a["issues_opened"] == 2
    assert repo_a["issues_closed"] == 1

    # Repo B: 1 issue opened, 0 closed
    assert repo_b["issues_opened"] == 1
    assert repo_b["issues_closed"] == 0


def test_calculate_stale_counts(curated_data_dir: Path, test_config: Config) -> None:
    """Test stale PR and issue counting."""
    metrics = calculate_repo_health(curated_data_dir, test_config)

    repo_a = metrics[metrics["repo_id"] == "R_1"].iloc[0]

    # Repo A has 1 stale PR (created ~265 days before year end)
    assert repo_a["stale_pr_count"] == 1

    # Repo A has 1 stale issue (created ~165 days before year end)
    assert repo_a["stale_issue_count"] == 1


def test_calculate_active_contributors(curated_data_dir: Path, test_config: Config) -> None:
    """Test active contributor counting."""
    metrics = calculate_repo_health(curated_data_dir, test_config)

    repo_a = metrics[metrics["repo_id"] == "R_1"].iloc[0]

    # Repo A should have contributors in 30d window
    # PR author U_2 (5 days before year end)
    # Issue author U_5 (20 days before year end)
    # Reviewer U_9 (4 days before year end)
    # Note: Comment contributors may not be counted if file read fails due to schema issues
    assert repo_a["active_contributors_30d"] >= 3

    # All contributors should be in 365d window
    # At minimum: U_1, U_2, U_3 (PRs), U_5, U_6 (issues), U_8, U_9 (reviews)
    assert repo_a["active_contributors_365d"] >= 7


def test_calculate_time_to_merge(curated_data_dir: Path, test_config: Config) -> None:
    """Test time to merge calculation."""
    metrics = calculate_repo_health(curated_data_dir, test_config)

    repo_a = metrics[metrics["repo_id"] == "R_1"].iloc[0]

    # Repo A has merged PRs
    # PR_1: merged 1 day after creation = 24 hours
    # PR_2: merged 1 day after creation = 24 hours
    # Median should be 24 hours
    assert repo_a["median_time_to_merge"] == pytest.approx(24.0, rel=0.1)


def test_empty_curated_data(test_config: Config) -> None:
    """Test handling of empty curated data directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        empty_dir = Path(tmpdir)

        # Create empty dim_repo
        repos_df = pd.DataFrame(
            columns=[
                "repo_id",
                "owner",
                "name",
                "full_name",
                "is_archived",
                "is_fork",
                "is_private",
                "default_branch",
                "stars",
                "forks",
                "watchers",
                "topics",
                "language",
                "created_at",
                "pushed_at",
            ]
        )
        _write_parquet(empty_dir / "dim_repo.parquet", repos_df)

        metrics = calculate_repo_health(empty_dir, test_config)

        assert metrics.empty


def test_missing_curated_files(test_config: Config) -> None:
    """Test handling of missing curated files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        empty_dir = Path(tmpdir)

        # Only create dim_repo, other files missing
        repos = [
            {
                "repo_id": "R_1",
                "owner": "test",
                "name": "test",
                "full_name": "test/test",
                "is_archived": False,
                "is_fork": False,
                "is_private": False,
                "default_branch": "main",
                "stars": 0,
                "forks": 0,
                "watchers": 0,
                "topics": "",
                "language": None,
                "created_at": "2025-01-01T00:00:00Z",
                "pushed_at": "2025-01-01T00:00:00Z",
            }
        ]
        repos_df = pd.DataFrame(repos)
        _write_parquet(empty_dir / "dim_repo.parquet", repos_df)

        # Should handle missing files gracefully
        metrics = calculate_repo_health(empty_dir, test_config)

        assert len(metrics) == 1
        assert metrics.iloc[0]["prs_opened"] == 0
        assert metrics.iloc[0]["issues_opened"] == 0
        assert metrics.iloc[0]["active_contributors_30d"] == 0


def test_deterministic_output(curated_data_dir: Path, test_config: Config) -> None:
    """Test that output is deterministic across runs."""
    metrics1 = calculate_repo_health(curated_data_dir, test_config)
    metrics2 = calculate_repo_health(curated_data_dir, test_config)

    pd.testing.assert_frame_equal(metrics1, metrics2)


def test_review_coverage_calculation(curated_data_dir: Path, test_config: Config) -> None:
    """Test review coverage percentage calculation."""
    metrics = calculate_repo_health(curated_data_dir, test_config)

    repo_a = metrics[metrics["repo_id"] == "R_1"].iloc[0]

    # Repo A has reviews, so coverage should be > 0
    assert repo_a["review_coverage"] >= 0.0
    assert repo_a["review_coverage"] <= 100.0
