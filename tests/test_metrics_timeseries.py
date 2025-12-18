"""Tests for time series metrics calculator."""

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gh_year_end.config import (
    AuthConfig,
    CollectionConfig,
    Config,
    DiscoveryConfig,
    GitHubConfig,
    RateLimitConfig,
    ReportConfig,
    StorageConfig,
    TargetConfig,
    WindowsConfig,
)
from gh_year_end.metrics.timeseries import (
    _get_month_end,
    _get_month_start,
    _get_week_end,
    _get_week_start,
    calculate_time_series,
    save_time_series_metrics,
)


@pytest.fixture
def test_config() -> Config:
    """Create test configuration."""
    return Config(
        github=GitHubConfig(
            target=TargetConfig(mode="org", name="test-org"),
            auth=AuthConfig(token_env="GITHUB_TOKEN"),
            discovery=DiscoveryConfig(),
            windows=WindowsConfig(
                year=2025,
                since=datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC),
                until=datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
            ),
        ),
        rate_limit=RateLimitConfig(),
        collection=CollectionConfig(),
        storage=StorageConfig(),
        report=ReportConfig(),
    )


@pytest.fixture
def sample_pr_data(tmp_path: Path) -> Path:
    """Create sample PR Parquet file."""
    data = [
        {
            "pr_id": "PR_1",
            "repo_id": "REPO_A",
            "number": 1,
            "author_user_id": "USER_1",
            "created_at": datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC),
            "updated_at": datetime(2025, 1, 16, 10, 0, 0, tzinfo=UTC),
            "closed_at": None,
            "merged_at": datetime(2025, 1, 16, 10, 0, 0, tzinfo=UTC),
            "state": "merged",
            "is_draft": False,
            "labels": "",
            "milestone": None,
            "additions": 10,
            "deletions": 5,
            "changed_files": 2,
            "title_len": 20,
            "body_len": 100,
        },
        {
            "pr_id": "PR_2",
            "repo_id": "REPO_A",
            "number": 2,
            "author_user_id": "USER_2",
            "created_at": datetime(2025, 1, 20, 14, 0, 0, tzinfo=UTC),
            "updated_at": datetime(2025, 1, 21, 14, 0, 0, tzinfo=UTC),
            "closed_at": datetime(2025, 1, 21, 14, 0, 0, tzinfo=UTC),
            "merged_at": None,
            "state": "closed",
            "is_draft": False,
            "labels": "",
            "milestone": None,
            "additions": 5,
            "deletions": 2,
            "changed_files": 1,
            "title_len": 15,
            "body_len": 50,
        },
        {
            "pr_id": "PR_3",
            "repo_id": "REPO_B",
            "number": 1,
            "author_user_id": "USER_1",
            "created_at": datetime(2025, 2, 1, 9, 0, 0, tzinfo=UTC),
            "updated_at": datetime(2025, 2, 2, 9, 0, 0, tzinfo=UTC),
            "closed_at": None,
            "merged_at": datetime(2025, 2, 2, 9, 0, 0, tzinfo=UTC),
            "state": "merged",
            "is_draft": False,
            "labels": "",
            "milestone": None,
            "additions": 20,
            "deletions": 10,
            "changed_files": 3,
            "title_len": 25,
            "body_len": 150,
        },
    ]

    df = pd.DataFrame(data)
    schema = pa.schema(
        [
            pa.field("pr_id", pa.string()),
            pa.field("repo_id", pa.string()),
            pa.field("number", pa.int64()),
            pa.field("author_user_id", pa.string()),
            pa.field("created_at", pa.timestamp("us", tz="UTC")),
            pa.field("updated_at", pa.timestamp("us", tz="UTC")),
            pa.field("closed_at", pa.timestamp("us", tz="UTC")),
            pa.field("merged_at", pa.timestamp("us", tz="UTC")),
            pa.field("state", pa.string()),
            pa.field("is_draft", pa.bool_()),
            pa.field("labels", pa.string()),
            pa.field("milestone", pa.string()),
            pa.field("additions", pa.int64()),
            pa.field("deletions", pa.int64()),
            pa.field("changed_files", pa.int64()),
            pa.field("title_len", pa.int64()),
            pa.field("body_len", pa.int64()),
        ]
    )

    table = pa.Table.from_pandas(df, schema=schema)
    output_path = tmp_path / "fact_pull_request.parquet"
    pq.write_table(table, output_path)
    return tmp_path


@pytest.fixture
def sample_issue_data(tmp_path: Path) -> Path:
    """Create sample issue Parquet file."""
    data = [
        {
            "issue_id": "ISSUE_1",
            "repo_id": "REPO_A",
            "number": 10,
            "author_user_id": "USER_1",
            "created_at": datetime(2025, 1, 10, 8, 0, 0, tzinfo=UTC),
            "updated_at": datetime(2025, 1, 12, 8, 0, 0, tzinfo=UTC),
            "closed_at": datetime(2025, 1, 12, 8, 0, 0, tzinfo=UTC),
            "state": "closed",
            "labels": "bug",
            "title_len": 30,
            "body_len": 200,
        },
        {
            "issue_id": "ISSUE_2",
            "repo_id": "REPO_B",
            "number": 5,
            "author_user_id": "USER_2",
            "created_at": datetime(2025, 1, 25, 11, 0, 0, tzinfo=UTC),
            "updated_at": datetime(2025, 1, 25, 11, 0, 0, tzinfo=UTC),
            "closed_at": None,
            "state": "open",
            "labels": "enhancement",
            "title_len": 25,
            "body_len": 150,
        },
    ]

    df = pd.DataFrame(data)
    schema = pa.schema(
        [
            pa.field("issue_id", pa.string()),
            pa.field("repo_id", pa.string()),
            pa.field("number", pa.int64()),
            pa.field("author_user_id", pa.string()),
            pa.field("created_at", pa.timestamp("us", tz="UTC")),
            pa.field("updated_at", pa.timestamp("us", tz="UTC")),
            pa.field("closed_at", pa.timestamp("us", tz="UTC")),
            pa.field("state", pa.string()),
            pa.field("labels", pa.string()),
            pa.field("title_len", pa.int64()),
            pa.field("body_len", pa.int64()),
        ]
    )

    table = pa.Table.from_pandas(df, schema=schema)
    output_path = tmp_path / "fact_issue.parquet"
    pq.write_table(table, output_path)
    return tmp_path


class TestWeekHelpers:
    """Test week boundary calculation helpers."""

    def test_week_start_monday(self):
        """Test week start for a Monday."""
        # 2025-01-06 is a Monday
        d = date(2025, 1, 6)
        assert _get_week_start(d) == date(2025, 1, 6)

    def test_week_start_sunday(self):
        """Test week start for a Sunday."""
        # 2025-01-12 is a Sunday
        d = date(2025, 1, 12)
        assert _get_week_start(d) == date(2025, 1, 6)  # Previous Monday

    def test_week_start_wednesday(self):
        """Test week start for a Wednesday."""
        # 2025-01-08 is a Wednesday
        d = date(2025, 1, 8)
        assert _get_week_start(d) == date(2025, 1, 6)  # Monday of same week

    def test_week_end_monday(self):
        """Test week end for a Monday."""
        # 2025-01-06 is a Monday
        d = date(2025, 1, 6)
        assert _get_week_end(d) == date(2025, 1, 12)  # Sunday of same week

    def test_week_end_sunday(self):
        """Test week end for a Sunday."""
        # 2025-01-12 is a Sunday
        d = date(2025, 1, 12)
        assert _get_week_end(d) == date(2025, 1, 12)  # Same day

    def test_week_boundaries_span_7_days(self):
        """Test that week boundaries always span exactly 7 days."""
        test_dates = [
            date(2025, 1, 1),  # Wednesday
            date(2025, 2, 15),  # Saturday
            date(2025, 6, 30),  # Monday
            date(2025, 12, 31),  # Wednesday
        ]

        for d in test_dates:
            start = _get_week_start(d)
            end = _get_week_end(d)
            assert (end - start).days == 6  # 6 days difference = 7 day span


class TestMonthHelpers:
    """Test month boundary calculation helpers."""

    def test_month_start_first_day(self):
        """Test month start for first day of month."""
        d = date(2025, 1, 1)
        assert _get_month_start(d) == date(2025, 1, 1)

    def test_month_start_mid_month(self):
        """Test month start for middle of month."""
        d = date(2025, 1, 15)
        assert _get_month_start(d) == date(2025, 1, 1)

    def test_month_start_last_day(self):
        """Test month start for last day of month."""
        d = date(2025, 1, 31)
        assert _get_month_start(d) == date(2025, 1, 1)

    def test_month_end_first_day(self):
        """Test month end for first day of month."""
        d = date(2025, 1, 1)
        assert _get_month_end(d) == date(2025, 1, 31)

    def test_month_end_mid_month(self):
        """Test month end for middle of month."""
        d = date(2025, 1, 15)
        assert _get_month_end(d) == date(2025, 1, 31)

    def test_month_end_last_day(self):
        """Test month end for last day of month."""
        d = date(2025, 1, 31)
        assert _get_month_end(d) == date(2025, 1, 31)

    def test_month_end_february_non_leap_year(self):
        """Test month end for February in non-leap year."""
        d = date(2025, 2, 15)
        assert _get_month_end(d) == date(2025, 2, 28)

    def test_month_end_february_leap_year(self):
        """Test month end for February in leap year."""
        d = date(2024, 2, 15)
        assert _get_month_end(d) == date(2024, 2, 29)

    def test_month_end_december(self):
        """Test month end for December."""
        d = date(2025, 12, 15)
        assert _get_month_end(d) == date(2025, 12, 31)


class TestCalculateTimeSeries:
    """Test time series calculation."""

    def test_empty_curated_path(self, tmp_path: Path, test_config: Config):
        """Test with no curated files."""
        df = calculate_time_series(tmp_path, test_config)
        assert df.empty
        assert list(df.columns) == [
            "year",
            "period_type",
            "period_start",
            "period_end",
            "scope",
            "repo_id",
            "metric_key",
            "value",
        ]

    def test_pr_metrics(self, sample_pr_data: Path, test_config: Config):
        """Test PR time series metrics."""
        df = calculate_time_series(sample_pr_data, test_config)

        assert not df.empty
        assert "prs_opened" in df["metric_key"].values
        assert "prs_merged" in df["metric_key"].values
        assert "prs_closed" in df["metric_key"].values

        # Check org-wide metrics exist
        org_metrics = df[df["scope"] == "org"]
        assert not org_metrics.empty

        # Check per-repo metrics exist
        repo_metrics = df[df["scope"] == "repo"]
        assert not repo_metrics.empty
        assert "REPO_A" in repo_metrics["repo_id"].values
        assert "REPO_B" in repo_metrics["repo_id"].values

        # Check both week and month periods exist
        assert "week" in df["period_type"].values
        assert "month" in df["period_type"].values

    def test_issue_metrics(self, sample_issue_data: Path, test_config: Config):
        """Test issue time series metrics."""
        df = calculate_time_series(sample_issue_data, test_config)

        assert not df.empty
        assert "issues_opened" in df["metric_key"].values
        assert "issues_closed" in df["metric_key"].values

        # Verify issue opened count (summed across all periods)
        # Two issues created, counted in both weekly and monthly periods
        issues_opened_weekly = df[
            (df["metric_key"] == "issues_opened")
            & (df["scope"] == "org")
            & (df["period_type"] == "week")
        ]
        assert issues_opened_weekly["value"].sum() == 2  # Two issues created

        # Verify issue closed count (summed across all periods)
        issues_closed_weekly = df[
            (df["metric_key"] == "issues_closed")
            & (df["scope"] == "org")
            & (df["period_type"] == "week")
        ]
        assert issues_closed_weekly["value"].sum() == 1  # One issue closed

    def test_deterministic_ordering(self, sample_pr_data: Path, test_config: Config):
        """Test that results are deterministically ordered."""
        df1 = calculate_time_series(sample_pr_data, test_config)
        df2 = calculate_time_series(sample_pr_data, test_config)

        # DataFrames should be identical
        pd.testing.assert_frame_equal(df1, df2)

    def test_period_boundaries(self, sample_pr_data: Path, test_config: Config):
        """Test that period boundaries are correctly calculated."""
        df = calculate_time_series(sample_pr_data, test_config)

        # Check that period_start <= period_end for all records
        assert (df["period_start"] <= df["period_end"]).all()

        # Check weekly periods span Monday to Sunday
        weekly = df[df["period_type"] == "week"]
        for _, row in weekly.iterrows():
            assert row["period_start"].isoweekday() == 1  # Monday
            assert row["period_end"].isoweekday() == 7  # Sunday
            assert (row["period_end"] - row["period_start"]).days == 6

        # Check monthly periods span first to last day
        monthly = df[df["period_type"] == "month"]
        for _, row in monthly.iterrows():
            assert row["period_start"].day == 1
            # period_end should be last day of month
            # Verify by checking next day is in next month
            next_day = row["period_end"] + pd.Timedelta(days=1)
            assert next_day.day == 1


class TestSaveTimeSeries:
    """Test saving time series to Parquet."""

    def test_save_empty_dataframe(self, tmp_path: Path):
        """Test saving an empty DataFrame."""
        df = pd.DataFrame(
            columns=[
                "year",
                "period_type",
                "period_start",
                "period_end",
                "scope",
                "repo_id",
                "metric_key",
                "value",
            ]
        )

        output_path = tmp_path / "metrics_time_series.parquet"
        save_time_series_metrics(df, output_path)

        assert output_path.exists()

        # Read back and verify schema
        table = pq.read_table(output_path)
        assert len(table) == 0
        assert "year" in table.column_names
        assert "period_type" in table.column_names
        assert "metric_key" in table.column_names

    def test_save_and_load_roundtrip(
        self, tmp_path: Path, sample_pr_data: Path, test_config: Config
    ):
        """Test that saved metrics can be loaded back correctly."""
        df_original = calculate_time_series(sample_pr_data, test_config)

        output_path = tmp_path / "metrics_time_series.parquet"
        save_time_series_metrics(df_original, output_path)

        # Load back
        table = pq.read_table(output_path)
        df_loaded = table.to_pandas()

        # Verify all columns present
        assert set(df_loaded.columns) == set(df_original.columns)

        # Verify data types (PyArrow uses numpy dtypes, not pandas nullable dtypes)
        assert pd.api.types.is_integer_dtype(df_loaded["year"])
        assert pd.api.types.is_integer_dtype(df_loaded["value"])

        # Verify record count
        assert len(df_loaded) == len(df_original)
