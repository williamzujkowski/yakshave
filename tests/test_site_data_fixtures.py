"""Tests for sample site data fixtures.

Validates that the minimal test dataset is well-formed and complete.
"""


def test_sample_site_data_dir_exists(sample_site_data_dir):
    """Test that sample site data directory exists."""
    assert sample_site_data_dir.exists()
    assert sample_site_data_dir.is_dir()


def test_all_data_files_present(sample_site_data_dir):
    """Test that all required data files are present."""
    required_files = [
        "summary.json",
        "leaderboards.json",
        "timeseries.json",
        "repo_health.json",
        "hygiene_scores.json",
        "awards.json",
    ]

    for filename in required_files:
        file_path = sample_site_data_dir / filename
        assert file_path.exists(), f"Missing required file: {filename}"


def test_summary_structure(load_sample_summary):
    """Test summary.json has required fields."""
    assert "year" in load_sample_summary
    assert "target" in load_sample_summary
    assert "total_contributors" in load_sample_summary
    assert "total_prs_merged" in load_sample_summary
    assert "total_repos" in load_sample_summary
    assert "hygiene" in load_sample_summary

    # Validate types
    assert isinstance(load_sample_summary["year"], int)
    assert isinstance(load_sample_summary["total_contributors"], int)
    assert isinstance(load_sample_summary["hygiene"], dict)


def test_leaderboards_structure(load_sample_leaderboards):
    """Test leaderboards.json has required structure."""
    assert "leaderboards" in load_sample_leaderboards
    assert "metrics_available" in load_sample_leaderboards

    leaderboards = load_sample_leaderboards["leaderboards"]
    assert len(leaderboards) > 0

    # Check first metric has org and repos keys
    first_metric = next(iter(leaderboards.values()))
    assert "org" in first_metric
    assert "repos" in first_metric

    # Validate org leaderboard entry structure
    if first_metric["org"]:
        entry = first_metric["org"][0]
        assert "rank" in entry
        assert "user_id" in entry
        assert "login" in entry
        assert "value" in entry


def test_timeseries_structure(load_sample_timeseries):
    """Test timeseries.json has required structure."""
    assert "timeseries" in load_sample_timeseries
    assert "period_types" in load_sample_timeseries
    assert "metrics_available" in load_sample_timeseries

    timeseries = load_sample_timeseries["timeseries"]
    assert len(timeseries) > 0

    # Check first period type has metrics
    first_period = next(iter(timeseries.values()))
    assert len(first_period) > 0

    # Check first metric has org/repos and data points
    first_metric = next(iter(first_period.values()))
    assert "org" in first_metric

    if first_metric["org"]:
        point = first_metric["org"][0]
        assert "period_start" in point
        assert "period_end" in point
        assert "value" in point


def test_repo_health_structure(load_sample_repo_health):
    """Test repo_health.json has required structure."""
    assert "repos" in load_sample_repo_health
    assert "total_repos" in load_sample_repo_health

    repos = load_sample_repo_health["repos"]
    assert len(repos) > 0

    # Check first repo entry
    first_repo = next(iter(repos.values()))
    required_fields = [
        "repo_full_name",
        "year",
        "prs_opened",
        "prs_merged",
        "issues_opened",
        "issues_closed",
        "review_coverage",
        "stale_pr_count",
        "stale_issue_count",
    ]

    for field in required_fields:
        assert field in first_repo, f"Missing field: {field}"


def test_hygiene_scores_structure(load_sample_hygiene_scores):
    """Test hygiene_scores.json has required structure."""
    assert "repos" in load_sample_hygiene_scores
    assert "summary" in load_sample_hygiene_scores

    repos = load_sample_hygiene_scores["repos"]
    assert len(repos) > 0

    # Check first repo entry
    first_repo = next(iter(repos.values()))
    required_fields = [
        "repo_full_name",
        "year",
        "score",
        "has_readme",
        "has_license",
        "has_ci_workflows",
    ]

    for field in required_fields:
        assert field in first_repo, f"Missing field: {field}"

    # Validate summary
    summary = load_sample_hygiene_scores["summary"]
    assert "total_repos" in summary
    assert "average_score" in summary
    assert "min_score" in summary
    assert "max_score" in summary


def test_awards_structure(load_sample_awards):
    """Test awards.json has required structure."""
    assert "awards" in load_sample_awards
    assert "categories" in load_sample_awards
    assert "total_awards" in load_sample_awards

    awards = load_sample_awards["awards"]
    assert len(awards) > 0

    # Check categories
    categories = load_sample_awards["categories"]
    assert "individual" in categories
    assert "repository" in categories

    # Check first award entry
    first_category = next(iter(awards.values()))
    if first_category:
        award = first_category[0]
        assert "award_key" in award
        assert "title" in award
        assert "description" in award
        assert "category" in award


def test_all_sample_data_loads(all_sample_data):
    """Test that all sample data files can be loaded at once."""
    assert len(all_sample_data) == 6
    assert "summary" in all_sample_data
    assert "leaderboards" in all_sample_data
    assert "timeseries" in all_sample_data
    assert "repo_health" in all_sample_data
    assert "hygiene_scores" in all_sample_data
    assert "awards" in all_sample_data


def test_setup_test_site_data(setup_test_site_data):
    """Test that site data can be copied to temp directory."""
    site_data_dir = setup_test_site_data

    assert site_data_dir.exists()
    assert site_data_dir.is_dir()

    # Check all files were copied
    required_files = [
        "summary.json",
        "leaderboards.json",
        "timeseries.json",
        "repo_health.json",
        "hygiene_scores.json",
        "awards.json",
    ]

    for filename in required_files:
        file_path = site_data_dir / filename
        assert file_path.exists(), f"Missing file in temp site: {filename}"


def test_data_consistency(all_sample_data):
    """Test that data is consistent across files."""
    summary = all_sample_data["summary"]
    repo_health = all_sample_data["repo_health"]
    hygiene_scores = all_sample_data["hygiene_scores"]

    # Repo counts should match
    assert summary["total_repos"] == repo_health["total_repos"]
    assert summary["total_repos"] == hygiene_scores["summary"]["total_repos"]

    # Hygiene score should match
    assert summary["hygiene"]["average_score"] == hygiene_scores["summary"]["average_score"]
    assert summary["hygiene"]["min_score"] == hygiene_scores["summary"]["min_score"]
    assert summary["hygiene"]["max_score"] == hygiene_scores["summary"]["max_score"]


def test_leaderboard_data_quality(load_sample_leaderboards):
    """Test that leaderboard data has valid rankings and values."""
    leaderboards = load_sample_leaderboards["leaderboards"]

    for metric_key, metric_data in leaderboards.items():
        # Check org leaderboard
        org_leaders = metric_data["org"]
        if org_leaders:
            # Ranks should be sequential starting from 1
            ranks = [entry["rank"] for entry in org_leaders]
            assert ranks[0] == 1, f"First rank should be 1 for {metric_key}"

            # Values should be descending
            values = [entry["value"] for entry in org_leaders]
            assert values == sorted(values, reverse=True), (
                f"Values should be descending for {metric_key}"
            )

            # User IDs should be unique
            user_ids = [entry["user_id"] for entry in org_leaders]
            assert len(user_ids) == len(set(user_ids)), (
                f"User IDs should be unique for {metric_key}"
            )


def test_timeseries_data_quality(load_sample_timeseries):
    """Test that timeseries data has valid temporal ordering."""
    timeseries = load_sample_timeseries["timeseries"]

    for period_type, period_data in timeseries.items():
        for metric_key, metric_data in period_data.items():
            org_series = metric_data["org"]
            if org_series:
                # Period starts should be chronologically ordered
                period_starts = [point["period_start"] for point in org_series]
                assert period_starts == sorted(period_starts), (
                    f"Periods should be chronologically ordered for {period_type}/{metric_key}"
                )

                # All values should be non-negative
                values = [point["value"] for point in org_series]
                assert all(v >= 0 for v in values), (
                    f"Values should be non-negative for {metric_key}"
                )
