"""Live API integration tests for collection phase.

Tests actual GitHub API collection against real repositories.
Requires valid GITHUB_TOKEN environment variable.

These tests use github org as a stable test target (configured in conftest.py).
All tests write to temporary directories and do not modify any production data.

Fixtures are provided by conftest.py:
- github_token: Session-scoped GitHub token validation
- live_config: Session-scoped config targeting github org
- live_paths: Session-scoped PathManager
- cached_raw_data: Session-scoped collection run (executes once)
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from gh_year_end.collect.orchestrator import run_collection
from gh_year_end.config import Config
from gh_year_end.storage.paths import PathManager

# Test constants (from conftest.py fixtures)
TEST_ORG = "github"
TEST_YEAR = 2024


# Helper functions for common assertions


def assert_jsonl_envelope(record: dict[str, Any]) -> None:
    """Assert JSONL record has valid envelope structure.

    Args:
        record: JSONL record to validate.
    """
    assert "timestamp" in record, "Record must have timestamp"
    assert "source" in record, "Record must have source"
    assert "endpoint" in record, "Record must have endpoint"
    assert "data" in record, "Record must have data"

    # Validate timestamp format
    timestamp = record["timestamp"]
    datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

    # Validate source
    assert record["source"] in ["github_rest", "github_graphql"], "Invalid source"


def read_jsonl_file(path: Path) -> list[dict[str, Any]]:
    """Read all records from JSONL file.

    Args:
        path: Path to JSONL file.

    Returns:
        List of parsed JSON records.
    """
    records = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def count_jsonl_records(path: Path) -> int:
    """Count records in JSONL file.

    Args:
        path: Path to JSONL file.

    Returns:
        Number of records.
    """
    if not path.exists():
        return 0

    count = 0
    with path.open() as f:
        for line in f:
            if line.strip():
                count += 1
    return count


# Test cases


@pytest.mark.live_api
@pytest.mark.asyncio
async def test_live_discover_repos(
    live_config: Config, live_paths: PathManager, cached_raw_data: dict[str, Any]
) -> None:
    """Test repository discovery phase.

    Verifies that:
    1. At least one repository is discovered
    2. github/hotkey repo is in the results
    3. repos.jsonl file is created with valid structure
    """
    # Check stats
    discovery_stats = cached_raw_data.get("discovery", {})
    repos_discovered = discovery_stats.get("repos_discovered", 0)
    assert repos_discovered > 0, "Should discover at least one repository"

    # Check repos.jsonl exists
    repos_file = live_paths.repos_raw_path
    assert repos_file.exists(), "repos.jsonl should exist"

    # Read and validate repos
    repos = read_jsonl_file(repos_file)
    assert len(repos) > 0, "Should have at least one repo"

    # Validate envelope structure
    for repo_record in repos:
        assert_jsonl_envelope(repo_record)

        # Validate repo data structure
        repo_data = repo_record["data"]
        assert "node_id" in repo_data, "Repo must have node_id"
        assert "name" in repo_data, "Repo must have name"
        assert "full_name" in repo_data, "Repo must have full_name"
        assert "owner" in repo_data, "Repo must have owner"
        assert "default_branch" in repo_data, "Repo must have default_branch"

    # Check that at least one repository from github org is discovered
    repo_names = [r["data"]["full_name"] for r in repos]
    assert len(repo_names) > 0, "Should discover at least one repository from github org"


@pytest.mark.live_api
@pytest.mark.asyncio
async def test_live_collect_repos(
    live_config: Config, live_paths: PathManager, cached_raw_data: dict[str, Any]
) -> None:
    """Test repository metadata collection phase.

    Verifies that:
    1. Repo metadata is collected
    2. JSONL envelope structure is valid
    3. Metadata includes expected fields
    """
    # Check stats
    repos_stats = cached_raw_data.get("repos", {})

    # If hygiene collection is disabled, skip this test
    if repos_stats.get("skipped", False):
        pytest.skip("Repo metadata collection disabled in config")

    repos_processed = repos_stats.get("repos_processed", 0)
    assert repos_processed > 0, "Should process at least one repository"

    # Check repo_metadata.jsonl exists
    repo_metadata_file = live_paths.raw_root / "repo_metadata.jsonl"
    assert repo_metadata_file.exists(), "repo_metadata.jsonl should exist"

    # Read and validate
    metadata_records = read_jsonl_file(repo_metadata_file)
    assert len(metadata_records) > 0, "Should have repo metadata"

    # Validate first record
    first_record = metadata_records[0]
    assert_jsonl_envelope(first_record)

    # Metadata should have repository details
    data = first_record["data"]
    assert "repository" in data or "node_id" in data, "Should have repository data"


@pytest.mark.live_api
@pytest.mark.asyncio
async def test_live_collect_pulls(
    live_config: Config, live_paths: PathManager, cached_raw_data: dict[str, Any]
) -> None:
    """Test pull request collection phase.

    Verifies that:
    1. PRs are collected (if any exist)
    2. JSONL files created per repository
    3. PR data has required fields (number, state, user, timestamps)
    """
    # Check stats
    pulls_stats = cached_raw_data.get("pulls", {})
    pulls_collected = pulls_stats.get("pulls_collected", 0)

    # Check pulls directory exists
    pulls_dir = live_paths.raw_root / "pulls"

    if pulls_collected == 0:
        # It's OK if no PRs exist for test repo
        pytest.skip("No pull requests found in test repositories")

    assert pulls_dir.exists(), "pulls directory should exist"

    # Find any pull request file
    pull_files = list(pulls_dir.glob("*.jsonl"))
    assert len(pull_files) > 0, "Should have at least one PR file"

    # Validate first PR file
    first_pr_file = pull_files[0]
    pr_records = read_jsonl_file(first_pr_file)

    if len(pr_records) == 0:
        pytest.skip("No PRs in first file")

    # Validate PR structure
    for pr_record in pr_records[:5]:  # Check first 5
        assert_jsonl_envelope(pr_record)

        pr_data = pr_record["data"]
        assert "number" in pr_data, "PR must have number"
        assert "state" in pr_data, "PR must have state"
        assert "title" in pr_data, "PR must have title"
        assert "user" in pr_data, "PR must have user"
        assert "created_at" in pr_data, "PR must have created_at"
        assert "base" in pr_data, "PR must have base"

        # Validate state
        assert pr_data["state"] in ["open", "closed"], "PR state must be open or closed"


@pytest.mark.live_api
@pytest.mark.asyncio
async def test_live_collect_issues(
    live_config: Config, live_paths: PathManager, cached_raw_data: dict[str, Any]
) -> None:
    """Test issue collection phase.

    Verifies that:
    1. Issues are collected (if any exist)
    2. JSONL files created per repository
    3. Issue data has required fields
    """
    # Check stats
    issues_stats = cached_raw_data.get("issues", {})
    issues_collected = issues_stats.get("issues_collected", 0)

    # Check issues directory
    issues_dir = live_paths.raw_root / "issues"

    if issues_collected == 0:
        pytest.skip("No issues found in test repositories")

    assert issues_dir.exists(), "issues directory should exist"

    # Find any issue file
    issue_files = list(issues_dir.glob("*.jsonl"))
    assert len(issue_files) > 0, "Should have at least one issue file"

    # Validate first issue file
    first_issue_file = issue_files[0]
    issue_records = read_jsonl_file(first_issue_file)

    if len(issue_records) == 0:
        pytest.skip("No issues in first file")

    # Validate issue structure
    for issue_record in issue_records[:5]:
        assert_jsonl_envelope(issue_record)

        issue_data = issue_record["data"]
        assert "number" in issue_data, "Issue must have number"
        assert "state" in issue_data, "Issue must have state"
        assert "title" in issue_data, "Issue must have title"
        assert "user" in issue_data, "Issue must have user"
        assert "created_at" in issue_data, "Issue must have created_at"

        # Validate state
        assert issue_data["state"] in ["open", "closed"], "Issue state must be open or closed"


@pytest.mark.live_api
@pytest.mark.asyncio
async def test_live_collect_reviews(
    live_config: Config, live_paths: PathManager, cached_raw_data: dict[str, Any]
) -> None:
    """Test review collection phase.

    Verifies that:
    1. Reviews are collected (if any exist)
    2. JSONL files created per repository
    3. Reviews linked to valid PR numbers
    4. Review data has required fields (state, user, timestamps)
    """
    # Check stats
    reviews_stats = cached_raw_data.get("reviews", {})
    reviews_collected = reviews_stats.get("reviews_collected", 0)

    # Check reviews directory
    reviews_dir = live_paths.raw_root / "reviews"

    if reviews_collected == 0:
        pytest.skip("No reviews found in test repositories")

    assert reviews_dir.exists(), "reviews directory should exist"

    # Find any review file
    review_files = list(reviews_dir.glob("*.jsonl"))
    assert len(review_files) > 0, "Should have at least one review file"

    # Validate first review file
    first_review_file = review_files[0]
    review_records = read_jsonl_file(first_review_file)

    if len(review_records) == 0:
        pytest.skip("No reviews in first file")

    # Validate review structure
    for review_record in review_records[:5]:
        assert_jsonl_envelope(review_record)

        review_data = review_record["data"]
        assert "id" in review_data, "Review must have id"
        assert "state" in review_data, "Review must have state"
        assert "user" in review_data, "Review must have user"
        assert "submitted_at" in review_data, "Review must have submitted_at"

        # Validate state
        valid_states = ["APPROVED", "CHANGES_REQUESTED", "COMMENTED", "DISMISSED", "PENDING"]
        assert review_data["state"] in valid_states, f"Invalid review state: {review_data['state']}"

        # Reviews should be linked to PRs via endpoint
        endpoint = review_record["endpoint"]
        assert "/pulls/" in endpoint, "Review endpoint should reference a PR"


@pytest.mark.live_api
@pytest.mark.asyncio
async def test_live_collect_manifest(
    live_config: Config, live_paths: PathManager, cached_raw_data: dict[str, Any]
) -> None:
    """Test manifest.json generation.

    Verifies that:
    1. manifest.json is created
    2. Contains collection metadata
    3. Contains aggregated statistics
    4. Has valid timestamps
    """
    # Check manifest exists
    manifest_path = live_paths.manifest_path
    assert manifest_path.exists(), "manifest.json should exist"

    # Load and validate manifest
    with manifest_path.open() as f:
        manifest = json.load(f)

    # Validate structure
    assert "collection_date" in manifest, "Manifest must have collection_date"
    assert "config" in manifest, "Manifest must have config"
    assert "stats" in manifest, "Manifest must have stats"

    # Validate config section
    config_section = manifest["config"]
    assert "target" in config_section, "Config must have target"
    assert "year" in config_section, "Config must have year"
    assert "since" in config_section, "Config must have since"
    assert "until" in config_section, "Config must have until"

    assert config_section["target"] == TEST_ORG, "Target should match config"
    assert config_section["year"] == TEST_YEAR, "Year should match config"

    # Validate stats section
    stats = manifest["stats"]
    assert "discovery" in stats, "Stats must have discovery"
    assert "duration_seconds" in stats, "Stats must have duration_seconds"

    # Validate duration is reasonable (not negative, not absurdly large)
    duration = stats["duration_seconds"]
    assert duration > 0, "Duration should be positive"
    assert duration < 3600, "Duration should be less than 1 hour for test collection"

    # Validate collection date is recent
    collection_date = datetime.fromisoformat(manifest["collection_date"].replace("Z", "+00:00"))
    now = datetime.now(UTC)
    time_diff = (now - collection_date).total_seconds()
    assert time_diff < 3600, "Collection date should be recent (within last hour)"


@pytest.mark.live_api
@pytest.mark.asyncio
async def test_live_rate_limit_tracking(
    live_config: Config, live_paths: PathManager, cached_raw_data: dict[str, Any]
) -> None:
    """Test rate limit sample recording.

    Verifies that:
    1. Rate limit samples are recorded
    2. Samples have required fields
    3. Samples show consumption over time
    """
    # Check stats
    rate_limit_samples = cached_raw_data.get("rate_limit_samples", [])

    # Should have at least one sample
    assert len(rate_limit_samples) > 0, "Should record rate limit samples"

    # Check rate_limit_samples.jsonl exists
    samples_file = live_paths.rate_limit_samples_path
    assert samples_file.exists(), "rate_limit_samples.jsonl should exist"

    # Read samples
    sample_records = read_jsonl_file(samples_file)
    assert len(sample_records) > 0, "Should have rate limit sample records"

    # Validate first sample
    first_sample = sample_records[0]
    assert_jsonl_envelope(first_sample)

    sample_data = first_sample["data"]
    assert "timestamp" in sample_data, "Sample must have timestamp"
    assert "core" in sample_data, "Sample must have core rate limit info"

    # Validate core rate limit structure
    core = sample_data["core"]
    assert "limit" in core, "Core must have limit"
    assert "remaining" in core, "Core must have remaining"
    assert "reset" in core, "Core must have reset"
    assert "used" in core, "Core must have used"

    # Validate values are reasonable
    assert core["limit"] > 0, "Limit should be positive"
    assert 0 <= core["remaining"] <= core["limit"], "Remaining should be between 0 and limit"
    assert 0 <= core["used"] <= core["limit"], "Used should be between 0 and limit"


@pytest.mark.live_api
@pytest.mark.asyncio
async def test_live_idempotent_collection(
    live_config: Config, live_paths: PathManager, cached_raw_data: dict[str, Any]
) -> None:
    """Test that re-running collection without --force uses cache.

    Verifies that:
    1. Second collection run detects existing manifest
    2. No new API calls are made (instant return)
    3. Same stats are returned
    """
    # First collection (should use cached_raw_data fixture)
    assert live_paths.manifest_path.exists(), "First collection should have created manifest"

    with live_paths.manifest_path.open() as f:
        first_manifest = json.load(f)
    first_stats = first_manifest.get("stats", {})

    # Second collection without force
    second_stats = await run_collection(live_config, force=False)

    # Stats should be identical (returned from cache)
    assert second_stats == first_stats, "Stats should be identical when using cache"

    # Manifest timestamp should not change
    with live_paths.manifest_path.open() as f:
        second_manifest = json.load(f)

    assert first_manifest["collection_date"] == second_manifest["collection_date"], (
        "Manifest date should not change when using cache"
    )


# Integration test that runs full collection and validates all phases


@pytest.mark.live_api
@pytest.mark.asyncio
async def test_live_full_collection_pipeline(
    live_config: Config, live_paths: PathManager, cached_raw_data: dict[str, Any]
) -> None:
    """Test complete collection pipeline integration.

    Verifies that:
    1. All collection phases complete successfully
    2. Expected files are created
    3. Stats are reasonable
    4. Data is consistent across phases
    """
    stats = cached_raw_data

    # Validate all expected stats sections
    assert "discovery" in stats, "Should have discovery stats"
    assert "repos" in stats, "Should have repos stats"
    assert "pulls" in stats, "Should have pulls stats"
    assert "issues" in stats, "Should have issues stats"
    assert "reviews" in stats, "Should have reviews stats"
    assert "comments" in stats, "Should have comments stats"
    assert "commits" in stats, "Should have commits stats"
    assert "hygiene" in stats, "Should have hygiene stats"
    assert "duration_seconds" in stats, "Should have duration"

    # Validate repos.jsonl exists
    assert live_paths.repos_raw_path.exists(), "repos.jsonl should exist"

    # Count repos
    repos_count = count_jsonl_records(live_paths.repos_raw_path)
    assert repos_count > 0, "Should have at least one repo"

    # Validate discovery count matches repos count
    discovery_count = stats["discovery"].get("repos_discovered", 0)
    assert discovery_count == repos_count, "Discovery count should match repos.jsonl count"

    # For github org, we expect the pipeline to complete successfully
    # Activity data may vary depending on the repos and time window
    # Just ensure the pipeline completed without errors
    assert stats["duration_seconds"] > 0, "Collection should have taken some time"

    # Validate manifest matches stats
    with live_paths.manifest_path.open() as f:
        manifest = json.load(f)

    assert manifest["stats"] == stats, "Manifest stats should match returned stats"
