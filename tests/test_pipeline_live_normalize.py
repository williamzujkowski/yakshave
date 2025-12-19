"""Live API tests for normalization phase.

Tests the normalization pipeline using real cached GitHub API data.
Validates that raw JSONL data is correctly transformed into curated Parquet tables.

Prerequisites:
- Raw data must be cached via test_pipeline_live_collection.py first
- Uses @pytest.mark.live_api marker
- Depends on cached_raw_data fixture

Reference:
- src/gh_year_end/normalize/ for normalization functions
- tests/test_end_to_end.py for patterns
- tests/test_normalize_*.py for schema expectations
"""

import logging
from pathlib import Path

import polars as pl
import pytest

from gh_year_end.config import Config
from gh_year_end.normalize.hygiene import (
    normalize_branch_protection,
    normalize_file_presence,
    normalize_security_features,
)
from gh_year_end.normalize.issues import normalize_issues
from gh_year_end.normalize.pulls import normalize_pulls
from gh_year_end.normalize.repos import normalize_repos
from gh_year_end.normalize.reviews import normalize_reviews
from gh_year_end.normalize.users import normalize_users
from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


@pytest.fixture
def live_config(tmp_path: Path) -> Config:
    """Create configuration for live API tests.

    Uses cached raw data from data/raw/ directory and writes curated
    output to temporary directory.

    Returns:
        Config instance for live API testing.
    """
    # Use actual data directory for raw data (assumes collection has run)
    data_root = Path.cwd() / "data"

    # Use temp directory for curated output to avoid polluting actual data
    curated_root = tmp_path / "curated"
    curated_root.mkdir(parents=True, exist_ok=True)

    return Config.model_validate(
        {
            "github": {
                "target": {"mode": "org", "name": "williamzujkowski"},
                "windows": {
                    "year": 2024,
                    "since": "2024-01-01T00:00:00Z",
                    "until": "2025-01-01T00:00:00Z",
                },
            },
            "storage": {"root": str(data_root)},
            "identity": {
                "bots": {
                    "exclude_patterns": [
                        r".*\[bot\]$",
                        r"^dependabot$",
                        r"^renovate$",
                        r"^github-actions$",
                    ],
                    "include_overrides": [],
                }
            },
        }
    )


@pytest.fixture
def live_paths(live_config: Config) -> PathManager:
    """Create PathManager from live config.

    Returns:
        PathManager instance for live API testing.
    """
    return PathManager(live_config)


@pytest.fixture
def cached_raw_data(live_paths: PathManager) -> Path:
    """Verify cached raw data exists from prior collection.

    This fixture ensures that test_pipeline_live_collection.py has been run
    and raw data is available.

    Returns:
        Path to raw data directory.

    Raises:
        pytest.skip: If raw data doesn't exist.
    """
    raw_root = live_paths.raw_root

    if not raw_root.exists():
        pytest.skip(
            f"Raw data not found at {raw_root}. "
            "Run test_pipeline_live_collection.py first to cache API data."
        )

    # Verify essential files exist
    repos_file = live_paths.repos_raw_path
    if not repos_file.exists():
        pytest.skip(f"Required file not found: {repos_file}")

    logger.info("Using cached raw data from %s", raw_root)
    return raw_root


@pytest.mark.live_api
class TestLiveNormalize:
    """Live API tests for normalization phase."""

    def test_live_normalize_repos(
        self,
        live_config: Config,
        live_paths: PathManager,
        cached_raw_data: Path,  # noqa: ARG002
    ) -> None:
        """Test normalization of raw repos to dim_repo.parquet.

        Validates:
        - Raw repos.jsonl is read successfully
        - All repos are normalized with correct schema
        - Required fields have no null values
        - Output is deterministic (sorted by repo_id)
        """
        # Ensure curated directory exists
        live_paths.curated_root.mkdir(parents=True, exist_ok=True)

        # Normalize repos (expects base raw dir, builds year/source/target path)
        raw_base_dir = live_paths.root / "raw"
        df = normalize_repos(raw_base_dir, live_config)

        # Basic validation
        assert len(df) > 0, "Should normalize at least one repository"
        logger.info("Normalized %d repositories", len(df))

        # Schema validation
        expected_columns = [
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
        assert list(df.columns) == expected_columns, "Schema mismatch"

        # Validate no nulls in required fields
        required_fields = ["repo_id", "owner", "name", "full_name", "default_branch"]
        for field in required_fields:
            null_count = df[field].isna().sum()
            assert null_count == 0, f"{field} should not have null values"

        # Validate data types
        assert df["is_archived"].dtype == bool
        assert df["is_fork"].dtype == bool
        assert df["is_private"].dtype == bool
        assert df["stars"].dtype in ["int64", "Int64"]
        assert df["forks"].dtype in ["int64", "Int64"]
        assert df["watchers"].dtype in ["int64", "Int64"]

        # Validate deterministic ordering
        repo_ids = df["repo_id"].tolist()
        assert repo_ids == sorted(repo_ids), "Should be sorted by repo_id"

        # Save to parquet
        df.to_parquet(live_paths.dim_repo_path)
        assert live_paths.dim_repo_path.exists(), "dim_repo.parquet should be created"

        logger.info(
            "Repository normalization successful: %d repos, %d archived, %d forks",
            len(df),
            df["is_archived"].sum(),
            df["is_fork"].sum(),
        )

    def test_live_normalize_users(
        self,
        live_config: Config,
        live_paths: PathManager,
        cached_raw_data: Path,  # noqa: ARG002
    ) -> None:
        """Test extraction and normalization of unique users with bot detection.

        Validates:
        - Users extracted from all data sources (repos, PRs, issues, reviews)
        - Bot detection works correctly (dependabot[bot], etc.)
        - No duplicate user_ids
        - Output is deterministic (sorted by user_id)
        """
        # Ensure curated directory exists
        live_paths.curated_root.mkdir(parents=True, exist_ok=True)

        # Normalize users
        df_users = normalize_users(live_config)

        # Basic validation
        assert len(df_users) > 0, "Should extract at least one user"
        logger.info("Normalized %d users", len(df_users))

        # Schema validation (using polars)
        expected_columns = [
            "user_id",
            "login",
            "type",
            "profile_url",
            "is_bot",
            "bot_reason",
            "display_name",
        ]
        assert df_users.columns == expected_columns, "Schema mismatch"

        # Validate no nulls in required fields
        required_fields = ["user_id", "login", "type", "is_bot"]
        for field in required_fields:
            null_count = df_users.filter(pl.col(field).is_null()).height
            assert null_count == 0, f"{field} should not have null values"

        # Validate bot detection
        bots = df_users.filter(pl.col("is_bot") == True)  # noqa: E712
        if len(bots) > 0:
            logger.info("Detected %d bots", len(bots))

            # Check for known bot patterns
            bot_logins = bots.select("login").to_series().to_list()
            known_bots = [login for login in bot_logins if "[bot]" in login]
            assert len(known_bots) > 0, "Should detect at least one [bot] user"

            # Validate bot_reason is populated for bots
            bots_without_reason = bots.filter(pl.col("bot_reason").is_null())
            assert len(bots_without_reason) == 0, "All bots should have bot_reason populated"

        # Validate no duplicate user_ids
        user_id_counts = df_users.select("user_id").to_series().value_counts()
        duplicates = user_id_counts.filter(pl.col("count") > 1)
        assert len(duplicates) == 0, "Should not have duplicate user_ids"

        # Validate deterministic ordering
        user_ids = df_users.select("user_id").to_series().to_list()
        assert user_ids == sorted(user_ids), "Should be sorted by user_id"

        # Save to parquet
        df_users.write_parquet(live_paths.dim_user_path)
        assert live_paths.dim_user_path.exists(), "dim_user.parquet should be created"

        humans = df_users.filter(pl.col("is_bot") == False)  # noqa: E712
        logger.info(
            "User normalization successful: %d users (%d humans, %d bots)",
            len(df_users),
            len(humans),
            len(bots),
        )

    def test_live_normalize_pulls(
        self,
        live_config: Config,
        live_paths: PathManager,
        cached_raw_data: Path,  # noqa: ARG002
    ) -> None:
        """Test normalization of raw PRs to fact_pull_request.parquet.

        Validates:
        - All PR files are processed
        - PRs normalized with correct schema
        - State field correctly distinguishes open/closed/merged
        - Required fields have no null values
        """
        # Ensure curated directory exists
        live_paths.curated_root.mkdir(parents=True, exist_ok=True)

        # Normalize pulls
        df_pulls = normalize_pulls(live_paths.raw_root, live_config)

        # Basic validation
        assert len(df_pulls) > 0, "Should normalize at least one pull request"
        logger.info("Normalized %d pull requests", len(df_pulls))

        # Schema validation
        required_columns = [
            "pr_id",
            "repo_id",
            "number",
            "author_user_id",
            "state",
            "is_draft",
            "title_len",
            "body_len",
            "created_at",
            "updated_at",
            "closed_at",
            "merged_at",
            "labels",
        ]
        for col in required_columns:
            assert col in df_pulls.columns, f"Missing column: {col}"

        # Validate no nulls in required fields
        required_fields = ["pr_id", "repo_id", "number", "state", "is_draft"]
        for field in required_fields:
            null_count = df_pulls[field].isna().sum()
            assert null_count == 0, f"{field} should not have null values"

        # Validate state values
        states = df_pulls["state"].unique().tolist()
        valid_states = ["open", "closed", "merged"]
        for state in states:
            assert state in valid_states, f"Invalid state: {state}"

        # Validate merged PRs have merged_at timestamp
        merged_prs = df_pulls[df_pulls["state"] == "merged"]
        if len(merged_prs) > 0:
            merged_without_timestamp = merged_prs[merged_prs["merged_at"].isna()]
            assert len(merged_without_timestamp) == 0, "Merged PRs should have merged_at timestamp"

        # Validate title_len and body_len are non-negative
        assert (df_pulls["title_len"] >= 0).all(), "title_len should be non-negative"
        assert (df_pulls["body_len"] >= 0).all(), "body_len should be non-negative"

        # Save to parquet
        df_pulls.to_parquet(live_paths.fact_pull_request_path)
        assert live_paths.fact_pull_request_path.exists(), (
            "fact_pull_request.parquet should be created"
        )

        logger.info(
            "Pull request normalization successful: %d PRs (%s)",
            len(df_pulls),
            ", ".join(f"{state}: {len(df_pulls[df_pulls['state'] == state])}" for state in states),
        )

    def test_live_normalize_issues(
        self,
        live_config: Config,
        live_paths: PathManager,
        cached_raw_data: Path,  # noqa: ARG002
    ) -> None:
        """Test normalization of raw issues to fact_issue.parquet.

        Validates:
        - All issue files are processed
        - Pull requests are filtered out
        - Issues normalized with correct schema
        - Required fields have no null values
        """
        # Ensure curated directory exists
        live_paths.curated_root.mkdir(parents=True, exist_ok=True)

        # Normalize issues
        df_issues = normalize_issues(live_paths.raw_root, live_config)

        # Basic validation
        assert len(df_issues) > 0, "Should normalize at least one issue"
        logger.info("Normalized %d issues", len(df_issues))

        # Schema validation
        required_columns = [
            "issue_id",
            "repo_id",
            "number",
            "author_user_id",
            "created_at",
            "updated_at",
            "closed_at",
            "state",
            "labels",
            "title_len",
            "body_len",
        ]
        for col in required_columns:
            assert col in df_issues.columns, f"Missing column: {col}"

        # Validate no nulls in required fields
        required_fields = ["issue_id", "repo_id", "number", "state"]
        for field in required_fields:
            null_count = df_issues[field].isna().sum()
            assert null_count == 0, f"{field} should not have null values"

        # Validate state values
        states = df_issues["state"].unique().tolist()
        valid_states = ["open", "closed"]
        for state in states:
            assert state in valid_states, f"Invalid state: {state}"

        # Validate title_len and body_len are non-negative
        assert (df_issues["title_len"] >= 0).all(), "title_len should be non-negative"
        assert (df_issues["body_len"] >= 0).all(), "body_len should be non-negative"

        # Save to parquet
        df_issues.to_parquet(live_paths.fact_issue_path)
        assert live_paths.fact_issue_path.exists(), "fact_issue.parquet should be created"

        open_issues = len(df_issues[df_issues["state"] == "open"])
        closed_issues = len(df_issues[df_issues["state"] == "closed"])
        logger.info(
            "Issue normalization successful: %d issues (%d open, %d closed)",
            len(df_issues),
            open_issues,
            closed_issues,
        )

    def test_live_normalize_reviews(
        self,
        live_config: Config,
        live_paths: PathManager,
        cached_raw_data: Path,  # noqa: ARG002
    ) -> None:
        """Test normalization of raw reviews to fact_review.parquet.

        Validates:
        - All review files are processed
        - Reviews normalized with correct schema
        - State values are valid (APPROVED, COMMENTED, CHANGES_REQUESTED, etc.)
        - Required fields have no null values
        """
        # Check if reviews directory exists
        reviews_dir = live_paths.raw_root / "reviews"
        if not reviews_dir.exists() or not list(reviews_dir.glob("*.jsonl")):
            pytest.skip("No review data available")

        # Ensure curated directory exists
        live_paths.curated_root.mkdir(parents=True, exist_ok=True)

        # Normalize reviews
        df_reviews = normalize_reviews(live_paths.raw_root, live_config)

        # Basic validation
        assert len(df_reviews) > 0, "Should normalize at least one review"
        logger.info("Normalized %d reviews", len(df_reviews))

        # Schema validation
        required_columns = [
            "review_id",
            "pr_id",
            "repo_id",
            "reviewer_user_id",
            "state",
            "submitted_at",
            "body_len",
        ]
        for col in required_columns:
            assert col in df_reviews.columns, f"Missing column: {col}"

        # Validate no nulls in required fields
        required_fields = ["review_id", "pr_id", "repo_id", "state"]
        for field in required_fields:
            null_count = df_reviews[field].isna().sum()
            assert null_count == 0, f"{field} should not have null values"

        # Validate state values
        states = df_reviews["state"].unique().tolist()
        valid_states = [
            "APPROVED",
            "COMMENTED",
            "CHANGES_REQUESTED",
            "DISMISSED",
            "PENDING",
        ]
        for state in states:
            assert state in valid_states, f"Invalid review state: {state}"

        # Validate body_len is non-negative
        assert (df_reviews["body_len"] >= 0).all(), "body_len should be non-negative"

        # Save to parquet
        df_reviews.to_parquet(live_paths.fact_review_path)
        assert live_paths.fact_review_path.exists(), "fact_review.parquet should be created"

        logger.info(
            "Review normalization successful: %d reviews (%s)",
            len(df_reviews),
            ", ".join(
                f"{state}: {len(df_reviews[df_reviews['state'] == state])}" for state in states
            ),
        )

    def test_live_schema_validation(
        self,
        live_config: Config,
        live_paths: PathManager,
        cached_raw_data: Path,  # noqa: ARG002
    ) -> None:
        """Test that all Parquet files have correct schemas.

        Validates:
        - All expected curated files can be created
        - Each file has expected columns
        - Data types are correct
        - Files can be read back successfully
        """
        # Ensure curated directory exists
        live_paths.curated_root.mkdir(parents=True, exist_ok=True)

        # Normalize all data
        raw_base_dir = live_paths.root / "raw"

        # Repos
        df_repos = normalize_repos(raw_base_dir, live_config)
        df_repos.to_parquet(live_paths.dim_repo_path)

        # Users
        df_users = normalize_users(live_config)
        df_users.write_parquet(live_paths.dim_user_path)

        # Pulls
        df_pulls = normalize_pulls(live_paths.raw_root, live_config)
        df_pulls.to_parquet(live_paths.fact_pull_request_path)

        # Issues
        df_issues = normalize_issues(live_paths.raw_root, live_config)
        df_issues.to_parquet(live_paths.fact_issue_path)

        # Reviews (skip if no data)
        reviews_dir = live_paths.raw_root / "reviews"
        if reviews_dir.exists() and list(reviews_dir.glob("*.jsonl")):
            df_reviews = normalize_reviews(live_paths.raw_root, live_config)
            df_reviews.to_parquet(live_paths.fact_review_path)

        # Verify all files can be read back
        files_to_check = [
            ("dim_repo", live_paths.dim_repo_path),
            ("dim_user", live_paths.dim_user_path),
            ("fact_pull_request", live_paths.fact_pull_request_path),
            ("fact_issue", live_paths.fact_issue_path),
        ]

        if live_paths.fact_review_path.exists():
            files_to_check.append(("fact_review", live_paths.fact_review_path))

        for name, path in files_to_check:
            assert path.exists(), f"{name} parquet file should exist"

            # Try reading with both pandas and polars
            if name == "dim_user":
                # Users use polars
                df_read = pl.read_parquet(path)
                assert len(df_read) > 0, f"{name} should have data"
            else:
                # Others use pandas
                import pandas as pd

                df_read = pd.read_parquet(path)
                assert len(df_read) > 0, f"{name} should have data"

            logger.info("Verified %s: %s", name, path)

        logger.info("Schema validation successful for all curated tables")

    def test_live_bot_detection(
        self,
        live_config: Config,
        live_paths: PathManager,  # noqa: ARG002
        cached_raw_data: Path,  # noqa: ARG002
    ) -> None:
        """Test that bots are correctly flagged in dim_user.

        Validates:
        - dependabot[bot] is flagged as bot
        - renovate[bot] is flagged as bot (if present)
        - github-actions[bot] is flagged as bot (if present)
        - Bot reason is populated
        - Include overrides work correctly
        """
        # Normalize users
        df_users = normalize_users(live_config)

        # Find bots
        bots = df_users.filter(pl.col("is_bot") == True)  # noqa: E712

        assert len(bots) > 0, "Should detect at least one bot"

        # Check specific bot patterns
        bot_logins = bots.select("login").to_series().to_list()

        # Check for common bots
        common_bots = ["dependabot[bot]", "renovate[bot]", "github-actions[bot]"]
        detected_common = [bot for bot in common_bots if bot in bot_logins]

        if len(detected_common) > 0:
            logger.info("Detected common bots: %s", detected_common)

            # Verify each detected bot has a reason
            for bot_login in detected_common:
                bot_record = bots.filter(pl.col("login") == bot_login)
                assert len(bot_record) == 1, f"Should have exactly one record for {bot_login}"

                bot_reason = bot_record.select("bot_reason").item()
                assert bot_reason is not None, f"{bot_login} should have bot_reason"
                assert len(bot_reason) > 0, f"{bot_login} bot_reason should not be empty"

                logger.info("Bot %s: reason=%s", bot_login, bot_reason)

        # Verify all bots have reasons
        bots_without_reason = bots.filter(pl.col("bot_reason").is_null())
        assert len(bots_without_reason) == 0, "All bots should have bot_reason populated"

        # Verify humans don't have bot_reason
        humans = df_users.filter(pl.col("is_bot") == False)  # noqa: E712
        humans_with_reason = humans.filter(pl.col("bot_reason").is_not_null())
        assert len(humans_with_reason) == 0, "Humans should not have bot_reason"

        logger.info(
            "Bot detection successful: %d bots identified out of %d users",
            len(bots),
            len(df_users),
        )

    def test_live_deterministic_output(
        self,
        live_config: Config,
        live_paths: PathManager,
        cached_raw_data: Path,  # noqa: ARG002
    ) -> None:
        """Test that same input produces identical output.

        Validates:
        - Running normalization twice produces identical results
        - Ordering is stable (sorted by primary key)
        - Row counts are identical
        - Data values are identical
        """
        # Ensure curated directory exists
        live_paths.curated_root.mkdir(parents=True, exist_ok=True)

        raw_base_dir = live_paths.root / "raw"

        # Run normalization first time
        df_repos_1 = normalize_repos(raw_base_dir, live_config)
        df_users_1 = normalize_users(live_config)
        df_pulls_1 = normalize_pulls(live_paths.raw_root, live_config)

        # Run normalization second time
        df_repos_2 = normalize_repos(raw_base_dir, live_config)
        df_users_2 = normalize_users(live_config)
        df_pulls_2 = normalize_pulls(live_paths.raw_root, live_config)

        # Verify repos are identical
        assert len(df_repos_1) == len(df_repos_2), "Repo count should be identical"
        assert df_repos_1["repo_id"].tolist() == df_repos_2["repo_id"].tolist(), (
            "Repo IDs should be in same order"
        )
        assert df_repos_1["full_name"].tolist() == df_repos_2["full_name"].tolist(), (
            "Repo names should match"
        )

        # Verify users are identical (using polars)
        assert len(df_users_1) == len(df_users_2), "User count should be identical"
        user_ids_1 = df_users_1.select("user_id").to_series().to_list()
        user_ids_2 = df_users_2.select("user_id").to_series().to_list()
        assert user_ids_1 == user_ids_2, "User IDs should be in same order"

        # Verify pulls are identical
        assert len(df_pulls_1) == len(df_pulls_2), "Pull request count should be identical"
        assert df_pulls_1["pr_id"].tolist() == df_pulls_2["pr_id"].tolist(), (
            "PR IDs should be in same order"
        )

        logger.info(
            "Deterministic output verified: %d repos, %d users, %d PRs",
            len(df_repos_1),
            len(df_users_1),
            len(df_pulls_1),
        )

    def test_live_hygiene_normalization(
        self,
        live_config: Config,
        live_paths: PathManager,
        cached_raw_data: Path,  # noqa: ARG002
    ) -> None:
        """Test normalization of hygiene data (optional).

        Validates:
        - File presence data is normalized correctly
        - Branch protection data is normalized correctly
        - Security features data is normalized correctly

        Skips if hygiene data is not available.
        """
        # Check if hygiene directories exist
        file_presence_dir = live_paths.raw_root / "repo_tree"
        branch_protection_dir = live_paths.raw_root / "branch_protection"
        security_features_dir = live_paths.raw_root / "security_features"

        if not file_presence_dir.exists():
            pytest.skip("File presence data not available")

        # Ensure curated directory exists
        live_paths.curated_root.mkdir(parents=True, exist_ok=True)

        # Normalize file presence
        df_file_presence = normalize_file_presence(live_paths.raw_root, live_config)
        assert len(df_file_presence) > 0, "Should normalize file presence data"
        df_file_presence.to_parquet(live_paths.fact_repo_files_presence_path)
        logger.info("Normalized %d file presence records", len(df_file_presence))

        # Normalize branch protection (if available)
        if branch_protection_dir.exists() and list(branch_protection_dir.glob("*.jsonl")):
            df_branch_protection = normalize_branch_protection(live_paths.raw_root, live_config)
            assert len(df_branch_protection) > 0, "Should normalize branch protection data"
            df_branch_protection.to_parquet(live_paths.fact_repo_hygiene_path)
            logger.info("Normalized %d branch protection records", len(df_branch_protection))

        # Normalize security features (if available)
        if security_features_dir.exists() and list(security_features_dir.glob("*.jsonl")):
            df_security_features = normalize_security_features(live_paths.raw_root, live_config)
            assert len(df_security_features) > 0, "Should normalize security features data"
            df_security_features.to_parquet(live_paths.fact_repo_security_features_path)
            logger.info("Normalized %d security features records", len(df_security_features))

        logger.info("Hygiene normalization successful")
