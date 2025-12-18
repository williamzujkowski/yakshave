"""Path management for data storage."""

from pathlib import Path
from typing import Literal

from gh_year_end.config import Config


class PathManager:
    """Manages paths for raw, curated, metrics, and site data.

    All paths follow a consistent structure:
    - Raw: data/raw/year=YYYY/source=github/target=<name>/
    - Curated: data/curated/year=YYYY/
    - Metrics: data/metrics/year=YYYY/
    - Site: site/year=YYYY/
    """

    def __init__(self, config: Config) -> None:
        """Initialize path manager with configuration.

        Args:
            config: Application configuration.
        """
        self.config = config
        self.root = Path(config.storage.root)
        self.year = config.github.windows.year
        self.target = config.github.target.name

    @property
    def raw_root(self) -> Path:
        """Root path for raw data."""
        return self.root / f"raw/year={self.year}/source=github/target={self.target}"

    @property
    def curated_root(self) -> Path:
        """Root path for curated Parquet tables."""
        return self.root / f"curated/year={self.year}"

    @property
    def metrics_root(self) -> Path:
        """Root path for metrics Parquet tables."""
        return self.root / f"metrics/year={self.year}"

    @property
    def site_root(self) -> Path:
        """Root path for generated site."""
        return Path(self.config.report.output_dir) / f"year={self.year}"

    # Raw data paths

    @property
    def manifest_path(self) -> Path:
        """Path to the collection manifest."""
        return self.raw_root / "manifest.json"

    @property
    def rate_limit_samples_path(self) -> Path:
        """Path to rate limit samples JSONL."""
        return self.raw_root / "rate_limit_samples.jsonl"

    @property
    def repos_raw_path(self) -> Path:
        """Path to raw repos JSONL."""
        return self.raw_root / "repos.jsonl"

    def pulls_raw_path(self, repo_full_name: str) -> Path:
        """Path to raw pulls JSONL for a repo."""
        return self.raw_root / "pulls" / f"{self._safe_name(repo_full_name)}.jsonl"

    def issues_raw_path(self, repo_full_name: str) -> Path:
        """Path to raw issues JSONL for a repo."""
        return self.raw_root / "issues" / f"{self._safe_name(repo_full_name)}.jsonl"

    def reviews_raw_path(self, repo_full_name: str) -> Path:
        """Path to raw reviews JSONL for a repo."""
        return self.raw_root / "reviews" / f"{self._safe_name(repo_full_name)}.jsonl"

    def issue_comments_raw_path(self, repo_full_name: str) -> Path:
        """Path to raw issue comments JSONL for a repo."""
        return self.raw_root / "issue_comments" / f"{self._safe_name(repo_full_name)}.jsonl"

    def review_comments_raw_path(self, repo_full_name: str) -> Path:
        """Path to raw review comments JSONL for a repo."""
        return self.raw_root / "review_comments" / f"{self._safe_name(repo_full_name)}.jsonl"

    def commits_raw_path(self, repo_full_name: str) -> Path:
        """Path to raw commits JSONL for a repo."""
        return self.raw_root / "commits" / f"{self._safe_name(repo_full_name)}.jsonl"

    def repo_tree_raw_path(self, repo_full_name: str) -> Path:
        """Path to raw repo tree JSONL for a repo."""
        return self.raw_root / "repo_tree" / f"{self._safe_name(repo_full_name)}.jsonl"

    def branch_protection_raw_path(self, repo_full_name: str) -> Path:
        """Path to raw branch protection JSONL for a repo."""
        return self.raw_root / "branch_protection" / f"{self._safe_name(repo_full_name)}.jsonl"

    def security_features_raw_path(self, repo_full_name: str) -> Path:
        """Path to raw security features JSONL for a repo."""
        return self.raw_root / "security_features" / f"{self._safe_name(repo_full_name)}.jsonl"

    # Curated data paths

    def curated_path(
        self,
        table: Literal[
            "dim_user",
            "dim_repo",
            "dim_time",
            "dim_identity_rule",
            "fact_pull_request",
            "fact_issue",
            "fact_review",
            "fact_issue_comment",
            "fact_review_comment",
            "fact_commit",
            "fact_commit_file",
            "fact_repo_files_presence",
            "fact_repo_hygiene",
            "fact_repo_security_features",
        ],
    ) -> Path:
        """Path to a curated Parquet table."""
        return self.curated_root / f"{table}.parquet"

    # Convenience properties for common curated tables

    @property
    def dim_user_path(self) -> Path:
        """Path to dim_user Parquet table."""
        return self.curated_path("dim_user")

    @property
    def dim_repo_path(self) -> Path:
        """Path to dim_repo Parquet table."""
        return self.curated_path("dim_repo")

    @property
    def dim_identity_rule_path(self) -> Path:
        """Path to dim_identity_rule Parquet table."""
        return self.curated_path("dim_identity_rule")

    @property
    def fact_pull_request_path(self) -> Path:
        """Path to fact_pull_request Parquet table."""
        return self.curated_path("fact_pull_request")

    @property
    def fact_issue_path(self) -> Path:
        """Path to fact_issue Parquet table."""
        return self.curated_path("fact_issue")

    @property
    def fact_review_path(self) -> Path:
        """Path to fact_review Parquet table."""
        return self.curated_path("fact_review")

    @property
    def fact_issue_comment_path(self) -> Path:
        """Path to fact_issue_comment Parquet table."""
        return self.curated_path("fact_issue_comment")

    @property
    def fact_review_comment_path(self) -> Path:
        """Path to fact_review_comment Parquet table."""
        return self.curated_path("fact_review_comment")

    @property
    def fact_commit_path(self) -> Path:
        """Path to fact_commit Parquet table."""
        return self.curated_path("fact_commit")

    @property
    def fact_commit_file_path(self) -> Path:
        """Path to fact_commit_file Parquet table."""
        return self.curated_path("fact_commit_file")

    @property
    def fact_repo_files_presence_path(self) -> Path:
        """Path to fact_repo_files_presence Parquet table."""
        return self.curated_path("fact_repo_files_presence")

    @property
    def fact_repo_hygiene_path(self) -> Path:
        """Path to fact_repo_hygiene Parquet table."""
        return self.curated_path("fact_repo_hygiene")

    @property
    def fact_repo_security_features_path(self) -> Path:
        """Path to fact_repo_security_features Parquet table."""
        return self.curated_path("fact_repo_security_features")

    # Metrics paths

    def metrics_path(
        self,
        table: Literal[
            "metrics_leaderboard",
            "metrics_repo_health",
            "metrics_time_series",
            "metrics_repo_hygiene_score",
            "metrics_awards",
        ],
    ) -> Path:
        """Path to a metrics Parquet table."""
        return self.metrics_root / f"{table}.parquet"

    # Convenience properties for metrics tables

    @property
    def metrics_leaderboard_path(self) -> Path:
        """Path to metrics_leaderboard Parquet table."""
        return self.metrics_path("metrics_leaderboard")

    @property
    def metrics_repo_health_path(self) -> Path:
        """Path to metrics_repo_health Parquet table."""
        return self.metrics_path("metrics_repo_health")

    @property
    def metrics_time_series_path(self) -> Path:
        """Path to metrics_time_series Parquet table."""
        return self.metrics_path("metrics_time_series")

    @property
    def metrics_repo_hygiene_score_path(self) -> Path:
        """Path to metrics_repo_hygiene_score Parquet table."""
        return self.metrics_path("metrics_repo_hygiene_score")

    @property
    def metrics_awards_path(self) -> Path:
        """Path to metrics_awards Parquet table."""
        return self.metrics_path("metrics_awards")

    # Site paths

    @property
    def site_data_path(self) -> Path:
        """Path to site data directory (JSON exports)."""
        return self.site_root / "data"

    @property
    def site_assets_path(self) -> Path:
        """Path to site assets directory."""
        return self.site_root / "assets"

    # Utility methods

    def ensure_directories(self) -> None:
        """Create all required directories."""
        directories = [
            self.raw_root,
            self.raw_root / "pulls",
            self.raw_root / "issues",
            self.raw_root / "reviews",
            self.raw_root / "issue_comments",
            self.raw_root / "review_comments",
            self.raw_root / "commits",
            self.raw_root / "repo_tree",
            self.raw_root / "branch_protection",
            self.raw_root / "security_features",
            self.curated_root,
            self.metrics_root,
            self.site_root,
            self.site_data_path,
            self.site_assets_path,
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _safe_name(name: str) -> str:
        """Convert a name to a safe filename.

        Replaces / with __ to handle org/repo names.
        """
        return name.replace("/", "__")
