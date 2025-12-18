"""Configuration loading and validation."""

from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, model_validator


class TargetConfig(BaseModel):
    """GitHub target configuration."""

    mode: str = Field(pattern=r"^(org|user)$")
    name: str


class AuthConfig(BaseModel):
    """GitHub authentication configuration."""

    token_env: str = "GITHUB_TOKEN"


class DiscoveryConfig(BaseModel):
    """Repository discovery configuration."""

    include_forks: bool = False
    include_archived: bool = False
    visibility: str = Field(default="all", pattern=r"^(all|public|private)$")


class WindowsConfig(BaseModel):
    """Time window configuration."""

    year: int
    since: datetime
    until: datetime

    @model_validator(mode="after")
    def validate_boundaries(self) -> "WindowsConfig":
        """Validate that since/until align with year boundary."""
        expected_since = datetime(self.year, 1, 1, 0, 0, 0)
        expected_until = datetime(self.year + 1, 1, 1, 0, 0, 0)

        if self.since.replace(tzinfo=None) != expected_since:
            msg = f"since must be {expected_since.isoformat()}Z for year {self.year}"
            raise ValueError(msg)

        if self.until.replace(tzinfo=None) != expected_until:
            msg = f"until must be {expected_until.isoformat()}Z for year {self.year}"
            raise ValueError(msg)

        return self


class GitHubConfig(BaseModel):
    """GitHub configuration section."""

    target: TargetConfig
    auth: AuthConfig = Field(default_factory=AuthConfig)
    discovery: DiscoveryConfig = Field(default_factory=DiscoveryConfig)
    windows: WindowsConfig


class RateLimitConfig(BaseModel):
    """Rate limiting configuration."""

    strategy: str = Field(default="adaptive", pattern=r"^(adaptive|fixed)$")
    max_concurrency: int = Field(default=4, ge=1, le=10)
    min_sleep_seconds: float = Field(default=1.0, ge=0)
    max_sleep_seconds: float = Field(default=60.0, ge=1)
    sample_rate_limit_endpoint_every_n_requests: int = Field(default=50, ge=1)


class BotConfig(BaseModel):
    """Bot detection configuration."""

    exclude_patterns: list[str] = Field(
        default_factory=lambda: [r".*\[bot\]$", r"^dependabot$", r"^renovate\[bot\]$"]
    )
    include_overrides: list[str] = Field(default_factory=list)


class IdentityConfig(BaseModel):
    """Identity resolution configuration."""

    bots: BotConfig = Field(default_factory=BotConfig)
    humans_only: bool = True


class CollectionEnableConfig(BaseModel):
    """Enabled collectors configuration."""

    pulls: bool = True
    issues: bool = True
    reviews: bool = True
    comments: bool = True
    commits: bool = True
    hygiene: bool = True


class CommitsConfig(BaseModel):
    """Commits collection configuration."""

    include_files: bool = True
    classify_files: bool = True


class BranchProtectionConfig(BaseModel):
    """Branch protection collection configuration."""

    mode: str = Field(default="sample", pattern=r"^(skip|best_effort|sample)$")
    sample_top_repos_by: str = Field(default="prs_merged")
    sample_count: int = Field(default=25, ge=1)


class SecurityFeaturesConfig(BaseModel):
    """Security features collection configuration."""

    best_effort: bool = True


class HygieneConfig(BaseModel):
    """Hygiene collection configuration."""

    paths: list[str] = Field(
        default_factory=lambda: [
            "SECURITY.md",
            "README.md",
            "LICENSE",
            "CONTRIBUTING.md",
            "CODE_OF_CONDUCT.md",
            "CODEOWNERS",
            ".github/CODEOWNERS",
        ]
    )
    workflow_prefixes: list[str] = Field(default_factory=lambda: [".github/workflows/"])
    branch_protection: BranchProtectionConfig = Field(default_factory=BranchProtectionConfig)
    security_features: SecurityFeaturesConfig = Field(default_factory=SecurityFeaturesConfig)


class CollectionConfig(BaseModel):
    """Collection configuration section."""

    enable: CollectionEnableConfig = Field(default_factory=CollectionEnableConfig)
    commits: CommitsConfig = Field(default_factory=CommitsConfig)
    hygiene: HygieneConfig = Field(default_factory=HygieneConfig)


class StorageConfig(BaseModel):
    """Storage configuration section."""

    root: Path = Field(default=Path("./data"))
    raw_format: str = Field(default="jsonl", pattern=r"^jsonl$")
    curated_format: str = Field(default="parquet", pattern=r"^parquet$")
    dataset_version: str = Field(default="v1")


class ReportConfig(BaseModel):
    """Report configuration section."""

    title: str = "Year in Review"
    output_dir: Path = Field(default=Path("./site"))
    theme: str = Field(default="engineer_exec_toggle")
    awards_config: Path | None = None


class Config(BaseModel):
    """Root configuration model."""

    github: GitHubConfig
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    identity: IdentityConfig = Field(default_factory=IdentityConfig)
    collection: CollectionConfig = Field(default_factory=CollectionConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    report: ReportConfig = Field(default_factory=ReportConfig)


def load_config(path: Path) -> Config:
    """Load and validate configuration from YAML file.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        Validated Config object.

    Raises:
        FileNotFoundError: If the config file doesn't exist.
        ValidationError: If the config is invalid.
    """
    if not path.exists():
        msg = f"Config file not found: {path}"
        raise FileNotFoundError(msg)

    with path.open() as f:
        raw_config: dict[str, Any] = yaml.safe_load(f)

    return Config.model_validate(raw_config)
