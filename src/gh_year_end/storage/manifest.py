"""Manifest management for tracking collection runs."""

import hashlib
import json
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from gh_year_end import __version__


@dataclass
class EndpointStats:
    """Statistics for a single endpoint."""

    endpoint: str
    records_fetched: int = 0
    requests_made: int = 0
    failures: int = 0
    retries: int = 0


@dataclass
class Manifest:
    """Collection manifest tracking run metadata and statistics.

    The manifest is written to manifest.json in the raw data directory
    and tracks everything needed for reproducibility and auditing.
    """

    run_id: str = field(default_factory=lambda: str(uuid4()))
    tool_version: str = field(default=__version__)
    git_commit: str = field(default="")
    config_digest: str = field(default="")
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = field(default=None)
    target_mode: str = field(default="")
    target_name: str = field(default="")
    year: int = field(default=0)
    repos_processed: list[str] = field(default_factory=list)
    endpoint_stats: dict[str, EndpointStats] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Initialize git commit if not set."""
        if not self.git_commit:
            self.git_commit = self._get_git_commit()

    @staticmethod
    def _get_git_commit() -> str:
        """Get current git commit hash."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()[:12]
        except (subprocess.CalledProcessError, FileNotFoundError):
            return "unknown"

    def set_config_digest(self, config_path: Path) -> None:
        """Calculate and set config file digest.

        Args:
            config_path: Path to the config file.
        """
        with config_path.open("rb") as f:
            self.config_digest = hashlib.sha256(f.read()).hexdigest()[:16]

    def record_endpoint(
        self,
        endpoint: str,
        records: int = 0,
        requests: int = 0,
        failures: int = 0,
        retries: int = 0,
    ) -> None:
        """Record statistics for an endpoint.

        Args:
            endpoint: Endpoint name (e.g., "pulls", "issues").
            records: Number of records fetched.
            requests: Number of requests made.
            failures: Number of failures.
            retries: Number of retries.
        """
        if endpoint not in self.endpoint_stats:
            self.endpoint_stats[endpoint] = EndpointStats(endpoint=endpoint)

        stats = self.endpoint_stats[endpoint]
        stats.records_fetched += records
        stats.requests_made += requests
        stats.failures += failures
        stats.retries += retries

    def add_repo(self, repo_full_name: str) -> None:
        """Add a processed repo to the list.

        Args:
            repo_full_name: Full name of the repo (e.g., "owner/repo").
        """
        if repo_full_name not in self.repos_processed:
            self.repos_processed.append(repo_full_name)

    def add_error(self, error: str) -> None:
        """Add an error to the errors list.

        Args:
            error: Error message.
        """
        self.errors.append(error)

    def finish(self) -> None:
        """Mark the manifest as finished."""
        self.finished_at = datetime.now(UTC)
        # Sort repos for deterministic output
        self.repos_processed.sort()

    def to_dict(self) -> dict[str, Any]:
        """Convert manifest to dictionary for JSON serialization."""
        return {
            "run_id": self.run_id,
            "tool_version": self.tool_version,
            "git_commit": self.git_commit,
            "config_digest": self.config_digest,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "target": {
                "mode": self.target_mode,
                "name": self.target_name,
            },
            "year": self.year,
            "repos_processed": self.repos_processed,
            "repos_count": len(self.repos_processed),
            "endpoint_stats": {
                name: {
                    "records_fetched": stats.records_fetched,
                    "requests_made": stats.requests_made,
                    "failures": stats.failures,
                    "retries": stats.retries,
                }
                for name, stats in self.endpoint_stats.items()
            },
            "totals": {
                "records_fetched": sum(s.records_fetched for s in self.endpoint_stats.values()),
                "requests_made": sum(s.requests_made for s in self.endpoint_stats.values()),
                "failures": sum(s.failures for s in self.endpoint_stats.values()),
                "retries": sum(s.retries for s in self.endpoint_stats.values()),
            },
            "errors": self.errors,
            "errors_count": len(self.errors),
        }

    def save(self, path: Path) -> None:
        """Save manifest to JSON file.

        Args:
            path: Path to save the manifest.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        """Load manifest from JSON file.

        Args:
            path: Path to the manifest file.

        Returns:
            Loaded Manifest instance.

        Raises:
            FileNotFoundError: If manifest doesn't exist.
        """
        with path.open() as f:
            data = json.load(f)

        manifest = cls(
            run_id=data["run_id"],
            tool_version=data["tool_version"],
            git_commit=data["git_commit"],
            config_digest=data["config_digest"],
            started_at=datetime.fromisoformat(data["started_at"]),
            finished_at=(
                datetime.fromisoformat(data["finished_at"]) if data["finished_at"] else None
            ),
            target_mode=data["target"]["mode"],
            target_name=data["target"]["name"],
            year=data["year"],
            repos_processed=data["repos_processed"],
            errors=data.get("errors", []),
        )

        # Restore endpoint stats
        for name, stats_data in data.get("endpoint_stats", {}).items():
            manifest.endpoint_stats[name] = EndpointStats(
                endpoint=name,
                records_fetched=stats_data.get("records_fetched", 0),
                requests_made=stats_data.get("requests_made", 0),
                failures=stats_data.get("failures", 0),
                retries=stats_data.get("retries", 0),
            )

        return manifest
