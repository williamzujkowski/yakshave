"""Checkpoint and resume system for data collection.

Provides granular progress tracking at the repo + endpoint + page level,
allowing collection to resume from exact point of interruption.
Handles signal interrupts (SIGINT, SIGTERM) gracefully and ensures
atomic writes via temp files.
"""

import fcntl
import hashlib
import json
import logging
import os
import signal
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from gh_year_end.config import Config

logger = logging.getLogger(__name__)


class CheckpointStatus(str, Enum):
    """Status of a checkpoint item (phase, repo, endpoint)."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass
class EndpointProgress:
    """Progress tracking for a single endpoint within a repo.

    Tracks page-level progress to enable resuming from exact point
    of interruption within paginated API calls.
    """

    status: CheckpointStatus = CheckpointStatus.PENDING
    pages_collected: int = 0
    records_collected: int = 0
    last_page_written: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "status": self.status.value,
            "pages_collected": self.pages_collected,
            "records_collected": self.records_collected,
            "last_page_written": self.last_page_written,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EndpointProgress":
        """Create from dictionary."""
        return cls(
            status=CheckpointStatus(data["status"]),
            pages_collected=data["pages_collected"],
            records_collected=data["records_collected"],
            last_page_written=data["last_page_written"],
        )


@dataclass
class RepoProgress:
    """Progress tracking for a single repository.

    Tracks all endpoints for the repo with their individual progress,
    errors, and timing information.
    """

    status: CheckpointStatus = CheckpointStatus.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    endpoints: dict[str, EndpointProgress] = field(default_factory=dict)
    error: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "endpoints": {name: ep.to_dict() for name, ep in self.endpoints.items()},
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RepoProgress":
        """Create from dictionary."""
        return cls(
            status=CheckpointStatus(data["status"]),
            started_at=datetime.fromisoformat(data["started_at"]) if data["started_at"] else None,
            completed_at=(
                datetime.fromisoformat(data["completed_at"]) if data["completed_at"] else None
            ),
            endpoints={
                name: EndpointProgress.from_dict(ep_data)
                for name, ep_data in data.get("endpoints", {}).items()
            },
            error=data.get("error"),
        )


class CheckpointManager:
    """Manages checkpoint state for collection runs.

    Provides:
    - Atomic checkpoint writes via temp file + rename
    - File locking for concurrent safety
    - Signal handling for graceful shutdown (SIGINT, SIGTERM)
    - Config validation via digest to detect changes
    - Phase-level progress tracking
    - Repo-level progress tracking
    - Endpoint + page level resumption

    Thread/process-safe for concurrent access.
    """

    def __init__(self, checkpoint_path: Path, lock_path: Path | None = None) -> None:
        """Initialize checkpoint manager.

        Args:
            checkpoint_path: Path to checkpoint JSON file.
            lock_path: Path to lock file. Defaults to checkpoint_path + '.lock'.
        """
        self.checkpoint_path = checkpoint_path
        self.lock_path = lock_path or Path(str(checkpoint_path) + ".lock")
        self._data: dict[str, Any] = {}
        self._lock_file: Any = None
        self._signal_handlers_installed = False

    def exists(self) -> bool:
        """Check if checkpoint file exists.

        Returns:
            True if checkpoint exists, False otherwise.
        """
        return self.checkpoint_path.exists()

    def load(self) -> None:
        """Load checkpoint from disk.

        Raises:
            FileNotFoundError: If checkpoint doesn't exist.
            json.JSONDecodeError: If checkpoint is corrupted.
        """
        if not self.exists():
            msg = f"Checkpoint not found: {self.checkpoint_path}"
            raise FileNotFoundError(msg)

        with self.checkpoint_path.open() as f:
            self._data = json.load(f)

        logger.info("Loaded checkpoint from %s", self.checkpoint_path)

    def save(self) -> None:
        """Save checkpoint to disk atomically.

        Uses temp file + atomic rename to prevent corruption
        from interrupted writes.
        """
        # Ensure parent directory exists
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to temp file first
        fd, temp_path = tempfile.mkstemp(
            dir=self.checkpoint_path.parent,
            prefix=".checkpoint_",
            suffix=".tmp",
        )

        try:
            # Write using file descriptor
            with os.fdopen(fd, "w") as f:
                json.dump(self._data, f, indent=2)

            # Atomic rename
            temp_path_obj = Path(temp_path)
            temp_path_obj.replace(self.checkpoint_path)
            logger.debug("Saved checkpoint to %s", self.checkpoint_path)

        except Exception:
            # Clean up temp file on error
            Path(temp_path).unlink(missing_ok=True)
            raise

    def delete_if_exists(self) -> None:
        """Delete checkpoint and lock files if they exist."""
        self.checkpoint_path.unlink(missing_ok=True)
        self.lock_path.unlink(missing_ok=True)
        self._data = {}
        logger.info("Deleted checkpoint at %s", self.checkpoint_path)

    def create_new(self, config: Config) -> None:
        """Create new checkpoint from config.

        Args:
            config: Application configuration.
        """
        self._data = {
            "version": "1.0",
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
            "config_digest": self._compute_config_digest(config),
            "target": {
                "mode": config.github.target.mode,
                "name": config.github.target.name,
            },
            "year": config.github.windows.year,
            "phases": {},
            "repos": {},
        }
        self.save()
        logger.info("Created new checkpoint at %s", self.checkpoint_path)

    def validate_config(self, config: Config) -> bool:
        """Validate that config matches checkpoint.

        Args:
            config: Application configuration.

        Returns:
            True if config matches checkpoint digest, False otherwise.
        """
        stored_digest: str = str(self._data.get("config_digest", ""))
        current_digest = self._compute_config_digest(config)
        return bool(stored_digest == current_digest)

    def _compute_config_digest(self, config: Config) -> str:
        """Compute SHA256 digest of config.

        Args:
            config: Application configuration.

        Returns:
            Hex digest (first 16 chars) of config.
        """
        # Serialize config to JSON for hashing
        config_json = config.model_dump_json(exclude_none=True)
        digest = hashlib.sha256(config_json.encode()).hexdigest()
        return digest[:16]

    # Phase tracking methods

    def set_current_phase(self, phase: str) -> None:
        """Set current phase.

        Args:
            phase: Phase name (e.g., "discovery", "pulls", "issues").
        """
        self._data["current_phase"] = phase
        if phase not in self._data["phases"]:
            self._data["phases"][phase] = {
                "status": CheckpointStatus.IN_PROGRESS.value,
                "started_at": datetime.now(UTC).isoformat(),
            }
        self._data["updated_at"] = datetime.now(UTC).isoformat()
        self.save()
        logger.info("Set current phase: %s", phase)

    def mark_phase_complete(self, phase: str) -> None:
        """Mark phase as complete.

        Args:
            phase: Phase name.
        """
        if phase not in self._data["phases"]:
            self._data["phases"][phase] = {"started_at": datetime.now(UTC).isoformat()}

        self._data["phases"][phase]["status"] = CheckpointStatus.COMPLETE.value
        self._data["phases"][phase]["completed_at"] = datetime.now(UTC).isoformat()
        self._data["updated_at"] = datetime.now(UTC).isoformat()
        self.save()
        logger.info("Marked phase complete: %s", phase)

    def is_phase_complete(self, phase: str) -> bool:
        """Check if phase is complete.

        Args:
            phase: Phase name.

        Returns:
            True if phase is marked complete, False otherwise.
        """
        phases: dict[str, Any] = self._data.get("phases", {})
        phase_data: dict[str, Any] = phases.get(phase, {})
        status: str | None = phase_data.get("status")
        return bool(status == CheckpointStatus.COMPLETE.value)

    # Repo tracking methods

    def update_repos(self, repos: list[dict[str, Any]]) -> None:
        """Initialize or update repo list in checkpoint.

        Args:
            repos: List of repository metadata dicts with 'full_name' key.
        """
        for repo in repos:
            repo_name = repo["full_name"]
            if repo_name not in self._data["repos"]:
                self._data["repos"][repo_name] = RepoProgress().to_dict()

        self._data["updated_at"] = datetime.now(UTC).isoformat()
        self.save()
        logger.info("Updated checkpoint with %d repos", len(repos))

    def get_repos_to_process(
        self,
        retry_failed: bool = False,
        from_repo: str | None = None,
    ) -> list[str]:
        """Get list of repos that need processing.

        Args:
            retry_failed: If True, include failed repos.
            from_repo: If specified, start from this repo (inclusive).

        Returns:
            List of repo names to process.
        """
        all_repos = sorted(self._data.get("repos", {}).keys())

        # Filter by status
        repos_to_process = []
        for repo_name in all_repos:
            repo_progress = RepoProgress.from_dict(self._data["repos"][repo_name])
            status = repo_progress.status

            if status == CheckpointStatus.COMPLETE:
                continue
            if status == CheckpointStatus.FAILED and not retry_failed:
                continue

            repos_to_process.append(repo_name)

        # Filter by from_repo
        if from_repo:
            try:
                start_idx = repos_to_process.index(from_repo)
                repos_to_process = repos_to_process[start_idx:]
            except ValueError:
                logger.warning("from_repo %s not found in repos to process", from_repo)

        return repos_to_process

    def mark_repo_endpoint_in_progress(self, repo: str, endpoint: str) -> None:
        """Mark repo endpoint as in progress.

        Args:
            repo: Repository full name.
            endpoint: Endpoint name (e.g., "pulls", "issues").
        """
        if repo not in self._data["repos"]:
            self._data["repos"][repo] = RepoProgress().to_dict()

        repo_data = self._data["repos"][repo]
        repo_progress = RepoProgress.from_dict(repo_data)

        # Update repo status if still pending
        if repo_progress.status == CheckpointStatus.PENDING:
            repo_progress.status = CheckpointStatus.IN_PROGRESS
            repo_progress.started_at = datetime.now(UTC)

        # Initialize endpoint if doesn't exist
        if endpoint not in repo_progress.endpoints:
            repo_progress.endpoints[endpoint] = EndpointProgress()

        # Mark endpoint in progress
        repo_progress.endpoints[endpoint].status = CheckpointStatus.IN_PROGRESS

        self._data["repos"][repo] = repo_progress.to_dict()
        self._data["updated_at"] = datetime.now(UTC).isoformat()
        self.save()

    def mark_repo_endpoint_complete(self, repo: str, endpoint: str) -> None:
        """Mark repo endpoint as complete.

        Args:
            repo: Repository full name.
            endpoint: Endpoint name.
        """
        if repo not in self._data["repos"]:
            return

        repo_data = self._data["repos"][repo]
        repo_progress = RepoProgress.from_dict(repo_data)

        if endpoint in repo_progress.endpoints:
            repo_progress.endpoints[endpoint].status = CheckpointStatus.COMPLETE

        # Check if all endpoints are complete
        all_complete = all(
            ep.status == CheckpointStatus.COMPLETE for ep in repo_progress.endpoints.values()
        )

        if all_complete:
            repo_progress.status = CheckpointStatus.COMPLETE
            repo_progress.completed_at = datetime.now(UTC)

        self._data["repos"][repo] = repo_progress.to_dict()
        self._data["updated_at"] = datetime.now(UTC).isoformat()
        self.save()

    def mark_repo_endpoint_failed(
        self,
        repo: str,
        endpoint: str,
        error: str,
        retryable: bool = True,
    ) -> None:
        """Mark repo endpoint as failed.

        Args:
            repo: Repository full name.
            endpoint: Endpoint name.
            error: Error message.
            retryable: Whether error is retryable.
        """
        if repo not in self._data["repos"]:
            self._data["repos"][repo] = RepoProgress().to_dict()

        repo_data = self._data["repos"][repo]
        repo_progress = RepoProgress.from_dict(repo_data)

        if endpoint not in repo_progress.endpoints:
            repo_progress.endpoints[endpoint] = EndpointProgress()

        repo_progress.endpoints[endpoint].status = CheckpointStatus.FAILED

        # Store error info
        repo_progress.error = {
            "endpoint": endpoint,
            "message": error,
            "retryable": retryable,
            "timestamp": datetime.now(UTC).isoformat(),
        }

        # Mark repo as failed if error is not retryable
        if not retryable:
            repo_progress.status = CheckpointStatus.FAILED

        self._data["repos"][repo] = repo_progress.to_dict()
        self._data["updated_at"] = datetime.now(UTC).isoformat()
        self.save()
        logger.warning("Marked %s/%s as failed: %s", repo, endpoint, error)

    def is_repo_endpoint_complete(self, repo: str, endpoint: str) -> bool:
        """Check if repo endpoint is complete.

        Args:
            repo: Repository full name.
            endpoint: Endpoint name.

        Returns:
            True if endpoint is marked complete, False otherwise.
        """
        repo_data = self._data.get("repos", {}).get(repo)
        if not repo_data:
            return False

        repo_progress = RepoProgress.from_dict(repo_data)
        endpoint_progress = repo_progress.endpoints.get(endpoint)

        if not endpoint_progress:
            return False

        return endpoint_progress.status == CheckpointStatus.COMPLETE

    def get_resume_page(self, repo: str, endpoint: str) -> int:
        """Get page number to resume from.

        Args:
            repo: Repository full name.
            endpoint: Endpoint name.

        Returns:
            Page number to resume from (1-indexed). Returns 1 if no progress.
        """
        repo_data = self._data.get("repos", {}).get(repo)
        if not repo_data:
            return 1

        repo_progress = RepoProgress.from_dict(repo_data)
        endpoint_progress = repo_progress.endpoints.get(endpoint)

        if not endpoint_progress:
            return 1

        # Resume from last_page_written + 1
        return endpoint_progress.last_page_written + 1

    def update_progress(
        self,
        repo: str,
        endpoint: str,
        page: int,
        records: int,
    ) -> None:
        """Update progress for repo endpoint.

        Args:
            repo: Repository full name.
            endpoint: Endpoint name.
            page: Page number just completed.
            records: Number of records collected from this page.
        """
        if repo not in self._data["repos"]:
            self._data["repos"][repo] = RepoProgress().to_dict()

        repo_data = self._data["repos"][repo]
        repo_progress = RepoProgress.from_dict(repo_data)

        if endpoint not in repo_progress.endpoints:
            repo_progress.endpoints[endpoint] = EndpointProgress()

        endpoint_progress = repo_progress.endpoints[endpoint]
        endpoint_progress.pages_collected += 1
        endpoint_progress.records_collected += records
        endpoint_progress.last_page_written = page

        self._data["repos"][repo] = repo_progress.to_dict()
        self._data["updated_at"] = datetime.now(UTC).isoformat()

        # Save periodically (every 10 pages or every 100 records)
        if (
            endpoint_progress.pages_collected % 10 == 0
            or endpoint_progress.records_collected % 100 == 0
        ):
            self.save()

    def get_stats(self) -> dict[str, Any]:
        """Get checkpoint statistics.

        Returns:
            Dictionary with checkpoint stats:
                - total_repos: Total number of repos
                - repos_complete: Number of complete repos
                - repos_in_progress: Number of in-progress repos
                - repos_pending: Number of pending repos
                - repos_failed: Number of failed repos
                - phases: Phase status summary
        """
        repos = self._data.get("repos", {})
        stats: dict[str, int] = {
            "total_repos": len(repos),
            "repos_complete": 0,
            "repos_in_progress": 0,
            "repos_pending": 0,
            "repos_failed": 0,
        }

        for repo_data in repos.values():
            repo_progress = RepoProgress.from_dict(repo_data)
            status = repo_progress.status

            if status == CheckpointStatus.COMPLETE:
                stats["repos_complete"] += 1
            elif status == CheckpointStatus.IN_PROGRESS:
                stats["repos_in_progress"] += 1
            elif status == CheckpointStatus.FAILED:
                stats["repos_failed"] += 1
            else:
                stats["repos_pending"] += 1

        return {
            **stats,
            "phases": self._data.get("phases", {}),
        }

    # Signal handling

    def install_signal_handlers(self) -> None:
        """Install signal handlers for graceful shutdown.

        Handles SIGINT (Ctrl+C) and SIGTERM by saving checkpoint
        before exiting.
        """
        if self._signal_handlers_installed:
            return

        def signal_handler(signum: int, frame: Any) -> None:
            """Handle interrupt signals."""
            sig_name = signal.Signals(signum).name
            logger.warning("Received %s, saving checkpoint before exit...", sig_name)
            self.save()
            logger.info("Checkpoint saved. Exiting.")
            raise KeyboardInterrupt

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        self._signal_handlers_installed = True
        logger.debug("Installed signal handlers for graceful shutdown")

    # Context manager support

    def __enter__(self) -> "CheckpointManager":
        """Context manager entry."""
        # Acquire file lock
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_file = self.lock_path.open("w")
        fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX)
        logger.debug("Acquired checkpoint lock")
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        # Save on exit if there was an exception
        if exc_type is not None:
            logger.error("Exception during checkpoint context: %s", exc_val)
            try:
                self.save()
            except Exception as e:
                logger.error("Failed to save checkpoint on exception: %s", e)

        # Release lock
        if self._lock_file:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
            self._lock_file.close()
            self._lock_file = None
            logger.debug("Released checkpoint lock")
