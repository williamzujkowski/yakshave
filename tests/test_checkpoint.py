"""Tests for checkpoint management system."""

import json
import signal
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gh_year_end.config import Config, load_config
from gh_year_end.storage.checkpoint import (
    CheckpointManager,
    CheckpointStatus,
    EndpointProgress,
    RepoProgress,
)


@pytest.fixture
def temp_checkpoint_dir(tmp_path: Path) -> Path:
    """Create temporary directory for checkpoint files."""
    return tmp_path / "checkpoints"


@pytest.fixture
def checkpoint_path(temp_checkpoint_dir: Path) -> Path:
    """Create checkpoint file path."""
    return temp_checkpoint_dir / "checkpoint.json"


@pytest.fixture
def lock_path(temp_checkpoint_dir: Path) -> Path:
    """Create lock file path."""
    return temp_checkpoint_dir / "checkpoint.json.lock"


@pytest.fixture
def sample_config() -> Config:
    """Load sample config for testing."""
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        pytest.skip("Sample config not found")
    return load_config(config_path)


@pytest.fixture
def sample_repos() -> list[dict[str, str]]:
    """Create sample repo list."""
    return [
        {"full_name": "org/repo1", "name": "repo1"},
        {"full_name": "org/repo2", "name": "repo2"},
        {"full_name": "org/repo3", "name": "repo3"},
    ]


class TestEndpointProgress:
    """Test EndpointProgress dataclass."""

    def test_to_dict(self) -> None:
        """Test conversion to dictionary."""
        progress = EndpointProgress(
            status=CheckpointStatus.IN_PROGRESS,
            pages_collected=5,
            records_collected=150,
            last_page_written=5,
        )

        result = progress.to_dict()
        assert result["status"] == "in_progress"
        assert result["pages_collected"] == 5
        assert result["records_collected"] == 150
        assert result["last_page_written"] == 5

    def test_from_dict(self) -> None:
        """Test creation from dictionary."""
        data = {
            "status": "complete",
            "pages_collected": 10,
            "records_collected": 300,
            "last_page_written": 10,
        }

        progress = EndpointProgress.from_dict(data)
        assert progress.status == CheckpointStatus.COMPLETE
        assert progress.pages_collected == 10
        assert progress.records_collected == 300
        assert progress.last_page_written == 10

    def test_defaults(self) -> None:
        """Test default values."""
        progress = EndpointProgress()
        assert progress.status == CheckpointStatus.PENDING
        assert progress.pages_collected == 0
        assert progress.records_collected == 0
        assert progress.last_page_written == 0


class TestRepoProgress:
    """Test RepoProgress dataclass."""

    def test_to_dict(self) -> None:
        """Test conversion to dictionary."""
        started = datetime.now(UTC)
        completed = datetime.now(UTC)

        progress = RepoProgress(
            status=CheckpointStatus.COMPLETE,
            started_at=started,
            completed_at=completed,
            endpoints={
                "pulls": EndpointProgress(status=CheckpointStatus.COMPLETE),
                "issues": EndpointProgress(status=CheckpointStatus.COMPLETE),
            },
            error=None,
        )

        result = progress.to_dict()
        assert result["status"] == "complete"
        assert result["started_at"] == started.isoformat()
        assert result["completed_at"] == completed.isoformat()
        assert "pulls" in result["endpoints"]
        assert "issues" in result["endpoints"]
        assert result["error"] is None

    def test_from_dict(self) -> None:
        """Test creation from dictionary."""
        started = datetime.now(UTC)
        data = {
            "status": "in_progress",
            "started_at": started.isoformat(),
            "completed_at": None,
            "endpoints": {
                "pulls": {
                    "status": "complete",
                    "pages_collected": 5,
                    "records_collected": 100,
                    "last_page_written": 5,
                }
            },
            "error": None,
        }

        progress = RepoProgress.from_dict(data)
        assert progress.status == CheckpointStatus.IN_PROGRESS
        assert progress.started_at == started
        assert progress.completed_at is None
        assert "pulls" in progress.endpoints
        assert progress.endpoints["pulls"].status == CheckpointStatus.COMPLETE


class TestCheckpointManager:
    """Test CheckpointManager class."""

    def test_exists(self, checkpoint_path: Path, lock_path: Path) -> None:
        """Test exists method."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        assert not manager.exists()

        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        checkpoint_path.write_text("{}")
        assert manager.exists()

    def test_create_new(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test creating new checkpoint."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        assert checkpoint_path.exists()
        with checkpoint_path.open() as f:
            data = json.load(f)

        assert data["version"] == "1.0"
        assert "created_at" in data
        assert "config_digest" in data
        assert data["target"]["name"] == sample_config.github.target.name
        assert data["year"] == sample_config.github.windows.year
        assert data["phases"] == {}
        assert data["repos"] == {}

    def test_save_load(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test save and load cycle."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Modify data
        manager._data["test_key"] = "test_value"
        manager.save()

        # Load in new manager
        manager2 = CheckpointManager(checkpoint_path, lock_path)
        manager2.load()
        assert manager2._data["test_key"] == "test_value"

    def test_atomic_save(self, checkpoint_path: Path, lock_path: Path) -> None:
        """Test atomic save with temp file."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager._data = {"test": "data"}
        manager.save()

        # Verify no temp files remain
        temp_files = list(checkpoint_path.parent.glob(".checkpoint_*.tmp"))
        assert len(temp_files) == 0

        # Verify data is correct
        with checkpoint_path.open() as f:
            data = json.load(f)
        assert data["test"] == "data"

    def test_delete_if_exists(self, checkpoint_path: Path, lock_path: Path) -> None:
        """Test deleting checkpoint."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager._data = {"test": "data"}
        manager.save()
        assert checkpoint_path.exists()

        manager.delete_if_exists()
        assert not checkpoint_path.exists()
        assert manager._data == {}

    def test_validate_config(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test config validation."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Should match
        assert manager.validate_config(sample_config)

        # Modify config digest
        manager._data["config_digest"] = "wrong_digest"
        assert not manager.validate_config(sample_config)

    def test_phase_tracking(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test phase tracking methods."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Set current phase
        manager.set_current_phase("discovery")
        assert manager._data["current_phase"] == "discovery"
        assert manager._data["phases"]["discovery"]["status"] == "in_progress"

        # Check incomplete
        assert not manager.is_phase_complete("discovery")

        # Mark complete
        manager.mark_phase_complete("discovery")
        assert manager.is_phase_complete("discovery")

    def test_update_repos(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test updating repos."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        assert len(manager._data["repos"]) == 3
        assert "org/repo1" in manager._data["repos"]
        assert "org/repo2" in manager._data["repos"]
        assert "org/repo3" in manager._data["repos"]

    def test_get_repos_to_process(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test getting repos to process."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        # All repos should be pending
        repos = manager.get_repos_to_process()
        assert len(repos) == 3

        # Mark one complete
        manager.mark_repo_endpoint_in_progress("org/repo1", "pulls")
        manager.mark_repo_endpoint_complete("org/repo1", "pulls")
        repo_progress = RepoProgress.from_dict(manager._data["repos"]["org/repo1"])
        repo_progress.status = CheckpointStatus.COMPLETE
        manager._data["repos"]["org/repo1"] = repo_progress.to_dict()

        repos = manager.get_repos_to_process()
        assert len(repos) == 2
        assert "org/repo1" not in repos

        # Test from_repo
        repos = manager.get_repos_to_process(from_repo="org/repo3")
        assert repos == ["org/repo3"]

    def test_mark_repo_endpoint_lifecycle(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test repo endpoint lifecycle."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        repo = "org/repo1"
        endpoint = "pulls"

        # Mark in progress
        manager.mark_repo_endpoint_in_progress(repo, endpoint)
        repo_data = manager._data["repos"][repo]
        assert repo_data["status"] == "in_progress"
        assert repo_data["endpoints"][endpoint]["status"] == "in_progress"

        # Mark complete
        manager.mark_repo_endpoint_complete(repo, endpoint)
        repo_data = manager._data["repos"][repo]
        assert repo_data["endpoints"][endpoint]["status"] == "complete"

    def test_mark_repo_endpoint_failed(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test marking endpoint as failed."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        repo = "org/repo1"
        endpoint = "pulls"
        error_msg = "Rate limit exceeded"

        manager.mark_repo_endpoint_failed(repo, endpoint, error_msg, retryable=True)
        repo_data = manager._data["repos"][repo]
        assert repo_data["endpoints"][endpoint]["status"] == "failed"
        assert repo_data["error"]["message"] == error_msg
        assert repo_data["error"]["retryable"] is True

        # Repo should still be pending/in_progress for retryable errors
        assert repo_data["status"] != "failed"

        # Non-retryable error should mark repo as failed
        manager.mark_repo_endpoint_failed(repo, endpoint, "403 Forbidden", retryable=False)
        repo_data = manager._data["repos"][repo]
        assert repo_data["status"] == "failed"

    def test_is_repo_endpoint_complete(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test checking if endpoint is complete."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        repo = "org/repo1"
        endpoint = "pulls"

        assert not manager.is_repo_endpoint_complete(repo, endpoint)

        manager.mark_repo_endpoint_in_progress(repo, endpoint)
        assert not manager.is_repo_endpoint_complete(repo, endpoint)

        manager.mark_repo_endpoint_complete(repo, endpoint)
        assert manager.is_repo_endpoint_complete(repo, endpoint)

    def test_get_resume_page(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test getting resume page number."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        repo = "org/repo1"
        endpoint = "pulls"

        # Should start at page 1
        assert manager.get_resume_page(repo, endpoint) == 1

        # Update progress
        manager.mark_repo_endpoint_in_progress(repo, endpoint)
        manager.update_progress(repo, endpoint, page=1, records=30)
        manager.update_progress(repo, endpoint, page=2, records=30)

        # Should resume from page 3
        assert manager.get_resume_page(repo, endpoint) == 3

    def test_update_progress(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test updating progress."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        repo = "org/repo1"
        endpoint = "pulls"

        manager.mark_repo_endpoint_in_progress(repo, endpoint)
        manager.update_progress(repo, endpoint, page=1, records=50)
        manager.update_progress(repo, endpoint, page=2, records=50)

        repo_data = manager._data["repos"][repo]
        endpoint_data = repo_data["endpoints"][endpoint]

        assert endpoint_data["pages_collected"] == 2
        assert endpoint_data["records_collected"] == 100
        assert endpoint_data["last_page_written"] == 2

    def test_get_stats(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test getting stats."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        stats = manager.get_stats()
        assert stats["total_repos"] == 3
        assert stats["repos_pending"] == 3
        assert stats["repos_complete"] == 0
        assert stats["repos_in_progress"] == 0
        assert stats["repos_failed"] == 0

        # Mark one in progress
        manager.mark_repo_endpoint_in_progress("org/repo1", "pulls")
        stats = manager.get_stats()
        assert stats["repos_in_progress"] == 1
        assert stats["repos_pending"] == 2

    def test_context_manager(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test context manager usage."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        with manager:
            manager._data["test"] = "value"
            manager.save()

        # Lock should be released
        assert manager._lock_file is None

    def test_signal_handlers(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test signal handler installation."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Install handlers
        manager.install_signal_handlers()
        assert manager._signal_handlers_installed

        # Installing again should be no-op
        manager.install_signal_handlers()

    def test_load_nonexistent_checkpoint(
        self,
        checkpoint_path: Path,
        lock_path: Path,
    ) -> None:
        """Test loading checkpoint that doesn't exist."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        with pytest.raises(FileNotFoundError, match="Checkpoint not found"):
            manager.load()

    def test_save_with_io_error(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test save handles IO errors gracefully."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Make parent directory read-only to trigger error
        checkpoint_path.parent.chmod(0o444)

        try:
            manager._data["test"] = "value"
            with pytest.raises(PermissionError):
                manager.save()
        finally:
            # Restore permissions
            checkpoint_path.parent.chmod(0o755)

    def test_get_repos_to_process_with_failed(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test getting repos to process including failed ones."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        # Mark one repo as failed
        manager.mark_repo_endpoint_failed("org/repo1", "pulls", "Error", retryable=False)

        # Without retry_failed, should exclude failed repo
        repos = manager.get_repos_to_process(retry_failed=False)
        assert "org/repo1" not in repos
        assert len(repos) == 2

        # With retry_failed, should include failed repo
        repos = manager.get_repos_to_process(retry_failed=True)
        assert "org/repo1" in repos
        assert len(repos) == 3

    def test_get_repos_to_process_from_nonexistent_repo(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test from_repo with nonexistent repo name."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        # Nonexistent repo should log warning and return all repos
        repos = manager.get_repos_to_process(from_repo="org/nonexistent")
        assert len(repos) == 3  # Returns all pending repos since from_repo not found

    def test_mark_repo_endpoint_complete_all_endpoints(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test that repo is marked complete when all endpoints are complete."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        repo = "org/repo1"
        endpoints = ["pulls", "issues", "reviews"]

        # Mark all endpoints in progress and complete
        for endpoint in endpoints:
            manager.mark_repo_endpoint_in_progress(repo, endpoint)
            manager.mark_repo_endpoint_complete(repo, endpoint)

        # Repo should be marked complete
        repo_data = manager._data["repos"][repo]
        assert repo_data["status"] == "complete"
        assert repo_data["completed_at"] is not None

    def test_mark_repo_endpoint_complete_nonexistent_repo(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test marking nonexistent repo endpoint as complete."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Should not crash
        manager.mark_repo_endpoint_complete("org/nonexistent", "pulls")
        assert "org/nonexistent" not in manager._data["repos"]

    def test_is_repo_endpoint_complete_nonexistent_repo(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test checking endpoint complete for nonexistent repo."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Should return False
        assert not manager.is_repo_endpoint_complete("org/nonexistent", "pulls")

    def test_is_repo_endpoint_complete_nonexistent_endpoint(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test checking nonexistent endpoint."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        # Should return False
        assert not manager.is_repo_endpoint_complete("org/repo1", "nonexistent")

    def test_get_resume_page_nonexistent_repo(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test getting resume page for nonexistent repo."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Should return 1 (start from beginning)
        assert manager.get_resume_page("org/nonexistent", "pulls") == 1

    def test_get_resume_page_nonexistent_endpoint(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test getting resume page for nonexistent endpoint."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        # Should return 1 (start from beginning)
        assert manager.get_resume_page("org/repo1", "nonexistent") == 1

    def test_update_progress_creates_repo_if_missing(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test that update_progress creates repo if it doesn't exist."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Update progress for nonexistent repo
        manager.update_progress("org/newrepo", "pulls", page=1, records=50)

        # Repo should be created
        assert "org/newrepo" in manager._data["repos"]
        repo_data = manager._data["repos"]["org/newrepo"]
        assert repo_data["endpoints"]["pulls"]["pages_collected"] == 1
        assert repo_data["endpoints"]["pulls"]["records_collected"] == 50

    def test_update_progress_creates_endpoint_if_missing(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test that update_progress creates endpoint if it doesn't exist."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        # Update progress for nonexistent endpoint
        manager.update_progress("org/repo1", "new_endpoint", page=1, records=50)

        # Endpoint should be created
        repo_data = manager._data["repos"]["org/repo1"]
        assert "new_endpoint" in repo_data["endpoints"]
        assert repo_data["endpoints"]["new_endpoint"]["pages_collected"] == 1

    def test_update_progress_periodic_save(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test that update_progress saves periodically."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        repo = "org/repo1"
        endpoint = "pulls"

        manager.mark_repo_endpoint_in_progress(repo, endpoint)

        # Modify checkpoint path time to verify it was saved
        initial_mtime = checkpoint_path.stat().st_mtime if checkpoint_path.exists() else 0

        # Update progress 9 times (shouldn't save)
        for i in range(1, 10):
            manager.update_progress(repo, endpoint, page=i, records=5)

        # 10th page should trigger save
        manager.update_progress(repo, endpoint, page=10, records=5)

        # Verify checkpoint was saved (mtime changed)
        final_mtime = checkpoint_path.stat().st_mtime
        assert final_mtime > initial_mtime

    def test_get_stats_with_all_statuses(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test get_stats with repos in all different statuses."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        repos = [{"full_name": f"org/repo{i}", "name": f"repo{i}"} for i in range(1, 5)]
        manager.update_repos(repos)

        # repo1: complete
        manager.mark_repo_endpoint_in_progress("org/repo1", "pulls")
        manager.mark_repo_endpoint_complete("org/repo1", "pulls")
        repo_progress = RepoProgress.from_dict(manager._data["repos"]["org/repo1"])
        repo_progress.status = CheckpointStatus.COMPLETE
        manager._data["repos"]["org/repo1"] = repo_progress.to_dict()

        # repo2: in progress
        manager.mark_repo_endpoint_in_progress("org/repo2", "pulls")

        # repo3: failed
        manager.mark_repo_endpoint_failed("org/repo3", "pulls", "Error", retryable=False)

        # repo4: pending (no changes)

        stats = manager.get_stats()
        assert stats["total_repos"] == 4
        assert stats["repos_complete"] == 1
        assert stats["repos_in_progress"] == 1
        assert stats["repos_failed"] == 1
        assert stats["repos_pending"] == 1

    def test_context_manager_with_exception(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test context manager saves on exception."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        try:
            with manager:
                manager._data["test"] = "value"
                raise ValueError("Test error")
        except ValueError:
            pass

        # Should have saved despite exception
        with checkpoint_path.open() as f:
            data = json.load(f)
        assert data["test"] == "value"

    def test_mark_phase_complete_creates_phase(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test marking phase complete creates it if it doesn't exist."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Mark phase complete without setting it current first
        manager.mark_phase_complete("new_phase")

        assert "new_phase" in manager._data["phases"]
        assert manager._data["phases"]["new_phase"]["status"] == "complete"

    def test_is_phase_complete_nonexistent_phase(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test checking nonexistent phase."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Should return False
        assert not manager.is_phase_complete("nonexistent")

    def test_mark_repo_endpoint_in_progress_updates_repo_status(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test marking endpoint in progress updates repo status from pending."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        repo = "org/repo1"
        # Verify repo starts as pending
        repo_data = manager._data["repos"][repo]
        assert repo_data["status"] == "pending"

        # Mark endpoint in progress
        manager.mark_repo_endpoint_in_progress(repo, "pulls")

        # Repo should now be in progress
        repo_data = manager._data["repos"][repo]
        assert repo_data["status"] == "in_progress"
        assert repo_data["started_at"] is not None

    def test_mark_repo_endpoint_in_progress_creates_repo(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test marking endpoint in progress creates repo if it doesn't exist."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Mark endpoint in progress for nonexistent repo
        manager.mark_repo_endpoint_in_progress("org/newrepo", "pulls")

        # Repo should be created
        assert "org/newrepo" in manager._data["repos"]
        repo_data = manager._data["repos"]["org/newrepo"]
        assert repo_data["status"] == "in_progress"
        assert "pulls" in repo_data["endpoints"]

    def test_mark_repo_endpoint_failed_creates_repo(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test marking endpoint failed creates repo if it doesn't exist."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Mark endpoint failed for nonexistent repo
        manager.mark_repo_endpoint_failed("org/newrepo", "pulls", "Error", retryable=True)

        # Repo should be created
        assert "org/newrepo" in manager._data["repos"]
        repo_data = manager._data["repos"]["org/newrepo"]
        assert "pulls" in repo_data["endpoints"]
        assert repo_data["error"]["message"] == "Error"

    def test_save_cleanup_on_error(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test that temp file is cleaned up on save error."""
        import os

        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Create a real temp file that we'll close to cause write to fail
        fd, temp_path = tempfile.mkstemp(
            dir=checkpoint_path.parent,
            prefix=".checkpoint_",
            suffix=".tmp",
        )
        os.close(fd)  # Close the file descriptor to cause failure

        # Mock tempfile.mkstemp to return the closed file descriptor
        with patch("tempfile.mkstemp") as mock_mkstemp:
            mock_mkstemp.return_value = (fd, temp_path)

            # Should raise error and clean up temp file
            with pytest.raises(OSError):
                manager._data["test"] = "value"
                manager.save()

            # Temp file should be cleaned up
            assert not Path(temp_path).exists()

    def test_signal_handler_saves_checkpoint(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test that signal handler saves checkpoint before raising."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.install_signal_handlers()

        # Modify data
        manager._data["test"] = "value"

        # Get the installed signal handler
        handler = signal.getsignal(signal.SIGINT)

        # Call the handler and expect KeyboardInterrupt
        with pytest.raises(KeyboardInterrupt):
            handler(signal.SIGINT, None)

        # Checkpoint should have been saved
        with checkpoint_path.open() as f:
            data = json.load(f)
        assert data["test"] == "value"

    def test_update_progress_with_100_records_triggers_save(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
        sample_repos: list[dict[str, str]],
    ) -> None:
        """Test that update_progress saves when 100 records threshold is hit."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)
        manager.update_repos(sample_repos)

        repo = "org/repo1"
        endpoint = "pulls"

        manager.mark_repo_endpoint_in_progress(repo, endpoint)

        initial_mtime = checkpoint_path.stat().st_mtime if checkpoint_path.exists() else 0

        # Update with 99 records (shouldn't save)
        manager.update_progress(repo, endpoint, page=1, records=99)

        # Update with 1 more record (total 100, should trigger save)
        manager.update_progress(repo, endpoint, page=2, records=1)

        # Verify checkpoint was saved
        final_mtime = checkpoint_path.stat().st_mtime
        assert final_mtime > initial_mtime


class TestCheckpointIntegration:
    """Integration tests for checkpoint system."""

    def test_full_collection_simulation(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Simulate a full collection run with checkpoint."""
        manager = CheckpointManager(checkpoint_path, lock_path)
        manager.create_new(sample_config)

        # Simulate repo discovery
        repos = [{"full_name": f"org/repo{i}", "name": f"repo{i}"} for i in range(1, 6)]
        manager.update_repos(repos)

        # Simulate processing
        endpoints = ["pulls", "issues", "reviews"]

        for repo in repos:
            repo_name = repo["full_name"]

            for endpoint in endpoints:
                manager.mark_repo_endpoint_in_progress(repo_name, endpoint)

                # Simulate pagination
                for page in range(1, 4):
                    manager.update_progress(repo_name, endpoint, page, 30)

                manager.mark_repo_endpoint_complete(repo_name, endpoint)

            # Mark repo complete when all endpoints are done
            repo_progress = RepoProgress.from_dict(manager._data["repos"][repo_name])
            repo_progress.status = CheckpointStatus.COMPLETE
            repo_progress.completed_at = datetime.now(UTC)
            manager._data["repos"][repo_name] = repo_progress.to_dict()

        manager.save()

        # Verify final state
        stats = manager.get_stats()
        assert stats["total_repos"] == 5
        assert stats["repos_complete"] == 5
        assert stats["repos_pending"] == 0

    def test_resume_after_interruption(
        self,
        checkpoint_path: Path,
        lock_path: Path,
        sample_config: Config,
    ) -> None:
        """Test resuming after interruption."""
        # First run - process some repos
        manager1 = CheckpointManager(checkpoint_path, lock_path)
        manager1.create_new(sample_config)

        repos = [{"full_name": f"org/repo{i}", "name": f"repo{i}"} for i in range(1, 4)]
        manager1.update_repos(repos)

        # Process first repo completely
        manager1.mark_repo_endpoint_in_progress("org/repo1", "pulls")
        manager1.update_progress("org/repo1", "pulls", 1, 50)
        manager1.mark_repo_endpoint_complete("org/repo1", "pulls")

        # Process second repo partially
        manager1.mark_repo_endpoint_in_progress("org/repo2", "pulls")
        manager1.update_progress("org/repo2", "pulls", 1, 50)
        manager1.update_progress("org/repo2", "pulls", 2, 50)
        # Simulate interruption here (don't mark complete)

        manager1.save()

        # Second run - resume from checkpoint
        manager2 = CheckpointManager(checkpoint_path, lock_path)
        manager2.load()

        # Should resume repo2 from page 3
        resume_page = manager2.get_resume_page("org/repo2", "pulls")
        assert resume_page == 3

        # repo3 should start from page 1
        resume_page = manager2.get_resume_page("org/repo3", "pulls")
        assert resume_page == 1
