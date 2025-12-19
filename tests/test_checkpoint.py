"""Tests for checkpoint management system."""

import json
from datetime import UTC, datetime
from pathlib import Path

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
