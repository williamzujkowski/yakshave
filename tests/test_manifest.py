"""Tests for manifest management."""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from gh_year_end.storage.manifest import Manifest


class TestManifestCreation:
    """Tests for manifest creation."""

    def test_default_values(self) -> None:
        """Test that manifest has sensible defaults."""
        manifest = Manifest()

        assert manifest.run_id is not None
        assert len(manifest.run_id) == 36  # UUID format
        assert manifest.tool_version is not None
        assert manifest.started_at is not None
        assert manifest.finished_at is None
        assert manifest.repos_processed == []
        assert manifest.errors == []

    def test_custom_values(self) -> None:
        """Test manifest with custom values."""
        manifest = Manifest(
            target_mode="org",
            target_name="test-org",
            year=2025,
        )

        assert manifest.target_mode == "org"
        assert manifest.target_name == "test-org"
        assert manifest.year == 2025


class TestManifestEndpointStats:
    """Tests for endpoint statistics tracking."""

    def test_record_endpoint_new(self) -> None:
        """Test recording stats for a new endpoint."""
        manifest = Manifest()
        manifest.record_endpoint("pulls", records=100, requests=10)

        assert "pulls" in manifest.endpoint_stats
        assert manifest.endpoint_stats["pulls"].records_fetched == 100
        assert manifest.endpoint_stats["pulls"].requests_made == 10

    def test_record_endpoint_accumulate(self) -> None:
        """Test that endpoint stats accumulate."""
        manifest = Manifest()
        manifest.record_endpoint("pulls", records=50, requests=5)
        manifest.record_endpoint("pulls", records=50, requests=5)

        assert manifest.endpoint_stats["pulls"].records_fetched == 100
        assert manifest.endpoint_stats["pulls"].requests_made == 10

    def test_record_endpoint_failures(self) -> None:
        """Test recording failures and retries."""
        manifest = Manifest()
        manifest.record_endpoint("issues", failures=2, retries=3)

        assert manifest.endpoint_stats["issues"].failures == 2
        assert manifest.endpoint_stats["issues"].retries == 3


class TestManifestRepos:
    """Tests for repo tracking."""

    def test_add_repo(self) -> None:
        """Test adding repos to processed list."""
        manifest = Manifest()
        manifest.add_repo("org/repo1")
        manifest.add_repo("org/repo2")

        assert "org/repo1" in manifest.repos_processed
        assert "org/repo2" in manifest.repos_processed
        assert len(manifest.repos_processed) == 2

    def test_add_repo_dedup(self) -> None:
        """Test that duplicate repos are not added."""
        manifest = Manifest()
        manifest.add_repo("org/repo1")
        manifest.add_repo("org/repo1")

        assert len(manifest.repos_processed) == 1


class TestManifestFinish:
    """Tests for manifest finishing."""

    def test_finish_sets_timestamp(self) -> None:
        """Test that finish sets the finished_at timestamp."""
        manifest = Manifest()
        assert manifest.finished_at is None

        manifest.finish()
        assert manifest.finished_at is not None
        assert manifest.finished_at >= manifest.started_at

    def test_finish_sorts_repos(self) -> None:
        """Test that finish sorts repos alphabetically."""
        manifest = Manifest()
        manifest.add_repo("z/repo")
        manifest.add_repo("a/repo")
        manifest.add_repo("m/repo")

        manifest.finish()

        assert manifest.repos_processed == ["a/repo", "m/repo", "z/repo"]


class TestManifestSerialization:
    """Tests for manifest serialization."""

    def test_to_dict(self) -> None:
        """Test converting manifest to dictionary."""
        manifest = Manifest(
            target_mode="org",
            target_name="test-org",
            year=2025,
        )
        manifest.add_repo("org/repo1")
        manifest.record_endpoint("pulls", records=100, requests=10)
        manifest.finish()

        data = manifest.to_dict()

        assert data["run_id"] == manifest.run_id
        assert data["target"]["mode"] == "org"
        assert data["target"]["name"] == "test-org"
        assert data["year"] == 2025
        assert data["repos_count"] == 1
        assert "endpoint_stats" in data
        assert data["totals"]["records_fetched"] == 100

    def test_save_and_load(self) -> None:
        """Test saving and loading manifest."""
        manifest = Manifest(
            target_mode="org",
            target_name="test-org",
            year=2025,
        )
        manifest.add_repo("org/repo1")
        manifest.add_repo("org/repo2")
        manifest.record_endpoint("pulls", records=100, requests=10)
        manifest.finish()

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "manifest.json"
            manifest.save(path)

            # Verify file exists and is valid JSON
            assert path.exists()
            with path.open() as f:
                data = json.load(f)
            assert data["year"] == 2025

            # Load and verify
            loaded = Manifest.load(path)
            assert loaded.run_id == manifest.run_id
            assert loaded.target_mode == "org"
            assert loaded.target_name == "test-org"
            assert loaded.year == 2025
            assert len(loaded.repos_processed) == 2

    def test_save_creates_parent_dirs(self) -> None:
        """Test that save creates parent directories."""
        manifest = Manifest()

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "nested" / "path" / "manifest.json"
            manifest.save(path)
            assert path.exists()


class TestManifestErrors:
    """Tests for error tracking."""

    def test_add_error(self) -> None:
        """Test adding errors."""
        manifest = Manifest()
        manifest.add_error("Error 1")
        manifest.add_error("Error 2")

        assert len(manifest.errors) == 2
        assert "Error 1" in manifest.errors
        assert "Error 2" in manifest.errors

    def test_errors_in_dict(self) -> None:
        """Test that errors appear in dict output."""
        manifest = Manifest()
        manifest.add_error("Test error")

        data = manifest.to_dict()
        assert data["errors"] == ["Test error"]
        assert data["errors_count"] == 1


class TestManifestGitCommit:
    """Tests for git commit tracking."""

    def test_git_commit_success(self) -> None:
        """Test that git commit is captured when available."""
        manifest = Manifest()
        # In a git repo, should get a commit hash
        assert manifest.git_commit != ""
        assert len(manifest.git_commit) <= 12

    def test_git_commit_failure_file_not_found(self) -> None:
        """Test git commit when git command is not found."""
        with patch("subprocess.run", side_effect=FileNotFoundError):
            manifest = Manifest()
            assert manifest.git_commit == "unknown"

    def test_git_commit_failure_subprocess_error(self) -> None:
        """Test git commit when subprocess fails."""
        import subprocess

        with patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "git")):
            manifest = Manifest()
            assert manifest.git_commit == "unknown"


class TestManifestConfigDigest:
    """Tests for config digest calculation."""

    def test_set_config_digest(self) -> None:
        """Test setting config digest from file."""
        manifest = Manifest()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text("year: 2025\nmode: org\n")

            manifest.set_config_digest(config_path)

            assert manifest.config_digest != ""
            assert len(manifest.config_digest) == 16  # First 16 chars of SHA256

    def test_set_config_digest_consistency(self) -> None:
        """Test that same content produces same digest."""
        manifest1 = Manifest()
        manifest2 = Manifest()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text("year: 2025\nmode: org\n")

            manifest1.set_config_digest(config_path)
            manifest2.set_config_digest(config_path)

            assert manifest1.config_digest == manifest2.config_digest

    def test_set_config_digest_different_content(self) -> None:
        """Test that different content produces different digest."""
        manifest1 = Manifest()
        manifest2 = Manifest()

        with tempfile.TemporaryDirectory() as tmpdir:
            config1_path = Path(tmpdir) / "config1.yaml"
            config2_path = Path(tmpdir) / "config2.yaml"

            config1_path.write_text("year: 2025\n")
            config2_path.write_text("year: 2024\n")

            manifest1.set_config_digest(config1_path)
            manifest2.set_config_digest(config2_path)

            assert manifest1.config_digest != manifest2.config_digest


class TestManifestLoadErrors:
    """Tests for manifest loading error cases."""

    def test_load_missing_file(self) -> None:
        """Test loading manifest from non-existent file."""
        import pytest

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "nonexistent.json"

            with pytest.raises(FileNotFoundError):
                Manifest.load(path)

    def test_load_corrupt_json(self) -> None:
        """Test loading manifest from corrupt JSON file."""
        import pytest

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "corrupt.json"
            path.write_text("not valid json {")

            with pytest.raises(json.JSONDecodeError):
                Manifest.load(path)

    def test_load_missing_optional_fields(self) -> None:
        """Test loading manifest with missing optional fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "manifest.json"

            # Minimal valid manifest data
            data = {
                "run_id": "test-id",
                "tool_version": "1.0.0",
                "git_commit": "abc123",
                "config_digest": "def456",
                "started_at": "2025-01-01T00:00:00+00:00",
                "finished_at": None,
                "target": {"mode": "org", "name": "test"},
                "year": 2025,
                "repos_processed": [],
            }

            with path.open("w") as f:
                json.dump(data, f)

            loaded = Manifest.load(path)
            assert loaded.errors == []
            assert loaded.endpoint_stats == {}


class TestManifestCompleteWorkflow:
    """Tests for complete manifest workflows."""

    def test_complete_collection_workflow(self) -> None:
        """Test a complete collection workflow scenario."""
        manifest = Manifest(target_mode="org", target_name="test-org", year=2025)

        # Simulate collection process
        manifest.add_repo("test-org/repo1")
        manifest.record_endpoint("pulls", records=50, requests=5)
        manifest.add_repo("test-org/repo2")
        manifest.record_endpoint("pulls", records=75, requests=8)
        manifest.record_endpoint("issues", records=100, requests=10, failures=1, retries=1)

        # Add some errors
        manifest.add_error("Rate limit hit during collection")

        # Finish collection
        manifest.finish()

        # Verify final state
        data = manifest.to_dict()

        assert len(data["repos_processed"]) == 2
        assert data["repos_count"] == 2
        assert data["repos_processed"] == ["test-org/repo1", "test-org/repo2"]  # Sorted

        assert data["totals"]["records_fetched"] == 225
        assert data["totals"]["requests_made"] == 23
        assert data["totals"]["failures"] == 1
        assert data["totals"]["retries"] == 1

        assert data["errors_count"] == 1
        assert data["finished_at"] is not None

    def test_roundtrip_with_all_fields(self) -> None:
        """Test save and load with all fields populated."""
        original = Manifest(target_mode="org", target_name="test-org", year=2025)

        # Populate all fields
        original.add_repo("org/repo1")
        original.add_repo("org/repo2")
        original.add_repo("org/repo3")
        original.record_endpoint("pulls", records=100, requests=10, failures=2, retries=3)
        original.record_endpoint("issues", records=50, requests=5, failures=1, retries=1)
        original.add_error("Error 1")
        original.add_error("Error 2")
        original.finish()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text("year: 2025\n")
            original.set_config_digest(config_path)

            path = Path(tmpdir) / "manifest.json"
            original.save(path)

            # Load and verify all fields
            loaded = Manifest.load(path)

            assert loaded.run_id == original.run_id
            assert loaded.tool_version == original.tool_version
            assert loaded.git_commit == original.git_commit
            assert loaded.config_digest == original.config_digest
            assert loaded.started_at == original.started_at
            assert loaded.finished_at == original.finished_at
            assert loaded.target_mode == original.target_mode
            assert loaded.target_name == original.target_name
            assert loaded.year == original.year
            assert loaded.repos_processed == original.repos_processed
            assert loaded.errors == original.errors

            # Check endpoint stats
            assert len(loaded.endpoint_stats) == 2
            assert loaded.endpoint_stats["pulls"].records_fetched == 100
            assert loaded.endpoint_stats["pulls"].requests_made == 10
            assert loaded.endpoint_stats["pulls"].failures == 2
            assert loaded.endpoint_stats["pulls"].retries == 3
            assert loaded.endpoint_stats["issues"].records_fetched == 50
            assert loaded.endpoint_stats["issues"].requests_made == 5


class TestEndpointStats:
    """Tests for EndpointStats dataclass."""

    def test_endpoint_stats_creation(self) -> None:
        """Test creating EndpointStats instance."""
        from gh_year_end.storage.manifest import EndpointStats

        stats = EndpointStats(endpoint="pulls")
        assert stats.endpoint == "pulls"
        assert stats.records_fetched == 0
        assert stats.requests_made == 0
        assert stats.failures == 0
        assert stats.retries == 0

    def test_endpoint_stats_with_values(self) -> None:
        """Test creating EndpointStats with values."""
        from gh_year_end.storage.manifest import EndpointStats

        stats = EndpointStats(
            endpoint="issues", records_fetched=100, requests_made=10, failures=2, retries=3
        )
        assert stats.endpoint == "issues"
        assert stats.records_fetched == 100
        assert stats.requests_made == 10
        assert stats.failures == 2
        assert stats.retries == 3
