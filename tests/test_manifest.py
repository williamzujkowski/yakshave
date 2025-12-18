"""Tests for manifest management."""

import json
import tempfile
from pathlib import Path

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
