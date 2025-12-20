"""Tests for report build system."""

# ruff: noqa: ARG002

import json
from pathlib import Path

import pytest

from gh_year_end.config import Config
from gh_year_end.report.build import (
    _copy_assets,
    _generate_root_redirect,
    _load_json_data,
    _render_templates,
    _verify_metrics_data_exists,
    _write_build_manifest,
    build_site,
    get_available_years,
)
from gh_year_end.storage.paths import PathManager

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def config() -> Config:
    """Load test configuration."""
    from gh_year_end.config import load_config

    return load_config(FIXTURES_DIR / "valid_config.yaml")


@pytest.fixture
def paths(config: Config, tmp_path: Path) -> PathManager:
    """Create a PathManager with temporary directories."""
    # Override paths to use tmp_path
    config.storage.root = str(tmp_path / "data")
    config.report.output_dir = str(tmp_path / "site_templates")
    return PathManager(config)


@pytest.fixture
def sample_metrics_data(paths: PathManager) -> None:
    """Create sample metrics data for testing (in JSON format)."""
    paths.site_data_path.mkdir(parents=True, exist_ok=True)

    # Create sample summary data
    summary_data = {
        "year": 2025,
        "target": {"mode": "user", "name": "test-user"},
        "total_contributors": 3,
        "total_repos": 5,
        "total_prs_opened": 18,
        "total_prs_merged": 14,
        "total_issues_opened": 9,
        "total_reviews_submitted": 30,
    }
    with (paths.site_data_path / "summary.json").open("w") as f:
        json.dump(summary_data, f, indent=2)

    # Create sample leaderboard data
    leaderboard_data = {
        "leaderboards": {
            "prs_opened": {
                "org": [
                    {"rank": 1, "user_id": "alice", "login": "alice", "value": 10},
                    {"rank": 2, "user_id": "bob", "login": "bob", "value": 5},
                    {"rank": 3, "user_id": "charlie", "login": "charlie", "value": 3},
                ],
                "repos": {},
            },
            "prs_merged": {
                "org": [
                    {"rank": 1, "user_id": "alice", "login": "alice", "value": 8},
                    {"rank": 2, "user_id": "bob", "login": "bob", "value": 4},
                    {"rank": 3, "user_id": "charlie", "login": "charlie", "value": 2},
                ],
                "repos": {},
            },
        },
        "metrics_available": ["prs_opened", "prs_merged"],
    }
    with (paths.site_data_path / "leaderboards.json").open("w") as f:
        json.dump(leaderboard_data, f, indent=2)

    # Create sample time series data
    timeseries_data = {
        "timeseries": {
            "week": {
                "prs_merged": {
                    "org": [
                        {"period_start": "2025-01-06", "period_end": "2025-01-12", "value": 4},
                        {"period_start": "2025-01-13", "period_end": "2025-01-19", "value": 6},
                        {"period_start": "2025-01-20", "period_end": "2025-01-26", "value": 2},
                    ],
                    "repos": {},
                }
            }
        },
        "period_types": ["week"],
        "metrics_available": ["prs_merged"],
    }
    with (paths.site_data_path / "timeseries.json").open("w") as f:
        json.dump(timeseries_data, f, indent=2)


@pytest.fixture
def sample_templates(config: Config, tmp_path: Path) -> Path:
    """Create sample templates for testing."""
    templates_dir = tmp_path / "site_templates" / "templates"
    templates_dir.mkdir(parents=True, exist_ok=True)

    # Create simple index.html template (uses new config structure)
    index_template = """<!DOCTYPE html>
<html>
<head>
    <title>{{ config.report.title }} - {{ config.github.windows.year }}</title>
</head>
<body>
    <h1>{{ config.report.title }} - {{ config.github.windows.year }}</h1>
    <p>Target: {{ config.github.target.name }} ({{ config.github.target.mode }})</p>
    <p>Build time: {{ build_time }}</p>
</body>
</html>"""
    (templates_dir / "index.html").write_text(index_template)

    # Create dashboard.html template
    dashboard_template = """<!DOCTYPE html>
<html>
<head>
    <title>Dashboard - {{ config.github.windows.year }}</title>
</head>
<body>
    <h1>Dashboard</h1>
    {% if summary %}
    <p>Summary: {{ summary.total_contributors }} contributors</p>
    {% endif %}
</body>
</html>"""
    (templates_dir / "dashboard.html").write_text(dashboard_template)

    return templates_dir


@pytest.fixture
def sample_assets(config: Config, tmp_path: Path) -> Path:
    """Create sample assets for testing."""
    assets_dir = tmp_path / "site_templates" / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    # Create sample CSS
    css_dir = assets_dir / "css"
    css_dir.mkdir(parents=True, exist_ok=True)
    (css_dir / "style.css").write_text("body { margin: 0; }")

    # Create sample JS
    js_dir = assets_dir / "js"
    js_dir.mkdir(parents=True, exist_ok=True)
    (js_dir / "main.js").write_text("console.log('loaded');")

    # Create sample image
    img_dir = assets_dir / "images"
    img_dir.mkdir(parents=True, exist_ok=True)
    (img_dir / "logo.png").write_bytes(b"fake-png-data")

    # Create favicon files
    (assets_dir / "favicon.ico").write_bytes(b"fake-ico-data")
    (assets_dir / "favicon-16x16.png").write_bytes(b"fake-png-16-data")
    (assets_dir / "favicon-32x32.png").write_bytes(b"fake-png-32-data")

    return assets_dir


class TestBuildSite:
    """Tests for build_site function."""

    def test_fails_if_metrics_data_missing(self, config: Config, paths: PathManager) -> None:
        """Test that build_site fails if metrics data doesn't exist."""
        with pytest.raises(ValueError, match="Metrics data not found"):
            build_site(config, paths)

    def test_fails_if_no_metrics_tables(self, config: Config, paths: PathManager) -> None:
        """Test that build_site fails if no metrics JSON files exist."""
        paths.site_data_path.mkdir(parents=True, exist_ok=True)

        with pytest.raises(ValueError, match="Missing required metrics files"):
            build_site(config, paths)

    def test_creates_site_directories(
        self, config: Config, paths: PathManager, sample_metrics_data: None
    ) -> None:
        """Test that build_site creates required directories."""
        build_site(config, paths)

        assert paths.site_root.exists()
        assert paths.site_data_path.exists()
        assert paths.site_assets_path.exists()

    def test_returns_statistics(
        self, config: Config, paths: PathManager, sample_metrics_data: None
    ) -> None:
        """Test that build_site returns proper statistics."""
        stats = build_site(config, paths)

        assert "start_time" in stats
        assert "end_time" in stats
        assert "duration_seconds" in stats
        assert "templates_rendered" in stats
        assert "data_files_written" in stats
        assert "assets_copied" in stats
        assert "errors" in stats

        # Verify types
        assert isinstance(stats["start_time"], str)
        assert isinstance(stats["end_time"], str)
        assert isinstance(stats["duration_seconds"], float)
        assert isinstance(stats["templates_rendered"], list)
        assert isinstance(stats["data_files_written"], int)
        assert isinstance(stats["assets_copied"], int)
        assert isinstance(stats["errors"], list)

        # Verify duration is positive
        assert stats["duration_seconds"] >= 0

    def test_loads_data_files(
        self, config: Config, paths: PathManager, sample_metrics_data: None
    ) -> None:
        """Test that build_site loads JSON data files."""
        stats = build_site(config, paths)

        # Should have loaded 3 files (summary, leaderboards, timeseries)
        assert stats["data_files_written"] == 3

        # Verify the JSON files that were created by the fixture still exist
        assert (paths.site_data_path / "summary.json").exists()
        assert (paths.site_data_path / "leaderboards.json").exists()
        assert (paths.site_data_path / "timeseries.json").exists()

    def test_renders_templates(
        self,
        config: Config,
        paths: PathManager,
        sample_metrics_data: None,
        sample_templates: Path,
    ) -> None:
        """Test that build_site renders templates."""
        stats = build_site(config, paths)

        # Should have rendered 2 templates
        assert len(stats["templates_rendered"]) == 2
        assert "index.html" in stats["templates_rendered"]
        assert "dashboard.html" in stats["templates_rendered"]

        # Verify files exist
        assert (paths.site_root / "index.html").exists()
        assert (paths.site_root / "dashboard.html").exists()

    def test_copies_assets(
        self,
        config: Config,
        paths: PathManager,
        sample_metrics_data: None,
        sample_assets: Path,
    ) -> None:
        """Test that build_site copies static assets."""
        stats = build_site(config, paths)

        # Should have copied 6 files (css, js, image, + 3 favicon files)
        assert stats["assets_copied"] == 6

        # Verify files exist
        assert (paths.site_assets_path / "css" / "style.css").exists()
        assert (paths.site_assets_path / "js" / "main.js").exists()
        assert (paths.site_assets_path / "images" / "logo.png").exists()

        # Verify favicon files exist
        assert (paths.site_assets_path / "favicon.ico").exists()
        assert (paths.site_assets_path / "favicon-16x16.png").exists()
        assert (paths.site_assets_path / "favicon-32x32.png").exists()

    def test_creates_manifest(
        self, config: Config, paths: PathManager, sample_metrics_data: None
    ) -> None:
        """Test that build_site creates build manifest."""
        build_site(config, paths)

        manifest_path = paths.site_root / "manifest.json"
        assert manifest_path.exists()

        # Verify manifest content
        with manifest_path.open() as f:
            manifest = json.load(f)

        assert manifest["version"] == "1.0"
        assert manifest["year"] == config.github.windows.year
        assert manifest["target"] == config.github.target.name
        assert "build_time" in manifest

    def test_handles_missing_templates_gracefully(
        self, config: Config, paths: PathManager, sample_metrics_data: None
    ) -> None:
        """Test that missing templates directory doesn't crash build."""
        # Don't create templates directory
        stats = build_site(config, paths)

        # Should complete without error
        assert len(stats["errors"]) == 0
        assert len(stats["templates_rendered"]) == 0

    def test_handles_missing_assets_gracefully(
        self, config: Config, paths: PathManager, sample_metrics_data: None
    ) -> None:
        """Test that missing assets directory doesn't crash build."""
        # Don't create assets directory
        stats = build_site(config, paths)

        # Should complete without error
        assert len(stats["errors"]) == 0
        assert stats["assets_copied"] == 0

    def test_generates_root_redirect(
        self, config: Config, paths: PathManager, sample_metrics_data: None, tmp_path: Path
    ) -> None:
        """Test that root redirect is generated."""
        build_site(config, paths)

        # Check that root redirect was created
        site_base_dir = Path(config.report.output_dir)
        redirect_path = site_base_dir / "index.html"
        assert redirect_path.exists()

        # Verify it redirects to the current year
        content = redirect_path.read_text()
        assert f"url=/{config.github.windows.year}/" in content


class TestVerifyMetricsDataExists:
    """Tests for _verify_metrics_data_exists function."""

    def test_raises_if_metrics_dir_missing(self, paths: PathManager) -> None:
        """Test that it raises ValueError if metrics directory doesn't exist."""
        with pytest.raises(ValueError, match="Metrics data not found"):
            _verify_metrics_data_exists(paths)

    def test_raises_if_no_json_files(self, paths: PathManager) -> None:
        """Test that it raises ValueError if required JSON files don't exist."""
        paths.site_data_path.mkdir(parents=True, exist_ok=True)

        with pytest.raises(ValueError, match="Missing required metrics files"):
            _verify_metrics_data_exists(paths)

    def test_succeeds_with_json_files(
        self, paths: PathManager, sample_metrics_data: None
    ) -> None:
        """Test that it succeeds when JSON files exist."""
        # Should not raise
        _verify_metrics_data_exists(paths)


class TestLoadJsonData:
    """Tests for _load_json_data function."""

    def test_loads_json_files(self, paths: PathManager, sample_metrics_data: None) -> None:
        """Test that JSON files are loaded."""
        data_context = _load_json_data(paths.site_data_path)

        # Should have loaded summary and leaderboards at minimum
        assert "summary" in data_context
        assert "leaderboards" in data_context
        assert "timeseries" in data_context

    def test_json_content_valid(self, paths: PathManager, sample_metrics_data: None) -> None:
        """Test that JSON content is loaded correctly."""
        data_context = _load_json_data(paths.site_data_path)

        # Verify summary data
        summary = data_context["summary"]
        assert summary["year"] == 2025
        assert summary["total_contributors"] == 3

        # Verify leaderboard structure
        leaderboards = data_context["leaderboards"]
        assert "leaderboards" in leaderboards
        assert "prs_opened" in leaderboards["leaderboards"]

    def test_handles_missing_files_gracefully(self, paths: PathManager) -> None:
        """Test that missing JSON files are handled gracefully."""
        paths.site_data_path.mkdir(parents=True, exist_ok=True)

        # Create only summary.json
        summary_data = {"year": 2025}
        with (paths.site_data_path / "summary.json").open("w") as f:
            json.dump(summary_data, f)

        # Should not raise, just load what's available
        data_context = _load_json_data(paths.site_data_path)

        assert "summary" in data_context
        assert "leaderboards" not in data_context

    def test_handles_invalid_json_gracefully(self, paths: PathManager) -> None:
        """Test that invalid JSON files are skipped."""
        paths.site_data_path.mkdir(parents=True, exist_ok=True)

        # Create invalid JSON file
        invalid_file = paths.site_data_path / "summary.json"
        invalid_file.write_text("not valid json{")

        # Should not raise, just skip the file
        data_context = _load_json_data(paths.site_data_path)

        # Should not have summary since it failed to load
        assert "summary" not in data_context


class TestRenderTemplates:
    """Tests for _render_templates function."""

    def test_renders_all_templates(
        self, config: Config, paths: PathManager, sample_templates: Path, sample_metrics_data: None
    ) -> None:
        """Test that all templates are rendered."""
        data_context = _load_json_data(paths.site_data_path)

        rendered = _render_templates(sample_templates, paths.site_root, data_context, config)

        assert len(rendered) == 2
        assert "index.html" in rendered
        assert "dashboard.html" in rendered

    def test_rendered_html_contains_data(
        self, config: Config, paths: PathManager, sample_templates: Path, sample_metrics_data: None
    ) -> None:
        """Test that rendered HTML contains template variables."""
        data_context = _load_json_data(paths.site_data_path)

        _render_templates(sample_templates, paths.site_root, data_context, config)

        # Check index.html
        index_content = (paths.site_root / "index.html").read_text()
        assert str(config.github.windows.year) in index_content
        assert config.report.title in index_content
        assert config.github.target.name in index_content

    def test_handles_missing_templates_dir(
        self, config: Config, paths: PathManager, tmp_path: Path
    ) -> None:
        """Test that missing templates directory is handled gracefully."""
        nonexistent_dir = tmp_path / "nonexistent_templates"

        # Should not raise, just return empty list
        rendered = _render_templates(nonexistent_dir, paths.site_root, {}, config)
        assert len(rendered) == 0

    def test_handles_template_render_errors(
        self, config: Config, paths: PathManager, tmp_path: Path
    ) -> None:
        """Test that template rendering errors are handled gracefully."""
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir(parents=True, exist_ok=True)

        # Create template with undefined variable reference
        bad_template = "{{ undefined_variable }}"
        (templates_dir / "bad.html").write_text(bad_template)

        # Should not raise, but may skip the bad template
        rendered = _render_templates(templates_dir, paths.site_root, {}, config)

        # Either rendered with empty value or skipped
        assert isinstance(rendered, list)


class TestCopyAssets:
    """Tests for _copy_assets function."""

    def test_copies_all_files(self, paths: PathManager, sample_assets: Path) -> None:
        """Test that all asset files are copied."""
        files_copied = _copy_assets(sample_assets, paths.site_assets_path)

        assert files_copied == 6

    def test_preserves_directory_structure(self, paths: PathManager, sample_assets: Path) -> None:
        """Test that directory structure is preserved."""
        _copy_assets(sample_assets, paths.site_assets_path)

        assert (paths.site_assets_path / "css" / "style.css").exists()
        assert (paths.site_assets_path / "js" / "main.js").exists()
        assert (paths.site_assets_path / "images" / "logo.png").exists()

        # Verify favicon files are copied
        assert (paths.site_assets_path / "favicon.ico").exists()
        assert (paths.site_assets_path / "favicon-16x16.png").exists()
        assert (paths.site_assets_path / "favicon-32x32.png").exists()

    def test_copies_file_content(self, paths: PathManager, sample_assets: Path) -> None:
        """Test that file content is copied correctly."""
        _copy_assets(sample_assets, paths.site_assets_path)

        css_content = (paths.site_assets_path / "css" / "style.css").read_text()
        assert css_content == "body { margin: 0; }"

        js_content = (paths.site_assets_path / "js" / "main.js").read_text()
        assert js_content == "console.log('loaded');"

    def test_handles_missing_source(self, paths: PathManager, tmp_path: Path) -> None:
        """Test that missing source directory is handled gracefully."""
        nonexistent_dir = tmp_path / "nonexistent_assets"

        files_copied = _copy_assets(nonexistent_dir, paths.site_assets_path)

        assert files_copied == 0

    def test_cleans_existing_destination(self, paths: PathManager, sample_assets: Path) -> None:
        """Test that existing destination is cleaned before copying."""
        # Create existing file in destination
        paths.site_assets_path.mkdir(parents=True, exist_ok=True)
        old_file = paths.site_assets_path / "old_file.txt"
        old_file.write_text("should be removed")

        _copy_assets(sample_assets, paths.site_assets_path)

        # Old file should be gone
        assert not old_file.exists()

        # New files should exist
        assert (paths.site_assets_path / "css" / "style.css").exists()


class TestWriteBuildManifest:
    """Tests for _write_build_manifest function."""

    def test_creates_manifest_file(self, config: Config, paths: PathManager) -> None:
        """Test that manifest file is created."""
        paths.site_root.mkdir(parents=True, exist_ok=True)

        stats = {
            "templates_rendered": ["index.html"],
            "data_files_written": 2,
            "assets_copied": 3,
            "errors": [],
        }

        _write_build_manifest(paths.site_root, config, stats)

        manifest_path = paths.site_root / "manifest.json"
        assert manifest_path.exists()

    def test_manifest_contains_metadata(self, config: Config, paths: PathManager) -> None:
        """Test that manifest contains correct metadata."""
        paths.site_root.mkdir(parents=True, exist_ok=True)

        stats = {
            "templates_rendered": ["index.html", "dashboard.html"],
            "data_files_written": 2,
            "assets_copied": 3,
            "errors": [],
        }

        _write_build_manifest(paths.site_root, config, stats)

        manifest_path = paths.site_root / "manifest.json"
        with manifest_path.open() as f:
            manifest = json.load(f)

        assert manifest["version"] == "1.0"
        assert manifest["year"] == config.github.windows.year
        assert manifest["target"] == config.github.target.name
        assert manifest["target_mode"] == config.github.target.mode
        assert "build_time" in manifest
        assert manifest["templates_rendered"] == ["index.html", "dashboard.html"]
        assert manifest["data_files_written"] == 2
        assert manifest["assets_copied"] == 3
        assert manifest["errors"] == []

    def test_manifest_includes_errors(self, config: Config, paths: PathManager) -> None:
        """Test that manifest includes error messages."""
        paths.site_root.mkdir(parents=True, exist_ok=True)

        stats = {
            "templates_rendered": [],
            "data_files_written": 0,
            "assets_copied": 0,
            "errors": ["Failed to render template", "Missing data file"],
        }

        _write_build_manifest(paths.site_root, config, stats)

        manifest_path = paths.site_root / "manifest.json"
        with manifest_path.open() as f:
            manifest = json.load(f)

        assert len(manifest["errors"]) == 2
        assert "Failed to render template" in manifest["errors"]


class TestGetAvailableYears:
    """Tests for get_available_years function."""

    def test_returns_empty_list_if_dir_missing(self, tmp_path: Path) -> None:
        """Test that empty list is returned if directory doesn't exist."""
        nonexistent_dir = tmp_path / "nonexistent"
        years = get_available_years(nonexistent_dir)
        assert years == []

    def test_finds_year_directories(self, tmp_path: Path) -> None:
        """Test that year directories are found and sorted."""
        # Create year directories
        (tmp_path / "2023").mkdir()
        (tmp_path / "2024").mkdir()
        (tmp_path / "2025").mkdir()

        years = get_available_years(tmp_path)

        assert years == [2025, 2024, 2023]

    def test_ignores_non_year_directories(self, tmp_path: Path) -> None:
        """Test that non-year directories are ignored."""
        # Create valid year directories
        (tmp_path / "2023").mkdir()
        (tmp_path / "2024").mkdir()

        # Create invalid directories
        (tmp_path / "data").mkdir()
        (tmp_path / "assets").mkdir()
        (tmp_path / "123").mkdir()  # Too short
        (tmp_path / "12345").mkdir()  # Too long
        (tmp_path / "abcd").mkdir()  # Not a number

        years = get_available_years(tmp_path)

        assert years == [2024, 2023]

    def test_ignores_files(self, tmp_path: Path) -> None:
        """Test that files are ignored, only directories are checked."""
        # Create year directories
        (tmp_path / "2023").mkdir()
        (tmp_path / "2024").mkdir()

        # Create files that look like years
        (tmp_path / "2025.txt").write_text("not a directory")
        (tmp_path / "2026").write_text("file, not directory")

        years = get_available_years(tmp_path)

        # Should only include directories
        assert 2023 in years
        assert 2024 in years
        assert 2025 not in years
        assert 2026 not in years

    def test_filters_unreasonable_years(self, tmp_path: Path) -> None:
        """Test that years outside reasonable range are filtered."""
        # Create valid year directories
        (tmp_path / "2023").mkdir()

        # Create unreasonable years
        (tmp_path / "1999").mkdir()  # Too old
        (tmp_path / "2101").mkdir()  # Too far in future

        years = get_available_years(tmp_path)

        assert years == [2023]


class TestGenerateRootRedirect:
    """Tests for _generate_root_redirect function."""

    def test_creates_redirect_file(self, tmp_path: Path) -> None:
        """Test that redirect index.html is created."""
        _generate_root_redirect(tmp_path, 2025)

        redirect_path = tmp_path / "index.html"
        assert redirect_path.exists()

    def test_redirect_contains_meta_refresh(self, tmp_path: Path) -> None:
        """Test that redirect file contains meta refresh tag."""
        _generate_root_redirect(tmp_path, 2025)

        content = (tmp_path / "index.html").read_text()
        assert 'meta http-equiv="refresh"' in content
        assert "url=/2025/" in content

    def test_redirect_contains_fallback_link(self, tmp_path: Path) -> None:
        """Test that redirect file contains fallback link."""
        _generate_root_redirect(tmp_path, 2025)

        content = (tmp_path / "index.html").read_text()
        assert 'href="/2025/"' in content
        assert "click here" in content

    def test_redirect_shows_target_year(self, tmp_path: Path) -> None:
        """Test that redirect page shows the target year."""
        _generate_root_redirect(tmp_path, 2024)

        content = (tmp_path / "index.html").read_text()
        assert "2024" in content

    def test_redirect_is_valid_html(self, tmp_path: Path) -> None:
        """Test that generated HTML is valid."""
        _generate_root_redirect(tmp_path, 2025)

        content = (tmp_path / "index.html").read_text()
        assert "<!DOCTYPE html>" in content
        assert "<html" in content
        assert "</html>" in content
        assert "<head>" in content
        assert "</head>" in content
        assert "<body>" in content
        assert "</body>" in content
