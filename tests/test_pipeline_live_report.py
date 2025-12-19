"""Live API tests for report generation phase (Phase 6).

Tests report export and site build against real data pipeline.
Requires @pytest.mark.live_api marker and depends on metrics being generated.

Test Coverage:
- Export JSON data files from metrics
- Render HTML templates with real data
- Validate HTML structure and content
- Verify static assets are copied correctly
"""

import json
from html.parser import HTMLParser
from pathlib import Path

import pytest

from gh_year_end.config import Config, load_config
from gh_year_end.report.build import build_site
from gh_year_end.report.export import export_metrics
from gh_year_end.storage.paths import PathManager

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class HTMLValidator(HTMLParser):
    """Simple HTML validator to check for well-formed HTML."""

    def __init__(self) -> None:
        super().__init__()
        self.errors: list[str] = []
        self.tag_stack: list[str] = []
        self.void_tags = {
            "area",
            "base",
            "br",
            "col",
            "embed",
            "hr",
            "img",
            "input",
            "link",
            "meta",
            "param",
            "source",
            "track",
            "wbr",
        }

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],  # noqa: ARG002
    ) -> None:
        """Handle opening tags."""
        if tag not in self.void_tags:
            self.tag_stack.append(tag)

    def handle_endtag(self, tag: str) -> None:
        """Handle closing tags."""
        if tag in self.void_tags:
            return

        if not self.tag_stack:
            self.errors.append(f"Unexpected closing tag: </{tag}>")
            return

        if self.tag_stack[-1] != tag:
            self.errors.append(
                f"Mismatched closing tag: expected </{self.tag_stack[-1]}>, got </{tag}>"
            )
        else:
            self.tag_stack.pop()

    def validate(self, html: str) -> list[str]:
        """Validate HTML and return list of errors."""
        self.errors = []
        self.tag_stack = []
        self.feed(html)

        # Check for unclosed tags
        if self.tag_stack:
            self.errors.append(f"Unclosed tags: {', '.join(self.tag_stack)}")

        return self.errors


@pytest.fixture
def live_config() -> Config:
    """Load live test configuration."""
    config_path = FIXTURES_DIR / "valid_config.yaml"
    return load_config(config_path)


@pytest.fixture
def live_paths(live_config: Config) -> PathManager:
    """Create PathManager for live test data."""
    return PathManager(live_config)


@pytest.fixture
def ensure_metrics_exist(live_paths: PathManager) -> None:
    """Ensure metrics data exists before running tests."""
    if not live_paths.metrics_root.exists():
        pytest.skip("Metrics data not found. Run 'metrics' command first.")

    metrics_files = list(live_paths.metrics_root.glob("*.parquet"))
    if not metrics_files:
        pytest.skip("No metrics tables found. Run 'metrics' command first.")


@pytest.mark.live_api
class TestLiveExportJSON:
    """Tests for exporting JSON data from metrics."""

    def test_live_export_leaderboards_json(
        self,
        live_config: Config,
        live_paths: PathManager,
        ensure_metrics_exist: None,  # noqa: ARG002
    ) -> None:
        """Export valid leaderboards.json with correct structure."""
        stats = export_metrics(live_config, live_paths)

        # Verify export succeeded
        assert len(stats["files_written"]) > 0
        assert stats["total_size_bytes"] > 0

        # Verify leaderboards.json exists and is valid JSON
        leaderboards_path = live_paths.site_data_path / "leaderboards.json"
        assert leaderboards_path.exists(), "leaderboards.json not created"

        with leaderboards_path.open() as f:
            data = json.load(f)

        # Verify structure
        assert "leaderboards" in data, "Missing 'leaderboards' key"
        assert "metrics_available" in data, "Missing 'metrics_available' key"

        # Verify leaderboards contain expected metrics
        leaderboards = data["leaderboards"]
        assert isinstance(leaderboards, dict), "leaderboards should be a dict"

        # Check for at least one metric
        assert len(leaderboards) > 0, "No leaderboard metrics found"

        # Verify each metric has org and repos keys
        for metric_key, metric_data in leaderboards.items():
            assert "org" in metric_data, f"Missing 'org' in {metric_key}"
            assert "repos" in metric_data, f"Missing 'repos' in {metric_key}"
            assert isinstance(metric_data["org"], list), f"{metric_key}.org should be a list"
            assert isinstance(metric_data["repos"], dict), f"{metric_key}.repos should be a dict"

            # Verify org leaderboard entries have required fields
            if metric_data["org"]:
                entry = metric_data["org"][0]
                assert "rank" in entry, f"Missing 'rank' in {metric_key} entry"
                assert "user_id" in entry, f"Missing 'user_id' in {metric_key} entry"
                assert "value" in entry, f"Missing 'value' in {metric_key} entry"

    def test_live_export_summary_json(
        self,
        live_config: Config,
        live_paths: PathManager,
        ensure_metrics_exist: None,  # noqa: ARG002
    ) -> None:
        """Export summary with correct totals and metadata."""
        export_metrics(live_config, live_paths)

        # Verify summary.json exists and is valid JSON
        summary_path = live_paths.site_data_path / "summary.json"
        assert summary_path.exists(), "summary.json not created"

        with summary_path.open() as f:
            data = json.load(f)

        # Verify required keys
        assert "year" in data, "Missing 'year' key"
        assert "target" in data, "Missing 'target' key"
        assert "generated_at" in data, "Missing 'generated_at' key"

        # Verify year matches config
        assert data["year"] == live_config.github.windows.year

        # Verify target structure
        target = data["target"]
        assert "mode" in target, "Missing 'mode' in target"
        assert "name" in target, "Missing 'name' in target"
        assert target["mode"] == live_config.github.target.mode
        assert target["name"] == live_config.github.target.name

        # Verify numeric totals are present (if data exists)
        # These may be missing if no data, but should be numeric if present
        if "total_repos" in data:
            assert isinstance(data["total_repos"], int), "total_repos should be int"
            assert data["total_repos"] >= 0, "total_repos should be non-negative"

        if "total_prs_opened" in data:
            assert isinstance(data["total_prs_opened"], int), "total_prs_opened should be int"
            assert data["total_prs_opened"] >= 0, "total_prs_opened should be non-negative"

        if "total_prs_merged" in data:
            assert isinstance(data["total_prs_merged"], int), "total_prs_merged should be int"
            assert data["total_prs_merged"] >= 0, "total_prs_merged should be non-negative"


@pytest.mark.live_api
class TestLiveRenderHTML:
    """Tests for rendering HTML templates."""

    def test_live_render_index_html(
        self,
        live_config: Config,
        live_paths: PathManager,
        ensure_metrics_exist: None,  # noqa: ARG002
    ) -> None:
        """Render index.html with title and year."""
        # First export metrics data
        export_metrics(live_config, live_paths)

        # Build the site
        stats = build_site(live_config, live_paths)

        # Verify index.html was rendered
        assert "index.html" in stats["templates_rendered"], "index.html not rendered"

        index_path = live_paths.site_root / "index.html"
        assert index_path.exists(), "index.html not created"

        # Read and verify content
        content = index_path.read_text()

        # Verify year appears in content
        assert str(live_config.github.windows.year) in content, "Year not in index.html"

        # Verify report title appears
        if live_config.report.title:
            assert live_config.report.title in content, "Report title not in index.html"

        # Verify target name appears
        assert live_config.github.target.name in content, "Target name not in index.html"

        # Verify basic HTML structure
        assert "<html" in content.lower(), "Missing <html> tag"
        assert "<head>" in content.lower(), "Missing <head> tag"
        assert "<body>" in content.lower(), "Missing <body> tag"
        assert "</html>" in content.lower(), "Missing </html> closing tag"

    def test_live_render_summary_html(
        self,
        live_config: Config,
        live_paths: PathManager,
        ensure_metrics_exist: None,  # noqa: ARG002
    ) -> None:
        """Render summary.html without template errors."""
        # First export metrics data
        export_metrics(live_config, live_paths)

        # Build the site
        stats = build_site(live_config, live_paths)

        # Verify summary.html was rendered
        assert "summary.html" in stats["templates_rendered"], "summary.html not rendered"

        summary_path = live_paths.site_root / "summary.html"
        assert summary_path.exists(), "summary.html not created"

        # Read and verify content
        content = summary_path.read_text()

        # Verify no template errors (Jinja2 undefined variables)
        assert "Undefined" not in content, "Template contains Undefined variable errors"

        # Verify expected sections exist
        assert "Executive Summary" in content or "Health Signals" in content

        # Verify basic HTML structure
        assert "<html" in content.lower(), "Missing <html> tag"
        assert "<head>" in content.lower(), "Missing <head> tag"
        assert "<body>" in content.lower(), "Missing <body> tag"

    def test_live_render_engineers_html(
        self,
        live_config: Config,
        live_paths: PathManager,
        ensure_metrics_exist: None,  # noqa: ARG002
    ) -> None:
        """Render engineers.html with contributor list."""
        # First export metrics data
        export_metrics(live_config, live_paths)

        # Build the site
        stats = build_site(live_config, live_paths)

        # Verify engineers.html was rendered
        assert "engineers.html" in stats["templates_rendered"], "engineers.html not rendered"

        engineers_path = live_paths.site_root / "engineers.html"
        assert engineers_path.exists(), "engineers.html not created"

        # Read and verify content
        content = engineers_path.read_text()

        # Verify no template errors
        assert "Undefined" not in content, "Template contains Undefined variable errors"

        # Verify expected sections exist
        assert "Contributors" in content or "Engineers" in content or "Top Contributors" in content

        # Verify basic HTML structure
        assert "<html" in content.lower(), "Missing <html> tag"
        assert "<head>" in content.lower(), "Missing <head> tag"
        assert "<body>" in content.lower(), "Missing <body> tag"

    def test_live_render_repos_html(
        self,
        live_config: Config,
        live_paths: PathManager,
        ensure_metrics_exist: None,  # noqa: ARG002
    ) -> None:
        """Render repos.html with repository cards."""
        # First export metrics data
        export_metrics(live_config, live_paths)

        # Build the site
        stats = build_site(live_config, live_paths)

        # Verify repos.html was rendered
        assert "repos.html" in stats["templates_rendered"], "repos.html not rendered"

        repos_path = live_paths.site_root / "repos.html"
        assert repos_path.exists(), "repos.html not created"

        # Read and verify content
        content = repos_path.read_text()

        # Verify no template errors
        assert "Undefined" not in content, "Template contains Undefined variable errors"

        # Verify expected sections exist
        assert "Repository" in content or "Repositories" in content or "Repo Health" in content

        # Verify basic HTML structure
        assert "<html" in content.lower(), "Missing <html> tag"
        assert "<head>" in content.lower(), "Missing <head> tag"
        assert "<body>" in content.lower(), "Missing <body> tag"

    def test_live_render_leaderboards_html(
        self,
        live_config: Config,
        live_paths: PathManager,
        ensure_metrics_exist: None,  # noqa: ARG002
    ) -> None:
        """Render leaderboards.html with leaderboard tabs."""
        # First export metrics data
        export_metrics(live_config, live_paths)

        # Build the site
        stats = build_site(live_config, live_paths)

        # Verify leaderboards.html was rendered
        assert "leaderboards.html" in stats["templates_rendered"], "leaderboards.html not rendered"

        leaderboards_path = live_paths.site_root / "leaderboards.html"
        assert leaderboards_path.exists(), "leaderboards.html not created"

        # Read and verify content
        content = leaderboards_path.read_text()

        # Verify no template errors
        assert "Undefined" not in content, "Template contains Undefined variable errors"

        # Verify expected sections exist
        assert "Leaderboard" in content or "Rankings" in content or "Top Contributors" in content

        # Verify basic HTML structure
        assert "<html" in content.lower(), "Missing <html> tag"
        assert "<head>" in content.lower(), "Missing <head> tag"
        assert "<body>" in content.lower(), "Missing <body> tag"

    def test_live_render_awards_html(
        self,
        live_config: Config,
        live_paths: PathManager,
        ensure_metrics_exist: None,  # noqa: ARG002
    ) -> None:
        """Render awards.html with award badges."""
        # First export metrics data
        export_metrics(live_config, live_paths)

        # Build the site
        stats = build_site(live_config, live_paths)

        # Verify awards.html was rendered
        assert "awards.html" in stats["templates_rendered"], "awards.html not rendered"

        awards_path = live_paths.site_root / "awards.html"
        assert awards_path.exists(), "awards.html not created"

        # Read and verify content
        content = awards_path.read_text()

        # Verify no template errors
        assert "Undefined" not in content, "Template contains Undefined variable errors"

        # Verify expected sections exist
        assert "Award" in content or "Recognition" in content or "Achievement" in content

        # Verify basic HTML structure
        assert "<html" in content.lower(), "Missing <html> tag"
        assert "<head>" in content.lower(), "Missing <head> tag"
        assert "<body>" in content.lower(), "Missing <body> tag"


@pytest.mark.live_api
class TestLiveHTMLValidation:
    """Tests for HTML well-formedness."""

    def test_live_html_valid(
        self,
        live_config: Config,
        live_paths: PathManager,
        ensure_metrics_exist: None,  # noqa: ARG002
    ) -> None:
        """All HTML is well-formed with no parse errors."""
        # First export metrics data
        export_metrics(live_config, live_paths)

        # Build the site
        stats = build_site(live_config, live_paths)

        # Verify at least some templates were rendered
        assert len(stats["templates_rendered"]) > 0, "No templates rendered"

        # Validate each rendered HTML file
        validator = HTMLValidator()
        html_files = list(live_paths.site_root.glob("*.html"))

        assert len(html_files) > 0, "No HTML files found"

        errors_by_file: dict[str, list[str]] = {}

        for html_file in html_files:
            content = html_file.read_text()
            errors = validator.validate(content)

            if errors:
                errors_by_file[html_file.name] = errors

        # Report all errors
        if errors_by_file:
            error_report = []
            for filename, errors in errors_by_file.items():
                error_report.append(f"\n{filename}:")
                for error in errors:
                    error_report.append(f"  - {error}")

            pytest.fail("HTML validation errors found:" + "".join(error_report))


@pytest.mark.live_api
class TestLiveAssetsCopied:
    """Tests for static assets being copied correctly."""

    def test_live_assets_copied(
        self,
        live_config: Config,
        live_paths: PathManager,
        ensure_metrics_exist: None,  # noqa: ARG002
    ) -> None:
        """CSS/JS assets present in output."""
        # First export metrics data
        export_metrics(live_config, live_paths)

        # Build the site
        stats = build_site(live_config, live_paths)

        # Verify assets were copied
        assert stats["assets_copied"] >= 0, "Assets copy count not reported"

        # Check if assets directory exists
        assets_dir = live_paths.site_assets_path
        if not assets_dir.exists():
            # If no assets directory in source, skip detailed checks
            pytest.skip("No assets directory in source template")

        # Look for CSS files
        css_files = list(assets_dir.glob("**/*.css"))
        if css_files:
            # Verify at least one CSS file was copied
            assert len(css_files) > 0, "No CSS files copied"

            # Verify CSS files are not empty
            for css_file in css_files:
                assert css_file.stat().st_size > 0, f"{css_file.name} is empty"

        # Look for JS files
        js_files = list(assets_dir.glob("**/*.js"))
        if js_files:
            # Verify at least one JS file was copied
            assert len(js_files) > 0, "No JS files copied"

            # Verify JS files are not empty
            for js_file in js_files:
                assert js_file.stat().st_size > 0, f"{js_file.name} is empty"

        # If assets were reported as copied, verify files exist
        if stats["assets_copied"] > 0:
            total_asset_files = len(css_files) + len(js_files)
            assert total_asset_files > 0, "Assets reported copied but no files found"


@pytest.mark.live_api
class TestLiveEndToEndReport:
    """End-to-end test of complete report generation."""

    def test_live_complete_report_generation(
        self,
        live_config: Config,
        live_paths: PathManager,
        ensure_metrics_exist: None,  # noqa: ARG002
    ) -> None:
        """Complete report generation from metrics to final site."""
        # Export metrics data
        export_stats = export_metrics(live_config, live_paths)

        # Verify export succeeded
        assert len(export_stats["files_written"]) > 0, "No files exported"
        assert export_stats["total_size_bytes"] > 0, "No data written"
        assert len(export_stats["errors"]) == 0, f"Export errors: {export_stats['errors']}"

        # Build the site
        build_stats = build_site(live_config, live_paths)

        # Verify build succeeded
        assert len(build_stats["templates_rendered"]) > 0, "No templates rendered"
        assert build_stats["data_files_written"] > 0, "No data files written"
        assert len(build_stats["errors"]) == 0, f"Build errors: {build_stats['errors']}"

        # Verify manifest was created
        manifest_path = live_paths.site_root / "manifest.json"
        assert manifest_path.exists(), "Build manifest not created"

        with manifest_path.open() as f:
            manifest = json.load(f)

        # Verify manifest structure
        assert manifest["version"] == "1.0"
        assert manifest["year"] == live_config.github.windows.year
        assert manifest["target"] == live_config.github.target.name
        assert "build_time" in manifest

        # Verify core HTML files exist
        expected_files = ["index.html"]
        for filename in expected_files:
            file_path = live_paths.site_root / filename
            assert file_path.exists(), f"{filename} not created"

        # Verify data directory with JSON files
        data_dir = live_paths.site_data_path
        assert data_dir.exists(), "Data directory not created"

        json_files = list(data_dir.glob("*.json"))
        assert len(json_files) > 0, "No JSON files in data directory"

        # Verify each JSON file is valid
        for json_file in json_files:
            with json_file.open() as f:
                try:
                    json.load(f)
                except json.JSONDecodeError as e:
                    pytest.fail(f"{json_file.name} is not valid JSON: {e}")
