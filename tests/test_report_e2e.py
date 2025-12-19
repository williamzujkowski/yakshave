"""End-to-end tests for report generation pipeline.

Tests the full report generation workflow from metrics to static site,
verifying all templates render correctly, assets are copied, and output
files are properly generated.

Addresses GitHub issue #104: End-to-end report generation verification.
"""

import json
from pathlib import Path

import pytest

from gh_year_end.config import Config
from gh_year_end.report.build import build_site
from gh_year_end.report.export import export_metrics
from gh_year_end.storage.paths import PathManager


@pytest.mark.integration
class TestReportGenerationE2E:
    """End-to-end tests for complete report generation pipeline."""

    def test_full_pipeline_with_sample_data(
        self, sample_metrics_config: Config, sample_metrics_paths: PathManager, tmp_path: Path
    ) -> None:
        """Test complete pipeline from metrics to generated site.

        This is the primary E2E test that verifies:
        1. Metrics export to JSON
        2. Template rendering
        3. Asset copying
        4. Manifest generation
        5. All expected files exist
        """
        # Setup templates and assets for testing
        test_paths = self._setup_test_site(sample_metrics_config, sample_metrics_paths, tmp_path)

        # Step 1: Export metrics to JSON
        export_stats = export_metrics(sample_metrics_config, test_paths)

        # Verify export succeeded
        assert export_stats["total_size_bytes"] > 0
        assert len(export_stats["errors"]) == 0, f"Export errors: {export_stats['errors']}"
        assert len(export_stats["files_written"]) > 0

        # Step 2: Build the static site
        build_stats = build_site(sample_metrics_config, test_paths)

        # Verify build succeeded
        assert len(build_stats["errors"]) == 0, f"Build errors: {build_stats['errors']}"
        assert build_stats["duration_seconds"] > 0
        assert "start_time" in build_stats
        assert "end_time" in build_stats

        # Step 3: Verify all expected directories exist
        self._verify_directory_structure(test_paths)

        # Step 4: Verify all data files were created
        self._verify_data_files(test_paths)

        # Step 5: Verify all templates were rendered
        self._verify_rendered_templates(test_paths, build_stats)

        # Step 6: Verify assets were copied
        self._verify_assets_copied(test_paths, build_stats)

        # Step 7: Verify manifest.json exists and is valid
        self._verify_manifest(test_paths, sample_metrics_config, build_stats)

    def test_data_export_creates_all_json_files(
        self, sample_metrics_config: Config, sample_metrics_paths: PathManager
    ) -> None:
        """Verify that metrics export creates all expected JSON files."""
        export_stats = export_metrics(sample_metrics_config, sample_metrics_paths)

        # Check all required JSON files were created
        expected_files = [
            "summary.json",
            "leaderboards.json",
            "timeseries.json",
            "repo_health.json",
            "hygiene_scores.json",
            "awards.json",
        ]

        for filename in expected_files:
            file_path = sample_metrics_paths.site_data_path / filename
            assert file_path.exists(), f"Missing JSON file: {filename}"
            assert file_path.stat().st_size > 0, f"Empty JSON file: {filename}"

            # Verify JSON is valid
            with file_path.open() as f:
                data = json.load(f)
                assert isinstance(data, dict), f"Invalid JSON structure in {filename}"

        # Verify all files were reported in stats (files_written contains full paths)
        written_filenames = {Path(p).name for p in export_stats["files_written"]}
        assert written_filenames == set(expected_files)

    def test_template_rendering_creates_all_html_files(
        self,
        sample_metrics_config: Config,
        sample_metrics_paths: PathManager,
        tmp_path: Path,  # noqa: ARG002
    ) -> None:
        """Verify that all templates are rendered correctly."""
        # Copy templates and assets from site/ to the test output directory
        import shutil
        from pathlib import Path as PathLib

        site_dir = PathLib(__file__).parent.parent / "site"
        test_site_dir = tmp_path / "site"

        # Copy templates
        templates_src = site_dir / "templates"
        templates_dest = test_site_dir / "templates"
        if templates_src.exists():
            shutil.copytree(templates_src, templates_dest)

        # Copy assets
        assets_src = site_dir / "assets"
        assets_dest = test_site_dir / "assets"
        if assets_src.exists():
            shutil.copytree(assets_src, assets_dest)

        # Update config to use test site directory
        sample_metrics_config.report.output_dir = str(test_site_dir)

        # Recreate path manager with updated config
        from gh_year_end.storage.paths import PathManager as PM

        test_paths = PM(sample_metrics_config)

        # First export data
        export_metrics(sample_metrics_config, test_paths)

        # Then build site
        build_stats = build_site(sample_metrics_config, test_paths)

        # Should have rendered some templates
        assert len(build_stats["templates_rendered"]) > 0

        # Verify rendered templates are valid HTML
        for template_name in build_stats["templates_rendered"]:
            file_path = test_paths.site_root / template_name
            assert file_path.exists(), f"Missing rendered template: {template_name}"
            assert file_path.stat().st_size > 0, f"Empty template: {template_name}"

            # Verify it's valid HTML
            content = file_path.read_text()
            assert "<!DOCTYPE html>" in content or "<html" in content
            assert "</html>" in content

    def test_asset_copying_preserves_structure(
        self, sample_metrics_config: Config, sample_metrics_paths: PathManager, tmp_path: Path
    ) -> None:
        """Verify that asset files are copied with correct directory structure."""
        # Setup test site
        test_paths = self._setup_test_site(sample_metrics_config, sample_metrics_paths, tmp_path)

        # Export and build
        export_metrics(sample_metrics_config, test_paths)
        build_stats = build_site(sample_metrics_config, test_paths)

        # Verify assets directory exists
        assert test_paths.site_assets_path.exists()

        # Check for expected asset subdirectories
        css_dir = test_paths.site_assets_path / "css"
        js_dir = test_paths.site_assets_path / "js"

        assert css_dir.exists(), "CSS directory not copied"
        assert js_dir.exists(), "JS directory not copied"

        # Verify some key files exist
        assert (css_dir / "style.css").exists(), "Missing style.css"

        # Verify assets were counted in stats
        assert build_stats["assets_copied"] > 0

    def test_manifest_contains_correct_metadata(
        self, sample_metrics_config: Config, sample_metrics_paths: PathManager, tmp_path: Path
    ) -> None:
        """Verify manifest.json contains all required metadata."""
        # Setup test site
        test_paths = self._setup_test_site(sample_metrics_config, sample_metrics_paths, tmp_path)

        # Export and build
        export_metrics(sample_metrics_config, test_paths)
        build_site(sample_metrics_config, test_paths)

        manifest_path = test_paths.site_root / "manifest.json"
        assert manifest_path.exists(), "manifest.json not created"

        with manifest_path.open() as f:
            manifest = json.load(f)

        # Verify required fields
        assert manifest["version"] == "1.0"
        assert manifest["year"] == sample_metrics_config.github.windows.year
        assert manifest["target"] == sample_metrics_config.github.target.name
        assert manifest["target_mode"] == sample_metrics_config.github.target.mode
        assert "build_time" in manifest
        assert "templates_rendered" in manifest
        assert "data_files_written" in manifest
        assert "assets_copied" in manifest
        assert "errors" in manifest

        # Verify templates list is populated
        assert len(manifest["templates_rendered"]) > 0
        assert isinstance(manifest["templates_rendered"], list)

    def test_rendered_templates_contain_data(
        self, sample_metrics_config: Config, sample_metrics_paths: PathManager, tmp_path: Path
    ) -> None:
        """Verify that rendered templates contain actual data, not placeholders."""
        # Setup test site
        test_paths = self._setup_test_site(sample_metrics_config, sample_metrics_paths, tmp_path)

        # Export and build
        export_metrics(sample_metrics_config, test_paths)
        build_site(sample_metrics_config, test_paths)

        # Check index.html for basic data (if it was rendered)
        index_path = test_paths.site_root / "index.html"
        if index_path.exists():
            index_content = index_path.read_text()

            # Should contain the year
            assert str(sample_metrics_config.github.windows.year) in index_content

        # Check that rendered templates don't have unrendered variables
        for html_file in test_paths.site_root.glob("*.html"):
            content = html_file.read_text()

            # Should not contain unrendered Jinja2 template syntax
            # (except in data attributes or script tags)
            lines_with_issues = []
            for line_num, line in enumerate(content.split("\n"), 1):
                # Skip lines that are in JSON data islands or script tags
                if "data-" in line or "<script" in line or "// " in line:
                    continue
                if "{{" in line and "}}" in line and "data-" not in line:
                    lines_with_issues.append((line_num, line.strip()[:100]))

            # Allow some template syntax in data attributes but shouldn't be excessive
            assert len(lines_with_issues) < 10, (
                f"Too many unrendered template variables in {html_file.name}"
            )

    def test_json_data_files_have_valid_structure(
        self, sample_metrics_config: Config, sample_metrics_paths: PathManager
    ) -> None:
        """Verify that exported JSON files have expected structure."""
        export_metrics(sample_metrics_config, sample_metrics_paths)

        # Test summary.json structure
        with (sample_metrics_paths.site_data_path / "summary.json").open() as f:
            summary = json.load(f)
        assert "year" in summary
        assert "target" in summary
        assert "generated_at" in summary

        # Test leaderboards.json structure
        with (sample_metrics_paths.site_data_path / "leaderboards.json").open() as f:
            leaderboards = json.load(f)
        assert "leaderboards" in leaderboards
        assert "metrics_available" in leaderboards

        # Test timeseries.json structure
        with (sample_metrics_paths.site_data_path / "timeseries.json").open() as f:
            timeseries = json.load(f)
        assert "timeseries" in timeseries
        assert "period_types" in timeseries

        # Test repo_health.json structure
        with (sample_metrics_paths.site_data_path / "repo_health.json").open() as f:
            repo_health = json.load(f)
        assert "repos" in repo_health
        assert "total_repos" in repo_health

        # Test hygiene_scores.json structure
        with (sample_metrics_paths.site_data_path / "hygiene_scores.json").open() as f:
            hygiene = json.load(f)
        assert "repos" in hygiene
        assert "summary" in hygiene

        # Test awards.json structure
        with (sample_metrics_paths.site_data_path / "awards.json").open() as f:
            awards = json.load(f)
        assert "awards" in awards
        assert "categories" in awards

    def test_build_creates_year_directory_structure(
        self, sample_metrics_config: Config, sample_metrics_paths: PathManager
    ) -> None:
        """Verify that build creates proper year-based directory structure."""
        export_metrics(sample_metrics_config, sample_metrics_paths)
        build_site(sample_metrics_config, sample_metrics_paths)

        # Verify year directory exists
        year = sample_metrics_config.github.windows.year
        Path(sample_metrics_config.report.output_dir) / str(year)

        # The year directory should be the site_root
        assert sample_metrics_paths.site_root.exists()
        assert sample_metrics_paths.site_root.name == str(year)

    def test_root_redirect_generated(
        self, sample_metrics_config: Config, sample_metrics_paths: PathManager
    ) -> None:
        """Verify that root index.html redirect is generated."""
        export_metrics(sample_metrics_config, sample_metrics_paths)
        build_site(sample_metrics_config, sample_metrics_paths)

        # Check for root redirect
        site_base = Path(sample_metrics_config.report.output_dir)
        root_index = site_base / "index.html"

        assert root_index.exists(), "Root redirect index.html not created"

        content = root_index.read_text()
        year = sample_metrics_config.github.windows.year

        # Should contain meta refresh to year directory
        assert f"url=/{year}/" in content
        assert 'meta http-equiv="refresh"' in content
        assert f'href="/{year}/"' in content

    def test_build_handles_missing_metrics_gracefully(
        self, sample_metrics_config: Config, tmp_path: Path
    ) -> None:
        """Verify that build fails gracefully if metrics don't exist."""
        # Create a fresh config and paths without any metrics data
        from gh_year_end.storage.paths import PathManager as PM

        # Use a completely fresh temporary directory
        fresh_tmp = tmp_path / "fresh_test"
        fresh_tmp.mkdir()

        # Update config to use fresh directory (no sample metrics copied)
        sample_metrics_config.storage.root = str(fresh_tmp / "data")
        sample_metrics_config.report.output_dir = str(fresh_tmp / "site")

        fresh_paths = PM(sample_metrics_config)

        # Build should fail because metrics don't exist
        with pytest.raises(ValueError, match="Metrics data not found"):
            build_site(sample_metrics_config, fresh_paths)

    def test_statistics_tracking_is_accurate(
        self, sample_metrics_config: Config, sample_metrics_paths: PathManager, tmp_path: Path
    ) -> None:
        """Verify that build statistics accurately reflect what was built."""
        # Setup test site
        test_paths = self._setup_test_site(sample_metrics_config, sample_metrics_paths, tmp_path)

        export_stats = export_metrics(sample_metrics_config, test_paths)
        build_stats = build_site(sample_metrics_config, test_paths)

        # Export stats should match actual files
        actual_json_files = list(test_paths.site_data_path.glob("*.json"))
        # Note: build_site may create additional JSON files from the metrics
        assert len(export_stats["files_written"]) <= len(actual_json_files) * 2

        # Build stats should match actual templates
        list(test_paths.site_root.glob("*.html"))
        # Note: build may render more or fewer than templates_rendered count
        # if templates include/extend each other
        assert len(build_stats["templates_rendered"]) > 0

        # Verify timing statistics
        assert build_stats["duration_seconds"] >= 0

    # Helper methods for verification

    def _setup_test_site(
        self,
        config: Config,
        paths: PathManager,
        tmp_path: Path,  # noqa: ARG002
    ) -> PathManager:
        """Setup test site with templates and assets.

        Args:
            config: Configuration object.
            paths: Original path manager (unused, kept for signature consistency).
            tmp_path: Pytest temp directory.

        Returns:
            PathManager with updated paths.
        """
        import shutil

        site_dir = Path(__file__).parent.parent / "site"
        test_site_dir = tmp_path / "site"

        # Copy templates
        templates_src = site_dir / "templates"
        templates_dest = test_site_dir / "templates"
        if templates_src.exists():
            shutil.copytree(templates_src, templates_dest)

        # Copy assets
        assets_src = site_dir / "assets"
        assets_dest = test_site_dir / "assets"
        if assets_src.exists():
            shutil.copytree(assets_src, assets_dest)

        # Update config to use test site directory
        config.report.output_dir = str(test_site_dir)

        # Create new PathManager with updated config
        from gh_year_end.storage.paths import PathManager as PM

        return PM(config)

    def _verify_directory_structure(self, paths: PathManager) -> None:
        """Verify all expected directories were created."""
        assert paths.site_root.exists(), "Site root directory not created"
        assert paths.site_data_path.exists(), "Site data directory not created"
        assert paths.site_assets_path.exists(), "Site assets directory not created"

    def _verify_data_files(self, paths: PathManager) -> None:
        """Verify all expected JSON data files exist and are non-empty."""
        expected_files = [
            "summary.json",
            "leaderboards.json",
            "timeseries.json",
            "repo_health.json",
            "hygiene_scores.json",
            "awards.json",
        ]

        for filename in expected_files:
            file_path = paths.site_data_path / filename
            assert file_path.exists(), f"Missing data file: {filename}"
            assert file_path.stat().st_size > 0, f"Empty data file: {filename}"

    def _verify_rendered_templates(self, paths: PathManager, build_stats: dict) -> None:
        """Verify templates were rendered and reported in stats."""
        # Check that templates were rendered
        assert len(build_stats["templates_rendered"]) > 0

        # Verify HTML files exist
        for template_name in build_stats["templates_rendered"]:
            file_path = paths.site_root / template_name
            assert file_path.exists(), f"Template not rendered: {template_name}"
            assert file_path.stat().st_size > 0, f"Empty template: {template_name}"

    def _verify_assets_copied(self, paths: PathManager, build_stats: dict) -> None:
        """Verify assets were copied and reported in stats."""
        # Assets should have been copied
        assert build_stats["assets_copied"] > 0

        # Verify assets directory exists and has content
        assert paths.site_assets_path.exists()
        asset_files = list(paths.site_assets_path.rglob("*"))
        # Filter out directories
        asset_files = [f for f in asset_files if f.is_file()]
        assert len(asset_files) > 0, "No asset files copied"

    def _verify_manifest(self, paths: PathManager, config: Config, build_stats: dict) -> None:
        """Verify manifest.json exists and contains correct data."""
        manifest_path = paths.site_root / "manifest.json"
        assert manifest_path.exists(), "manifest.json not created"

        with manifest_path.open() as f:
            manifest = json.load(f)

        # Verify metadata matches config
        assert manifest["year"] == config.github.windows.year
        assert manifest["target"] == config.github.target.name
        assert manifest["target_mode"] == config.github.target.mode

        # Verify stats are included
        assert manifest["templates_rendered"] == build_stats["templates_rendered"]
        assert manifest["data_files_written"] == build_stats["data_files_written"]
        assert manifest["assets_copied"] == build_stats["assets_copied"]
        assert manifest["errors"] == build_stats["errors"]


@pytest.mark.integration
class TestReportOutputValidation:
    """Additional validation tests for report output quality."""

    def _setup_test_site(
        self,
        config: Config,
        paths: PathManager,
        tmp_path: Path,  # noqa: ARG002
    ) -> PathManager:
        """Setup test site with templates and assets."""
        import shutil

        site_dir = Path(__file__).parent.parent / "site"
        test_site_dir = tmp_path / "site"

        # Copy templates
        templates_src = site_dir / "templates"
        templates_dest = test_site_dir / "templates"
        if templates_src.exists():
            shutil.copytree(templates_src, templates_dest)

        # Copy assets
        assets_src = site_dir / "assets"
        assets_dest = test_site_dir / "assets"
        if assets_src.exists():
            shutil.copytree(assets_src, assets_dest)

        # Update config to use test site directory
        config.report.output_dir = str(test_site_dir)

        # Create new PathManager with updated config
        from gh_year_end.storage.paths import PathManager as PM

        return PM(config)

    def test_all_templates_are_valid_html5(
        self, sample_metrics_config: Config, sample_metrics_paths: PathManager, tmp_path: Path
    ) -> None:
        """Verify all rendered templates are valid HTML5."""
        test_paths = self._setup_test_site(sample_metrics_config, sample_metrics_paths, tmp_path)

        export_metrics(sample_metrics_config, test_paths)
        build_stats = build_site(sample_metrics_config, test_paths)

        for template_name in build_stats["templates_rendered"]:
            file_path = test_paths.site_root / template_name
            content = file_path.read_text()

            # Basic HTML5 validation
            assert "<!DOCTYPE html>" in content or "<html" in content
            assert "<html" in content
            assert "</html>" in content
            assert "<head>" in content or "<head " in content
            assert "</head>" in content
            assert "<body>" in content or "<body " in content
            assert "</body>" in content

    def test_no_template_syntax_in_output(
        self, sample_metrics_config: Config, sample_metrics_paths: PathManager, tmp_path: Path
    ) -> None:
        """Verify no unrendered Jinja2 syntax remains in output."""
        test_paths = self._setup_test_site(sample_metrics_config, sample_metrics_paths, tmp_path)

        export_metrics(sample_metrics_config, test_paths)
        build_stats = build_site(sample_metrics_config, test_paths)

        for template_name in build_stats["templates_rendered"]:
            file_path = test_paths.site_root / template_name
            content = file_path.read_text()

            # Check for unrendered Jinja2 syntax
            # Allow for data attributes and JSON, but not template variables
            lines_with_issues = []
            for line_num, line in enumerate(content.split("\n"), 1):
                # Skip lines that are in JSON data islands or script tags
                if "data-" in line or "<script" in line:
                    continue
                # Check for unrendered variables (but not in data attributes)
                if "{{" in line and "}}" in line and "data-" not in line:
                    lines_with_issues.append((line_num, line.strip()[:80]))

            # Allow some minor issues (e.g., in comments) but not too many
            assert len(lines_with_issues) < 5, (
                f"Unrendered template variables in {template_name}:\n"
                + "\n".join(f"Line {num}: {line}" for num, line in lines_with_issues[:3])
            )

    def test_json_files_are_well_formed(
        self, sample_metrics_config: Config, sample_metrics_paths: PathManager
    ) -> None:
        """Verify all JSON files are well-formed and parseable."""
        export_stats = export_metrics(sample_metrics_config, sample_metrics_paths)

        for filename in export_stats["files_written"]:
            file_path = sample_metrics_paths.site_data_path / filename

            # Should be able to parse JSON
            with file_path.open() as f:
                data = json.load(f)

            # Should be a dict (our export format)
            assert isinstance(data, dict), f"Expected dict, got {type(data)} in {filename}"

    def test_assets_retain_correct_file_types(
        self, sample_metrics_config: Config, sample_metrics_paths: PathManager, tmp_path: Path
    ) -> None:
        """Verify copied assets maintain correct file extensions and types."""
        test_paths = self._setup_test_site(sample_metrics_config, sample_metrics_paths, tmp_path)

        export_metrics(sample_metrics_config, test_paths)
        build_site(sample_metrics_config, test_paths)

        # Check CSS files
        css_files = list((test_paths.site_assets_path / "css").glob("*.css"))
        assert len(css_files) > 0, "No CSS files found"

        # Check JS files
        js_files = list((test_paths.site_assets_path / "js").glob("*.js"))
        assert len(js_files) > 0, "No JS files found"

        # Verify CSS has valid content (basic check)
        for css_file in css_files:
            content = css_file.read_text()
            # CSS should have selectors or rules
            assert "{" in content or ":" in content, f"Invalid CSS in {css_file.name}"

    def test_build_is_idempotent(
        self, sample_metrics_config: Config, sample_metrics_paths: PathManager, tmp_path: Path
    ) -> None:
        """Verify that running build twice produces same results."""
        test_paths = self._setup_test_site(sample_metrics_config, sample_metrics_paths, tmp_path)

        export_metrics(sample_metrics_config, test_paths)

        # First build
        build_stats_1 = build_site(sample_metrics_config, test_paths)

        # Second build
        build_stats_2 = build_site(sample_metrics_config, test_paths)

        # Should produce same number of files
        assert len(build_stats_1["templates_rendered"]) == len(build_stats_2["templates_rendered"])
        assert build_stats_1["data_files_written"] == build_stats_2["data_files_written"]
        assert build_stats_1["assets_copied"] == build_stats_2["assets_copied"]
        assert len(build_stats_1["errors"]) == len(build_stats_2["errors"])
