"""Smoke tests for generated static site.

These tests verify the structure, content, and validity of the generated
static site without requiring a browser or network access.
"""

import json
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def minimal_metrics_data() -> dict[str, Any]:
    """Create minimal metrics data for testing.

    Returns:
        Dictionary with minimal test data for all metrics tables.
    """
    return {
        "leaderboards": [
            {
                "user_id": 1,
                "username": "test_user",
                "prs_opened": 10,
                "prs_merged": 8,
                "issues_opened": 5,
                "reviews_submitted": 15,
            }
        ],
        "time_series": [
            {
                "date": "2025-01-01",
                "prs_opened": 2,
                "prs_merged": 1,
                "issues_opened": 1,
            },
            {
                "date": "2025-01-08",
                "prs_opened": 3,
                "prs_merged": 2,
                "issues_opened": 2,
            },
        ],
        "repo_health": [
            {
                "repo_id": 1,
                "repo_name": "test-org/test-repo",
                "health_score": 85,
                "prs_merged": 50,
                "mean_time_to_merge_hours": 24.5,
            }
        ],
        "hygiene_scores": [
            {
                "repo_id": 1,
                "repo_name": "test-org/test-repo",
                "hygiene_score": 75,
                "has_readme": True,
                "has_license": True,
                "has_security": False,
            }
        ],
        "awards": [
            {
                "award_id": "top_contributor",
                "user_id": 1,
                "username": "test_user",
                "description": "Most PRs merged",
            }
        ],
    }


@pytest.fixture
def site_dir(tmp_path: Path, minimal_metrics_data: dict[str, Any]) -> Path:
    """Create a minimal site structure with test data.

    Args:
        tmp_path: Temporary directory provided by pytest.
        minimal_metrics_data: Minimal test data.

    Returns:
        Path to the generated site directory.
    """
    site_root = tmp_path / "site" / "2025"
    site_root.mkdir(parents=True, exist_ok=True)

    # Create data directory with JSON files
    data_dir = site_root / "data"
    data_dir.mkdir(exist_ok=True)

    for metric_name, data in minimal_metrics_data.items():
        json_file = data_dir / f"{metric_name}.json"
        json_file.write_text(json.dumps(data, indent=2))

    # Create manifest.json
    manifest = {
        "generated_at": "2025-01-15T12:00:00Z",
        "year": 2025,
        "metrics": list(minimal_metrics_data.keys()),
        "version": "v1",
    }
    (data_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

    # Create assets directory
    assets_dir = site_root / "assets"
    assets_dir.mkdir(exist_ok=True)

    # Create minimal CSS
    css_file = assets_dir / "style.css"
    css_file.write_text(
        """
body {
    font-family: sans-serif;
    margin: 0;
    padding: 20px;
}
.container {
    max-width: 1200px;
    margin: 0 auto;
}
"""
    )

    # Create minimal JS
    js_file = assets_dir / "main.js"
    js_file.write_text(
        """
// Main application logic
document.addEventListener('DOMContentLoaded', function() {
    console.log('Site loaded');
});
"""
    )

    # Create D3.js placeholder (simulating CDN or bundled version)
    d3_file = assets_dir / "d3.min.js"
    d3_file.write_text("// D3.js v7 (placeholder for testing)")

    # Create minimal HTML pages
    index_html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GitHub Year in Review - 2025</title>
    <link rel="stylesheet" href="assets/style.css">
</head>
<body>
    <div class="container">
        <h1>GitHub Year in Review - 2025</h1>
        <div id="content"></div>
        <div id="charts"></div>
    </div>
    <script src="assets/d3.min.js"></script>
    <script src="assets/main.js"></script>
</body>
</html>
"""
    (site_root / "index.html").write_text(index_html)

    # Create exec view
    exec_html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Executive Summary - 2025</title>
    <link rel="stylesheet" href="assets/style.css">
</head>
<body>
    <div class="container">
        <h1>Executive Summary</h1>
        <div id="summary-charts"></div>
    </div>
    <script src="assets/d3.min.js"></script>
    <script src="assets/main.js"></script>
</body>
</html>
"""
    (site_root / "exec.html").write_text(exec_html)

    # Create engineer view
    engineer_html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Engineer Drilldown - 2025</title>
    <link rel="stylesheet" href="assets/style.css">
</head>
<body>
    <div class="container">
        <h1>Engineer Drilldown</h1>
        <div id="leaderboards"></div>
        <div id="detail-charts"></div>
    </div>
    <script src="assets/d3.min.js"></script>
    <script src="assets/main.js"></script>
</body>
</html>
"""
    (site_root / "engineer.html").write_text(engineer_html)

    return site_root


class TestSiteStructure:
    """Tests for site directory structure and file presence."""

    def test_site_root_exists(self, site_dir: Path) -> None:
        """Test that site root directory exists."""
        assert site_dir.exists()
        assert site_dir.is_dir()

    def test_html_files_exist(self, site_dir: Path) -> None:
        """Test that all expected HTML files exist."""
        expected_files = ["index.html", "exec.html", "engineer.html"]
        for filename in expected_files:
            file_path = site_dir / filename
            assert file_path.exists(), f"Missing HTML file: {filename}"
            assert file_path.is_file()

    def test_data_directory_exists(self, site_dir: Path) -> None:
        """Test that data directory exists and contains JSON files."""
        data_dir = site_dir / "data"
        assert data_dir.exists()
        assert data_dir.is_dir()

        # Check for JSON files
        json_files = list(data_dir.glob("*.json"))
        assert len(json_files) > 0, "No JSON files found in data directory"

    def test_assets_directory_exists(self, site_dir: Path) -> None:
        """Test that assets directory exists."""
        assets_dir = site_dir / "assets"
        assert assets_dir.exists()
        assert assets_dir.is_dir()

    def test_css_files_exist(self, site_dir: Path) -> None:
        """Test that CSS files exist."""
        css_file = site_dir / "assets" / "style.css"
        assert css_file.exists(), "Missing CSS file: style.css"
        assert css_file.stat().st_size > 0, "CSS file is empty"

    def test_js_files_exist(self, site_dir: Path) -> None:
        """Test that JavaScript files exist."""
        expected_js = ["main.js", "d3.min.js"]
        for filename in expected_js:
            js_file = site_dir / "assets" / filename
            assert js_file.exists(), f"Missing JS file: {filename}"
            assert js_file.stat().st_size > 0, f"JS file is empty: {filename}"

    def test_manifest_exists(self, site_dir: Path) -> None:
        """Test that manifest.json exists."""
        manifest_path = site_dir / "data" / "manifest.json"
        assert manifest_path.exists(), "Missing manifest.json"


class TestHTMLValidation:
    """Tests for HTML structure and validity."""

    def test_html_has_doctype(self, site_dir: Path) -> None:
        """Test that HTML files have DOCTYPE declaration."""
        html_files = ["index.html", "exec.html", "engineer.html"]
        for filename in html_files:
            content = (site_dir / filename).read_text()
            assert "<!DOCTYPE html>" in content, f"{filename} missing DOCTYPE declaration"

    def test_html_has_charset(self, site_dir: Path) -> None:
        """Test that HTML files have charset meta tag."""
        html_files = ["index.html", "exec.html", "engineer.html"]
        for filename in html_files:
            content = (site_dir / filename).read_text()
            assert 'charset="UTF-8"' in content or "charset='UTF-8'" in content, (
                f"{filename} missing charset meta tag"
            )

    def test_html_has_viewport(self, site_dir: Path) -> None:
        """Test that HTML files have viewport meta tag."""
        html_files = ["index.html", "exec.html", "engineer.html"]
        for filename in html_files:
            content = (site_dir / filename).read_text()
            assert 'name="viewport"' in content, f"{filename} missing viewport meta tag"

    def test_html_has_title(self, site_dir: Path) -> None:
        """Test that HTML files have title tag."""
        html_files = ["index.html", "exec.html", "engineer.html"]
        for filename in html_files:
            content = (site_dir / filename).read_text()
            assert "<title>" in content, f"{filename} missing title tag"
            assert "</title>" in content, f"{filename} missing closing title tag"

    def test_html_links_to_css(self, site_dir: Path) -> None:
        """Test that HTML files link to CSS."""
        html_files = ["index.html", "exec.html", "engineer.html"]
        for filename in html_files:
            content = (site_dir / filename).read_text()
            assert 'rel="stylesheet"' in content, f"{filename} missing stylesheet link"
            assert "assets/style.css" in content, f"{filename} missing link to style.css"

    def test_html_includes_js(self, site_dir: Path) -> None:
        """Test that HTML files include JavaScript."""
        html_files = ["index.html", "exec.html", "engineer.html"]
        for filename in html_files:
            content = (site_dir / filename).read_text()
            assert "<script" in content, f"{filename} missing script tag"
            assert "assets/main.js" in content, f"{filename} missing main.js"

    def test_html_includes_d3(self, site_dir: Path) -> None:
        """Test that HTML files include D3.js."""
        html_files = ["index.html", "exec.html", "engineer.html"]
        for filename in html_files:
            content = (site_dir / filename).read_text()
            assert "d3" in content.lower(), f"{filename} missing D3.js reference"

    def test_html_has_chart_containers(self, site_dir: Path) -> None:
        """Test that HTML files have containers for charts."""
        # index.html should have general content and charts
        index_content = (site_dir / "index.html").read_text()
        assert 'id="content"' in index_content or 'id="charts"' in index_content

        # exec.html should have summary charts
        exec_content = (site_dir / "exec.html").read_text()
        assert "charts" in exec_content.lower()

        # engineer.html should have detailed charts
        engineer_content = (site_dir / "engineer.html").read_text()
        assert "charts" in engineer_content.lower() or "leaderboard" in engineer_content.lower()

    def test_no_broken_internal_links(self, site_dir: Path) -> None:
        """Test that internal links point to existing files."""
        html_files = ["index.html", "exec.html", "engineer.html"]
        for filename in html_files:
            content = (site_dir / filename).read_text()

            # Check CSS links
            if 'href="assets/style.css"' in content:
                assert (site_dir / "assets" / "style.css").exists()

            # Check JS links
            if 'src="assets/main.js"' in content:
                assert (site_dir / "assets" / "main.js").exists()

            if 'src="assets/d3.min.js"' in content:
                assert (site_dir / "assets" / "d3.min.js").exists()


class TestJSONData:
    """Tests for JSON data files."""

    def test_manifest_is_valid_json(self, site_dir: Path) -> None:
        """Test that manifest.json is valid JSON."""
        manifest_path = site_dir / "data" / "manifest.json"
        content = manifest_path.read_text()
        manifest = json.loads(content)
        assert isinstance(manifest, dict)

    def test_manifest_has_required_fields(self, site_dir: Path) -> None:
        """Test that manifest has required fields."""
        manifest_path = site_dir / "data" / "manifest.json"
        manifest = json.loads(manifest_path.read_text())

        required_fields = ["generated_at", "year", "metrics", "version"]
        for field in required_fields:
            assert field in manifest, f"Manifest missing required field: {field}"

    def test_manifest_metrics_list_not_empty(self, site_dir: Path) -> None:
        """Test that manifest lists metrics."""
        manifest_path = site_dir / "data" / "manifest.json"
        manifest = json.loads(manifest_path.read_text())

        assert isinstance(manifest["metrics"], list)
        assert len(manifest["metrics"]) > 0, "Manifest has no metrics listed"

    def test_all_json_files_valid(self, site_dir: Path) -> None:
        """Test that all JSON files are valid JSON."""
        data_dir = site_dir / "data"
        json_files = list(data_dir.glob("*.json"))

        for json_file in json_files:
            content = json_file.read_text()
            try:
                data = json.loads(content)
                assert data is not None
            except json.JSONDecodeError as e:
                pytest.fail(f"Invalid JSON in {json_file.name}: {e}")

    def test_metrics_json_files_exist(self, site_dir: Path) -> None:
        """Test that expected metrics JSON files exist."""
        data_dir = site_dir / "data"
        expected_metrics = [
            "leaderboards",
            "time_series",
            "repo_health",
            "hygiene_scores",
            "awards",
        ]

        for metric in expected_metrics:
            json_file = data_dir / f"{metric}.json"
            assert json_file.exists(), f"Missing metrics file: {metric}.json"

    def test_leaderboards_data_structure(self, site_dir: Path) -> None:
        """Test that leaderboards.json has correct structure."""
        data_path = site_dir / "data" / "leaderboards.json"
        data = json.loads(data_path.read_text())

        assert isinstance(data, list), "Leaderboards should be an array"
        if len(data) > 0:
            item = data[0]
            required_fields = ["user_id", "username"]
            for field in required_fields:
                assert field in item, f"Leaderboard item missing field: {field}"

    def test_time_series_data_structure(self, site_dir: Path) -> None:
        """Test that time_series.json has correct structure."""
        data_path = site_dir / "data" / "time_series.json"
        data = json.loads(data_path.read_text())

        assert isinstance(data, list), "Time series should be an array"
        if len(data) > 0:
            item = data[0]
            assert "date" in item, "Time series item missing date field"

    def test_repo_health_data_structure(self, site_dir: Path) -> None:
        """Test that repo_health.json has correct structure."""
        data_path = site_dir / "data" / "repo_health.json"
        data = json.loads(data_path.read_text())

        assert isinstance(data, list), "Repo health should be an array"
        if len(data) > 0:
            item = data[0]
            required_fields = ["repo_id", "repo_name", "health_score"]
            for field in required_fields:
                assert field in item, f"Repo health item missing field: {field}"

    def test_no_empty_data_arrays(self, site_dir: Path) -> None:
        """Test that metrics JSON files have data (not empty arrays)."""
        data_dir = site_dir / "data"
        metrics_files = ["leaderboards.json", "time_series.json", "repo_health.json"]

        for filename in metrics_files:
            if (data_dir / filename).exists():
                data = json.loads((data_dir / filename).read_text())
                if isinstance(data, list):
                    assert len(data) > 0, f"{filename} has empty data array"


class TestAssets:
    """Tests for CSS and JavaScript assets."""

    def test_css_is_parseable(self, site_dir: Path) -> None:
        """Test that CSS file is parseable (basic syntax check)."""
        css_file = site_dir / "assets" / "style.css"
        content = css_file.read_text()

        # Basic syntax checks
        assert "{" in content, "CSS missing opening brace"
        assert "}" in content, "CSS missing closing brace"

        # Check for balanced braces
        open_braces = content.count("{")
        close_braces = content.count("}")
        assert open_braces == close_braces, "CSS has unbalanced braces"

    def test_js_is_parseable(self, site_dir: Path) -> None:
        """Test that JavaScript files have basic syntax validity."""
        js_files = ["main.js"]
        for filename in js_files:
            js_file = site_dir / "assets" / filename
            content = js_file.read_text()

            # Basic checks - should not have obvious syntax errors
            # Check for balanced braces and parentheses
            if "{" in content:
                open_braces = content.count("{")
                close_braces = content.count("}")
                # Allow some flexibility for minified code
                assert abs(open_braces - close_braces) <= 1, f"{filename} has unbalanced braces"

    def test_d3_library_present(self, site_dir: Path) -> None:
        """Test that D3.js library is present."""
        d3_file = site_dir / "assets" / "d3.min.js"
        assert d3_file.exists(), "D3.js library file missing"
        assert d3_file.stat().st_size > 0, "D3.js library file is empty"


@pytest.mark.integration
class TestSiteGeneration:
    """Integration tests for site generation from metrics data."""

    def test_generate_site_creates_all_files(
        self,
        tmp_path: Path,  # noqa: ARG002
        minimal_metrics_data: dict[str, Any],  # noqa: ARG002
    ) -> None:
        """Test that site generation creates all expected files.

        This is a placeholder for when report generation is implemented.

        Args:
            tmp_path: Temporary directory for test output.
            minimal_metrics_data: Test metrics data.
        """
        # When report generation is implemented, this test will verify:
        # 1. All expected HTML files are created
        # 2. All metrics are exported to JSON
        # 3. Assets are bundled correctly
        pytest.skip("Report generation not yet implemented")

    def test_site_renders_without_errors(self, site_dir: Path) -> None:
        """Test that site structure is complete for offline viewing.

        Args:
            site_dir: Path to generated site.
        """
        # Verify all critical files exist
        assert (site_dir / "index.html").exists()
        assert (site_dir / "data" / "manifest.json").exists()
        assert (site_dir / "assets" / "style.css").exists()
        assert (site_dir / "assets" / "main.js").exists()

        # Verify no template syntax errors in HTML
        html_files = list(site_dir.glob("*.html"))
        for html_file in html_files:
            content = html_file.read_text()
            # Check for unrendered Jinja2 syntax
            assert "{{" not in content, f"{html_file.name} has unrendered template syntax"
            assert "{%" not in content, f"{html_file.name} has unrendered template blocks"

    def test_charts_have_data_containers(self, site_dir: Path) -> None:
        """Test that HTML pages have containers for D3 charts.

        Args:
            site_dir: Path to generated site.
        """
        html_files = ["index.html", "exec.html", "engineer.html"]
        for filename in html_files:
            content = (site_dir / filename).read_text()

            # Check for div elements with IDs (common pattern for D3)
            assert 'id="' in content, f"{filename} has no elements with IDs for charts"

            # Verify at least one container element
            div_with_id = False
            lines = content.split("\n")
            for line in lines:
                if "<div" in line and 'id="' in line:
                    div_with_id = True
                    break

            assert div_with_id, f"{filename} has no div containers with IDs for charts"


class TestFilePermissions:
    """Tests for file permissions and accessibility."""

    def test_files_are_readable(self, site_dir: Path) -> None:
        """Test that all generated files are readable."""
        # Check HTML files
        for html_file in site_dir.glob("*.html"):
            assert html_file.stat().st_mode & 0o444, f"{html_file.name} is not readable"

        # Check JSON files
        for json_file in (site_dir / "data").glob("*.json"):
            assert json_file.stat().st_mode & 0o444, f"{json_file.name} is not readable"

        # Check assets
        for asset_file in (site_dir / "assets").iterdir():
            assert asset_file.stat().st_mode & 0o444, f"{asset_file.name} is not readable"

    def test_directories_are_accessible(self, site_dir: Path) -> None:
        """Test that directories are accessible."""
        directories = [site_dir, site_dir / "data", site_dir / "assets"]
        for directory in directories:
            if directory.exists():
                assert directory.stat().st_mode & 0o555, f"{directory.name} is not accessible"
