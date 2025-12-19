# Playwright Website Validation

**Status**: Complete
**Issue**: #103
**Created**: 2025-12-18
**Test Suite**: `/home/william/git/yakshave/tests/test_playwright_website_validation.py`

## Overview

Automated Playwright tests for validating the gh-year-end static website. This test suite provides comprehensive validation of all pages, navigation, interactive elements, data visualizations, and accessibility features.

## Test Results Summary

**Total Tests**: 48
**Passed**: 48 (100%)
**Failed**: 0
**Execution Time**: ~19 seconds

## Test Coverage

### 1. Page Loading (24 tests)

Tests that all 6 main pages load correctly with proper structure:

- `index.html` - Overview page with statistics and charts
- `summary.html` - Executive summary view
- `engineers.html` - Contributors page
- `leaderboards.html` - Leaderboards and rankings
- `repos.html` - Repository health information
- `awards.html` - Awards and recognition

**Validations per page**:
- HTTP 200 status code
- Correct page title
- Site header present
- Sidebar navigation present
- Site footer present

**Status**: All 24 tests passing

### 2. Year Selector Dropdown (3 tests)

Tests the year selector dropdown functionality in the header:

- `test_year_selector_visible` - Verifies button is visible with current year (2024)
- `test_year_selector_toggle` - Tests opening and closing via aria-hidden attribute
- `test_year_selector_has_options` - Validates year options exist (2024 active, 2025 available)

**Status**: All 3 tests passing

### 3. Theme Toggle (3 tests)

Tests dark/light mode toggle functionality:

- `test_theme_toggle_button_visible` - Verifies toggle button is visible
- `test_theme_toggle_switches_modes` - Tests switching between light and dark modes via data-theme attribute
- `test_theme_persists_across_pages` - Validates theme preference persists during navigation

**Status**: All 3 tests passing

### 4. Navigation (3 tests)

Tests sidebar navigation functionality:

- `test_sidebar_navigation_links` - Verifies all 6 nav links have correct href attributes
- `test_navigation_to_each_page` - Tests clicking links navigates to correct pages
- `test_active_nav_link_highlights` - Validates active page is highlighted in navigation

**Status**: All 3 tests passing

### 5. Data Visualizations (6 tests)

Tests that data renders correctly on various pages:

- `test_index_page_stat_cards` - Validates 4 stat cards with values (Contributors, PRs, Reviews, Repos)
- `test_index_page_highlights` - Validates 4 highlight cards (Most Active Month, Avg Review Time, etc.)
- `test_index_page_activity_chart` - Verifies D3.js SVG chart renders on overview page
- `test_leaderboards_page_has_tables` - Validates leaderboards page content
- `test_repos_page_has_content` - Validates repositories page content
- `test_awards_page_has_content` - Validates awards page content

**Status**: All 6 tests passing

### 6. Responsive Design (2 tests)

Tests site usability across different viewport sizes:

- `test_mobile_viewport` - Tests at 375x667 (mobile)
- `test_tablet_viewport` - Tests at 768x1024 (tablet)

**Status**: All 2 tests passing

### 7. Accessibility (3 tests)

Tests ARIA attributes and accessibility features:

- `test_theme_toggle_has_aria_label` - Validates aria-label on theme toggle button
- `test_year_selector_has_aria_attributes` - Validates aria-label and aria-expanded on year selector
- `test_nav_has_aria_label` - Validates aria-label on main navigation

**Status**: All 3 tests passing

### 8. View Toggle (2 tests)

Tests executive vs engineer view switching:

- `test_view_toggle_buttons_visible` - Verifies both view toggle buttons are present
- `test_view_toggle_switches_views` - Tests switching between exec and engineer views

**Status**: All 2 tests passing

### 9. Error Handling (2 tests)

Tests error scenarios and edge cases:

- `test_missing_page_returns_404` - Validates 404 for non-existent pages
- `test_page_loads_without_javascript_errors` - Checks for console errors during page load

**Status**: All 2 tests passing

## Setup Instructions

### Prerequisites

1. Install Playwright and dependencies:
   ```bash
   uv pip install playwright pytest-playwright
   uv run python -m playwright install chromium
   ```

2. Copy test data to site directory:
   ```bash
   python scripts/copy_test_site_data.py --year 2024 --force
   ```

3. Start local HTTP server:
   ```bash
   cd site/2024
   python -m http.server 8888 --bind 127.0.0.1 &
   ```

### Running Tests

Run all tests:
```bash
uv run pytest tests/test_playwright_website_validation.py -v
```

Run specific test class:
```bash
uv run pytest tests/test_playwright_website_validation.py::TestYearSelector -v
```

Run specific test:
```bash
uv run pytest tests/test_playwright_website_validation.py::TestThemeToggle::test_theme_toggle_switches_modes -v
```

Run with headed browser (see what's happening):
```bash
uv run pytest tests/test_playwright_website_validation.py -v --headed
```

Run with specific browser:
```bash
uv run pytest tests/test_playwright_website_validation.py -v --browser firefox
```

## Test Configuration

The test suite uses:
- **Base URL**: `http://127.0.0.1:8888`
- **Viewport**: 1920x1080 (configurable per test)
- **Browser**: Chromium (can use Firefox, WebKit)
- **Timeout**: Default Playwright timeouts apply

## Key Findings

### Validated Features

1. **All Pages Load Successfully**: All 6 main pages return HTTP 200 and render correctly
2. **Year Selector Works**: Dropdown opens/closes, shows available years (2024, 2025)
3. **Theme Toggle Works**: Successfully switches between light and dark modes
4. **Theme Persists**: Theme preference is maintained across page navigation
5. **Navigation Works**: All sidebar links navigate correctly, active page is highlighted
6. **Data Renders**: Stat cards, highlights, and D3.js charts render with data
7. **Responsive**: Site is usable on mobile (375px) and tablet (768px) viewports
8. **Accessible**: ARIA labels and attributes are present on interactive elements
9. **View Toggle Works**: Can switch between executive and engineer views
10. **Error Handling**: 404 pages work, no JavaScript console errors

### Test Data

The tests use sample data from `/home/william/git/yakshave/tests/fixtures/sample_site_data/`:
- `summary.json` - Overall statistics
- `leaderboards.json` - Rankings data
- `timeseries.json` - Activity timeline data
- `repo_health.json` - Repository health scores
- `hygiene_scores.json` - Code hygiene metrics
- `awards.json` - Awards and recognition data

### Browser Compatibility

Tests run on Chromium by default. The suite can be extended to test multiple browsers:
- Chromium (tested)
- Firefox (supported)
- WebKit (supported)

## Continuous Integration

This test suite can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Install Playwright
  run: |
    uv pip install playwright pytest-playwright
    uv run python -m playwright install chromium

- name: Start HTTP server
  run: |
    cd site/2024
    python -m http.server 8888 --bind 127.0.0.1 &
    sleep 2

- name: Run Playwright tests
  run: uv run pytest tests/test_playwright_website_validation.py -v
```

## Maintenance

### Adding New Tests

To add tests for new pages or features:

1. Add page to `PAGES` list if it's a new page
2. Create new test class for new feature category
3. Follow existing patterns for consistency
4. Use descriptive test names and docstrings

### Updating Tests

When site structure changes:

1. Update selectors if HTML structure changes
2. Update expected values if data format changes
3. Update viewport sizes if responsive breakpoints change
4. Run tests locally before committing

## Known Limitations

1. Tests require local HTTP server to be running
2. D3.js charts require 2-second wait for rendering
3. Some tests assume specific data structure from fixtures
4. Mobile tests use standard viewport sizes (may need device-specific testing)

## Future Enhancements

Potential improvements for the test suite:

1. **Cross-browser testing**: Run on Firefox and WebKit
2. **Screenshot comparison**: Visual regression testing
3. **Performance testing**: Measure page load times, chart render times
4. **Interaction testing**: More complex user flows (multi-page navigation)
5. **Data validation**: Verify data values match expected ranges
6. **Network testing**: Test offline mode, slow connections
7. **Animation testing**: Verify CSS transitions and animations
8. **Print testing**: Test print stylesheets
9. **SEO testing**: Validate meta tags, structured data
10. **Security testing**: Check for XSS, CSP headers

## Troubleshooting

### HTTP Server Not Running
```bash
# Check if server is running
curl http://127.0.0.1:8888/index.html

# Start server if not running
cd site/2024 && python -m http.server 8888 --bind 127.0.0.1 &
```

### Playwright Not Installed
```bash
# Install Playwright and browsers
uv pip install playwright pytest-playwright
uv run python -m playwright install chromium
```

### Test Data Missing
```bash
# Copy test fixtures to site directory
python scripts/copy_test_site_data.py --year 2024 --force
```

### Tests Timing Out
```bash
# Increase timeout in pytest.ini or via CLI
uv run pytest tests/test_playwright_website_validation.py -v --timeout=60
```

## References

- [Playwright Python Documentation](https://playwright.dev/python/)
- [pytest-playwright Plugin](https://github.com/microsoft/playwright-pytest)
- [gh-year-end Project Documentation](../README.md)
- [Test Data Setup](TEST_DATA_QUICK_START.md)

## Changelog

### 2025-12-18 - Initial Release
- Created comprehensive test suite with 48 tests
- Validated all 6 main pages
- Tested year selector, theme toggle, navigation
- Validated data visualizations, responsive design, accessibility
- All tests passing (48/48)
