# Playwright Tests - Quick Start Guide

**5-Minute Setup and Run**

## Quick Setup

```bash
# 1. Install dependencies
uv pip install playwright pytest-playwright
uv run python -m playwright install chromium

# 2. Copy test data
python scripts/copy_test_site_data.py --year 2024 --force

# 3. Start HTTP server
cd site/2024 && python -m http.server 8888 --bind 127.0.0.1 &

# 4. Run tests
uv run pytest tests/test_playwright_website_validation.py -v
```

## Expected Output

```
============================= test session starts ==============================
collected 48 items

TestPageLoading::test_page_loads_successfully[chromium-index.html] PASSED [  2%]
TestPageLoading::test_page_has_header[chromium-index.html] PASSED [  4%]
...
TestPageLoading::test_page_has_footer[chromium-awards.html] PASSED [100%]

============================= 48 passed in 19.28s ==============================
```

## Common Commands

```bash
# Run all tests
uv run pytest tests/test_playwright_website_validation.py -v

# Run specific test category
uv run pytest tests/test_playwright_website_validation.py::TestYearSelector -v
uv run pytest tests/test_playwright_website_validation.py::TestThemeToggle -v
uv run pytest tests/test_playwright_website_validation.py::TestNavigation -v

# Run with headed browser (see browser window)
uv run pytest tests/test_playwright_website_validation.py -v --headed

# Run with slowmo (slow down actions for visibility)
uv run pytest tests/test_playwright_website_validation.py -v --headed --slowmo 1000

# Generate HTML test report
uv run pytest tests/test_playwright_website_validation.py --html=report.html --self-contained-html
```

## What Gets Tested

- ✅ All 6 pages load (index, summary, engineers, leaderboards, repos, awards)
- ✅ Year selector dropdown functionality
- ✅ Theme toggle (light/dark mode)
- ✅ Navigation between pages
- ✅ Data visualizations (charts, cards, tables)
- ✅ Responsive design (mobile, tablet)
- ✅ Accessibility (ARIA labels)
- ✅ Error handling (404s, JS errors)

## Troubleshooting

**Server not running?**
```bash
# Kill any existing server on port 8888
pkill -f "python -m http.server 8888"

# Start fresh
cd site/2024 && python -m http.server 8888 --bind 127.0.0.1 &
```

**Test data missing?**
```bash
# Check data files exist
ls -la site/2024/data/*.json

# Re-copy if needed
python scripts/copy_test_site_data.py --year 2024 --force
```

**Playwright not installed?**
```bash
# Verify Playwright is installed
uv pip list | grep playwright

# Install if missing
uv pip install playwright pytest-playwright
uv run python -m playwright install chromium
```

## Full Documentation

See `/home/william/git/yakshave/docs/PLAYWRIGHT_VALIDATION.md` for comprehensive documentation.
