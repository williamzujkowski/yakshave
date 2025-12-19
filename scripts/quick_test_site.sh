#!/bin/bash
# Quick script to generate test data and build a test site
# Usage: ./scripts/quick_test_site.sh [year]
#
# If no year is provided, reads from config/config.yaml

set -e

# Extract year from config if not provided
if [ -z "$1" ]; then
    if [ -f "config/config.yaml" ]; then
        YEAR=$(grep -m1 "year:" config/config.yaml | awk '{print $2}')
        echo "Using year from config: ${YEAR}"
    else
        YEAR=2024
        echo "No config found, using default year: ${YEAR}"
    fi
else
    YEAR=$1
fi

DATA_DIR="data/metrics/year=${YEAR}"
SITE_DIR="site/${YEAR}"

echo "Quick Test Site Builder"
echo "======================"
echo "Year: ${YEAR}"
echo ""

# Step 1: Generate test data
echo "[1/3] Generating test metrics data..."
uv run python scripts/setup_test_data.py --year "${YEAR}" --output "${DATA_DIR}"

# Step 2: Export to JSON and build site
echo ""
echo "[2/3] Building site from test data..."
uv run gh-year-end report --config config/config.yaml

# Step 3: Summary
echo ""
echo "[3/3] Build complete!"
echo ""
echo "Site location: ${SITE_DIR}"
echo "Data files:"
ls -lh "${DATA_DIR}"/*.parquet | awk '{print "  - " $9 " (" $5 ")"}'

echo ""
echo "To view the site:"
echo "  Option 1: Open in browser"
echo "    open ${SITE_DIR}/index.html"
echo ""
echo "  Option 2: Start local server"
echo "    python -m http.server --directory ${SITE_DIR} 8000"
echo "    Then visit: http://localhost:8000"
echo ""
