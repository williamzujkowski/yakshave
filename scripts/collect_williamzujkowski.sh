#!/bin/bash
set -euo pipefail

# collect_williamzujkowski.sh
# Collects GitHub data for williamzujkowski for years 2024 and 2025

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
error() {
    echo -e "${RED}ERROR: $1${NC}" >&2
}

success() {
    echo -e "${GREEN}$1${NC}"
}

info() {
    echo -e "${BLUE}$1${NC}"
}

warning() {
    echo -e "${YELLOW}$1${NC}"
}

# Check for GITHUB_TOKEN
if [[ -z "${GITHUB_TOKEN:-}" ]]; then
    error "GITHUB_TOKEN environment variable is not set"
    echo "Please set it with: export GITHUB_TOKEN=ghp_xxxxx"
    exit 1
fi

info "Starting data collection for williamzujkowski"
echo

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Configuration files
CONFIG_2024="config/williamzujkowski_2024.yaml"
CONFIG_2025="config/williamzujkowski_2025.yaml"

# Verify config files exist
if [[ ! -f "$CONFIG_2024" ]]; then
    error "Config file not found: $CONFIG_2024"
    exit 1
fi

if [[ ! -f "$CONFIG_2025" ]]; then
    error "Config file not found: $CONFIG_2025"
    exit 1
fi

# Track overall start time
OVERALL_START=$(date +%s)

# Function to run collection for a specific year
run_collection() {
    local year=$1
    local config=$2

    info "========================================="
    info "Collecting data for year: $year"
    info "Config: $config"
    info "========================================="
    echo

    local start_time=$(date +%s)

    # Run collection
    if uv run gh-year-end all --config "$config"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        local minutes=$((duration / 60))
        local seconds=$((duration % 60))

        success "Completed $year collection in ${minutes}m ${seconds}s"
        echo
        return 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        local minutes=$((duration / 60))
        local seconds=$((duration % 60))

        error "Failed $year collection after ${minutes}m ${seconds}s"
        echo
        return 1
    fi
}

# Collection status tracking
FAILED_YEARS=()

# Collect 2024 data
if ! run_collection 2024 "$CONFIG_2024"; then
    FAILED_YEARS+=("2024")
    warning "Continuing to 2025 despite 2024 failure..."
    echo
fi

# Collect 2025 data
if ! run_collection 2025 "$CONFIG_2025"; then
    FAILED_YEARS+=("2025")
fi

# Calculate total time
OVERALL_END=$(date +%s)
TOTAL_DURATION=$((OVERALL_END - OVERALL_START))
TOTAL_MINUTES=$((TOTAL_DURATION / 60))
TOTAL_SECONDS=$((TOTAL_DURATION % 60))

# Final summary
echo
info "========================================="
info "Collection Summary"
info "========================================="
echo "Total time: ${TOTAL_MINUTES}m ${TOTAL_SECONDS}s"

if [[ ${#FAILED_YEARS[@]} -eq 0 ]]; then
    success "All collections completed successfully!"
    echo
    info "Data location: ./data/"
    info "Site output: ./site/"
    exit 0
else
    error "Failed collections: ${FAILED_YEARS[*]}"
    echo
    info "Check logs above for details"
    exit 1
fi
