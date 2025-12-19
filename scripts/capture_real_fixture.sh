#!/usr/bin/env bash
# Capture real data fixture from williamzujkowski user account (2024)
#
# This script performs a one-time collection of real GitHub data and saves it
# as a permanent test fixture. The fixture can be used for testing without
# making any API calls.
#
# Usage:
#   export GITHUB_TOKEN=ghp_xxxxx
#   ./scripts/capture_real_fixture.sh
#
# Expected:
#   - Time: 2-5 minutes
#   - API calls: 150-300 requests
#   - Storage: 2-5 MB

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}  Capturing Real Data Fixture: williamzujkowski 2024${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

# Check for GitHub token
if [ -z "$GITHUB_TOKEN" ]; then
    echo -e "${RED}Error: GITHUB_TOKEN environment variable not set${NC}"
    echo "Please set your GitHub token:"
    echo "  export GITHUB_TOKEN=ghp_xxxxx"
    exit 1
fi

# Define paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURE_DIR="$REPO_ROOT/tests/fixtures/real_williamzujkowski_2024"
CONFIG_FILE="$REPO_ROOT/tests/fixtures/real_williamzujkowski_2024_config.yaml"
TEMP_DATA_DIR="$REPO_ROOT/data/temp_fixture_capture"

echo -e "${YELLOW}Configuration:${NC}"
echo "  Repository root: $REPO_ROOT"
echo "  Fixture directory: $FIXTURE_DIR"
echo "  Temp data directory: $TEMP_DATA_DIR"
echo "  Config file: $CONFIG_FILE"
echo

# Check if fixture already exists
if [ -d "$FIXTURE_DIR/raw" ]; then
    echo -e "${YELLOW}Warning: Fixture already exists at $FIXTURE_DIR${NC}"
    read -p "Do you want to replace it? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 0
    fi
    echo "Removing existing fixture..."
    rm -rf "$FIXTURE_DIR/raw"
fi

# Create fixture directory
mkdir -p "$FIXTURE_DIR"

# Create config file if it doesn't exist
if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${BLUE}Creating configuration file...${NC}"
    cat > "$CONFIG_FILE" << 'EOF'
# Real data fixture configuration for williamzujkowski (2024)
# This config is used to capture a one-time snapshot of real GitHub data
# for testing purposes.

github:
  target:
    mode: user
    name: williamzujkowski
  auth:
    token_env: GITHUB_TOKEN
  discovery:
    include_forks: false
    include_archived: true  # Include for realistic diversity
    visibility: public
  windows:
    year: 2024
    since: "2024-01-01T00:00:00Z"
    until: "2025-01-01T00:00:00Z"

rate_limit:
  strategy: adaptive
  max_concurrency: 2  # Conservative for fixture capture
  min_sleep_seconds: 1
  max_sleep_seconds: 60
  sample_rate_limit_endpoint_every_n_requests: 50

identity:
  bots:
    exclude_patterns:
      - ".*\\[bot\\]$"
      - "^dependabot$"
      - "^renovate\\[bot\\]$"
    include_overrides: []
  humans_only: true

collection:
  enable:
    pulls: true
    issues: true
    reviews: true
    comments: true
    commits: true
    hygiene: true
  commits:
    include_files: true
    classify_files: true
  hygiene:
    paths:
      - "SECURITY.md"
      - "README.md"
      - "LICENSE"
      - "CONTRIBUTING.md"
      - "CODE_OF_CONDUCT.md"
      - "CODEOWNERS"
      - ".github/CODEOWNERS"
    workflow_prefixes:
      - ".github/workflows/"
    branch_protection:
      mode: sample
      sample_top_repos_by: prs_merged
      sample_count: 10
    security_features:
      best_effort: true

storage:
  root: "./data/temp_fixture_capture"
  raw_format: jsonl
  curated_format: parquet
  dataset_version: "v1"

report:
  title: "williamzujkowski 2024 Year in Review (Test Fixture)"
  output_dir: "./site"
  theme: "engineer_exec_toggle"
EOF
    echo -e "${GREEN}✓ Config created${NC}"
fi

# Clean up temp directory if it exists
if [ -d "$TEMP_DATA_DIR" ]; then
    echo -e "${YELLOW}Cleaning up previous temp data...${NC}"
    rm -rf "$TEMP_DATA_DIR"
fi

echo
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}  Step 1: Collecting data from GitHub API${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

# Run collection
cd "$REPO_ROOT"
if uv run gh-year-end collect --config "$CONFIG_FILE" --force; then
    echo
    echo -e "${GREEN}✓ Collection completed successfully${NC}"
else
    echo
    echo -e "${RED}✗ Collection failed${NC}"
    exit 1
fi

echo
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}  Step 2: Moving data to fixture directory${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

# Copy raw data to fixture directory
if [ -d "$TEMP_DATA_DIR/raw" ]; then
    echo "Copying raw data..."
    cp -r "$TEMP_DATA_DIR/raw" "$FIXTURE_DIR/"
    echo -e "${GREEN}✓ Raw data copied${NC}"
else
    echo -e "${RED}Error: Raw data directory not found at $TEMP_DATA_DIR/raw${NC}"
    exit 1
fi

echo
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}  Step 3: Creating documentation${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

# Create README
CAPTURE_DATE=$(date -I)
cat > "$FIXTURE_DIR/README.md" << EOF
# Real Data Fixture: williamzujkowski 2024

This directory contains a snapshot of real GitHub data from the williamzujkowski
user account for the year 2024. This data is used for testing the gh-year-end
tool with realistic patterns without making API calls.

## Metadata

- **Captured**: $CAPTURE_DATE
- **Source**: williamzujkowski GitHub user account
- **Period**: 2024-01-01 to 2025-01-01
- **Type**: Public repositories only
- **Includes**: Forks excluded, archived repos included

## Contents

The fixture includes:

- **Repositories**: Public repos with activity in 2024
- **Pull Requests**: All PRs created/updated in 2024
- **Issues**: All issues created/updated in 2024
- **Reviews**: Code reviews on 2024 PRs
- **Comments**: Issue and review comments from 2024
- **Commits**: Commits from 2024
- **Hygiene**: Repository health data (branch protection, security features, file presence)

## Data Structure

\`\`\`
real_williamzujkowski_2024/
├── README.md (this file)
├── raw/
│   └── year=2024/
│       └── source=github/
│           └── target=williamzujkowski/
│               ├── repos.jsonl
│               ├── pulls/
│               ├── issues/
│               ├── reviews/
│               ├── issue_comments/
│               ├── review_comments/
│               ├── commits/
│               ├── repo_tree/
│               ├── branch_protection/
│               └── security_features/
└── stats.txt (collection statistics)
\`\`\`

## Usage

This fixture is used in tests that need realistic data without API calls:

\`\`\`python
# tests/test_real_data_smoke.py
REAL_FIXTURES_DIR = Path(__file__).parent / "fixtures" / "real_williamzujkowski_2024"
\`\`\`

Or with the CLI:

\`\`\`bash
# Use the fixture directly (no API calls)
uv run gh-year-end normalize --config tests/fixtures/real_williamzujkowski_2024_config.yaml
uv run gh-year-end metrics --config tests/fixtures/real_williamzujkowski_2024_config.yaml
uv run gh-year-end report --config tests/fixtures/real_williamzujkowski_2024_config.yaml
\`\`\`

## Refreshing the Fixture

To update this fixture with fresh data:

\`\`\`bash
export GITHUB_TOKEN=ghp_xxxxx
./scripts/capture_real_fixture.sh
\`\`\`

Recommended refresh frequency: Quarterly or when testing new features.

## Statistics

- **Last Updated**: $CAPTURE_DATE
- **Repositories**: $(find "$FIXTURE_DIR/raw" -name "repos.jsonl" -exec wc -l {} \; | awk '{print $1}')
- **Total Size**: $(du -sh "$FIXTURE_DIR" | cut -f1)

## Notes

- All data is from public repositories
- No sensitive information (tokens, secrets) is included
- Data is frozen at capture time and won't change
- Bot accounts are included but can be filtered during normalization
- This is a real snapshot, not synthetic data - expect realistic patterns and edge cases
EOF

echo -e "${GREEN}✓ README created${NC}"

# Create statistics file
echo "Collecting statistics..."
cat > "$FIXTURE_DIR/stats.txt" << EOF
Fixture Capture Statistics
Generated: $CAPTURE_DATE

Files:
$(find "$FIXTURE_DIR/raw" -type f -name "*.jsonl" | wc -l) JSONL files

Total records by type:
EOF

# Count records in different files
if [ -f "$FIXTURE_DIR/raw/year=2024/source=github/target=williamzujkowski/repos.jsonl" ]; then
    REPO_COUNT=$(wc -l < "$FIXTURE_DIR/raw/year=2024/source=github/target=williamzujkowski/repos.jsonl")
    echo "  Repositories: $REPO_COUNT" >> "$FIXTURE_DIR/stats.txt"
fi

PULLS_DIR="$FIXTURE_DIR/raw/year=2024/source=github/target=williamzujkowski/pulls"
if [ -d "$PULLS_DIR" ]; then
    PULL_COUNT=$(find "$PULLS_DIR" -name "*.jsonl" -exec wc -l {} + | tail -1 | awk '{print $1}')
    echo "  Pull Requests: $PULL_COUNT" >> "$FIXTURE_DIR/stats.txt"
fi

ISSUES_DIR="$FIXTURE_DIR/raw/year=2024/source=github/target=williamzujkowski/issues"
if [ -d "$ISSUES_DIR" ]; then
    ISSUE_COUNT=$(find "$ISSUES_DIR" -name "*.jsonl" -exec wc -l {} + | tail -1 | awk '{print $1}')
    echo "  Issues: $ISSUE_COUNT" >> "$FIXTURE_DIR/stats.txt"
fi

echo "" >> "$FIXTURE_DIR/stats.txt"
echo "Storage:" >> "$FIXTURE_DIR/stats.txt"
echo "  Total size: $(du -sh "$FIXTURE_DIR" | cut -f1)" >> "$FIXTURE_DIR/stats.txt"

echo -e "${GREEN}✓ Statistics collected${NC}"

echo
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}  Step 4: Cleanup${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

# Clean up temp directory
echo "Removing temporary data directory..."
rm -rf "$TEMP_DATA_DIR"
echo -e "${GREEN}✓ Cleanup complete${NC}"

echo
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  ✓ Fixture capture complete!${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo
echo -e "${GREEN}Fixture location:${NC} $FIXTURE_DIR"
echo -e "${GREEN}Total size:${NC} $(du -sh "$FIXTURE_DIR" | cut -f1)"
echo
echo "Statistics:"
cat "$FIXTURE_DIR/stats.txt" | tail -n +4
echo
echo -e "${BLUE}Next steps:${NC}"
echo "  1. Review the fixture: ls -R $FIXTURE_DIR"
echo "  2. Test the fixture: uv run gh-year-end normalize --config $CONFIG_FILE"
echo "  3. Commit to repository: git add tests/fixtures/real_williamzujkowski_2024/"
echo
echo -e "${YELLOW}Note:${NC} The fixture is now ready to use for testing with zero API calls."
