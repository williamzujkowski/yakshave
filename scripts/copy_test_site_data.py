#!/usr/bin/env python3
"""Copy test site data fixtures to site/YYYY/data/ directory.

This script copies the minimal test dataset from tests/fixtures/sample_site_data/
to the site directory for rapid iteration and testing of visualizations.

Usage:
    python scripts/copy_test_site_data.py [--year YYYY]

Example:
    python scripts/copy_test_site_data.py --year 2024
"""

import argparse
import json
from datetime import datetime
from pathlib import Path


def copy_test_data(year: int, force: bool = False) -> None:
    """Copy test data to site directory.

    Args:
        year: Target year for the site data
        force: Overwrite existing data without confirmation
    """
    # Define paths
    project_root = Path(__file__).parent.parent
    fixtures_dir = project_root / "tests" / "fixtures" / "sample_site_data"
    site_data_dir = project_root / "site" / str(year) / "data"

    # Validate fixtures directory exists
    if not fixtures_dir.exists():
        print(f"Error: Fixtures directory not found at {fixtures_dir}")
        return

    # Check if site data directory exists and has files
    if site_data_dir.exists() and any(site_data_dir.glob("*.json")) and not force:
        response = input(
            f"Site data directory {site_data_dir} already contains files. "
            "Overwrite? (y/N): "
        )
        if response.lower() != "y":
            print("Aborted.")
            return

    # Create site data directory
    site_data_dir.mkdir(parents=True, exist_ok=True)

    # List of data files to copy
    data_files = [
        "summary.json",
        "leaderboards.json",
        "timeseries.json",
        "repo_health.json",
        "hygiene_scores.json",
        "awards.json",
    ]

    # Copy each file and update year/timestamp
    copied_count = 0
    for filename in data_files:
        src_file = fixtures_dir / filename
        dst_file = site_data_dir / filename

        if not src_file.exists():
            print(f"Warning: Source file not found: {src_file}")
            continue

        # Read, update, and write
        with src_file.open("r") as f:
            data = json.load(f)

        # Update year and timestamp if applicable
        if "year" in data:
            data["year"] = year
        if "generated_at" in data:
            data["generated_at"] = datetime.now().isoformat()

        with dst_file.open("w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"Copied: {filename} -> {dst_file}")
        copied_count += 1

    print(f"\nSuccess! Copied {copied_count} files to {site_data_dir}")
    print(f"\nYou can now test the site by opening: site/{year}/index.html")


def main() -> None:
    """Parse arguments and copy test data."""
    parser = argparse.ArgumentParser(
        description="Copy test site data to site/YYYY/data/ directory",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Copy test data for year 2024:
    python scripts/copy_test_site_data.py --year 2024

  Force overwrite existing data:
    python scripts/copy_test_site_data.py --year 2024 --force
        """,
    )

    parser.add_argument(
        "--year",
        type=int,
        default=datetime.now().year,
        help="Target year for site data (default: current year)",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing data without confirmation",
    )

    args = parser.parse_args()

    copy_test_data(args.year, args.force)


if __name__ == "__main__":
    main()
