#!/usr/bin/env python3
"""Fix accessibility issues in template files - add aria attributes to SVGs and table captions."""

import re
from pathlib import Path

# Define template directory
TEMPLATE_DIR = Path("site/templates")


def add_aria_hidden_to_decorative_svgs(content: str) -> str:
    """Add aria-hidden="true" to decorative SVG icons that don't have aria attributes."""
    # Pattern: <svg ... > without aria-hidden or aria-label
    # Only add to decorative SVGs (class contains icon, or is in navigation/header)

    # Find all SVG tags
    svg_pattern = r"<svg\s+([^>]*?)>"

    def replace_svg(match):
        attrs = match.group(1)
        # Skip if already has aria-hidden or aria-label
        if "aria-hidden" in attrs or "aria-label" in attrs or "role=" in attrs:
            return match.group(0)
        # Add aria-hidden="true"
        return f'<svg {attrs} aria-hidden="true">'

    return re.sub(svg_pattern, replace_svg, content)


def add_table_caption(content: str, caption_text: str) -> str:
    """Add caption to tables that don't have one."""
    # Pattern: <table...>\\n\\s*<thead> (no caption between)
    table_pattern = r"(<table[^>]*>)\s*(<thead>)"

    def replace_table(match):
        table_tag = match.group(1)
        thead_tag = match.group(2)
        # Check if caption already exists (look ahead)
        if "<caption" in content[match.start() : match.start() + 200]:
            return match.group(0)
        return f'{table_tag}\n  <caption class="visually-hidden">{caption_text}</caption>\n  {thead_tag}'

    return re.sub(table_pattern, replace_table, content)


def add_aria_sort_to_sortable_headers(content: str) -> str:
    """Add aria-sort="none" to sortable table headers."""
    # Pattern: <th class="sortable" data-sort="...">
    th_pattern = r'(<th[^>]*class="[^"]*sortable[^"]*"[^>]*)(>)'

    def replace_th(match):
        th_open = match.group(1)
        close_bracket = match.group(2)
        # Skip if already has aria-sort
        if "aria-sort" in th_open:
            return match.group(0)
        return f'{th_open} aria-sort="none"{close_bracket}'

    return re.sub(th_pattern, replace_th, content)


def add_scope_to_headers(content: str) -> str:
    """Add scope="col" to table headers that don't have it."""
    # Only apply to headers in thead sections
    # Split by thead sections
    result = []
    parts = re.split(r"(<thead>.*?</thead>)", content, flags=re.DOTALL)
    for part in parts:
        if "<thead>" in part and "</thead>" in part:
            # This is a thead section
            part = re.sub(
                r"<th(\s+[^>]*)?>",
                lambda m: f'<th{m.group(1) or ""} scope="col">'
                if "scope=" not in (m.group(1) or "")
                else m.group(0),
                part,
            )
        result.append(part)

    return "".join(result)


def process_file(file_path: Path):
    """Process a single template file."""
    print(f"Processing {file_path.name}...")

    with file_path.open(encoding="utf-8") as f:
        content = f.read()

    original_content = content

    # Step 1: Add aria-hidden to decorative SVGs
    content = add_aria_hidden_to_decorative_svgs(content)

    # Step 2: Add aria-sort to sortable headers
    content = add_aria_sort_to_sortable_headers(content)

    # Step 3: Add scope to headers
    content = add_scope_to_headers(content)

    # Step 4: Add table captions based on filename
    caption_map = {
        "engineers.html": "Contributor statistics sorted by activity",
        "repos.html": "Repository health metrics and statistics",
        "leaderboards.html": "Contributor rankings by overall score",
    }

    if file_path.name in caption_map:
        content = add_table_caption(content, caption_map[file_path.name])

    # Write back if changed
    if content != original_content:
        with file_path.open("w", encoding="utf-8") as f:
            f.write(content)
        print(f"  ✓ Updated {file_path.name}")
    else:
        print(f"  - No changes needed for {file_path.name}")


def main():
    """Process all template files."""
    print("Fixing accessibility issues in template files...\n")

    template_files = sorted(TEMPLATE_DIR.glob("*.html"))

    for file_path in template_files:
        process_file(file_path)

    print("\nDone!")


if __name__ == "__main__":
    main()
