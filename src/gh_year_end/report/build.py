"""Site build system for generating static HTML reports.

Reads metrics data from JSON files and generates a complete static site
using Jinja2 templates and D3.js visualizations.

Note: This module exceeds the 400-line preference from CLAUDE.md (currently 653 lines)
due to its complexity as the core site builder. It coordinates template rendering,
data transformation, asset copying, and multi-view generation. The build logic is
cohesive and splitting would fragment the build pipeline.
"""

import json
import logging
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, TemplateNotFound

from gh_year_end.config import Config
from gh_year_end.report.contributors import (
    get_engineers_list,
    populate_activity_timelines,
)
from gh_year_end.report.transformers import (
    calculate_fun_facts,
    calculate_highlights,
    calculate_insights,
    calculate_risks,
    generate_chart_data,
    transform_activity_timeline,
    transform_awards_data,
    transform_leaderboards,
)
from gh_year_end.report.views import repos_view
from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


def get_available_years(site_base_dir: Path) -> list[int]:
    """Scan site directory for available year subdirectories.

    Args:
        site_base_dir: Base directory for site output (e.g., ./site/).

    Returns:
        Sorted list of years (integers) in descending order.
    """
    if not site_base_dir.exists():
        return []

    years = []
    for item in site_base_dir.iterdir():
        if item.is_dir() and item.name.isdigit() and len(item.name) == 4:
            try:
                year = int(item.name)
                # Sanity check: reasonable year range
                if 2000 <= year <= 2100:
                    years.append(year)
            except ValueError:
                continue

    return sorted(years, reverse=True)


def build_site(config: Config, paths: PathManager) -> dict[str, Any]:
    """Build the complete static site from templates and data.

    Args:
        config: Application configuration.
        paths: Path manager for data and output locations.

    Returns:
        Dictionary with build statistics.

    Raises:
        ValueError: If metrics data does not exist.
    """
    start_time = datetime.now(UTC)
    logger.info("Starting site build for year %d", config.github.windows.year)

    # Verify metrics data exists
    _verify_metrics_data_exists(paths)

    # Ensure site directories exist
    paths.site_root.mkdir(parents=True, exist_ok=True)
    paths.site_data_path.mkdir(parents=True, exist_ok=True)
    paths.site_assets_path.mkdir(parents=True, exist_ok=True)

    stats: dict[str, Any] = {
        "start_time": start_time.isoformat(),
        "templates_rendered": [],
        "data_files_written": 0,
        "assets_copied": 0,
        "errors": [],
    }

    try:
        # Load JSON data from site data directory
        logger.info("Loading metrics data from JSON files")
        data_context = _load_json_data(paths.site_data_path)
        stats["data_files_written"] = len(data_context)

        # Copy static assets
        logger.info("Copying static assets")
        assets_source = Path(config.report.output_dir) / "assets"
        if assets_source.exists():
            stats["assets_copied"] = _copy_assets(assets_source, paths.site_assets_path)
        else:
            logger.warning("Assets directory not found at %s", assets_source)

        # Render templates
        logger.info("Rendering templates")
        templates_source = Path(config.report.output_dir) / "templates"
        if templates_source.exists():
            rendered = _render_templates(templates_source, paths.site_root, data_context, config)
            stats["templates_rendered"] = rendered
        else:
            logger.warning("Templates directory not found at %s", templates_source)

        # Create build manifest
        _write_build_manifest(paths.site_root, config, stats)

        # Generate root redirect to most recent year
        site_base_dir = Path(config.report.output_dir)
        available_years = get_available_years(site_base_dir)
        if available_years:
            most_recent_year = available_years[0]
            _generate_root_redirect(site_base_dir, most_recent_year, config.report.base_path)

    except Exception as e:
        error_msg = f"Build failed: {e!s}"
        stats["errors"].append(error_msg)
        logger.exception("Site build failed")
        raise

    end_time = datetime.now(UTC)
    stats["end_time"] = end_time.isoformat()
    stats["duration_seconds"] = (end_time - start_time).total_seconds()

    logger.info(
        "Site build complete: %d templates, %d data files, %d assets, %d errors",
        len(stats["templates_rendered"]),
        stats["data_files_written"],
        stats["assets_copied"],
        len(stats["errors"]),
    )

    return stats


def _verify_metrics_data_exists(paths: PathManager) -> None:
    """Verify that metrics data exists in JSON format."""
    if not paths.site_data_path.exists():
        msg = f"Metrics data not found at {paths.site_data_path}. Run 'metrics' and 'export' commands first."
        raise ValueError(msg)

    # Check for required JSON files
    required_files = [
        "summary.json",
        "leaderboards.json",
    ]

    missing_files = [f for f in required_files if not (paths.site_data_path / f).exists()]
    if missing_files:
        msg = f"Missing required metrics files: {missing_files}. Run 'metrics' and 'export' commands first."
        raise ValueError(msg)

    json_files = list(paths.site_data_path.glob("*.json"))
    logger.info("Found %d JSON metrics files", len(json_files))


def _load_json_data(data_dir: Path) -> dict[str, Any]:
    """Load all JSON data files for report generation.

    Args:
        data_dir: Directory containing JSON metrics files.

    Returns:
        Dictionary mapping data keys to their content.
    """
    data = {}

    json_files = [
        "summary.json",
        "leaderboards.json",
        "timeseries.json",
        "repo_health.json",
        "hygiene_scores.json",
        "awards.json",
    ]

    for filename in json_files:
        filepath = data_dir / filename
        if filepath.exists():
            try:
                with filepath.open() as f:
                    content = json.load(f)
                data[filename.replace(".json", "")] = content
                logger.info("Loaded %s", filename)
            except Exception as e:
                logger.warning("Failed to load %s: %s", filename, e)
        else:
            logger.debug("Optional file not found: %s", filename)

    return data


def _render_templates(
    templates_dir: Path, output_dir: Path, data_context: dict[str, Any], config: Config
) -> list[str]:
    """Render Jinja2 templates with data context."""
    rendered_templates = []

    try:
        output_dir.mkdir(parents=True, exist_ok=True)

        env = Environment(
            loader=FileSystemLoader(templates_dir),
            autoescape=True,
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Add custom filters
        def format_date(value: str) -> str:
            if not value:
                return ""
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return dt.strftime("%B %d, %Y")
            except (ValueError, AttributeError):
                return str(value)

        def format_number(value: int | float | None) -> str:
            if value is None:
                return "0"
            try:
                if isinstance(value, float):
                    return f"{value:,.1f}"
                return f"{int(value):,}"
            except (ValueError, TypeError):
                return str(value)

        env.filters["format_date"] = format_date
        env.filters["format_number"] = format_number

        # Extract data from data_context (already loaded by _load_json_data)
        summary_data = data_context.get("summary", {})
        leaderboards_data = data_context.get("leaderboards", {})
        awards_data = data_context.get("awards", {})
        timeseries_data = data_context.get("timeseries", {})

        # Extract repo health and hygiene scores
        # These have a different structure from export.py
        repo_health_data = data_context.get("repo_health", {})
        hygiene_scores_data = data_context.get("hygiene_scores", {})

        # Convert repo health from dict to list format expected by templates
        repo_health_list: list[dict[str, Any]] = []
        if isinstance(repo_health_data, dict) and "repos" in repo_health_data:
            # Format from export.py: {"repos": {repo_id: {...}}}
            for repo_id, repo_data in repo_health_data["repos"].items():
                repo_health_list.append({"repo_id": repo_id, **repo_data})
        elif isinstance(repo_health_data, list):
            # List format from metrics - transform to expected structure
            for item in repo_health_data:
                repo_name = item.get("repo", "")
                repo_health_list.append(
                    {
                        "repo_id": repo_name,
                        "repo_full_name": repo_name,
                        "prs_merged": item.get("pr_count", 0),
                        "active_contributors_365d": item.get("contributor_count", 0),
                        "review_coverage": item.get("review_coverage", 0),
                        "median_time_to_merge": item.get("median_time_to_merge"),
                        **item,
                    }
                )

        # Convert hygiene scores from dict to list format
        hygiene_scores_list: list[dict[str, Any]] = []
        if isinstance(hygiene_scores_data, dict) and "repos" in hygiene_scores_data:
            # Format from export.py: {"repos": {repo_id: {...}}}
            for repo_id, repo_data in hygiene_scores_data["repos"].items():
                hygiene_scores_list.append({"repo_id": repo_id, **repo_data})
        elif isinstance(hygiene_scores_data, dict):
            # Dict format with repo names as keys: {"repo_name": {...}}
            for repo_name, repo_data in hygiene_scores_data.items():
                hygiene_scores_list.append(
                    {
                        "repo_id": repo_name,
                        "score": repo_data.get("score", 0),
                        **repo_data,
                    }
                )
        elif isinstance(hygiene_scores_data, list):
            # List format from metrics - transform to expected structure
            for item in hygiene_scores_data:
                repo_name = item.get("repo", "")
                hygiene_scores_list.append(
                    {
                        "repo_id": repo_name,
                        "score": item.get("score", 0),
                        **item,
                    }
                )

        # Sort repo_health_list by prs_merged descending for top performing repos
        repo_health_list.sort(key=lambda x: x.get("prs_merged", 0), reverse=True)

        # Merge repo data for repos.html template
        repos_merged = repos_view.merge_repo_data(repo_health_list, hygiene_scores_list)

        # Calculate hygiene aggregate for repos.html template
        hygiene_aggregate = repos_view.calculate_hygiene_aggregate(hygiene_scores_list)

        # Calculate repo summary for repos.html template
        repo_summary_stats = repos_view.calculate_repo_summary(repos_merged)

        # Calculate highlights from metrics data
        highlights_data = _calculate_highlights(summary_data, timeseries_data, repo_health_list)

        activity_timeline_data = _transform_activity_timeline(timeseries_data)

        # Get available years for multi-year navigation
        site_base_dir = Path(config.report.output_dir)
        available_years = get_available_years(site_base_dir)
        current_year = config.github.windows.year

        # Process awards data - transform from simple key-value to categorized format
        awards_by_category: dict[str, list[Any]] = {
            "individual": [],
            "repository": [],
            "risk": [],
        }

        # Extract special_mentions from awards_data if present
        special_mentions_data: dict[str, list[dict[str, Any]]] = {
            "first_contributions": [],
            "consistent_contributors": [],
            "largest_prs": [],
            "fastest_merges": [],
        }

        # Handle different award data formats
        if isinstance(awards_data, dict):
            # Extract special_mentions if present
            if "special_mentions" in awards_data:
                special_mentions_data.update(awards_data["special_mentions"])

            # Check if already in categorized format
            if "awards" in awards_data:
                for category in ["individual", "repository", "risk"]:
                    if category in awards_data["awards"]:
                        awards_by_category[category] = awards_data["awards"][category]
            elif any(k in awards_data for k in ["individual", "repository", "risk"]):
                # Direct category mapping - transform individual and repository awards
                awards_by_category = _transform_awards_data(awards_data)
            else:
                # Transform from simple format (top_pr_author, top_reviewer, etc.)
                awards_by_category = _transform_awards_data(awards_data)

        # Add repository awards if not already populated
        if not awards_by_category.get("repository"):
            awards_by_category["repository"] = _generate_repository_awards(repo_health_list)

        # Get base path for GitHub Pages subpath deployment
        base_path = config.report.base_path.rstrip("/") if config.report.base_path else ""

        # Pre-compute engineers list (used in multiple context keys)
        engineers_list = _get_engineers_list(leaderboards_data, timeseries_data)

        # Build template context
        context = {
            "config": {
                "report": {
                    "title": config.report.title,
                },
                "github": {
                    "target": {
                        "name": config.github.target.name,
                        "mode": config.github.target.mode,
                    },
                    "windows": {
                        "year": config.github.windows.year,
                        "since": config.github.windows.since.isoformat()
                        if config.github.windows.since
                        else "",
                        "until": config.github.windows.until.isoformat()
                        if config.github.windows.until
                        else "",
                    },
                },
            },
            "base_path": base_path,
            "current_year": current_year,
            "available_years": available_years,
            "summary": {
                "total_contributors": summary_data.get("total_contributors", 0),
                "total_prs_merged": summary_data.get("prs_merged", 0),
                "total_reviews": summary_data.get("total_reviews", 0),
                "total_repos": summary_data.get("total_repos", 0),
            },
            "highlights": highlights_data,
            "activity_timeline": activity_timeline_data,
            "stats": {
                "total_prs": summary_data.get("total_prs", 0),
                "total_issues": summary_data.get("total_issues", 0),
                "total_reviews": summary_data.get("total_reviews", 0),
                "total_repos": summary_data.get("total_repos", 0),
            },
            "generation_date": datetime.now(UTC).strftime("%Y-%m-%d"),
            "version": "1.0.0",
            "build_time": datetime.now(UTC).isoformat(),
            "leaderboards": _transform_leaderboards(leaderboards_data),
            "hygiene": hygiene_aggregate,
            # Calculate aggregate health signals for Executive Summary
            "health": _calculate_health_signals(
                summary_data, repo_health_list, hygiene_scores_list
            ),
            "repos": repos_merged,
            "repo_summary": repo_summary_stats,
            "awards": awards_by_category,
            "special_mentions": special_mentions_data,
            # Fun facts calculates available metrics; some return None due to missing PR detail data
            "fun_facts": _calculate_fun_facts(summary_data, timeseries_data, leaderboards_data),
            "engineers": engineers_list,
            "top_contributors": engineers_list[:10],
            "all_contributors": engineers_list,
            "contribution_timeline": [],
            "contribution_types": [],
            "contribution_by_repo": [],
            # Calculate insights from metrics data
            "insights": _calculate_insights(
                summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
            ),
            # Calculate risks from health metrics
            "risks": _calculate_risks(repo_health_list, hygiene_scores_list, summary_data),
            # Static recommendation - implement dynamic recommendations based on metrics
            "recommendations": [
                {
                    "priority": "low",
                    "title": "Foster Community Growth",
                    "description": "Encourage new contributors and improve retention.",
                    "actions": [
                        "Label issues as 'good first issue' for newcomers",
                        "Create contribution guidelines",
                        "Host regular contributor meetups",
                    ],
                }
            ],
            "top_repos": repos_merged[:10] if repos_merged else [],
        }

        # Generate chart data from metrics
        chart_data = generate_chart_data(
            timeseries_data, summary_data, leaderboards_data, repo_health_list
        )
        context.update(chart_data)

        # Render all HTML templates
        template_files = list(templates_dir.glob("*.html"))

        for template_file in template_files:
            try:
                template_name = template_file.name
                template = env.get_template(template_name)
                rendered = template.render(**context)

                output_path = output_dir / template_name
                output_path.write_text(rendered, encoding="utf-8")

                rendered_templates.append(template_name)
                logger.info("Rendered template: %s", template_name)

            except TemplateNotFound:
                logger.warning("Template not found: %s", template_name)
            except Exception as e:
                logger.warning("Failed to render %s: %s", template_name, e)
                continue

    except Exception as e:
        logger.warning("Template rendering failed: %s", e)

    return rendered_templates


def _transform_awards_data(awards_data: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Backward-compatible wrapper for transform_awards_data."""
    return transform_awards_data(awards_data)


def _transform_leaderboards(leaderboards_data: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Backward-compatible wrapper for transform_leaderboards."""
    return transform_leaderboards(leaderboards_data)


def _transform_activity_timeline(timeseries_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Backward-compatible wrapper for transform_activity_timeline."""
    return transform_activity_timeline(timeseries_data)


def _calculate_highlights(
    summary_data: dict[str, Any],
    timeseries_data: dict[str, Any],
    repo_health_list: list[dict[str, Any]],
) -> dict[str, Any]:
    """Backward-compatible wrapper for calculate_highlights."""
    return calculate_highlights(summary_data, timeseries_data, repo_health_list)


def _calculate_fun_facts(
    summary_data: dict[str, Any],
    timeseries_data: dict[str, Any],
    leaderboards_data: dict[str, Any],
) -> dict[str, Any]:
    """Backward-compatible wrapper for calculate_fun_facts."""
    return calculate_fun_facts(summary_data, timeseries_data, leaderboards_data)


def _get_engineers_list(
    leaderboards_data: dict[str, Any], timeseries_data: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Backward-compatible wrapper for get_engineers_list."""
    return get_engineers_list(leaderboards_data, timeseries_data)


def _populate_activity_timelines(
    contributors: list[dict[str, Any]], timeseries_data: dict[str, Any]
) -> None:
    """Backward-compatible wrapper for populate_activity_timelines."""
    return populate_activity_timelines(contributors, timeseries_data)


def _calculate_insights(
    summary_data: dict[str, Any],
    leaderboards_data: dict[str, Any],
    repo_health_list: list[dict[str, Any]],
    hygiene_scores_list: list[dict[str, Any]],
) -> dict[str, Any]:
    """Backward-compatible wrapper for calculate_insights."""
    return calculate_insights(
        summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
    )


def _generate_repository_awards(
    repo_health_list: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Generate repository awards from repo health data.

    Args:
        repo_health_list: List of repository health metrics.

    Returns:
        List of repository award dictionaries for template rendering.
    """
    awards = []

    if not repo_health_list:
        return awards

    # Most Active Repository - highest pr_count (or prs_merged)
    active_repos = [r for r in repo_health_list if r.get("pr_count", 0) > 0]
    if active_repos:
        most_active = max(active_repos, key=lambda x: x.get("pr_count", 0))
        awards.append(
            {
                "award_key": "most_active",
                "title": "Most Active Repository",
                "description": "Highest number of pull requests",
                "repo_name": most_active.get("repo", ""),
                "supporting_stats": f"{most_active.get('pr_count', 0)} PRs",
            }
        )

    # Best Reviewed Repository - highest review_coverage
    reviewed_repos = [r for r in repo_health_list if r.get("review_coverage", 0) > 0]
    if reviewed_repos:
        best_reviewed = max(reviewed_repos, key=lambda x: x.get("review_coverage", 0))
        awards.append(
            {
                "award_key": "best_reviewed",
                "title": "Best Reviewed Repository",
                "description": "Highest review coverage",
                "repo_name": best_reviewed.get("repo", ""),
                "supporting_stats": f"{best_reviewed.get('review_coverage', 0):.0f}% coverage",
            }
        )

    # Most Collaborative Repository - highest contributor_count
    collab_repos = [r for r in repo_health_list if r.get("contributor_count", 0) > 0]
    if collab_repos:
        most_collab = max(collab_repos, key=lambda x: x.get("contributor_count", 0))
        awards.append(
            {
                "award_key": "most_collaborative",
                "title": "Most Collaborative Repository",
                "description": "Most active contributors",
                "repo_name": most_collab.get("repo", ""),
                "supporting_stats": f"{most_collab.get('contributor_count', 0)} contributors",
            }
        )

    return awards


def _calculate_risks(
    repo_health_list: list[dict[str, Any]],
    hygiene_scores_list: list[dict[str, Any]],
    summary_data: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Backward-compatible wrapper for calculate_risks."""
    return calculate_risks(repo_health_list, hygiene_scores_list, summary_data)


def _calculate_health_signals(
    summary_data: dict[str, Any],
    repo_health_list: list[dict[str, Any]],
    hygiene_scores_list: list[dict[str, Any]],
) -> dict[str, Any]:
    """Calculate aggregate health signals for Executive Summary.

    Args:
        summary_data: Summary statistics from summary.json.
        repo_health_list: Repository health metrics.
        hygiene_scores_list: Repository hygiene scores.

    Returns:
        Dictionary with aggregate health signal values:
        - review_coverage: Overall % of PRs with reviews
        - avg_merge_time: Median merge time across repos (formatted)
        - stale_pr_count: Total stale PRs across all repos
        - active_contributors: Total contributors (from summary)
        - repos: Original repo health list (for backward compatibility)
        - hygiene: Original hygiene scores list (for backward compatibility)
    """
    health_signals: dict[str, Any] = {
        "repos": repo_health_list,
        "hygiene": hygiene_scores_list,
    }

    # 1. Calculate overall review coverage percentage (total_reviews / total_prs * 100)
    try:
        total_prs = summary_data.get("total_prs", 0)
        total_reviews = summary_data.get("total_reviews", 0)

        if total_prs > 0:
            review_coverage = (total_reviews / total_prs) * 100
            health_signals["review_coverage"] = round(review_coverage, 1)
        else:
            health_signals["review_coverage"] = 0
    except Exception as e:
        logger.warning("Failed to calculate review_coverage: %s", e)
        health_signals["review_coverage"] = 0

    # 2. Calculate median merge time from repo_health_list
    # Average of non-null median_time_to_merge values (in hours from collection)
    try:
        merge_times = []
        for repo in repo_health_list:
            median_time = repo.get("median_time_to_merge")
            if median_time is not None and median_time > 0:
                merge_times.append(median_time)

        if merge_times:
            avg_merge_time_hours = sum(merge_times) / len(merge_times)

            # Format as human-readable string
            if avg_merge_time_hours < 1:
                health_signals["avg_merge_time"] = f"{int(avg_merge_time_hours * 60)}m"
            elif avg_merge_time_hours < 24:
                health_signals["avg_merge_time"] = f"{avg_merge_time_hours:.1f}h"
            else:
                health_signals["avg_merge_time"] = f"{avg_merge_time_hours / 24:.1f}d"
        else:
            health_signals["avg_merge_time"] = "N/A"
    except Exception as e:
        logger.warning("Failed to calculate avg_merge_time: %s", e)
        health_signals["avg_merge_time"] = "N/A"

    # 3. Sum stale_pr_count from all repos
    # Note: stale_pr_count is not currently tracked in repo_health data
    # Defaulting to 0 until collection phase is updated to track stale PRs
    try:
        stale_pr_count = sum(repo.get("stale_pr_count", 0) for repo in repo_health_list)
        health_signals["stale_pr_count"] = stale_pr_count
    except Exception as e:
        logger.warning("Failed to calculate stale_pr_count: %s", e)
        health_signals["stale_pr_count"] = 0

    # 4. Use total_contributors from summary
    # This represents all contributors active during the year
    try:
        active_contributors = summary_data.get("total_contributors", 0)
        health_signals["active_contributors"] = active_contributors
    except Exception as e:
        logger.warning("Failed to calculate active_contributors: %s", e)
        health_signals["active_contributors"] = 0

    logger.info(
        "Calculated health signals: review_coverage=%.1f%%, avg_merge_time=%s, "
        "stale_pr_count=%d, active_contributors=%d",
        health_signals.get("review_coverage", 0),
        health_signals.get("avg_merge_time", "N/A"),
        health_signals.get("stale_pr_count", 0),
        health_signals.get("active_contributors", 0),
    )

    return health_signals


def _copy_assets(src: Path, dest: Path) -> int:
    """Copy static assets to output directory."""
    files_copied = 0

    try:
        if dest.exists():
            shutil.rmtree(dest)
        dest.mkdir(parents=True, exist_ok=True)

        if src.exists():
            for item in src.rglob("*"):
                if item.is_file():
                    rel_path = item.relative_to(src)
                    dest_path = dest / rel_path

                    dest_path.parent.mkdir(parents=True, exist_ok=True)

                    shutil.copy2(item, dest_path)
                    files_copied += 1

            logger.info("Copied %d asset files", files_copied)

    except Exception as e:
        logger.warning("Failed to copy assets: %s", e)

    return files_copied


def _write_build_manifest(output_dir: Path, config: Config, stats: dict[str, Any]) -> None:
    """Write build manifest with metadata."""
    manifest = {
        "version": "1.0",
        "year": config.github.windows.year,
        "target": config.github.target.name,
        "target_mode": config.github.target.mode,
        "build_time": datetime.now(UTC).isoformat(),
        "templates_rendered": stats["templates_rendered"],
        "data_files_written": stats["data_files_written"],
        "assets_copied": stats["assets_copied"],
        "errors": stats["errors"],
    }

    manifest_path = output_dir / "manifest.json"
    with manifest_path.open("w") as f:
        json.dump(manifest, f, indent=2)

    logger.info("Wrote build manifest to %s", manifest_path)


def _generate_root_redirect(site_base_dir: Path, target_year: int, base_path: str = "") -> None:
    """Generate root index.html that redirects to the most recent year.

    Args:
        site_base_dir: Base directory for site output (e.g., ./site/).
        target_year: Year to redirect to.
        base_path: Base URL path for GitHub Pages subpath (e.g., '/yakshave').
    """
    # Ensure base_path doesn't have trailing slash
    base_path = base_path.rstrip("/")
    redirect_url = f"{base_path}/{target_year}/"

    redirect_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="0; url={redirect_url}">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Redirecting...</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            margin: 0;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }}
        .redirect-container {{
            text-align: center;
        }}
        .redirect-container h1 {{
            font-size: 2.5rem;
            margin-bottom: 1rem;
        }}
        .redirect-container p {{
            font-size: 1.2rem;
            margin-bottom: 2rem;
        }}
        .redirect-container a {{
            color: white;
            text-decoration: underline;
        }}
    </style>
</head>
<body>
    <div class="redirect-container">
        <h1>Year in Review</h1>
        <p>Redirecting to {target_year}...</p>
        <p>If you are not redirected automatically, <a href="{redirect_url}">click here</a>.</p>
    </div>
</body>
</html>"""

    root_index_path = site_base_dir / "index.html"
    root_index_path.write_text(redirect_html, encoding="utf-8")
    logger.info("Generated root redirect to %d at %s", target_year, root_index_path)
