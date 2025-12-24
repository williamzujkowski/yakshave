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
from datetime import UTC, datetime, timedelta
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
    generate_engineer_charts,
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


def _collect_year_stats(site_dir: Path, years: list[int]) -> dict[int, dict[str, Any] | None]:
    """Collect summary stats for each available year.

    Args:
        site_dir: Base directory for site output (e.g., ./site/).
        years: List of years to collect stats for.

    Returns:
        Dictionary mapping year to stats dict or None if data unavailable.
        Stats dict contains: contributors, prs_merged, repos.
    """
    stats: dict[int, dict[str, Any] | None] = {}

    for year in years:
        summary_path = site_dir / str(year) / "data" / "summary.json"
        if summary_path.exists():
            try:
                with summary_path.open() as f:
                    data = json.load(f)
                stats[year] = {
                    "contributors": data.get("total_contributors", 0),
                    "prs_merged": data.get("prs_merged", 0),
                    "repos": data.get("total_repos", 0),
                }
                logger.info("Loaded stats for year %d: %s", year, stats[year])
            except Exception as e:
                logger.warning("Failed to load summary for year %d: %s", year, e)
                stats[year] = None
        else:
            logger.debug("No summary.json found for year %d", year)
            stats[year] = None

    return stats


def _build_years_list(
    available_years: list[int],
    year_stats: dict[int, dict[str, Any] | None],
    current_year: int,
    base_path: str,
) -> list[dict[str, Any]]:
    """Build years list for year_index.html template.

    Args:
        available_years: List of available years.
        year_stats: Dictionary mapping year to stats.
        current_year: Current year being rendered.
        base_path: Base URL path for links.

    Returns:
        List of year objects with path, year, description, stats, and flags.
    """
    years_list = []
    base_path_clean = base_path.rstrip("/")

    for year in available_years:
        stats = year_stats.get(year)
        is_current = year == current_year
        is_coming_soon = stats is None

        year_obj = {
            "year": year,
            "path": f"{base_path_clean}/{year}/",
            "description": f"GitHub activity and metrics for {year}",
            "is_current": is_current,
            "is_coming_soon": is_coming_soon,
            "stats": None,
        }

        # Only add stats if available
        if stats:
            year_obj["stats"] = {
                "contributors": stats.get("contributors", 0),
                "prs": stats.get("prs_merged", 0),
                "repos": stats.get("repos", 0),
            }

        years_list.append(year_obj)

    return years_list


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

        # Export search data for global search functionality
        logger.info("Exporting search data")
        _export_search_data(paths.site_root, data_context)

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

    Tries to load from the standard file names first. If data is empty or
    missing, falls back to metrics_*.json files and transforms them.

    Args:
        data_dir: Directory containing JSON metrics files.

    Returns:
        Dictionary mapping data keys to their content.
    """
    data: dict[str, Any] = {}

    # Standard file mappings
    json_files = [
        "summary.json",
        "leaderboards.json",
        "timeseries.json",
        "repo_health.json",
        "hygiene_scores.json",
        "awards.json",
    ]

    # Load standard files first
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

    # Check if we need to fall back to metrics_*.json files
    # This handles cases where stub files exist but have empty data
    data = _enrich_from_metrics_files(data_dir, data)

    return data


def _enrich_from_metrics_files(data_dir: Path, data: dict[str, Any]) -> dict[str, Any]:
    """Enrich data context from metrics_*.json files if standard files are empty.

    Args:
        data_dir: Directory containing metrics files.
        data: Existing data dictionary to enrich.

    Returns:
        Enriched data dictionary.
    """
    # Mapping from metrics file to data key and transform function
    metrics_mappings = [
        ("metrics_leaderboard.json", "leaderboards", _transform_metrics_leaderboard),
        ("metrics_time_series.json", "timeseries", _transform_metrics_timeseries),
        ("metrics_repo_health.json", "repo_health", _transform_metrics_repo_health),
        ("metrics_repo_hygiene_score.json", "hygiene_scores", _transform_metrics_hygiene),
        ("metrics_awards.json", "awards", _transform_metrics_awards),
    ]

    for metrics_file, data_key, transform_fn in metrics_mappings:
        # Check if existing data is empty or missing
        existing = data.get(data_key, {})
        is_empty = (
            not existing
            or (isinstance(existing, dict) and _is_empty_dict(existing))
            or (isinstance(existing, list) and len(existing) == 0)
        )

        if is_empty:
            metrics_path = data_dir / metrics_file
            if metrics_path.exists():
                try:
                    with metrics_path.open() as f:
                        metrics_content = json.load(f)
                    if metrics_content:
                        transformed = transform_fn(metrics_content)
                        if transformed:
                            data[data_key] = transformed
                            logger.info("Enriched %s from %s", data_key, metrics_file)
                except Exception as e:
                    logger.warning("Failed to load %s: %s", metrics_file, e)

    # Calculate and enrich summary data from metrics files
    existing_summary = data.get("summary", {})
    if _is_empty_dict(existing_summary) or not existing_summary.get("total_contributors"):
        calculated_summary = _calculate_summary_from_metrics(data_dir)
        if calculated_summary.get("total_contributors", 0) > 0:
            # Merge with existing summary, preferring calculated values
            if "summary" not in data:
                data["summary"] = {}
            data["summary"].update(calculated_summary)
            logger.info(
                "Enriched summary from metrics: %d contributors, %d PRs, %d repos",
                calculated_summary.get("total_contributors", 0),
                calculated_summary.get("prs_merged", 0),
                calculated_summary.get("total_repos", 0),
            )

    return data


def _is_empty_dict(d: dict[str, Any]) -> bool:
    """Check if a dict has only empty values."""
    if not d:
        return True
    for v in d.values():
        if isinstance(v, list) and len(v) > 0:
            return False
        if isinstance(v, dict) and not _is_empty_dict(v):
            return False
        if v and not isinstance(v, (list, dict)):
            return False
    return True


def _transform_metrics_leaderboard(
    metrics: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Transform metrics_leaderboard.json format to leaderboards.json format.

    Input: [{year, metric_key, scope, repo_id, user_id, value, rank}, ...]
    Output: {metric_key: [{user, count, avatar_url}, ...], ...}
    """
    if not isinstance(metrics, list):
        return {}

    result: dict[str, list[dict[str, Any]]] = {}

    # Group by metric_key, filter to org scope
    for item in metrics:
        if item.get("scope") != "org":
            continue

        metric_key = item.get("metric_key", "")
        user_id = item.get("user_id", "")
        value = item.get("value", 0)

        if not metric_key or not user_id:
            continue

        if metric_key not in result:
            result[metric_key] = []

        # Extract username from user_id if possible (user_id might be GitHub node ID)
        # For now, use user_id as-is since we don't have login mapping
        result[metric_key].append(
            {
                "user": user_id,
                "count": value,
                "avatar_url": "",
            }
        )

    # Sort each metric list by count descending
    for metric_key in result:
        result[metric_key].sort(key=lambda x: x["count"], reverse=True)

    return result


def _transform_metrics_timeseries(metrics: list[dict[str, Any]]) -> dict[str, Any]:
    """Transform metrics_time_series.json format to timeseries.json format.

    Input: [{year, period_type, period_start, period_end, scope, repo_id, metric_key, value}, ...]
    Output: {weekly: {metric_key: [{period, count}, ...]}, monthly: {...}}
    """
    if not isinstance(metrics, list):
        return {"weekly": {}, "monthly": {}}

    result: dict[str, dict[str, list[dict[str, Any]]]] = {
        "weekly": {},
        "monthly": {},
    }

    for item in metrics:
        if item.get("scope") != "org":
            continue

        period_type = item.get("period_type", "")
        metric_key = item.get("metric_key", "")
        period_start = item.get("period_start", "")
        value = item.get("value", 0)

        if not period_type or not metric_key or not period_start:
            continue

        period_bucket = "weekly" if period_type == "week" else "monthly"

        if metric_key not in result[period_bucket]:
            result[period_bucket][metric_key] = []

        result[period_bucket][metric_key].append(
            {
                "period": period_start,
                "count": value,
            }
        )

    # Sort each list by period
    for period_bucket in result:
        for metric_key in result[period_bucket]:
            result[period_bucket][metric_key].sort(key=lambda x: x["period"])

    return result


def _transform_metrics_repo_health(metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Transform metrics_repo_health.json to repo_health list format."""
    if not isinstance(metrics, list):
        return []
    # Already in list format, just return
    return metrics


def _transform_metrics_hygiene(metrics: Any) -> dict[str, Any]:
    """Transform metrics_repo_hygiene_score.json to hygiene_scores format."""
    if isinstance(metrics, dict):
        return metrics
    if isinstance(metrics, list):
        # Convert list to dict keyed by repo
        return {"scores": metrics}
    return {}


def _transform_metrics_awards(metrics: list[dict[str, Any]]) -> dict[str, Any]:
    """Transform metrics_awards.json to awards format.

    Input: [{award_key, title, description, category, winner_user_id, winner_repo_id,
             winner_name, supporting_stats}, ...]
    Output: {individual: [...], repository: [...], risk: [...], special_mentions: {...}}
    """
    if not isinstance(metrics, list):
        return {}

    result: dict[str, list[dict[str, Any]]] = {
        "individual": [],
        "repository": [],
        "risk": [],
    }

    for item in metrics:
        category = item.get("category", "individual")
        if category not in result:
            continue

        award = {
            "award_key": item.get("award_key", ""),
            "title": item.get("title", ""),
            "description": item.get("description", ""),
            "winner_name": item.get("winner_name", ""),
            "winner_avatar_url": "",
            "supporting_stats": item.get("supporting_stats", ""),
        }

        # Add repo_name for repository awards
        if category == "repository":
            award["repo_name"] = item.get("winner_name", "")

        result[category].append(award)

    return result


def _calculate_summary_from_metrics(data_dir: Path) -> dict[str, Any]:
    """Calculate summary statistics from metrics files.

    Reads metrics files to compute:
    - total_contributors: unique users in leaderboard
    - prs_merged: sum from timeseries (org scope)
    - total_repos: count of repos in repo_health
    - total_reviews: sum from timeseries (org scope)

    Returns:
        Summary dict with calculated statistics.
    """
    summary: dict[str, Any] = {
        "total_contributors": 0,
        "prs_merged": 0,
        "total_prs_merged": 0,
        "total_repos": 0,
        "total_reviews": 0,
    }

    # Get unique contributors from leaderboard
    leaderboard_path = data_dir / "metrics_leaderboard.json"
    if leaderboard_path.exists():
        try:
            with leaderboard_path.open() as f:
                leaderboard = json.load(f)
            if isinstance(leaderboard, list):
                unique_users: set[str] = set()
                for item in leaderboard:
                    if item.get("scope") != "org":
                        continue
                    user_id = item.get("user_id", "")
                    if user_id:
                        unique_users.add(user_id)
                summary["total_contributors"] = len(unique_users)
        except Exception as e:
            logger.warning("Failed to get contributors from leaderboard: %s", e)

    # Get PR and review counts from timeseries (org-scope weekly totals)
    timeseries_path = data_dir / "metrics_time_series.json"
    if timeseries_path.exists():
        try:
            with timeseries_path.open() as f:
                timeseries = json.load(f)
            if isinstance(timeseries, list):
                total_prs = 0
                total_reviews = 0
                for item in timeseries:
                    if item.get("scope") != "org":
                        continue
                    metric_key = item.get("metric_key", "")
                    value = item.get("value", 0)
                    if metric_key == "prs_merged":
                        total_prs += value
                    elif metric_key == "reviews_submitted":
                        total_reviews += value
                summary["prs_merged"] = total_prs
                summary["total_prs_merged"] = total_prs
                summary["total_reviews"] = total_reviews
        except Exception as e:
            logger.warning("Failed to get PR/review counts from timeseries: %s", e)

    # Get repo count from repo_health
    repo_health_path = data_dir / "metrics_repo_health.json"
    if repo_health_path.exists():
        try:
            with repo_health_path.open() as f:
                repo_health = json.load(f)
            if isinstance(repo_health, list):
                summary["total_repos"] = len(repo_health)
        except Exception as e:
            logger.warning("Failed to calculate repo count: %s", e)

    return summary


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

        def format_end_date(value: str) -> str:
            """Format exclusive end date as inclusive (subtract one day).

            The 'until' date in config is exclusive (e.g., 2026-01-01 means data up to
            but not including Jan 1, 2026). For display purposes, we want to show the
            last included day (Dec 31, 2025).
            """
            if not value:
                return ""
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                # Subtract one day to get the last included day
                last_day = dt - timedelta(days=1)
                return last_day.strftime("%B %d, %Y")
            except (ValueError, AttributeError):
                return str(value)

        env.filters["format_date"] = format_date
        env.filters["format_number"] = format_number
        env.filters["format_end_date"] = format_end_date

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
        elif isinstance(repo_health_data, dict) and "repositories" in repo_health_data:
            # New format: {"repositories": [...]} - extract list from repositories key
            repos_list = repo_health_data.get("repositories", [])
            if isinstance(repos_list, list):
                for item in repos_list:
                    if isinstance(item, dict):
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
        elif isinstance(hygiene_scores_data, dict) and "scores" in hygiene_scores_data:
            # New format: {"scores": [...]} - extract list from scores key
            scores_list = hygiene_scores_data.get("scores", [])
            if isinstance(scores_list, list):
                for item in scores_list:
                    if isinstance(item, dict):
                        repo_name = item.get("repo", "")
                        hygiene_scores_list.append(
                            {
                                "repo_id": repo_name,
                                "score": item.get("score", 0),
                                **item,
                            }
                        )
        elif isinstance(hygiene_scores_data, dict):
            # Dict format with repo names as keys: {"repo_name": {...}}
            for repo_name, repo_data in hygiene_scores_data.items():
                if isinstance(repo_data, dict):
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
        repos_merged = repos_view.merge_repo_data(
            repo_health_list,
            hygiene_scores_list,
            config.thresholds.hygiene_healthy,
            config.thresholds.hygiene_warning,
        )

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

        # Collect year statistics for year_index.html template
        year_stats = _collect_year_stats(site_base_dir, available_years)

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

        # Build years list with stats for year_index.html
        years_list = _build_years_list(available_years, year_stats, current_year, base_path)

        # Compute base_url - use config value or construct from target name
        base_url = config.report.base_url
        if not base_url and config.github.target.name:
            # Default to GitHub Pages URL pattern
            base_url = f"https://{config.github.target.name}.github.io"

        # Pre-compute engineers list (used in multiple context keys)
        engineers_list = _get_engineers_list(leaderboards_data, timeseries_data)

        # Generate engineer chart data
        engineer_charts = generate_engineer_charts(timeseries_data, summary_data, repo_health_list)

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
            "base_url": base_url,
            "target_name": config.github.target.name,
            "organization_name": config.report.organization_name or config.github.target.name,
            "site_description": config.report.description,
            "og_image": config.report.og_image,
            "theme_color": config.report.theme_color,
            "current_year": current_year,
            "available_years": available_years,
            "year_stats": year_stats,
            "years": years_list,
            "most_recent_year": available_years[0] if available_years else None,
            "most_recent_year_path": f"{base_path}/{available_years[0]}/"
            if available_years
            else None,
            "thresholds": {
                "hygiene_healthy": config.thresholds.hygiene_healthy,
                "hygiene_warning": config.thresholds.hygiene_warning,
                "hygiene_good": config.thresholds.hygiene_good,
                "hygiene_bad": config.thresholds.hygiene_bad,
                "review_coverage_good": config.thresholds.review_coverage_good,
                "review_coverage_bad": config.thresholds.review_coverage_bad,
                "max_review_time_hours": config.thresholds.max_review_time_hours,
                "stale_pr_days": config.thresholds.stale_pr_days,
                "high_pr_ratio": config.thresholds.high_pr_ratio,
                "leaderboard_top_n": config.thresholds.leaderboard_top_n,
                "contributor_chart_top_n": config.thresholds.contributor_chart_top_n,
            },
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
            # Engineer chart data for Engineers page
            "contribution_timeline": engineer_charts.get("contribution_timeline", []),
            "contribution_types": engineer_charts.get("contribution_types", []),
            "contribution_by_repo": engineer_charts.get("contribution_by_repo", []),
            # Calculate insights from metrics data
            "insights": _calculate_insights(
                summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
            ),
            # Calculate risks from health metrics
            "risks": _calculate_risks(
                repo_health_list,
                hygiene_scores_list,
                summary_data,
                config.thresholds.hygiene_good,
                config.thresholds.review_coverage_good,
                config.thresholds.max_review_time_hours,
            ),
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
            timeseries_data, summary_data, leaderboards_data, repo_health_list, hygiene_scores_list
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

        # Generate manifest.webmanifest and sw.js from templates
        year = config.github.windows.year
        _generate_manifest(output_dir, env, config, year)
        _generate_service_worker(output_dir, env, year)

        # Generate SEO files (sitemap.xml and robots.txt)
        if base_url:
            _generate_sitemap(site_base_dir, env, base_url, available_years)
            _generate_robots_txt(site_base_dir, env, base_url)
        else:
            logger.warning(
                "Skipping sitemap.xml and robots.txt generation: base_url not configured"
            )

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
    awards: list[dict[str, Any]] = []

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
    hygiene_good: int = 60,
    review_coverage_good: int = 50,
    max_review_time_hours: int = 48,
) -> list[dict[str, Any]]:
    """Backward-compatible wrapper for calculate_risks."""
    return calculate_risks(
        repo_health_list,
        hygiene_scores_list,
        summary_data,
        hygiene_good,
        review_coverage_good,
        max_review_time_hours,
    )


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


def _export_search_data(output_dir: Path, data_context: dict[str, Any]) -> None:
    """Export contributors and repos data for global search functionality.

    Args:
        output_dir: Directory where search JSON files will be written.
        data_context: Context data containing leaderboards and repo health info.
    """
    try:
        # Extract leaderboards data for contributors
        leaderboards_data = data_context.get("leaderboards", {})
        contributors_list = []

        # Get contributors from leaderboards structure
        # Structure contains prs_merged list with user and count
        if isinstance(leaderboards_data, dict):
            # Extract from prs_merged list (current format)
            if "prs_merged" in leaderboards_data and isinstance(
                leaderboards_data["prs_merged"], list
            ):
                for contributor in leaderboards_data["prs_merged"]:
                    contributors_list.append(
                        {
                            "login": contributor.get("user", ""),
                            "total_prs": contributor.get("count", 0),
                        }
                    )
            # Fallback: check for nested leaderboards.prs_merged.org format
            elif "leaderboards" in leaderboards_data:
                lb_data = leaderboards_data["leaderboards"]
                if "prs_merged" in lb_data and "org" in lb_data["prs_merged"]:
                    for contributor in lb_data["prs_merged"]["org"]:
                        contributors_list.append(
                            {
                                "login": contributor.get("login", ""),
                                "total_prs": contributor.get("value", 0),
                            }
                        )
            # Fallback: check for top_pr_authors format
            elif "top_pr_authors" in leaderboards_data:
                for author in leaderboards_data["top_pr_authors"]:
                    contributors_list.append(
                        {
                            "login": author.get("login", ""),
                            "total_prs": author.get("total_prs", 0),
                        }
                    )

        # Write contributors.json
        contributors_path = output_dir / "contributors.json"
        with contributors_path.open("w") as f:
            json.dump(contributors_list, f, indent=2)
        logger.info("Exported %d contributors to %s", len(contributors_list), contributors_path)

        # Extract repo health data for repositories
        repo_health_data = data_context.get("repo_health", {})
        repos_list = []

        if isinstance(repo_health_data, dict) and "repos" in repo_health_data:
            # Format from export: {"repos": {repo_id: {...}}}
            for repo_id, repo_data in repo_health_data["repos"].items():
                repos_list.append(
                    {
                        "repo_full_name": repo_data.get("repo_full_name", repo_id),
                        "prs_merged": repo_data.get("prs_merged", 0),
                    }
                )
        elif isinstance(repo_health_data, list):
            # List format from metrics
            for item in repo_health_data:
                repo_name = item.get("repo", "")
                repos_list.append(
                    {
                        "repo_full_name": repo_name,
                        "prs_merged": item.get("pr_count", 0),
                    }
                )

        # Write repos.json
        repos_path = output_dir / "repos.json"
        with repos_path.open("w") as f:
            json.dump(repos_list, f, indent=2)
        logger.info("Exported %d repositories to %s", len(repos_list), repos_path)

    except Exception as e:
        logger.warning("Failed to export search data: %s", e)


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


def _generate_manifest(
    output_dir: Path,
    template_env: Environment,
    config: Config,
    year: int,
) -> None:
    """Generate manifest.webmanifest from template."""
    template = template_env.get_template("manifest.webmanifest.j2")
    content = template.render(
        config=config,
        site_description=config.report.description,
        theme_color=config.report.theme_color,
        year=year,
    )

    manifest_path = output_dir / "manifest.webmanifest"
    manifest_path.write_text(content)
    logger.info("Generated manifest.webmanifest")


def _generate_service_worker(
    output_dir: Path,
    template_env: Environment,
    year: int,
) -> None:
    """Generate sw.js from template."""
    import time

    cache_version = int(time.time())

    template = template_env.get_template("sw.js.j2")
    content = template.render(
        year=year,
        cache_version=cache_version,
    )

    sw_path = output_dir / "sw.js"
    sw_path.write_text(content)
    logger.info("Generated sw.js")


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


def _generate_sitemap(
    output_dir: Path,
    template_env: Environment,
    base_url: str,
    years: list[int],
) -> None:
    """Generate sitemap.xml from template.

    Args:
        output_dir: Directory where sitemap.xml will be written.
        template_env: Jinja2 environment with template loader.
        base_url: Full base URL for the site (e.g., 'https://user.github.io/repo').
        years: List of years to include in sitemap.
    """
    pages = [
        "index.html",
        "summary.html",
        "engineers.html",
        "repos.html",
        "leaderboards.html",
        "awards.html",
    ]
    build_date = datetime.now(UTC).strftime("%Y-%m-%d")

    template = template_env.get_template("sitemap.xml.j2")
    content = template.render(
        base_url=base_url.rstrip("/"),
        years=years,
        pages=pages,
        build_date=build_date,
    )

    sitemap_path = output_dir / "sitemap.xml"
    sitemap_path.write_text(content, encoding="utf-8")
    logger.info("Generated sitemap.xml with %d years", len(years))


def _generate_robots_txt(
    output_dir: Path,
    template_env: Environment,
    base_url: str,
) -> None:
    """Generate robots.txt from template.

    Args:
        output_dir: Directory where robots.txt will be written.
        template_env: Jinja2 environment with template loader.
        base_url: Full base URL for the site (e.g., 'https://user.github.io/repo').
    """
    template = template_env.get_template("robots.txt.j2")
    content = template.render(base_url=base_url.rstrip("/"))

    robots_path = output_dir / "robots.txt"
    robots_path.write_text(content, encoding="utf-8")
    logger.info("Generated robots.txt")
