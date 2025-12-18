"""Orchestrator for metrics calculation.

Coordinates all metrics calculators and produces aggregated statistics.
"""

import logging
from datetime import UTC, datetime
from typing import Any

from gh_year_end.config import Config
from gh_year_end.metrics.leaderboards import calculate_leaderboards
from gh_year_end.storage.paths import PathManager

logger = logging.getLogger(__name__)


def run_metrics(config: Config) -> dict[str, Any]:
    """Run all metrics calculators and write output to metrics directory.

    Args:
        config: Application configuration.

    Returns:
        Dictionary with statistics about metrics calculation:
        - start_time: ISO8601 start timestamp
        - end_time: ISO8601 end timestamp
        - duration_seconds: Total duration
        - metrics_written: List of metric tables written
        - total_rows: Total rows across all metrics
        - errors: List of error messages

    Raises:
        ValueError: If curated data does not exist.
        Exception: If metrics calculation fails.
    """
    start_time = datetime.now(UTC)
    paths = PathManager(config)

    logger.info("Starting metrics calculation for year %d", config.github.windows.year)

    # Verify curated data exists
    _verify_curated_data_exists(paths)

    # Ensure metrics directory exists
    paths.metrics_root.mkdir(parents=True, exist_ok=True)

    stats: dict[str, Any] = {
        "start_time": start_time.isoformat(),
        "metrics_written": [],
        "total_rows": 0,
        "errors": [],
    }

    # Run each metrics calculator in order
    # Note: Calculators will be imported as they are implemented
    calculators = [
        ("leaderboards", _run_leaderboards),
        ("time_series", _run_time_series),
        ("repo_health", _run_repo_health),
        ("hygiene_scores", _run_hygiene_scores),
        ("awards", _run_awards),
    ]

    for name, calculator_fn in calculators:
        try:
            logger.info("Running %s calculator", name)
            result = calculator_fn(config, paths)

            if result["success"]:
                stats["metrics_written"].append(result["table_name"])
                stats["total_rows"] += result.get("row_count", 0)
                logger.info(
                    "Completed %s: %d rows written to %s",
                    name,
                    result.get("row_count", 0),
                    result["table_name"],
                )
            else:
                error_msg = f"{name}: {result.get('error', 'Unknown error')}"
                stats["errors"].append(error_msg)
                logger.warning("Skipped %s: %s", name, result.get("error"))

        except Exception as e:
            error_msg = f"{name}: {e!s}"
            stats["errors"].append(error_msg)
            logger.exception("Failed to calculate %s metrics", name)

    end_time = datetime.now(UTC)
    stats["end_time"] = end_time.isoformat()
    stats["duration_seconds"] = (end_time - start_time).total_seconds()

    logger.info(
        "Metrics calculation complete: %d tables, %d rows, %d errors",
        len(stats["metrics_written"]),
        stats["total_rows"],
        len(stats["errors"]),
    )

    return stats


def _verify_curated_data_exists(paths: PathManager) -> None:
    """Verify that curated data exists.

    Args:
        paths: Path manager.

    Raises:
        ValueError: If curated root does not exist or is empty.
    """
    if not paths.curated_root.exists():
        msg = f"Curated data not found at {paths.curated_root}. Run 'normalize' command first."
        raise ValueError(msg)

    # Check for at least one required table
    required_tables = ["dim_user", "dim_repo"]
    found_tables = []

    for table in required_tables:
        table_path = paths.curated_path(table)  # type: ignore[arg-type]
        if table_path.exists():
            found_tables.append(table)

    if not found_tables:
        msg = f"No curated tables found in {paths.curated_root}. Run 'normalize' command first."
        raise ValueError(msg)

    logger.info("Found %d curated tables: %s", len(found_tables), ", ".join(found_tables))


def _run_leaderboards(config: Config, paths: PathManager) -> dict[str, Any]:
    """Run leaderboards calculator.

    Calculate leaderboards for:
    - PRs opened/closed/merged
    - Issues opened/closed
    - Reviews submitted/approved/changes_requested
    - Comments (issue + review)

    Args:
        config: Application configuration.
        paths: Path manager.

    Returns:
        Result dictionary with success status, table_name, row_count, and optional error.
    """
    try:
        df = calculate_leaderboards(paths.curated_root, config)
        row_count = len(df)

        # Write to parquet
        output_path = paths.metrics_path("metrics_leaderboard")
        df.write_parquet(output_path)

        return {
            "success": True,
            "table_name": "metrics_leaderboard",
            "row_count": row_count,
        }
    except FileNotFoundError as e:
        logger.warning("Leaderboards calculator skipped: %s", e)
        return {
            "success": False,
            "table_name": "metrics_leaderboard",
            "error": str(e),
        }
    except Exception as e:
        logger.exception("Leaderboards calculator failed")
        return {
            "success": False,
            "table_name": "metrics_leaderboard",
            "error": str(e),
        }


def _run_time_series(config: Config, paths: PathManager) -> dict[str, Any]:
    """Run time series calculator.

    When implemented, this will calculate weekly/monthly time series for activity metrics.

    Args:
        config: Application configuration.
        paths: Path manager.

    Returns:
        Result dictionary with success status, table_name, row_count, and optional error.
    """
    logger.warning("Time series calculator not yet implemented")
    return {
        "success": False,
        "table_name": "metrics_time_series",
        "error": "Not implemented",
    }


def _run_repo_health(config: Config, paths: PathManager) -> dict[str, Any]:
    """Run repository health calculator.

    When implemented, this will calculate health metrics per repository.

    Args:
        config: Application configuration.
        paths: Path manager.

    Returns:
        Result dictionary with success status, table_name, row_count, and optional error.
    """
    logger.warning("Repository health calculator not yet implemented")
    return {
        "success": False,
        "table_name": "metrics_repo_health",
        "error": "Not implemented",
    }


def _run_hygiene_scores(config: Config, paths: PathManager) -> dict[str, Any]:
    """Run hygiene scores calculator.

    When implemented, this will calculate hygiene scores (0-100) for each repository.

    Args:
        config: Application configuration.
        paths: Path manager.

    Returns:
        Result dictionary with success status, table_name, row_count, and optional error.
    """
    logger.warning("Hygiene scores calculator not yet implemented")
    return {
        "success": False,
        "table_name": "metrics_repo_hygiene_score",
        "error": "Not implemented",
    }


def _run_awards(config: Config, paths: PathManager) -> dict[str, Any]:
    """Run awards calculator.

    When implemented, this will generate awards from the awards.yaml config.

    Args:
        config: Application configuration.
        paths: Path manager.

    Returns:
        Result dictionary with success status, table_name, row_count, and optional error.
    """
    logger.warning("Awards calculator not yet implemented")
    return {
        "success": False,
        "table_name": "metrics_awards",
        "error": "Not implemented",
    }
