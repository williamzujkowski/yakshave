"""Generate awards from metrics tables.

Reads metrics tables (leaderboards, repo_health, hygiene_scores) and applies
award definitions from config/awards.yaml to determine winners.

Awards can be:
- User awards (individual contributors)
- Repository awards
- Risk signals (flagged items)

Each award has criteria (metric-based or composite) and optional filters.
"""

from pathlib import Path
from typing import Any

import polars as pl
import yaml

from gh_year_end.storage.parquet_writer import read_parquet


class AwardsConfig:
    """Awards configuration loader and accessor."""

    def __init__(self, config_path: Path) -> None:
        """Load awards configuration from YAML.

        Args:
            config_path: Path to awards.yaml configuration file.

        Raises:
            FileNotFoundError: If config file doesn't exist.
            ValueError: If config is invalid.
        """
        if not config_path.exists():
            msg = f"Awards config not found: {config_path}"
            raise FileNotFoundError(msg)

        with config_path.open() as f:
            raw_config: dict[str, Any] = yaml.safe_load(f)

        self.user_awards: list[dict[str, Any]] = raw_config.get("user_awards", [])
        self.repo_awards: list[dict[str, Any]] = raw_config.get("repo_awards", [])
        self.risk_signals: list[dict[str, Any]] = raw_config.get("risk_signals", [])

        self._validate()

    def _validate(self) -> None:
        """Validate awards configuration.

        Raises:
            ValueError: If configuration is invalid.
        """
        # Validate user awards
        for award in self.user_awards:
            if "key" not in award or "title" not in award or "description" not in award:
                msg = f"User award missing required fields: {award}"
                raise ValueError(msg)

        # Validate repo awards
        for award in self.repo_awards:
            if "key" not in award or "title" not in award or "description" not in award:
                msg = f"Repo award missing required fields: {award}"
                raise ValueError(msg)

        # Validate risk signals
        for signal in self.risk_signals:
            if "key" not in signal or "title" not in signal or "description" not in signal:
                msg = f"Risk signal missing required fields: {signal}"
                raise ValueError(msg)


def generate_awards(
    metrics_path: Path,
    config_path: Path,
    year: int,
) -> pl.DataFrame:
    """Generate awards from metrics tables.

    Args:
        metrics_path: Path to metrics directory (e.g., data/metrics/year=2025/).
        config_path: Path to awards.yaml configuration file.
        year: Year for the awards.

    Returns:
        Polars DataFrame with schema:
            - award_key: str - Unique identifier
            - title: str - Display title
            - description: str - Award description
            - category: str - "individual", "repository", or "risk"
            - winner_user_id: str | None - For individual awards
            - winner_repo_id: str | None - For repository awards
            - winner_name: str - Display name (login or repo name)
            - supporting_stats: str - JSON blob with relevant stats

    Raises:
        FileNotFoundError: If required metrics files don't exist.
    """
    awards_config = AwardsConfig(config_path)

    # Load metrics tables
    leaderboard_path = metrics_path / "metrics_leaderboard.parquet"
    repo_health_path = metrics_path / "metrics_repo_health.parquet"
    hygiene_score_path = metrics_path / "metrics_repo_hygiene_score.parquet"

    # Read tables if they exist
    leaderboard = _read_parquet_if_exists(leaderboard_path)
    repo_health = _read_parquet_if_exists(repo_health_path)
    hygiene_scores = _read_parquet_if_exists(hygiene_score_path)

    # Generate awards
    awards = []

    # Process user awards
    if leaderboard is not None:
        awards.extend(_generate_user_awards(leaderboard, awards_config.user_awards, year))

    # Process repo awards
    if repo_health is not None or hygiene_scores is not None:
        awards.extend(
            _generate_repo_awards(
                repo_health,
                hygiene_scores,
                awards_config.repo_awards,
                year,
            )
        )

    # Process risk signals
    if hygiene_scores is not None:
        awards.extend(_generate_risk_signals(hygiene_scores, awards_config.risk_signals, year))

    # Convert to DataFrame
    if not awards:
        # Return empty DataFrame with correct schema
        return pl.DataFrame(
            schema={
                "award_key": pl.String,
                "title": pl.String,
                "description": pl.String,
                "category": pl.String,
                "winner_user_id": pl.String,
                "winner_repo_id": pl.String,
                "winner_name": pl.String,
                "supporting_stats": pl.String,
            }
        )

    return pl.DataFrame(awards)


def _read_parquet_if_exists(path: Path) -> pl.DataFrame | None:
    """Read Parquet file if it exists, otherwise return None.

    Args:
        path: Path to Parquet file.

    Returns:
        Polars DataFrame if file exists, None otherwise.
    """
    if not path.exists():
        return None

    table = read_parquet(path)
    # Type narrowing: from_arrow returns DataFrame, not Series
    df = pl.from_arrow(table)
    if not isinstance(df, pl.DataFrame):
        msg = f"Expected DataFrame from Parquet file, got {type(df)}"
        raise TypeError(msg)
    return df


def _generate_user_awards(
    leaderboard: pl.DataFrame,
    user_awards: list[dict[str, Any]],
    year: int,
) -> list[dict[str, Any]]:
    """Generate user/individual awards from leaderboard.

    Args:
        leaderboard: Metrics leaderboard DataFrame.
        user_awards: List of user award definitions.
        year: Year for filtering.

    Returns:
        List of award dictionaries.
    """
    awards = []

    for award_def in user_awards:
        key = award_def["key"]
        title = award_def["title"]
        description = award_def["description"]
        metric = award_def["metric"]
        direction = award_def.get("direction", "desc")  # desc = higher is better
        tie_breaker = award_def.get("tie_breaker")

        # Filter leaderboard for this metric
        metric_data = leaderboard.filter(
            (pl.col("year") == year) & (pl.col("metric_key") == metric)
        )

        if metric_data.is_empty():
            continue

        # Sort by value (and tie breaker if provided)
        if direction == "desc":
            metric_data = metric_data.sort("value", descending=True)
        else:
            metric_data = metric_data.sort("value", descending=False)

        # Get winner (first row)
        winner = metric_data.row(0, named=True)

        # Build supporting stats
        supporting_stats = {
            "metric": metric,
            "value": winner["value"],
            "rank": winner["rank"],
        }

        # Add tie breaker if present
        if tie_breaker:
            tie_data = leaderboard.filter(
                (pl.col("year") == year)
                & (pl.col("metric_key") == tie_breaker)
                & (pl.col("user_id") == winner["user_id"])
            )
            if not tie_data.is_empty():
                supporting_stats["tie_breaker"] = {
                    "metric": tie_breaker,
                    "value": tie_data.row(0, named=True)["value"],
                }

        awards.append(
            {
                "award_key": key,
                "title": title,
                "description": description,
                "category": "individual",
                "winner_user_id": winner["user_id"],
                "winner_repo_id": None,
                "winner_name": winner.get("user_login", "Unknown"),
                "supporting_stats": str(supporting_stats),
            }
        )

    return awards


def _generate_repo_awards(
    repo_health: pl.DataFrame | None,
    hygiene_scores: pl.DataFrame | None,
    repo_awards: list[dict[str, Any]],
    year: int,
) -> list[dict[str, Any]]:
    """Generate repository awards from repo health and hygiene metrics.

    Args:
        repo_health: Repository health metrics DataFrame.
        hygiene_scores: Repository hygiene scores DataFrame.
        repo_awards: List of repo award definitions.
        year: Year for filtering.

    Returns:
        List of award dictionaries.
    """
    awards = []

    for award_def in repo_awards:
        key = award_def["key"]
        title = award_def["title"]
        description = award_def["description"]
        metric = award_def["metric"]
        direction = award_def.get("direction", "desc")
        min_prs = award_def.get("min_prs", 0)

        # Determine which table to use based on metric
        if metric == "hygiene_score":
            if hygiene_scores is None:
                continue
            metric_data = hygiene_scores
        else:
            if repo_health is None:
                continue
            metric_data = repo_health

        # Apply filters
        if min_prs > 0 and repo_health is not None:
            # Join with repo_health to filter by min_prs
            repos_with_min_prs = repo_health.filter(pl.col("prs_merged") >= min_prs).select(
                "repo_id"
            )
            metric_data = metric_data.join(repos_with_min_prs, on="repo_id", how="inner")

        if metric_data.is_empty():
            continue

        # Check if metric column exists
        if metric not in metric_data.columns:
            continue

        # Filter out nulls
        metric_data = metric_data.filter(pl.col(metric).is_not_null())

        if metric_data.is_empty():
            continue

        # Sort by metric
        if direction == "desc":
            metric_data = metric_data.sort(metric, descending=True)
        else:
            metric_data = metric_data.sort(metric, descending=False)

        # Get winner
        winner = metric_data.row(0, named=True)

        # Build supporting stats
        supporting_stats = {
            "metric": metric,
            "value": winner[metric],
        }

        # Add additional context from repo_health
        if repo_health is not None:
            health_data = repo_health.filter(pl.col("repo_id") == winner["repo_id"])
            if not health_data.is_empty():
                health = health_data.row(0, named=True)
                supporting_stats["prs_merged"] = health.get("prs_merged", 0)
                supporting_stats["active_contributors_365d"] = health.get(
                    "active_contributors_365d", 0
                )

        awards.append(
            {
                "award_key": key,
                "title": title,
                "description": description,
                "category": "repository",
                "winner_user_id": None,
                "winner_repo_id": winner["repo_id"],
                "winner_name": winner.get("repo_full_name", winner.get("repo_id", "Unknown")),
                "supporting_stats": str(supporting_stats),
            }
        )

    return awards


def _generate_risk_signals(
    hygiene_scores: pl.DataFrame,
    risk_signals: list[dict[str, Any]],
    year: int,
) -> list[dict[str, Any]]:
    """Generate risk signals from hygiene scores.

    Args:
        hygiene_scores: Repository hygiene scores DataFrame.
        risk_signals: List of risk signal definitions.
        year: Year for filtering.

    Returns:
        List of risk signal dictionaries (as awards).
    """
    signals = []

    for signal_def in risk_signals:
        key = signal_def["key"]
        title = signal_def["title"]
        description = signal_def["description"]

        # Check if this is a filter-based or metric-based signal
        if "filter" in signal_def:
            # Filter-based signal (e.g., missing_security_md = True)
            filter_col = signal_def["filter"]
            if filter_col not in hygiene_scores.columns:
                continue

            flagged = hygiene_scores.filter(pl.col(filter_col) == True)  # noqa: E712

            if flagged.is_empty():
                continue

            # Count flagged repos
            count = len(flagged)

            signals.append(
                {
                    "award_key": key,
                    "title": title,
                    "description": description,
                    "category": "risk",
                    "winner_user_id": None,
                    "winner_repo_id": None,
                    "winner_name": f"{count} repositories",
                    "supporting_stats": str(
                        {
                            "count": count,
                            "filter": filter_col,
                        }
                    ),
                }
            )

        elif "metric" in signal_def:
            # Metric-based signal (e.g., stale_issue_count > threshold)
            metric = signal_def["metric"]
            threshold = signal_def.get("threshold", 0)

            if metric not in hygiene_scores.columns:
                continue

            flagged = hygiene_scores.filter(pl.col(metric) > threshold)

            if flagged.is_empty():
                continue

            count = len(flagged)

            signals.append(
                {
                    "award_key": key,
                    "title": title,
                    "description": description,
                    "category": "risk",
                    "winner_user_id": None,
                    "winner_repo_id": None,
                    "winner_name": f"{count} repositories",
                    "supporting_stats": str(
                        {
                            "count": count,
                            "metric": metric,
                            "threshold": threshold,
                        }
                    ),
                }
            )

    return signals
