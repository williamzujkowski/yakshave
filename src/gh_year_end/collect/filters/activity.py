"""Activity recency filter for repository discovery."""

from datetime import UTC, datetime, timedelta
from typing import Any

from .base import BaseFilter, FilterResult


class ActivityFilter(BaseFilter):
    """Filter repositories based on recent activity (pushed_at timestamp).

    Excludes repositories that haven't been pushed to within the configured
    time threshold. Supports Search API qualifier: 'pushed:>YYYY-MM-DD'
    """

    name = "activity"

    def is_enabled(self, config: Any) -> bool:
        """Activity filter is enabled when config has activity settings."""
        return hasattr(config, "activity") and config.activity.enabled

    def evaluate(self, repo: dict[str, Any], config: Any) -> FilterResult:
        """Evaluate repository activity recency.

        Args:
            repo: Repository data dictionary.
            config: DiscoveryConfig object.

        Returns:
            FilterResult indicating pass/fail.
        """
        pushed_at_str = repo.get("pushed_at")
        if not pushed_at_str:
            return FilterResult(
                passed=False,
                reason="Repository has no pushed_at timestamp",
                filter_name=self.name,
            )

        try:
            pushed_at = datetime.fromisoformat(pushed_at_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return FilterResult(
                passed=False,
                reason=f"Invalid pushed_at timestamp: {pushed_at_str}",
                filter_name=self.name,
            )

        # Calculate threshold
        min_days = config.activity.min_pushed_within_days
        threshold = datetime.now(UTC) - timedelta(days=min_days)

        if pushed_at < threshold:
            days_ago = (datetime.now(UTC) - pushed_at).days
            return FilterResult(
                passed=False,
                reason=f"Repository last pushed {days_ago} days ago (threshold: {min_days} days)",
                filter_name=self.name,
            )

        return FilterResult(passed=True, filter_name=self.name)

    def get_search_qualifier(self, config: Any) -> str | None:
        """Return Search API qualifier for activity recency."""
        if not self.is_enabled(config):
            return None

        # Calculate the date threshold
        min_days = config.activity.min_pushed_within_days
        threshold = datetime.now(UTC) - timedelta(days=min_days)
        date_str = threshold.strftime("%Y-%m-%d")

        return f"pushed:>{date_str}"
