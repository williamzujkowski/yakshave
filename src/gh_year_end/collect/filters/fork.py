"""Fork status filter for repository discovery."""

from typing import Any

from .base import BaseFilter, FilterResult


class ForkFilter(BaseFilter):
    """Filter repositories based on fork status.

    Excludes forked repositories when include_forks is False in config.
    Supports Search API qualifier: 'fork:false'
    """

    name = "fork"

    def is_enabled(self, config: Any) -> bool:
        """Fork filter is enabled when include_forks is False."""
        return not config.include_forks

    def evaluate(self, repo: dict[str, Any], config: Any) -> FilterResult:  # noqa: ARG002
        """Evaluate repository fork status.

        Args:
            repo: Repository data dictionary.
            config: DiscoveryConfig object.

        Returns:
            FilterResult indicating pass/fail.
        """
        is_fork = repo.get("fork", False)

        if is_fork:
            return FilterResult(
                passed=False,
                reason="Repository is a fork",
                filter_name=self.name,
            )

        return FilterResult(passed=True, filter_name=self.name)

    def get_search_qualifier(self, config: Any) -> str | None:
        """Return Search API qualifier for non-fork repositories."""
        if not self.is_enabled(config):
            return None
        return "fork:false"
