"""Archive status filter for repository discovery."""

from typing import Any

from .base import BaseFilter, FilterResult


class ArchiveFilter(BaseFilter):
    """Filter repositories based on archived status.

    Excludes archived repositories when include_archived is False in config.
    Supports Search API qualifier: 'archived:false'
    """

    name = "archive"

    def is_enabled(self, config: Any) -> bool:
        """Archive filter is enabled when include_archived is False."""
        return not config.include_archived

    def evaluate(self, repo: dict[str, Any], config: Any) -> FilterResult:  # noqa: ARG002
        """Evaluate repository archived status.

        Args:
            repo: Repository data dictionary.
            config: DiscoveryConfig object.

        Returns:
            FilterResult indicating pass/fail.
        """
        is_archived = repo.get("archived", False)

        if is_archived:
            return FilterResult(
                passed=False,
                reason="Repository is archived",
                filter_name=self.name,
            )

        return FilterResult(passed=True, filter_name=self.name)

    def get_search_qualifier(self, config: Any) -> str | None:
        """Return Search API qualifier for non-archived repositories."""
        if not self.is_enabled(config):
            return None
        return "archived:false"
