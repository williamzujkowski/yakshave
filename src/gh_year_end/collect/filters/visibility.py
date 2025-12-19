"""Visibility filter for repository discovery."""

from typing import Any

from .base import BaseFilter, FilterResult


class VisibilityFilter(BaseFilter):
    """Filter repositories based on visibility (public/private).

    Filters repositories when visibility config is not 'all'.
    Supports Search API qualifier: 'is:public' or 'is:private'
    """

    name = "visibility"

    def is_enabled(self, config: Any) -> bool:
        """Visibility filter is enabled when visibility is not 'all'."""
        return bool(config.visibility != "all")

    def evaluate(self, repo: dict[str, Any], config: Any) -> FilterResult:
        """Evaluate repository visibility.

        Args:
            repo: Repository data dictionary.
            config: DiscoveryConfig object.

        Returns:
            FilterResult indicating pass/fail.
        """
        # Get visibility from repo data (fallback to private field)
        repo_visibility = repo.get("visibility")
        if repo_visibility is None:
            repo_visibility = "private" if repo.get("private") else "public"

        target_visibility = config.visibility

        if repo_visibility != target_visibility:
            return FilterResult(
                passed=False,
                reason=f"Repository visibility is {repo_visibility}, expected {target_visibility}",
                filter_name=self.name,
            )

        return FilterResult(passed=True, filter_name=self.name)

    def get_search_qualifier(self, config: Any) -> str | None:
        """Return Search API qualifier for visibility."""
        if not self.is_enabled(config):
            return None
        return f"is:{config.visibility}"
