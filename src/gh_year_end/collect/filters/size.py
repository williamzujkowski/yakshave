"""Repository size filter for repository discovery."""

from typing import Any

from .base import BaseFilter, FilterResult


class SizeFilter(BaseFilter):
    """Filter repositories based on size (in KB).

    Excludes repositories outside the configured size range.
    Supports Search API qualifier: 'size:MIN..MAX'
    """

    name = "size"

    def is_enabled(self, config: Any) -> bool:
        """Size filter is enabled when config has size settings."""
        return hasattr(config, "size") and config.size.enabled

    def evaluate(self, repo: dict[str, Any], config: Any) -> FilterResult:
        """Evaluate repository size.

        Args:
            repo: Repository data dictionary.
            config: DiscoveryConfig object.

        Returns:
            FilterResult indicating pass/fail.
        """
        repo_size = repo.get("size", 0)
        min_kb = config.size.min_kb
        max_kb = config.size.max_kb

        if repo_size < min_kb:
            return FilterResult(
                passed=False,
                reason=f"Repository size {repo_size}KB below minimum {min_kb}KB",
                filter_name=self.name,
            )

        if repo_size > max_kb:
            return FilterResult(
                passed=False,
                reason=f"Repository size {repo_size}KB above maximum {max_kb}KB",
                filter_name=self.name,
            )

        return FilterResult(passed=True, filter_name=self.name)

    def get_search_qualifier(self, config: Any) -> str | None:
        """Return Search API qualifier for repository size."""
        if not self.is_enabled(config):
            return None

        min_kb = config.size.min_kb
        max_kb = config.size.max_kb

        return f"size:{min_kb}..{max_kb}"
