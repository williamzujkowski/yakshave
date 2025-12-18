"""Name pattern filter for repository discovery."""

import re
from typing import Any

from .base import BaseFilter, FilterResult


class NamePatternFilter(BaseFilter):
    """Filter repositories based on name patterns (regex).

    Supports:
    - include_regex: Repository name must match at least one pattern
    - exclude_regex: Repository name must not match any pattern

    Note: Search API support is partial (simple prefixes only).
    """

    name = "name_pattern"

    def is_enabled(self, config: Any) -> bool:
        """Name pattern filter is enabled when config has name_patterns."""
        if not hasattr(config, "name_patterns"):
            return False
        return bool(config.name_patterns.include_regex or config.name_patterns.exclude_regex)

    def evaluate(self, repo: dict[str, Any], config: Any) -> FilterResult:
        """Evaluate repository name against patterns.

        Args:
            repo: Repository data dictionary.
            config: DiscoveryConfig object.

        Returns:
            FilterResult indicating pass/fail.
        """
        repo_name = repo.get("name", "")

        # Check exclude patterns first (hard rejection)
        if config.name_patterns.exclude_regex:
            for pattern in config.name_patterns.exclude_regex:
                try:
                    if re.search(pattern, repo_name):
                        return FilterResult(
                            passed=False,
                            reason=f"Repository name '{repo_name}' matches exclude pattern: {pattern}",
                            filter_name=self.name,
                        )
                except re.error as e:
                    # Invalid regex pattern - log and skip
                    return FilterResult(
                        passed=False,
                        reason=f"Invalid exclude regex pattern '{pattern}': {e}",
                        filter_name=self.name,
                    )

        # Check include patterns (must match at least one)
        if config.name_patterns.include_regex:
            matched = False
            for pattern in config.name_patterns.include_regex:
                try:
                    if re.search(pattern, repo_name):
                        matched = True
                        break
                except re.error as e:
                    # Invalid regex pattern - log and skip
                    return FilterResult(
                        passed=False,
                        reason=f"Invalid include regex pattern '{pattern}': {e}",
                        filter_name=self.name,
                    )

            if not matched:
                return FilterResult(
                    passed=False,
                    reason=f"Repository name '{repo_name}' does not match any include patterns",
                    filter_name=self.name,
                )

        return FilterResult(passed=True, filter_name=self.name)

    def get_search_qualifier(self, config: Any) -> str | None:
        """Return Search API qualifier for name patterns.

        Note: Only supports simple prefix patterns (^prefix).
        Complex regex patterns cannot be expressed in Search API.
        """
        if not self.is_enabled(config):
            return None

        # Only generate qualifier for simple prefix patterns
        if config.name_patterns.include_regex:
            prefixes = []
            for pattern in config.name_patterns.include_regex:
                # Check if pattern is a simple prefix: ^prefix
                if pattern.startswith("^") and not any(
                    c in pattern for c in ["$", ".", "*", "+", "?", "[", "(", "|"]
                ):
                    prefix = pattern[1:]  # Remove leading ^
                    prefixes.append(prefix)

            if prefixes:
                # Multiple prefixes: (repo:prefix1 OR repo:prefix2)
                qualifiers = [f"repo:{prefix}" for prefix in prefixes]
                if len(qualifiers) == 1:
                    return qualifiers[0]
                return f"({' OR '.join(qualifiers)})"

        return None
