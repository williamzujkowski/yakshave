"""Language filter for repository discovery."""

from typing import Any

from .base import BaseFilter, FilterResult


class LanguageFilter(BaseFilter):
    """Filter repositories based on primary language.

    Supports include/exclude lists for languages.
    Supports Search API qualifier: 'language:NAME' (for includes only)
    """

    name = "language"

    def is_enabled(self, config: Any) -> bool:
        """Language filter is enabled when config has language settings."""
        if not hasattr(config, "language"):
            return False
        return bool(config.language.include or config.language.exclude)

    def evaluate(self, repo: dict[str, Any], config: Any) -> FilterResult:
        """Evaluate repository language.

        Args:
            repo: Repository data dictionary.
            config: DiscoveryConfig object.

        Returns:
            FilterResult indicating pass/fail.
        """
        repo_language = repo.get("language")

        # Check exclude list first
        if config.language.exclude and repo_language in config.language.exclude:
            return FilterResult(
                passed=False,
                reason=f"Repository language {repo_language} is excluded",
                filter_name=self.name,
            )

        # Check include list (if specified)
        if config.language.include:
            if not repo_language:
                return FilterResult(
                    passed=False,
                    reason="Repository has no language, but language filter requires specific languages",
                    filter_name=self.name,
                )
            if repo_language not in config.language.include:
                return FilterResult(
                    passed=False,
                    reason=f"Repository language {repo_language} not in include list",
                    filter_name=self.name,
                )

        return FilterResult(passed=True, filter_name=self.name)

    def get_search_qualifier(self, config: Any) -> str | None:
        """Return Search API qualifier for language.

        Note: Only supports include list (multiple languages via OR).
        Exclude lists cannot be expressed in Search API syntax.
        """
        if not self.is_enabled(config):
            return None

        # Only generate qualifier for include list
        if config.language.include:
            # Multiple languages: language:Python OR language:JavaScript
            qualifiers = [f"language:{lang}" for lang in config.language.include]
            if len(qualifiers) == 1:
                return qualifiers[0]
            return f"({' OR '.join(qualifiers)})"

        return None
