"""Topics filter for repository discovery."""

from typing import Any

from .base import BaseFilter, FilterResult


class TopicsFilter(BaseFilter):
    """Filter repositories based on topics.

    Supports:
    - require_any: At least one of these topics must be present
    - require_all: All of these topics must be present
    - exclude: None of these topics can be present

    Supports Search API qualifier: 'topic:NAME' for require_any/require_all
    """

    name = "topics"

    def is_enabled(self, config: Any) -> bool:
        """Topics filter is enabled when config has topics settings."""
        if not hasattr(config, "topics"):
            return False
        return bool(config.topics.require_any or config.topics.require_all or config.topics.exclude)

    def evaluate(self, repo: dict[str, Any], config: Any) -> FilterResult:
        """Evaluate repository topics.

        Args:
            repo: Repository data dictionary.
            config: DiscoveryConfig object.

        Returns:
            FilterResult indicating pass/fail.
        """
        repo_topics = set(repo.get("topics", []))

        # Check exclude list first (hard rejection)
        if config.topics.exclude:
            excluded_found = repo_topics & set(config.topics.exclude)
            if excluded_found:
                return FilterResult(
                    passed=False,
                    reason=f"Repository has excluded topics: {', '.join(excluded_found)}",
                    filter_name=self.name,
                )

        # Check require_all (all topics must be present)
        if config.topics.require_all:
            required_set = set(config.topics.require_all)
            missing = required_set - repo_topics
            if missing:
                return FilterResult(
                    passed=False,
                    reason=f"Repository missing required topics: {', '.join(missing)}",
                    filter_name=self.name,
                )

        # Check require_any (at least one topic must be present)
        if config.topics.require_any:
            required_any_set = set(config.topics.require_any)
            if not repo_topics & required_any_set:
                return FilterResult(
                    passed=False,
                    reason=f"Repository must have at least one of: {', '.join(config.topics.require_any)}",
                    filter_name=self.name,
                )

        return FilterResult(passed=True, filter_name=self.name)

    def get_search_qualifier(self, config: Any) -> str | None:
        """Return Search API qualifier for topics.

        Note: Only supports require_any and require_all.
        Exclude lists cannot be expressed in Search API syntax.
        """
        if not self.is_enabled(config):
            return None

        qualifiers = []

        # Add require_all topics (AND)
        if config.topics.require_all:
            qualifiers.extend([f"topic:{topic}" for topic in config.topics.require_all])

        # Add require_any topics (OR)
        if config.topics.require_any:
            any_qualifiers = [f"topic:{topic}" for topic in config.topics.require_any]
            if len(any_qualifiers) == 1:
                qualifiers.append(any_qualifiers[0])
            else:
                qualifiers.append(f"({' OR '.join(any_qualifiers)})")

        if not qualifiers:
            return None

        if len(qualifiers) == 1:
            return qualifiers[0]

        return " ".join(qualifiers)
