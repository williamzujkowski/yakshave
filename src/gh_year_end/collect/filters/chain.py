"""Filter chain for coordinating repository discovery filters.

Provides composable filtering with statistics tracking and search query generation.
"""

import logging
from collections import defaultdict
from typing import Any

from gh_year_end.config import DiscoveryConfig

from .activity import ActivityFilter
from .archive import ArchiveFilter
from .base import BaseFilter, FilterResult
from .fork import ForkFilter
from .language import LanguageFilter
from .name_pattern import NamePatternFilter
from .size import SizeFilter
from .topics import TopicsFilter
from .visibility import VisibilityFilter

logger = logging.getLogger(__name__)


class FilterChain:
    """Composable filter chain for repository discovery.

    Coordinates multiple filters, tracks rejection statistics,
    and generates Search API queries.
    """

    def __init__(self, config: DiscoveryConfig) -> None:
        """Initialize filter chain from discovery config.

        Args:
            config: Discovery configuration with filter settings.
        """
        self.config = config
        self.stats: dict[str, int] = defaultdict(int)

        # Initialize all filters
        self.filters: list[BaseFilter] = [
            ForkFilter(),
            ArchiveFilter(),
            VisibilityFilter(),
            ActivityFilter(),
            SizeFilter(),
            LanguageFilter(),
            TopicsFilter(),
            NamePatternFilter(),
        ]

    def evaluate(self, repo: dict[str, Any]) -> FilterResult:
        """Evaluate all enabled filters for a repository.

        Short-circuits on first failure.

        Args:
            repo: Repository data from GitHub API.

        Returns:
            FilterResult with pass/fail and rejection reason.
        """
        for filter_obj in self.filters:
            if not filter_obj.is_enabled(self.config):
                continue

            result = filter_obj.evaluate(repo, self.config)
            if not result.passed:
                return result

        return FilterResult(passed=True, filter_name="none")

    def get_search_query(self, org_or_user: str, mode: str = "org") -> str:
        """Build GitHub Search API query from filter configuration.

        Args:
            org_or_user: Organization or user name.
            mode: "org" or "user".

        Returns:
            Search query string for /search/repositories endpoint.
        """
        query_parts = []

        # Target
        if mode == "org":
            query_parts.append(f"org:{org_or_user}")
        else:
            query_parts.append(f"user:{org_or_user}")

        # Add qualifiers from enabled filters
        for filter_obj in self.filters:
            if not filter_obj.is_enabled(self.config):
                continue

            qualifier = filter_obj.get_search_qualifier(self.config)
            if qualifier:
                query_parts.append(qualifier)

        return " ".join(query_parts)

    def record_rejection(self, filter_name: str) -> None:
        """Record a filter rejection for statistics.

        Args:
            filter_name: Name of the filter that rejected the repo.
        """
        self.stats[filter_name] += 1

    def get_stats(self) -> dict[str, int]:
        """Get filter rejection statistics.

        Returns:
            Dictionary mapping filter names to rejection counts.
        """
        return dict(self.stats)
