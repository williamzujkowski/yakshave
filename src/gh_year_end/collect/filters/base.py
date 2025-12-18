"""Base filter interface for repository filtering."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class FilterResult:
    """Result of a filter evaluation.

    Attributes:
        passed: Whether the repository passed the filter.
        reason: Optional reason for rejection (None if passed).
        filter_name: Name of the filter that produced this result.
    """

    passed: bool
    reason: str | None = None
    filter_name: str = ""


class BaseFilter(ABC):
    """Abstract base class for repository filters.

    All filters must implement:
    - is_enabled(): Check if the filter is enabled in config
    - evaluate(): Evaluate a repository against the filter
    - get_search_qualifier(): Optional Search API qualifier string
    """

    name: str = "base"

    @abstractmethod
    def is_enabled(self, config: Any) -> bool:
        """Check if this filter is enabled in the configuration.

        Args:
            config: DiscoveryConfig object.

        Returns:
            True if the filter should be applied.
        """

    @abstractmethod
    def evaluate(self, repo: dict[str, Any], config: Any) -> FilterResult:
        """Evaluate a repository against this filter.

        Args:
            repo: Repository data dictionary from GitHub API.
            config: DiscoveryConfig object.

        Returns:
            FilterResult indicating pass/fail with optional reason.
        """

    def get_search_qualifier(self, config: Any) -> str | None:  # noqa: ARG002
        """Get Search API qualifier string for this filter.

        This allows filters to express their criteria as GitHub Search API
        qualifiers, enabling quick_scan mode to pre-filter at the API level.

        Args:
            config: DiscoveryConfig object.

        Returns:
            Search API qualifier string, or None if not applicable.
        """
        return None
