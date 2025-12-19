"""Repository discovery filters.

Provides composable filter chain for repository discovery with support
for activity, size, language, topics, name patterns, and more.
"""

from gh_year_end.collect.filters.activity import ActivityFilter
from gh_year_end.collect.filters.archive import ArchiveFilter
from gh_year_end.collect.filters.base import BaseFilter, FilterResult
from gh_year_end.collect.filters.chain import FilterChain
from gh_year_end.collect.filters.fork import ForkFilter
from gh_year_end.collect.filters.language import LanguageFilter
from gh_year_end.collect.filters.name_pattern import NamePatternFilter
from gh_year_end.collect.filters.size import SizeFilter
from gh_year_end.collect.filters.topics import TopicsFilter
from gh_year_end.collect.filters.visibility import VisibilityFilter

__all__ = [
    "ActivityFilter",
    "ArchiveFilter",
    "BaseFilter",
    "FilterChain",
    "FilterResult",
    "ForkFilter",
    "LanguageFilter",
    "NamePatternFilter",
    "SizeFilter",
    "TopicsFilter",
    "VisibilityFilter",
]
