"""Data collectors for GitHub API endpoints."""

from gh_year_end.collect.discovery import DiscoveryError, discover_repos

__all__ = [
    "DiscoveryError",
    "discover_repos",
]
