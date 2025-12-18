"""Data collectors for GitHub API endpoints."""

from gh_year_end.collect.comments import (
    CommentCollectionError,
    collect_issue_comments,
    collect_review_comments,
    read_issue_numbers,
    read_pr_numbers,
)
from gh_year_end.collect.commits import CommitCollectionError, collect_commits
from gh_year_end.collect.discovery import DiscoveryError, discover_repos
from gh_year_end.collect.hygiene import (
    HygieneCollectionError,
    collect_branch_protection,
    collect_repo_hygiene,
    collect_security_features,
)
from gh_year_end.collect.issues import collect_issues
from gh_year_end.collect.orchestrator import CollectionError, run_collection
from gh_year_end.collect.pulls import PullsCollectorError, collect_pulls
from gh_year_end.collect.repos import RepoMetadataError, collect_repo_metadata
from gh_year_end.collect.reviews import (
    ReviewCollectionStats,
    collect_reviews,
    collect_reviews_from_pr_iterator,
)

__all__ = [
    "CollectionError",
    "CommentCollectionError",
    "CommitCollectionError",
    "DiscoveryError",
    "HygieneCollectionError",
    "PullsCollectorError",
    "RepoMetadataError",
    "ReviewCollectionStats",
    "collect_branch_protection",
    "collect_commits",
    "collect_issue_comments",
    "collect_issues",
    "collect_pulls",
    "collect_repo_hygiene",
    "collect_repo_metadata",
    "collect_review_comments",
    "collect_reviews",
    "collect_reviews_from_pr_iterator",
    "collect_security_features",
    "discover_repos",
    "read_issue_numbers",
    "read_pr_numbers",
    "run_collection",
]
