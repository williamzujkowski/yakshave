"""Collection phases for GitHub data collection.

Each phase module handles a specific step in the collection pipeline.
"""

from gh_year_end.collect.phases.comments import run_comments_phase
from gh_year_end.collect.phases.commits import run_commits_phase
from gh_year_end.collect.phases.discovery import run_discovery_phase
from gh_year_end.collect.phases.hygiene import (
    run_branch_protection_phase,
    run_security_features_phase,
)
from gh_year_end.collect.phases.issues import run_issues_phase
from gh_year_end.collect.phases.pulls import run_pulls_phase
from gh_year_end.collect.phases.repos import run_repo_metadata_phase
from gh_year_end.collect.phases.reviews import run_reviews_phase

__all__ = [
    "run_branch_protection_phase",
    "run_comments_phase",
    "run_commits_phase",
    "run_discovery_phase",
    "run_issues_phase",
    "run_pulls_phase",
    "run_repo_metadata_phase",
    "run_reviews_phase",
    "run_security_features_phase",
]
