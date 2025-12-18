"""Normalizers to convert raw data to curated Parquet tables.

This module provides functions to normalize raw JSONL data into curated
Parquet tables with consistent schemas, bot detection, and identity resolution.

Each normalizer module handles specific tables:
- users: dim_user, dim_identity_rule
- repos: dim_repo
- pulls: fact_pull_request
- issues: fact_issue
- reviews: fact_review
- comments: fact_issue_comment, fact_review_comment
- commits: fact_commit, fact_commit_file
- hygiene: fact_repo_files_presence, fact_repo_hygiene, fact_repo_security_features
"""

# Import only modules that are ready (converted to polars)
from gh_year_end.normalize.identity import BotDetectionResult, BotDetector
from gh_year_end.normalize.users import normalize_identity_rules, normalize_users

__all__ = [
    "BotDetectionResult",
    "BotDetector",
    "normalize_identity_rules",
    "normalize_users",
]
