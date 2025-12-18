"""Report view generators.

Provides specialized views for different audiences:
- Engineer view: Detailed technical metrics with full leaderboards
- Executive view: High-level summary for leadership
"""

from gh_year_end.report.views.engineer_view import (
    filter_by_metric,
    filter_by_repo,
    filter_by_user,
    generate_engineer_view,
)

__all__ = [
    "filter_by_metric",
    "filter_by_repo",
    "filter_by_user",
    "generate_engineer_view",
]
