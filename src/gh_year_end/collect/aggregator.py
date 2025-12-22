"""In-memory metrics aggregation during collection.

This module provides a single-pass aggregator that computes all metrics
during collection, eliminating the need for separate normalize and metrics phases.
"""

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

BOT_PATTERNS = [
    "[bot]",
    "-bot-",
    "github-actions",
    "dependabot",
    "renovate",
    "greenkeeper",
    "snyk-bot",
    "codecov",
    "mergify",
    "imgbot",
    "allcontributors",
    "semantic-release-bot",
    "stale",
]


@dataclass
class MetricsAggregator:
    """Aggregate metrics in-memory during single-pass collection."""

    year: int
    target_name: str
    target_mode: str = "user"  # or "org"

    # Leaderboards: metric_key -> user_id -> count
    leaderboards: defaultdict[str, defaultdict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(int))
    )

    # Time series: period_metric -> list of {date, user_id, count}
    timeseries: defaultdict[str, list[dict[str, Any]]] = field(
        default_factory=lambda: defaultdict(list)
    )

    # Repo health: repo_id -> health metrics
    repo_health: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Hygiene: repo_id -> hygiene data
    hygiene: dict[str, dict[str, Any]] = field(default_factory=dict)

    # User cache: user_id -> user info
    users: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Repo cache: repo_id -> repo info
    repos: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Internal tracking for time series aggregation
    _weekly_counters: defaultdict[str, defaultdict[str, defaultdict[str, int]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    )
    _monthly_counters: defaultdict[str, defaultdict[str, defaultdict[str, int]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    )

    # Track contributors for new contributor detection
    _all_contributors_ever: set[str] = field(default_factory=set)
    _new_contributors_this_year: set[str] = field(default_factory=set)

    def _is_bot(self, user: dict[str, Any] | None) -> bool:
        """Check if user is a bot.

        Args:
            user: GitHub user object with at least 'login' and optionally 'type'

        Returns:
            True if user is detected as a bot
        """
        if not user:
            return True

        login = user.get("login", "").lower()
        user_type = user.get("type", "")

        # Check GitHub user type
        if user_type == "Bot":
            return True

        # Check login patterns
        return any(pattern.lower() in login for pattern in BOT_PATTERNS)

    def _cache_user(self, user: dict[str, Any] | None) -> None:
        """Cache user info for later enrichment.

        Args:
            user: GitHub user object
        """
        if not user:
            return

        login = user.get("login")
        if not login:
            return

        # Only cache if not already present
        if login not in self.users:
            self.users[login] = {
                "login": login,
                "avatar_url": user.get("avatar_url", ""),
                "is_bot": self._is_bot(user),
                "type": user.get("type", "User"),
            }

    def _cache_repo(self, repo: dict[str, Any]) -> None:
        """Cache repo info for later enrichment.

        Args:
            repo: GitHub repository object
        """
        full_name = repo.get("full_name")
        if not full_name:
            return

        if full_name not in self.repos:
            self.repos[full_name] = {
                "full_name": full_name,
                "name": repo.get("name", ""),
                "description": repo.get("description", ""),
                "html_url": repo.get("html_url", ""),
                "language": repo.get("language"),
                "stargazers_count": repo.get("stargazers_count", 0),
                "forks_count": repo.get("forks_count", 0),
                "archived": repo.get("archived", False),
                "private": repo.get("private", False),
            }

    def _get_week_key(self, dt: datetime) -> str:
        """Get week bucket key (YYYY-WNN).

        Args:
            dt: Datetime to bucket

        Returns:
            Week key like "2024-W01"
        """
        year, week, _ = dt.isocalendar()
        return f"{year}-W{week:02d}"

    def _get_month_key(self, dt: datetime) -> str:
        """Get month bucket key (YYYY-MM).

        Args:
            dt: Datetime to bucket

        Returns:
            Month key like "2024-01"
        """
        return f"{dt.year}-{dt.month:02d}"

    def _track_contributor(self, user_login: str, event_dt: datetime) -> None:
        """Track contributor for new contributor detection.

        Args:
            user_login: User login to track
            event_dt: Datetime of the contribution event
        """
        # Only track contributions from the target year
        if event_dt.year != self.year:
            return

        # If this is the first time we've seen this contributor
        if user_login not in self._all_contributors_ever:
            self._all_contributors_ever.add(user_login)
            self._new_contributors_this_year.add(user_login)

    def _increment_timeseries(
        self, dt: datetime, metric: str, user_login: str | None = None
    ) -> None:
        """Increment time series counters.

        Args:
            dt: Timestamp of the event
            metric: Metric name (e.g., 'prs_opened')
            user_login: Optional user login for per-user tracking
        """
        week_key = self._get_week_key(dt)
        month_key = self._get_month_key(dt)

        # Weekly counters
        if user_login:
            self._weekly_counters[metric][week_key][user_login] += 1
        else:
            self._weekly_counters[metric][week_key]["_total"] += 1

        # Monthly counters
        if user_login:
            self._monthly_counters[metric][month_key][user_login] += 1
        else:
            self._monthly_counters[metric][month_key]["_total"] += 1

    def add_repo(self, repo: dict[str, Any]) -> None:
        """Add repository to tracking.

        Args:
            repo: GitHub repository object
        """
        self._cache_repo(repo)

        full_name = repo.get("full_name")
        if not full_name:
            return

        # Initialize repo health tracking
        if full_name not in self.repo_health:
            self.repo_health[full_name] = {
                "contributors": set(),
                "pr_count": 0,
                "issue_count": 0,
                "review_count": 0,
                "comment_count": 0,
                "prs_with_reviews": set(),  # Track PRs that received reviews
                "merge_times": [],  # Track merge times in hours
            }

    def add_pr(self, repo_id: str, pr: dict[str, Any]) -> None:
        """Update metrics when a PR is collected.

        Args:
            repo_id: Repository full name (owner/repo)
            pr: GitHub pull request object
        """
        author = pr.get("user")
        self._cache_user(author)

        if self._is_bot(author):
            return

        author_login = author.get("login") if author else None
        if not author_login:
            return

        # Track in repo health
        if repo_id in self.repo_health:
            self.repo_health[repo_id]["contributors"].add(author_login)
            self.repo_health[repo_id]["pr_count"] += 1

        # Increment leaderboards
        self.leaderboards["prs_opened"][author_login] += 1

        created_at = pr.get("created_at")
        if created_at:
            created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            if created_dt.year == self.year:
                self._increment_timeseries(created_dt, "prs_opened", author_login)

        # Track merged PRs
        if pr.get("merged_at"):
            self.leaderboards["prs_merged"][author_login] += 1
            merged_at = pr.get("merged_at")
            if merged_at and created_at:
                merged_dt = datetime.fromisoformat(merged_at.replace("Z", "+00:00"))
                if merged_dt.year == self.year:
                    self._increment_timeseries(merged_dt, "prs_merged", author_login)

                # Track merge time for repo health
                if repo_id in self.repo_health:
                    created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    time_to_merge_hours = (merged_dt - created_dt).total_seconds() / 3600
                    self.repo_health[repo_id]["merge_times"].append(time_to_merge_hours)

                # Track merge time for repo health
                if repo_id in self.repo_health:
                    created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    time_to_merge_hours = (merged_dt - created_dt).total_seconds() / 3600
                    self.repo_health[repo_id]["merge_times"].append(time_to_merge_hours)

    def add_issue(self, repo_id: str, issue: dict[str, Any]) -> None:
        """Update metrics when an issue is collected.

        Args:
            repo_id: Repository full name (owner/repo)
            issue: GitHub issue object
        """
        # Skip pull requests (they have 'pull_request' key)
        if "pull_request" in issue:
            return

        author = issue.get("user")
        self._cache_user(author)

        if self._is_bot(author):
            return

        author_login = author.get("login") if author else None
        if not author_login:
            return

        # Track in repo health
        if repo_id in self.repo_health:
            self.repo_health[repo_id]["contributors"].add(author_login)
            self.repo_health[repo_id]["issue_count"] += 1

        # Increment leaderboards
        self.leaderboards["issues_opened"][author_login] += 1

        created_at = issue.get("created_at")
        if created_at:
            created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            if created_dt.year == self.year:
                self._increment_timeseries(created_dt, "issues_opened", author_login)

        # Track closed issues
        if issue.get("state") == "closed" and issue.get("closed_at"):
            self.leaderboards["issues_closed"][author_login] += 1
            closed_at = issue.get("closed_at")
            if closed_at:
                closed_dt = datetime.fromisoformat(closed_at.replace("Z", "+00:00"))
                if closed_dt.year == self.year:
                    self._increment_timeseries(closed_dt, "issues_closed", author_login)

    def add_review(self, repo_id: str, pr_number: int, review: dict[str, Any]) -> None:
        """Update metrics when a review is collected.

        Args:
            repo_id: Repository full name (owner/repo)
            pr_number: Pull request number
            review: GitHub review object
        """
        reviewer = review.get("user")
        self._cache_user(reviewer)

        if self._is_bot(reviewer):
            return

        reviewer_login = reviewer.get("login") if reviewer else None
        if not reviewer_login:
            return

        # Track in repo health
        if repo_id in self.repo_health:
            self.repo_health[repo_id]["contributors"].add(reviewer_login)
            self.repo_health[repo_id]["review_count"] += 1
            # Track that this PR received a review
            self.repo_health[repo_id]["prs_with_reviews"].add(pr_number)
            # Track that this PR received a review
            self.repo_health[repo_id]["prs_with_reviews"].add(pr_number)

        # Increment leaderboards
        self.leaderboards["reviews_submitted"][reviewer_login] += 1

        state = review.get("state", "").upper()
        if state == "APPROVED":
            self.leaderboards["approvals"][reviewer_login] += 1
        elif state == "CHANGES_REQUESTED":
            self.leaderboards["changes_requested"][reviewer_login] += 1

        submitted_at = review.get("submitted_at")
        if submitted_at:
            submitted_dt = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))
            if submitted_dt.year == self.year:
                self._increment_timeseries(submitted_dt, "reviews_submitted", reviewer_login)

    def add_comment(
        self, repo_id: str, comment: dict[str, Any], comment_type: str = "issue"
    ) -> None:
        """Update metrics when a comment is collected.

        Args:
            repo_id: Repository full name (owner/repo)
            comment: GitHub comment object
            comment_type: Type of comment ('issue', 'pr', 'review')
        """
        author = comment.get("user")
        self._cache_user(author)

        if self._is_bot(author):
            return

        author_login = author.get("login") if author else None
        if not author_login:
            return

        # Track in repo health
        if repo_id in self.repo_health:
            self.repo_health[repo_id]["contributors"].add(author_login)
            self.repo_health[repo_id]["comment_count"] += 1

        # Increment leaderboards
        self.leaderboards["comments_total"][author_login] += 1

        if comment_type in ("pr", "review"):
            self.leaderboards["review_comments_total"][author_login] += 1

        created_at = comment.get("created_at")
        if created_at:
            created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            if created_dt.year == self.year:
                self._increment_timeseries(created_dt, "comments_total", author_login)

    def set_hygiene(self, repo_id: str, hygiene_data: dict[str, Any]) -> None:
        """Set hygiene data for a repository.

        Args:
            repo_id: Repository full name (owner/repo)
            hygiene_data: Hygiene check results
        """
        self.hygiene[repo_id] = hygiene_data

    def compute_repo_health(self, repo_id: str) -> dict[str, Any]:
        """Compute health metrics for a repository.

        Args:
            repo_id: Repository full name (owner/repo)

        Returns:
            Health metrics dict
        """
        if repo_id not in self.repo_health:
            return {}

        health = self.repo_health[repo_id]

        # Calculate review coverage
        pr_count = health["pr_count"]
        prs_with_reviews_count = len(health["prs_with_reviews"])
        review_coverage = (prs_with_reviews_count / pr_count * 100) if pr_count > 0 else 0.0

        # Calculate median time to merge
        merge_times = health["merge_times"]
        median_time_to_merge = None
        if merge_times:
            sorted_times = sorted(merge_times)
            mid = len(sorted_times) // 2
            if len(sorted_times) % 2 == 0:
                median_time_to_merge = (sorted_times[mid - 1] + sorted_times[mid]) / 2
            else:
                median_time_to_merge = sorted_times[mid]

        return {
            "repo": repo_id,
            "contributor_count": len(health["contributors"]),
            "pr_count": pr_count,
            "issue_count": health["issue_count"],
            "review_count": health["review_count"],
            "comment_count": health["comment_count"],
            "review_coverage": round(review_coverage, 1),
            "median_time_to_merge": (
                round(median_time_to_merge, 1) if median_time_to_merge is not None else None
            ),
        }

    def _compute_summary(self) -> dict[str, Any]:
        """Compute summary statistics.

        Returns:
            Summary dict matching summary.json format
        """
        total_prs = sum(self.leaderboards["prs_opened"].values())
        total_issues = sum(self.leaderboards["issues_opened"].values())
        total_reviews = sum(self.leaderboards["reviews_submitted"].values())
        total_comments = sum(self.leaderboards["comments_total"].values())

        # Count unique contributors
        all_contributors = set()
        for user_login in self.leaderboards["prs_opened"]:
            all_contributors.add(user_login)
        for user_login in self.leaderboards["issues_opened"]:
            all_contributors.add(user_login)
        for user_login in self.leaderboards["reviews_submitted"]:
            all_contributors.add(user_login)

        return {
            "year": self.year,
            "target_name": self.target_name,
            "target_mode": self.target_mode,
            "total_repos": len(self.repos),
            "total_contributors": len(all_contributors),
            "total_prs": total_prs,
            "total_issues": total_issues,
            "total_reviews": total_reviews,
            "total_comments": total_comments,
            "prs_merged": sum(self.leaderboards["prs_merged"].values()),
            "issues_closed": sum(self.leaderboards["issues_closed"].values()),
            "new_contributors": len(self._new_contributors_this_year),
        }

    def _compute_leaderboards(self) -> dict[str, Any]:
        """Compute ranked leaderboards.

        Returns:
            Leaderboards dict matching leaderboards.json format
        """
        result = {}

        for metric, user_counts in self.leaderboards.items():
            # Filter out bots and sort by count
            ranked = [
                {
                    "user": user_login,
                    "count": count,
                    "avatar_url": self.users.get(user_login, {}).get("avatar_url", ""),
                }
                for user_login, count in user_counts.items()
                if not self.users.get(user_login, {}).get("is_bot", False)
            ]
            ranked.sort(key=lambda x: x["count"], reverse=True)

            result[metric] = ranked

        return result

    def _compute_timeseries(self) -> dict[str, Any]:
        """Compute time series data.

        Returns:
            Time series dict matching timeseries.json format
        """
        result: dict[str, dict[str, list[dict[str, Any]]]] = {
            "weekly": {},
            "monthly": {},
        }

        # Process weekly data
        for metric, periods in self._weekly_counters.items():
            result["weekly"][metric] = []
            for period, user_counts in sorted(periods.items()):
                for user_login, count in user_counts.items():
                    if user_login == "_total":
                        continue
                    if not self.users.get(user_login, {}).get("is_bot", False):
                        result["weekly"][metric].append(
                            {
                                "period": period,
                                "user": user_login,
                                "count": count,
                            }
                        )

        # Process monthly data
        for metric, periods in self._monthly_counters.items():
            result["monthly"][metric] = []
            for period, user_counts in sorted(periods.items()):
                for user_login, count in user_counts.items():
                    if user_login == "_total":
                        continue
                    if not self.users.get(user_login, {}).get("is_bot", False):
                        result["monthly"][metric].append(
                            {
                                "period": period,
                                "user": user_login,
                                "count": count,
                            }
                        )

        return result

    def _compute_awards(self) -> dict[str, Any]:
        """Compute awards based on metrics.

        Returns:
            Awards dict matching awards.json format
        """
        awards: dict[str, Any] = {}

        # Top PR author
        if self.leaderboards["prs_opened"]:
            top_pr = max(
                [
                    (user, count)
                    for user, count in self.leaderboards["prs_opened"].items()
                    if not self.users.get(user, {}).get("is_bot", False)
                ],
                key=lambda x: x[1],
                default=(None, 0),
            )
            if top_pr[0]:
                awards["top_pr_author"] = {
                    "user": top_pr[0],
                    "count": top_pr[1],
                    "avatar_url": self.users.get(top_pr[0], {}).get("avatar_url", ""),
                }

        # Top reviewer
        if self.leaderboards["reviews_submitted"]:
            top_reviewer = max(
                [
                    (user, count)
                    for user, count in self.leaderboards["reviews_submitted"].items()
                    if not self.users.get(user, {}).get("is_bot", False)
                ],
                key=lambda x: x[1],
                default=(None, 0),
            )
            if top_reviewer[0]:
                awards["top_reviewer"] = {
                    "user": top_reviewer[0],
                    "count": top_reviewer[1],
                    "avatar_url": self.users.get(top_reviewer[0], {}).get("avatar_url", ""),
                }

        # Top issue opener
        if self.leaderboards["issues_opened"]:
            top_issue = max(
                [
                    (user, count)
                    for user, count in self.leaderboards["issues_opened"].items()
                    if not self.users.get(user, {}).get("is_bot", False)
                ],
                key=lambda x: x[1],
                default=(None, 0),
            )
            if top_issue[0]:
                awards["top_issue_opener"] = {
                    "user": top_issue[0],
                    "count": top_issue[1],
                    "avatar_url": self.users.get(top_issue[0], {}).get("avatar_url", ""),
                }

        return awards

    def export(self) -> dict[str, Any]:
        """Export all metrics as JSON-serializable dict.

        Returns:
            Dict containing all metrics in the format expected by the website:
            {
                'summary': {...},
                'leaderboards': {...},
                'timeseries': {...},
                'repo_health': [...],
                'hygiene_scores': {...},
                'awards': {...}
            }
        """
        # Convert repo_health sets to lists for JSON serialization
        repo_health_list = []
        for repo_id in sorted(self.repo_health.keys()):
            health = self.compute_repo_health(repo_id)
            if health:
                repo_health_list.append(health)

        return {
            "summary": self._compute_summary(),
            "leaderboards": self._compute_leaderboards(),
            "timeseries": self._compute_timeseries(),
            "repo_health": repo_health_list,
            "hygiene_scores": self.hygiene,
            "awards": self._compute_awards(),
        }
