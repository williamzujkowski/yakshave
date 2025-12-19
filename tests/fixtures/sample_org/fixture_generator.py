"""Generate deterministic sample test data for end-to-end testing.

This module creates realistic GitHub data with:
- 5 repositories with varied activity
- 8 users (1 bot, 7 humans)
- 20 pull requests with mixed states
- 10 issues with varied engagement
- 15 reviews across PRs
- Comments on PRs and issues
- Commits associated with PRs
- Hygiene data for repos
"""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


def generate_envelope(
    data: dict[str, Any],
    endpoint: str,
    timestamp: str | None = None,
    page: int = 1,
) -> dict[str, Any]:
    """Generate JSONL envelope structure.

    Args:
        data: The payload data.
        endpoint: API endpoint path.
        timestamp: ISO8601 timestamp (defaults to 2025-01-15T00:00:00Z).
        page: Page number for pagination.

    Returns:
        Enveloped record with metadata.
    """
    return {
        "timestamp": timestamp or "2025-01-15T00:00:00Z",
        "source": "github_rest",
        "endpoint": endpoint,
        "request_id": f"test-{endpoint.replace('/', '-')}-{page}",
        "page": page,
        "data": data,
    }


def generate_users() -> list[dict[str, Any]]:
    """Generate sample users (7 humans + 1 bot)."""
    return [
        {
            "node_id": "U_alice",
            "login": "alice",
            "type": "User",
            "html_url": "https://github.com/alice",
            "name": "Alice Smith",
        },
        {
            "node_id": "U_bob",
            "login": "bob",
            "type": "User",
            "html_url": "https://github.com/bob",
            "name": "Bob Johnson",
        },
        {
            "node_id": "U_charlie",
            "login": "charlie",
            "type": "User",
            "html_url": "https://github.com/charlie",
            "name": "Charlie Davis",
        },
        {
            "node_id": "U_diana",
            "login": "diana",
            "type": "User",
            "html_url": "https://github.com/diana",
            "name": "Diana Martinez",
        },
        {
            "node_id": "U_eve",
            "login": "eve",
            "type": "User",
            "html_url": "https://github.com/eve",
            "name": "Eve Wilson",
        },
        {
            "node_id": "U_frank",
            "login": "frank",
            "type": "User",
            "html_url": "https://github.com/frank",
            "name": "Frank Brown",
        },
        {
            "node_id": "U_grace",
            "login": "grace",
            "type": "User",
            "html_url": "https://github.com/grace",
            "name": "Grace Taylor",
        },
        {
            "node_id": "U_dependabot",
            "login": "dependabot[bot]",
            "type": "Bot",
            "html_url": "https://github.com/apps/dependabot",
            "name": None,
        },
    ]


def generate_repos() -> list[dict[str, Any]]:
    """Generate sample repositories (5 repos with varied characteristics)."""
    base_date = datetime(2024, 1, 1, tzinfo=UTC)

    return [
        {
            "node_id": "R_001",
            "name": "backend-api",
            "full_name": "test-org/backend-api",
            "owner": {"login": "test-org", "type": "Organization"},
            "archived": False,
            "fork": False,
            "private": False,
            "default_branch": "main",
            "stargazers_count": 145,
            "forks_count": 23,
            "watchers_count": 145,
            "topics": ["python", "api", "backend"],
            "language": "Python",
            "created_at": (base_date - timedelta(days=365)).isoformat(),
            "pushed_at": (base_date + timedelta(days=350)).isoformat(),
            "size": 2048,
        },
        {
            "node_id": "R_002",
            "name": "frontend-web",
            "full_name": "test-org/frontend-web",
            "owner": {"login": "test-org", "type": "Organization"},
            "archived": False,
            "fork": False,
            "private": False,
            "default_branch": "main",
            "stargazers_count": 89,
            "forks_count": 12,
            "watchers_count": 89,
            "topics": ["javascript", "react", "frontend"],
            "language": "JavaScript",
            "created_at": (base_date - timedelta(days=300)).isoformat(),
            "pushed_at": (base_date + timedelta(days=360)).isoformat(),
            "size": 1536,
        },
        {
            "node_id": "R_003",
            "name": "mobile-app",
            "full_name": "test-org/mobile-app",
            "owner": {"login": "test-org", "type": "Organization"},
            "archived": False,
            "fork": False,
            "private": False,
            "default_branch": "develop",
            "stargazers_count": 67,
            "forks_count": 8,
            "watchers_count": 67,
            "topics": ["swift", "ios", "mobile"],
            "language": "Swift",
            "created_at": (base_date - timedelta(days=200)).isoformat(),
            "pushed_at": (base_date + timedelta(days=340)).isoformat(),
            "size": 1024,
        },
        {
            "node_id": "R_004",
            "name": "data-pipeline",
            "full_name": "test-org/data-pipeline",
            "owner": {"login": "test-org", "type": "Organization"},
            "archived": False,
            "fork": False,
            "private": False,
            "default_branch": "main",
            "stargazers_count": 34,
            "forks_count": 5,
            "watchers_count": 34,
            "topics": ["python", "data", "etl"],
            "language": "Python",
            "created_at": (base_date - timedelta(days=180)).isoformat(),
            "pushed_at": (base_date + timedelta(days=330)).isoformat(),
            "size": 768,
        },
        {
            "node_id": "R_005",
            "name": "docs-site",
            "full_name": "test-org/docs-site",
            "owner": {"login": "test-org", "type": "Organization"},
            "archived": False,
            "fork": False,
            "private": False,
            "default_branch": "main",
            "stargazers_count": 12,
            "forks_count": 2,
            "watchers_count": 12,
            "topics": ["documentation", "markdown"],
            "language": "HTML",
            "created_at": (base_date - timedelta(days=150)).isoformat(),
            "pushed_at": (base_date + timedelta(days=320)).isoformat(),
            "size": 256,
        },
    ]


def generate_pulls(
    repos: list[dict[str, Any]], users: list[dict[str, Any]]
) -> dict[str, list[dict[str, Any]]]:
    """Generate sample pull requests (20 PRs across repos).

    Returns:
        Dictionary mapping repo slug to list of PRs.
    """
    base_date = datetime(2025, 1, 1, tzinfo=UTC)

    # Distribution: backend-api (8), frontend-web (6), mobile-app (3), data-pipeline (2), docs-site (1)
    pulls: dict[str, list[dict[str, Any]]] = {}

    # Backend API PRs (8 total)
    pulls["test-org__backend-api"] = [
        {
            "node_id": "PR_001",
            "number": 101,
            "state": "closed",
            "draft": False,
            "title": "Add user authentication endpoint",
            "body": "Implements JWT-based authentication for user login and registration.",
            "user": {"node_id": "U_alice", "login": "alice"},
            "base": {"repo": {"node_id": "R_001", "full_name": "test-org/backend-api"}},
            "created_at": (base_date + timedelta(days=5)).isoformat(),
            "updated_at": (base_date + timedelta(days=8)).isoformat(),
            "closed_at": (base_date + timedelta(days=8)).isoformat(),
            "merged_at": (base_date + timedelta(days=8)).isoformat(),
            "labels": [{"name": "feature"}, {"name": "security"}],
            "milestone": {"title": "v2.0"},
            "additions": 450,
            "deletions": 120,
            "changed_files": 12,
        },
        {
            "node_id": "PR_002",
            "number": 102,
            "state": "closed",
            "draft": False,
            "title": "Fix memory leak in cache handler",
            "body": "Resolves issue #45 by properly closing database connections.",
            "user": {"node_id": "U_bob", "login": "bob"},
            "base": {"repo": {"node_id": "R_001", "full_name": "test-org/backend-api"}},
            "created_at": (base_date + timedelta(days=10)).isoformat(),
            "updated_at": (base_date + timedelta(days=12)).isoformat(),
            "closed_at": (base_date + timedelta(days=12)).isoformat(),
            "merged_at": (base_date + timedelta(days=12)).isoformat(),
            "labels": [{"name": "bug"}, {"name": "priority:high"}],
            "milestone": None,
            "additions": 80,
            "deletions": 45,
            "changed_files": 3,
        },
        {
            "node_id": "PR_003",
            "number": 103,
            "state": "open",
            "draft": False,
            "title": "Add rate limiting middleware",
            "body": "Implements token bucket algorithm for API rate limiting.",
            "user": {"node_id": "U_charlie", "login": "charlie"},
            "base": {"repo": {"node_id": "R_001", "full_name": "test-org/backend-api"}},
            "created_at": (base_date + timedelta(days=15)).isoformat(),
            "updated_at": (base_date + timedelta(days=18)).isoformat(),
            "closed_at": None,
            "merged_at": None,
            "labels": [{"name": "feature"}],
            "milestone": {"title": "v2.1"},
            "additions": 320,
            "deletions": 25,
            "changed_files": 7,
        },
        {
            "node_id": "PR_004",
            "number": 104,
            "state": "closed",
            "draft": False,
            "title": "Update dependencies",
            "body": "Bumps dependencies to latest stable versions.",
            "user": {"node_id": "U_dependabot", "login": "dependabot[bot]"},
            "base": {"repo": {"node_id": "R_001", "full_name": "test-org/backend-api"}},
            "created_at": (base_date + timedelta(days=20)).isoformat(),
            "updated_at": (base_date + timedelta(days=21)).isoformat(),
            "closed_at": (base_date + timedelta(days=21)).isoformat(),
            "merged_at": (base_date + timedelta(days=21)).isoformat(),
            "labels": [{"name": "dependencies"}],
            "milestone": None,
            "additions": 15,
            "deletions": 15,
            "changed_files": 2,
        },
        {
            "node_id": "PR_005",
            "number": 105,
            "state": "closed",
            "draft": False,
            "title": "Add API documentation",
            "body": "Comprehensive OpenAPI/Swagger documentation for all endpoints.",
            "user": {"node_id": "U_diana", "login": "diana"},
            "base": {"repo": {"node_id": "R_001", "full_name": "test-org/backend-api"}},
            "created_at": (base_date + timedelta(days=25)).isoformat(),
            "updated_at": (base_date + timedelta(days=28)).isoformat(),
            "closed_at": (base_date + timedelta(days=28)).isoformat(),
            "merged_at": (base_date + timedelta(days=28)).isoformat(),
            "labels": [{"name": "documentation"}],
            "milestone": {"title": "v2.0"},
            "additions": 890,
            "deletions": 50,
            "changed_files": 15,
        },
        {
            "node_id": "PR_006",
            "number": 106,
            "state": "closed",
            "draft": False,
            "title": "Optimize database queries",
            "body": "Adds indexes and improves query performance by 40%.",
            "user": {"node_id": "U_eve", "login": "eve"},
            "base": {"repo": {"node_id": "R_001", "full_name": "test-org/backend-api"}},
            "created_at": (base_date + timedelta(days=30)).isoformat(),
            "updated_at": (base_date + timedelta(days=33)).isoformat(),
            "closed_at": (base_date + timedelta(days=33)).isoformat(),
            "merged_at": (base_date + timedelta(days=33)).isoformat(),
            "labels": [{"name": "performance"}],
            "milestone": {"title": "v2.1"},
            "additions": 210,
            "deletions": 180,
            "changed_files": 8,
        },
        {
            "node_id": "PR_007",
            "number": 107,
            "state": "closed",
            "draft": False,
            "title": "Rejected: Alternative caching approach",
            "body": "This PR proposed a different caching strategy but was rejected in favor of PR_002.",
            "user": {"node_id": "U_frank", "login": "frank"},
            "base": {"repo": {"node_id": "R_001", "full_name": "test-org/backend-api"}},
            "created_at": (base_date + timedelta(days=9)).isoformat(),
            "updated_at": (base_date + timedelta(days=11)).isoformat(),
            "closed_at": (base_date + timedelta(days=11)).isoformat(),
            "merged_at": None,  # Closed but NOT merged - rejected PR
            "labels": [{"name": "wontfix"}],
            "milestone": None,
            "additions": 250,
            "deletions": 40,
            "changed_files": 5,
        },
        {
            "node_id": "PR_008",
            "number": 108,
            "state": "closed",
            "draft": False,
            "title": "Add health check endpoint",
            "body": "Implements /health endpoint for load balancer monitoring.",
            "user": {"node_id": "U_grace", "login": "grace"},
            "base": {"repo": {"node_id": "R_001", "full_name": "test-org/backend-api"}},
            "created_at": (base_date + timedelta(days=40)).isoformat(),
            "updated_at": (base_date + timedelta(days=41)).isoformat(),
            "closed_at": (base_date + timedelta(days=41)).isoformat(),
            "merged_at": (base_date + timedelta(days=41)).isoformat(),
            "labels": [{"name": "feature"}, {"name": "ops"}],
            "milestone": {"title": "v2.1"},
            "additions": 95,
            "deletions": 10,
            "changed_files": 4,
        },
        {
            "node_id": "PR_021",
            "number": 109,
            "state": "open",
            "draft": True,
            "title": "WIP: Implement GraphQL API",
            "body": "Work in progress for GraphQL endpoint support.",
            "user": {"node_id": "U_frank", "login": "frank"},
            "base": {"repo": {"node_id": "R_001", "full_name": "test-org/backend-api"}},
            "created_at": (base_date + timedelta(days=35)).isoformat(),
            "updated_at": (base_date + timedelta(days=36)).isoformat(),
            "closed_at": None,
            "merged_at": None,
            "labels": [{"name": "feature"}, {"name": "wip"}],
            "milestone": {"title": "v3.0"},
            "additions": 560,
            "deletions": 30,
            "changed_files": 18,
        },
    ]

    # Frontend Web PRs (6 total)
    pulls["test-org__frontend-web"] = [
        {
            "node_id": "PR_009",
            "number": 201,
            "state": "closed",
            "draft": False,
            "title": "Redesign login page",
            "body": "New responsive design for login/signup pages.",
            "user": {"node_id": "U_alice", "login": "alice"},
            "base": {"repo": {"node_id": "R_002", "full_name": "test-org/frontend-web"}},
            "created_at": (base_date + timedelta(days=7)).isoformat(),
            "updated_at": (base_date + timedelta(days=10)).isoformat(),
            "closed_at": (base_date + timedelta(days=10)).isoformat(),
            "merged_at": (base_date + timedelta(days=10)).isoformat(),
            "labels": [{"name": "ui"}, {"name": "enhancement"}],
            "milestone": {"title": "v1.5"},
            "additions": 670,
            "deletions": 340,
            "changed_files": 14,
        },
        {
            "node_id": "PR_010",
            "number": 202,
            "state": "closed",
            "draft": False,
            "title": "Fix navbar overflow on mobile",
            "body": "Resolves responsive layout issue on small screens.",
            "user": {"node_id": "U_bob", "login": "bob"},
            "base": {"repo": {"node_id": "R_002", "full_name": "test-org/frontend-web"}},
            "created_at": (base_date + timedelta(days=12)).isoformat(),
            "updated_at": (base_date + timedelta(days=13)).isoformat(),
            "closed_at": (base_date + timedelta(days=13)).isoformat(),
            "merged_at": (base_date + timedelta(days=13)).isoformat(),
            "labels": [{"name": "bug"}, {"name": "mobile"}],
            "milestone": None,
            "additions": 45,
            "deletions": 30,
            "changed_files": 2,
        },
        {
            "node_id": "PR_011",
            "number": 203,
            "state": "closed",
            "draft": False,
            "title": "Add dark mode support",
            "body": "Implements dark mode theme with automatic detection.",
            "user": {"node_id": "U_charlie", "login": "charlie"},
            "base": {"repo": {"node_id": "R_002", "full_name": "test-org/frontend-web"}},
            "created_at": (base_date + timedelta(days=18)).isoformat(),
            "updated_at": (base_date + timedelta(days=22)).isoformat(),
            "closed_at": (base_date + timedelta(days=22)).isoformat(),
            "merged_at": (base_date + timedelta(days=22)).isoformat(),
            "labels": [{"name": "feature"}, {"name": "ui"}],
            "milestone": {"title": "v1.5"},
            "additions": 520,
            "deletions": 180,
            "changed_files": 24,
        },
        {
            "node_id": "PR_012",
            "number": 204,
            "state": "open",
            "draft": False,
            "title": "Add user profile page",
            "body": "New profile page with editable user settings.",
            "user": {"node_id": "U_diana", "login": "diana"},
            "base": {"repo": {"node_id": "R_002", "full_name": "test-org/frontend-web"}},
            "created_at": (base_date + timedelta(days=27)).isoformat(),
            "updated_at": (base_date + timedelta(days=29)).isoformat(),
            "closed_at": None,
            "merged_at": None,
            "labels": [{"name": "feature"}],
            "milestone": {"title": "v1.6"},
            "additions": 430,
            "deletions": 20,
            "changed_files": 11,
        },
        {
            "node_id": "PR_013",
            "number": 205,
            "state": "closed",
            "draft": False,
            "title": "Upgrade React to v18",
            "body": "Updates React and related dependencies to latest version.",
            "user": {"node_id": "U_eve", "login": "eve"},
            "base": {"repo": {"node_id": "R_002", "full_name": "test-org/frontend-web"}},
            "created_at": (base_date + timedelta(days=32)).isoformat(),
            "updated_at": (base_date + timedelta(days=35)).isoformat(),
            "closed_at": (base_date + timedelta(days=35)).isoformat(),
            "merged_at": (base_date + timedelta(days=35)).isoformat(),
            "labels": [{"name": "dependencies"}, {"name": "maintenance"}],
            "milestone": {"title": "v1.5"},
            "additions": 125,
            "deletions": 95,
            "changed_files": 8,
        },
        {
            "node_id": "PR_014",
            "number": 206,
            "state": "closed",
            "draft": False,
            "title": "Fix form validation issues",
            "body": "Improves client-side form validation and error messages.",
            "user": {"node_id": "U_frank", "login": "frank"},
            "base": {"repo": {"node_id": "R_002", "full_name": "test-org/frontend-web"}},
            "created_at": (base_date + timedelta(days=38)).isoformat(),
            "updated_at": (base_date + timedelta(days=39)).isoformat(),
            "closed_at": (base_date + timedelta(days=39)).isoformat(),
            "merged_at": (base_date + timedelta(days=39)).isoformat(),
            "labels": [{"name": "bug"}, {"name": "ux"}],
            "milestone": {"title": "v1.5"},
            "additions": 180,
            "deletions": 120,
            "changed_files": 6,
        },
    ]

    # Mobile App PRs (3 total)
    pulls["test-org__mobile-app"] = [
        {
            "node_id": "PR_015",
            "number": 301,
            "state": "closed",
            "draft": False,
            "title": "Implement push notifications",
            "body": "Adds Firebase Cloud Messaging for push notifications.",
            "user": {"node_id": "U_alice", "login": "alice"},
            "base": {"repo": {"node_id": "R_003", "full_name": "test-org/mobile-app"}},
            "created_at": (base_date + timedelta(days=14)).isoformat(),
            "updated_at": (base_date + timedelta(days=19)).isoformat(),
            "closed_at": (base_date + timedelta(days=19)).isoformat(),
            "merged_at": (base_date + timedelta(days=19)).isoformat(),
            "labels": [{"name": "feature"}, {"name": "notifications"}],
            "milestone": {"title": "v2.0"},
            "additions": 780,
            "deletions": 50,
            "changed_files": 22,
        },
        {
            "node_id": "PR_016",
            "number": 302,
            "state": "closed",
            "draft": False,
            "title": "Fix crash on iOS 16",
            "body": "Resolves compatibility issue with iOS 16 causing app crashes.",
            "user": {"node_id": "U_bob", "login": "bob"},
            "base": {"repo": {"node_id": "R_003", "full_name": "test-org/mobile-app"}},
            "created_at": (base_date + timedelta(days=24)).isoformat(),
            "updated_at": (base_date + timedelta(days=25)).isoformat(),
            "closed_at": (base_date + timedelta(days=25)).isoformat(),
            "merged_at": (base_date + timedelta(days=25)).isoformat(),
            "labels": [{"name": "bug"}, {"name": "priority:critical"}],
            "milestone": {"title": "v1.9.1"},
            "additions": 35,
            "deletions": 28,
            "changed_files": 3,
        },
        {
            "node_id": "PR_017",
            "number": 303,
            "state": "open",
            "draft": False,
            "title": "Add offline mode support",
            "body": "Implements local caching for offline functionality.",
            "user": {"node_id": "U_charlie", "login": "charlie"},
            "base": {"repo": {"node_id": "R_003", "full_name": "test-org/mobile-app"}},
            "created_at": (base_date + timedelta(days=34)).isoformat(),
            "updated_at": (base_date + timedelta(days=37)).isoformat(),
            "closed_at": None,
            "merged_at": None,
            "labels": [{"name": "feature"}, {"name": "enhancement"}],
            "milestone": {"title": "v2.1"},
            "additions": 640,
            "deletions": 90,
            "changed_files": 16,
        },
    ]

    # Data Pipeline PRs (2 total)
    pulls["test-org__data-pipeline"] = [
        {
            "node_id": "PR_018",
            "number": 401,
            "state": "closed",
            "draft": False,
            "title": "Add Snowflake connector",
            "body": "Implements data export to Snowflake data warehouse.",
            "user": {"node_id": "U_diana", "login": "diana"},
            "base": {"repo": {"node_id": "R_004", "full_name": "test-org/data-pipeline"}},
            "created_at": (base_date + timedelta(days=16)).isoformat(),
            "updated_at": (base_date + timedelta(days=20)).isoformat(),
            "closed_at": (base_date + timedelta(days=20)).isoformat(),
            "merged_at": (base_date + timedelta(days=20)).isoformat(),
            "labels": [{"name": "feature"}, {"name": "integration"}],
            "milestone": {"title": "v3.0"},
            "additions": 410,
            "deletions": 30,
            "changed_files": 9,
        },
        {
            "node_id": "PR_019",
            "number": 402,
            "state": "closed",
            "draft": False,
            "title": "Fix duplicate record handling",
            "body": "Improves deduplication logic to prevent data inconsistencies.",
            "user": {"node_id": "U_eve", "login": "eve"},
            "base": {"repo": {"node_id": "R_004", "full_name": "test-org/data-pipeline"}},
            "created_at": (base_date + timedelta(days=28)).isoformat(),
            "updated_at": (base_date + timedelta(days=30)).isoformat(),
            "closed_at": (base_date + timedelta(days=30)).isoformat(),
            "merged_at": (base_date + timedelta(days=30)).isoformat(),
            "labels": [{"name": "bug"}, {"name": "data-quality"}],
            "milestone": {"title": "v2.5"},
            "additions": 145,
            "deletions": 88,
            "changed_files": 5,
        },
    ]

    # Docs Site PRs (1 total)
    pulls["test-org__docs-site"] = [
        {
            "node_id": "PR_020",
            "number": 501,
            "state": "closed",
            "draft": False,
            "title": "Update API reference documentation",
            "body": "Comprehensive update to API docs with new examples.",
            "user": {"node_id": "U_grace", "login": "grace"},
            "base": {"repo": {"node_id": "R_005", "full_name": "test-org/docs-site"}},
            "created_at": (base_date + timedelta(days=22)).isoformat(),
            "updated_at": (base_date + timedelta(days=24)).isoformat(),
            "closed_at": (base_date + timedelta(days=24)).isoformat(),
            "merged_at": (base_date + timedelta(days=24)).isoformat(),
            "labels": [{"name": "documentation"}],
            "milestone": None,
            "additions": 1250,
            "deletions": 420,
            "changed_files": 38,
        },
    ]

    return pulls


def generate_issues() -> dict[str, list[dict[str, Any]]]:
    """Generate sample issues (10 issues across repos).

    Returns:
        Dictionary mapping repo slug to list of issues.
    """
    base_date = datetime(2025, 1, 1, tzinfo=UTC)
    issues: dict[str, list[dict[str, Any]]] = {}

    # Backend API issues (5 total)
    issues["test-org__backend-api"] = [
        {
            "node_id": "ISS_001",
            "number": 45,
            "state": "closed",
            "title": "Memory leak in cache handler",
            "body": "Application memory usage grows over time. Suspect connection pooling issue.",
            "user": {"node_id": "U_bob", "login": "bob"},
            "labels": [{"name": "bug"}, {"name": "priority:high"}],
            "milestone": None,
            "created_at": (base_date + timedelta(days=8)).isoformat(),
            "updated_at": (base_date + timedelta(days=12)).isoformat(),
            "closed_at": (base_date + timedelta(days=12)).isoformat(),
            "repository": {"node_id": "R_001", "full_name": "test-org/backend-api"},
        },
        {
            "node_id": "ISS_002",
            "number": 46,
            "state": "open",
            "title": "Add WebSocket support",
            "body": "Feature request: Real-time updates via WebSocket connections.",
            "user": {"node_id": "U_charlie", "login": "charlie"},
            "labels": [{"name": "enhancement"}, {"name": "feature-request"}],
            "milestone": {"title": "v3.0"},
            "created_at": (base_date + timedelta(days=17)).isoformat(),
            "updated_at": (base_date + timedelta(days=17)).isoformat(),
            "closed_at": None,
            "repository": {"node_id": "R_001", "full_name": "test-org/backend-api"},
        },
        {
            "node_id": "ISS_003",
            "number": 47,
            "state": "open",
            "title": "Improve error messages in API responses",
            "body": "Current error messages are too generic. Need more specific feedback.",
            "user": {"node_id": "U_diana", "login": "diana"},
            "labels": [{"name": "enhancement"}, {"name": "dx"}],
            "milestone": {"title": "v2.1"},
            "created_at": (base_date + timedelta(days=26)).isoformat(),
            "updated_at": (base_date + timedelta(days=27)).isoformat(),
            "closed_at": None,
            "repository": {"node_id": "R_001", "full_name": "test-org/backend-api"},
        },
        {
            "node_id": "ISS_004",
            "number": 48,
            "state": "closed",
            "title": "Add request timeout configuration",
            "body": "Allow configurable timeout values for external service calls.",
            "user": {"node_id": "U_eve", "login": "eve"},
            "labels": [{"name": "enhancement"}],
            "milestone": {"title": "v2.1"},
            "created_at": (base_date + timedelta(days=31)).isoformat(),
            "updated_at": (base_date + timedelta(days=34)).isoformat(),
            "closed_at": (base_date + timedelta(days=34)).isoformat(),
            "repository": {"node_id": "R_001", "full_name": "test-org/backend-api"},
        },
        {
            "node_id": "ISS_005",
            "number": 49,
            "state": "open",
            "title": "Security: Add input sanitization",
            "body": "Need to sanitize user inputs to prevent injection attacks.",
            "user": {"node_id": "U_grace", "login": "grace"},
            "labels": [{"name": "security"}, {"name": "priority:high"}],
            "milestone": {"title": "v2.1"},
            "created_at": (base_date + timedelta(days=39)).isoformat(),
            "updated_at": (base_date + timedelta(days=40)).isoformat(),
            "closed_at": None,
            "repository": {"node_id": "R_001", "full_name": "test-org/backend-api"},
        },
    ]

    # Frontend Web issues (3 total)
    issues["test-org__frontend-web"] = [
        {
            "node_id": "ISS_006",
            "number": 78,
            "state": "closed",
            "title": "Navbar overflow on mobile devices",
            "body": "Navigation bar doesn't display correctly on screens < 768px.",
            "user": {"node_id": "U_alice", "login": "alice"},
            "labels": [{"name": "bug"}, {"name": "mobile"}],
            "milestone": None,
            "created_at": (base_date + timedelta(days=11)).isoformat(),
            "updated_at": (base_date + timedelta(days=13)).isoformat(),
            "closed_at": (base_date + timedelta(days=13)).isoformat(),
            "repository": {"node_id": "R_002", "full_name": "test-org/frontend-web"},
        },
        {
            "node_id": "ISS_007",
            "number": 79,
            "state": "open",
            "title": "Add accessibility improvements",
            "body": "Screen reader support and ARIA labels needed for better accessibility.",
            "user": {"node_id": "U_frank", "login": "frank"},
            "labels": [{"name": "enhancement"}, {"name": "a11y"}],
            "milestone": {"title": "v1.6"},
            "created_at": (base_date + timedelta(days=23)).isoformat(),
            "updated_at": (base_date + timedelta(days=24)).isoformat(),
            "closed_at": None,
            "repository": {"node_id": "R_002", "full_name": "test-org/frontend-web"},
        },
        {
            "node_id": "ISS_008",
            "number": 80,
            "state": "open",
            "title": "Form validation inconsistent",
            "body": "Some forms validate on submit, others on blur. Need consistency.",
            "user": {"node_id": "U_bob", "login": "bob"},
            "labels": [{"name": "bug"}, {"name": "ux"}],
            "milestone": {"title": "v1.5"},
            "created_at": (base_date + timedelta(days=37)).isoformat(),
            "updated_at": (base_date + timedelta(days=39)).isoformat(),
            "closed_at": None,
            "repository": {"node_id": "R_002", "full_name": "test-org/frontend-web"},
        },
    ]

    # Mobile App issues (2 total)
    issues["test-org__mobile-app"] = [
        {
            "node_id": "ISS_009",
            "number": 56,
            "state": "closed",
            "title": "App crashes on iOS 16",
            "body": "Reproducible crash when opening settings on iOS 16.x devices.",
            "user": {"node_id": "U_charlie", "login": "charlie"},
            "labels": [{"name": "bug"}, {"name": "priority:critical"}],
            "milestone": {"title": "v1.9.1"},
            "created_at": (base_date + timedelta(days=22)).isoformat(),
            "updated_at": (base_date + timedelta(days=25)).isoformat(),
            "closed_at": (base_date + timedelta(days=25)).isoformat(),
            "repository": {"node_id": "R_003", "full_name": "test-org/mobile-app"},
        },
        {
            "node_id": "ISS_010",
            "number": 57,
            "state": "open",
            "title": "Request: Biometric authentication",
            "body": "Add Face ID/Touch ID support for login.",
            "user": {"node_id": "U_diana", "login": "diana"},
            "labels": [{"name": "feature-request"}, {"name": "enhancement"}],
            "milestone": {"title": "v2.1"},
            "created_at": (base_date + timedelta(days=33)).isoformat(),
            "updated_at": (base_date + timedelta(days=35)).isoformat(),
            "closed_at": None,
            "repository": {"node_id": "R_003", "full_name": "test-org/mobile-app"},
        },
    ]

    return issues


def generate_reviews(pulls: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    """Generate sample reviews (15 reviews across PRs).

    Args:
        pulls: Pull requests to create reviews for.

    Returns:
        Dictionary mapping repo slug to list of reviews.
    """
    base_date = datetime(2025, 1, 1, tzinfo=UTC)
    reviews: dict[str, list[dict[str, Any]]] = {}

    # Backend API reviews
    reviews["test-org__backend-api"] = [
        # PR_001 reviews (2 reviews)
        {
            "node_id": "REV_001",
            "user": {"node_id": "U_bob", "login": "bob"},
            "body": "Great implementation! Just a few minor style suggestions.",
            "state": "COMMENTED",
            "submitted_at": (base_date + timedelta(days=6)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/backend-api/pulls/101",
        },
        {
            "node_id": "REV_002",
            "user": {"node_id": "U_charlie", "login": "charlie"},
            "body": "LGTM. Security implementation looks solid.",
            "state": "APPROVED",
            "submitted_at": (base_date + timedelta(days=7)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/backend-api/pulls/101",
        },
        # PR_002 reviews (2 reviews)
        {
            "node_id": "REV_003",
            "user": {"node_id": "U_alice", "login": "alice"},
            "body": "Good catch on the connection leak. Please add a test case.",
            "state": "CHANGES_REQUESTED",
            "submitted_at": (base_date + timedelta(days=11)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/backend-api/pulls/102",
        },
        {
            "node_id": "REV_004",
            "user": {"node_id": "U_alice", "login": "alice"},
            "body": "Tests added. Looks good now!",
            "state": "APPROVED",
            "submitted_at": (base_date + timedelta(days=12)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/backend-api/pulls/102",
        },
        # PR_003 reviews (1 review)
        {
            "node_id": "REV_005",
            "user": {"node_id": "U_diana", "login": "diana"},
            "body": "Implementation looks good. Consider adding metrics tracking.",
            "state": "COMMENTED",
            "submitted_at": (base_date + timedelta(days=17)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/backend-api/pulls/103",
        },
        # PR_005 reviews (2 reviews)
        {
            "node_id": "REV_006",
            "user": {"node_id": "U_frank", "login": "frank"},
            "body": "Excellent documentation! Very thorough.",
            "state": "APPROVED",
            "submitted_at": (base_date + timedelta(days=27)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/backend-api/pulls/105",
        },
        {
            "node_id": "REV_007",
            "user": {"node_id": "U_eve", "login": "eve"},
            "body": "Approved. Nice work on the examples.",
            "state": "APPROVED",
            "submitted_at": (base_date + timedelta(days=28)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/backend-api/pulls/105",
        },
    ]

    # Frontend Web reviews
    reviews["test-org__frontend-web"] = [
        # PR_009 reviews (2 reviews)
        {
            "node_id": "REV_008",
            "user": {"node_id": "U_charlie", "login": "charlie"},
            "body": "UI looks great! Small suggestion on color contrast.",
            "state": "COMMENTED",
            "submitted_at": (base_date + timedelta(days=9)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/frontend-web/pulls/201",
        },
        {
            "node_id": "REV_009",
            "user": {"node_id": "U_diana", "login": "diana"},
            "body": "Approved after contrast fix.",
            "state": "APPROVED",
            "submitted_at": (base_date + timedelta(days=10)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/frontend-web/pulls/201",
        },
        # PR_011 reviews (2 reviews)
        {
            "node_id": "REV_010",
            "user": {"node_id": "U_alice", "login": "alice"},
            "body": "Dark mode works well! Please test on different browsers.",
            "state": "COMMENTED",
            "submitted_at": (base_date + timedelta(days=20)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/frontend-web/pulls/203",
        },
        {
            "node_id": "REV_011",
            "user": {"node_id": "U_bob", "login": "bob"},
            "body": "Tested on Chrome, Firefox, Safari. All good!",
            "state": "APPROVED",
            "submitted_at": (base_date + timedelta(days=21)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/frontend-web/pulls/203",
        },
    ]

    # Mobile App reviews
    reviews["test-org__mobile-app"] = [
        # PR_015 reviews (2 reviews)
        {
            "node_id": "REV_012",
            "user": {"node_id": "U_charlie", "login": "charlie"},
            "body": "Notifications work well. Check permission handling on iOS 15.",
            "state": "COMMENTED",
            "submitted_at": (base_date + timedelta(days=17)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/mobile-app/pulls/301",
        },
        {
            "node_id": "REV_013",
            "user": {"node_id": "U_diana", "login": "diana"},
            "body": "Permissions fixed. LGTM!",
            "state": "APPROVED",
            "submitted_at": (base_date + timedelta(days=18)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/mobile-app/pulls/301",
        },
    ]

    # Data Pipeline reviews
    reviews["test-org__data-pipeline"] = [
        # PR_018 reviews (1 review)
        {
            "node_id": "REV_014",
            "user": {"node_id": "U_eve", "login": "eve"},
            "body": "Snowflake integration looks solid. Good error handling.",
            "state": "APPROVED",
            "submitted_at": (base_date + timedelta(days=19)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/data-pipeline/pulls/401",
        },
    ]

    # Docs Site reviews
    reviews["test-org__docs-site"] = [
        # PR_020 reviews (1 review)
        {
            "node_id": "REV_015",
            "user": {"node_id": "U_alice", "login": "alice"},
            "body": "Documentation is comprehensive and well-organized. Great work!",
            "state": "APPROVED",
            "submitted_at": (base_date + timedelta(days=23)).isoformat(),
            "pull_request_url": "https://api.github.com/repos/test-org/docs-site/pulls/501",
        },
    ]

    return reviews


def generate_hygiene_data(repos: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Generate sample hygiene data for repos.

    Args:
        repos: List of repositories.

    Returns:
        Dictionary mapping repo slug to hygiene data.
    """
    hygiene: dict[str, dict[str, Any]] = {}

    # Backend API - excellent hygiene
    hygiene["test-org__backend-api"] = {
        "file_presence": [
            {"path": "SECURITY.md", "exists": True, "size": 2048},
            {"path": "README.md", "exists": True, "size": 8192},
            {"path": "CONTRIBUTING.md", "exists": True, "size": 4096},
            {"path": "LICENSE", "exists": True, "size": 1024},
            {"path": "CODE_OF_CONDUCT.md", "exists": True, "size": 3072},
        ],
        "branch_protection": {
            "protection_enabled": True,
            "branch": "main",
            "required_reviews": {
                "required_approving_review_count": 2,
                "dismiss_stale_reviews": True,
                "require_code_owner_reviews": True,
            },
            "required_status_checks": {"strict": True},
            "allow_force_pushes": False,
            "allow_deletions": False,
            "enforce_admins": True,
        },
        "security_features": {
            "dependabot_alerts_enabled": True,
            "secret_scanning_enabled": True,
            "push_protection_enabled": True,
        },
    }

    # Frontend Web - good hygiene
    hygiene["test-org__frontend-web"] = {
        "file_presence": [
            {"path": "SECURITY.md", "exists": True, "size": 1536},
            {"path": "README.md", "exists": True, "size": 6144},
            {"path": "CONTRIBUTING.md", "exists": True, "size": 2048},
            {"path": "LICENSE", "exists": True, "size": 1024},
            {"path": "CODE_OF_CONDUCT.md", "exists": False, "size": 0},
        ],
        "branch_protection": {
            "protection_enabled": True,
            "branch": "main",
            "required_reviews": {
                "required_approving_review_count": 1,
                "dismiss_stale_reviews": False,
                "require_code_owner_reviews": False,
            },
            "allow_force_pushes": True,
            "allow_deletions": False,
            "enforce_admins": False,
        },
        "security_features": {
            "dependabot_alerts_enabled": True,
            "secret_scanning_enabled": True,
            "push_protection_enabled": False,
        },
    }

    # Mobile App - moderate hygiene
    hygiene["test-org__mobile-app"] = {
        "file_presence": [
            {"path": "SECURITY.md", "exists": False, "size": 0},
            {"path": "README.md", "exists": True, "size": 4096},
            {"path": "CONTRIBUTING.md", "exists": True, "size": 1024},
            {"path": "LICENSE", "exists": True, "size": 1024},
            {"path": "CODE_OF_CONDUCT.md", "exists": False, "size": 0},
        ],
        "branch_protection": {
            "protection_enabled": True,
            "branch": "develop",
            "required_reviews": {
                "required_approving_review_count": 1,
            },
            "allow_force_pushes": True,
            "allow_deletions": True,
            "enforce_admins": False,
        },
        "security_features": {
            "dependabot_alerts_enabled": True,
            "secret_scanning_enabled": False,
            "push_protection_enabled": False,
        },
    }

    # Data Pipeline - basic hygiene (no branch protection)
    hygiene["test-org__data-pipeline"] = {
        "file_presence": [
            {"path": "SECURITY.md", "exists": False, "size": 0},
            {"path": "README.md", "exists": True, "size": 3072},
            {"path": "CONTRIBUTING.md", "exists": False, "size": 0},
            {"path": "LICENSE", "exists": True, "size": 1024},
            {"path": "CODE_OF_CONDUCT.md", "exists": False, "size": 0},
        ],
        "branch_protection": {
            "protection_enabled": False,
            "branch": "main",
        },
        "security_features": {
            "dependabot_alerts_enabled": False,
            "secret_scanning_enabled": False,
            "push_protection_enabled": False,
        },
    }

    # Docs Site - minimal hygiene (no branch protection)
    hygiene["test-org__docs-site"] = {
        "file_presence": [
            {"path": "SECURITY.md", "exists": False, "size": 0},
            {"path": "README.md", "exists": True, "size": 1024},
            {"path": "CONTRIBUTING.md", "exists": False, "size": 0},
            {"path": "LICENSE", "exists": False, "size": 0},
            {"path": "CODE_OF_CONDUCT.md", "exists": False, "size": 0},
        ],
        "branch_protection": {
            "protection_enabled": False,
            "branch": "main",
        },
        "security_features": {
            "dependabot_alerts_enabled": False,
            "secret_scanning_enabled": False,
            "push_protection_enabled": False,
        },
    }

    return hygiene


def write_fixtures(output_dir: Path) -> None:
    """Write all fixture data to JSONL files.

    Args:
        output_dir: Base directory for fixture files.
    """
    users = generate_users()
    repos = generate_repos()
    pulls = generate_pulls(repos, users)
    issues = generate_issues()
    reviews = generate_reviews(pulls)
    hygiene = generate_hygiene_data(repos)

    # Create directory structure
    raw_dir = output_dir / "raw" / "year=2025" / "source=github" / "target=test-org"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Write repos.jsonl
    repos_file = raw_dir / "repos.jsonl"
    with repos_file.open("w") as f:
        for repo in repos:
            envelope = generate_envelope(repo, "/orgs/test-org/repos")
            f.write(json.dumps(envelope) + "\n")

    # Write pulls (one file per repo)
    pulls_dir = raw_dir / "pulls"
    pulls_dir.mkdir(exist_ok=True)

    for repo_slug, pr_list in pulls.items():
        pulls_file = pulls_dir / f"{repo_slug}.jsonl"
        with pulls_file.open("w") as f:
            for pr in pr_list:
                repo_name = pr["base"]["repo"]["full_name"]
                endpoint = f"/repos/{repo_name}/pulls/{pr['number']}"
                envelope = generate_envelope(pr, endpoint)
                f.write(json.dumps(envelope) + "\n")

    # Write issues (one file per repo)
    issues_dir = raw_dir / "issues"
    issues_dir.mkdir(exist_ok=True)

    for repo_slug, issue_list in issues.items():
        issues_file = issues_dir / f"{repo_slug}.jsonl"
        with issues_file.open("w") as f:
            for issue in issue_list:
                repo_name = issue["repository"]["full_name"]
                endpoint = f"/repos/{repo_name}/issues/{issue['number']}"
                envelope = generate_envelope(issue, endpoint)
                f.write(json.dumps(envelope) + "\n")

    # Write reviews (one file per repo)
    reviews_dir = raw_dir / "reviews"
    reviews_dir.mkdir(exist_ok=True)

    for repo_slug, review_list in reviews.items():
        reviews_file = reviews_dir / f"{repo_slug}.jsonl"
        with reviews_file.open("w") as f:
            for review in review_list:
                # Extract PR number from URL
                pr_number = review["pull_request_url"].split("/")[-1]
                repo_name = "/".join(review["pull_request_url"].split("/")[-4:-2])
                endpoint = f"/repos/{repo_name}/pulls/{pr_number}/reviews"
                envelope = generate_envelope(review, endpoint)
                f.write(json.dumps(envelope) + "\n")

    # Write hygiene data
    repo_tree_dir = raw_dir / "repo_tree"
    repo_tree_dir.mkdir(exist_ok=True)
    branch_protection_dir = raw_dir / "branch_protection"
    branch_protection_dir.mkdir(exist_ok=True)
    security_features_dir = raw_dir / "security_features"
    security_features_dir.mkdir(exist_ok=True)

    for repo_slug, hygiene_data in hygiene.items():
        repo_name = repo_slug.replace("__", "/")

        # Write file presence data
        repo_tree_file = repo_tree_dir / f"{repo_slug}.jsonl"
        with repo_tree_file.open("w") as f:
            for file_info in hygiene_data["file_presence"]:
                data = {
                    "repo": repo_name,
                    "path": file_info["path"],
                    "exists": file_info["exists"],
                    "size": file_info["size"],
                }
                endpoint = "file_presence"
                envelope = generate_envelope(data, endpoint)
                f.write(json.dumps(envelope) + "\n")

        # Write branch protection data
        branch_prot_file = branch_protection_dir / f"{repo_slug}.jsonl"
        with branch_prot_file.open("w") as f:
            data = {
                "repo": repo_name,
                **hygiene_data["branch_protection"],
            }
            endpoint = f"/repos/{repo_name}/branches/main/protection"
            envelope = generate_envelope(data, endpoint)
            f.write(json.dumps(envelope) + "\n")

        # Write security features data
        security_file = security_features_dir / f"{repo_slug}.jsonl"
        with security_file.open("w") as f:
            data = {
                "repo": repo_name,
                **hygiene_data["security_features"],
            }
            endpoint = f"/repos/{repo_name}/vulnerability-alerts"
            envelope = generate_envelope(data, endpoint)
            f.write(json.dumps(envelope) + "\n")

    print(f"Fixtures written to {output_dir}")
    print(f"  - {len(repos)} repositories")
    print(f"  - {len(users)} users")
    print(f"  - {sum(len(prs) for prs in pulls.values())} pull requests")
    print(f"  - {sum(len(iss) for iss in issues.values())} issues")
    print(f"  - {sum(len(revs) for revs in reviews.values())} reviews")
    print(f"  - {len(hygiene)} repositories with hygiene data")


if __name__ == "__main__":
    fixture_dir = Path(__file__).parent
    write_fixtures(fixture_dir)
