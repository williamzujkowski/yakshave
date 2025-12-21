"""GitHub GraphQL API client with pagination and rate limiting.

Async GraphQL client for GitHub API with cursor-based pagination,
pre-built queries, and integration with adaptive rate limiter.
"""

import logging
from collections.abc import AsyncIterator
from typing import Any, cast

import httpx

from gh_year_end.github.http import GitHubClient
from gh_year_end.github.ratelimit import AdaptiveRateLimiter, APIType

logger = logging.getLogger(__name__)


class GraphQLError(Exception):
    """Raised when GraphQL query returns errors."""

    def __init__(self, errors: list[dict[str, Any]]) -> None:
        self.errors = errors
        messages = [err.get("message", "Unknown error") for err in errors]
        super().__init__(f"GraphQL errors: {'; '.join(messages)}")


# GraphQL query templates
REPOSITORY_INFO_QUERY = """
query($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    id
    name
    nameWithOwner
    description
    createdAt
    updatedAt
    pushedAt
    url
    homepageUrl
    isPrivate
    isFork
    isArchived
    isDisabled
    isLocked
    isMirror
    isTemplate
    stargazerCount
    forkCount
    watchers {
      totalCount
    }
    issues {
      totalCount
    }
    pullRequests {
      totalCount
    }
    releases {
      totalCount
    }
    primaryLanguage {
      name
      color
    }
    languages(first: 10, orderBy: {field: SIZE, direction: DESC}) {
      edges {
        size
        node {
          name
          color
        }
      }
    }
    diskUsageKb: diskUsage
    defaultBranchRef {
      name
    }
    licenseInfo {
      name
      spdxId
    }
    owner {
      __typename
      login
      ... on User {
        name
        email
      }
      ... on Organization {
        name
        email
      }
    }
  }
}
"""

PULL_REQUESTS_QUERY = """
query($owner: String!, $name: String!, $after: String, $first: Int = 100) {
  repository(owner: $owner, name: $name) {
    pullRequests(first: $first, after: $after, orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      totalCount
      edges {
        node {
          id
          number
          title
          body
          state
          createdAt
          updatedAt
          closedAt
          mergedAt
          url
          additions
          deletions
          changedFiles
          isDraft
          merged
          mergeable
          author {
            __typename
            login
            ... on User {
              name
              email
            }
            ... on Bot {
              id
            }
          }
          assignees(first: 10) {
            nodes {
              login
              name
            }
          }
          labels(first: 20) {
            nodes {
              name
              color
            }
          }
          reviews(first: 50) {
            totalCount
            nodes {
              id
              state
              createdAt
              author {
                login
              }
            }
          }
          comments {
            totalCount
          }
          commits {
            totalCount
          }
          baseRefName
          headRefName
          baseRepository {
            nameWithOwner
          }
          headRepository {
            nameWithOwner
          }
        }
      }
    }
  }
}
"""

ISSUES_QUERY = """
query($owner: String!, $name: String!, $after: String, $first: Int = 100) {
  repository(owner: $owner, name: $name) {
    issues(first: $first, after: $after, orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      totalCount
      edges {
        node {
          id
          number
          title
          body
          state
          createdAt
          updatedAt
          closedAt
          url
          author {
            __typename
            login
            ... on User {
              name
              email
            }
            ... on Bot {
              id
            }
          }
          assignees(first: 10) {
            nodes {
              login
              name
            }
          }
          labels(first: 20) {
            nodes {
              name
              color
            }
          }
          comments {
            totalCount
          }
          participants(first: 20) {
            totalCount
            nodes {
              login
            }
          }
        }
      }
    }
  }
}
"""

USER_INFO_QUERY = """
query($login: String!) {
  user(login: $login) {
    id
    login
    name
    email
    bio
    company
    location
    websiteUrl
    twitterUsername
    createdAt
    updatedAt
    url
    avatarUrl
    isHireable
    isBountyHunter
    isCampusExpert
    isDeveloperProgramMember
    isEmployee
    isGitHubStar
    followers {
      totalCount
    }
    following {
      totalCount
    }
    repositories {
      totalCount
    }
    gists {
      totalCount
    }
    organizations(first: 20) {
      totalCount
      nodes {
        login
        name
      }
    }
    contributionsCollection {
      totalCommitContributions
      totalIssueContributions
      totalPullRequestContributions
      totalPullRequestReviewContributions
      totalRepositoryContributions
    }
  }
}
"""

ORGANIZATION_INFO_QUERY = """
query($login: String!) {
  organization(login: $login) {
    id
    login
    name
    email
    description
    websiteUrl
    twitterUsername
    location
    createdAt
    updatedAt
    url
    avatarUrl
    isVerified
    members {
      totalCount
    }
    repositories {
      totalCount
    }
    teams {
      totalCount
    }
    projects {
      totalCount
    }
  }
}
"""


class GraphQLClient:
    """GitHub GraphQL API client with pagination and rate limiting.

    Features:
    - Execute arbitrary GraphQL queries
    - Cursor-based pagination with async iteration
    - Pre-built queries for common operations
    - Integration with AdaptiveRateLimiter
    - Automatic error handling
    """

    GRAPHQL_ENDPOINT = "/graphql"

    def __init__(
        self,
        http_client: GitHubClient,
        rate_limiter: AdaptiveRateLimiter | None = None,
    ) -> None:
        """Initialize GraphQL client.

        Args:
            http_client: HTTP client for making requests.
            rate_limiter: Optional rate limiter for throttling.
        """
        self._http = http_client
        self._rate_limiter = rate_limiter

    async def execute(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL query.

        Args:
            query: GraphQL query string.
            variables: Optional query variables.

        Returns:
            GraphQL response data payload.

        Raises:
            GraphQLError: If response contains GraphQL errors.
        """
        # Acquire rate limiter if available
        if self._rate_limiter:
            await self._rate_limiter.acquire(APIType.GRAPHQL)

        try:
            # Build request payload
            payload: dict[str, Any] = {"query": query}
            if variables:
                payload["variables"] = variables

            # Execute POST request
            try:
                response = await self._http.post(self.GRAPHQL_ENDPOINT, json=payload)
            except httpx.HTTPStatusError as e:
                # Wrap HTTP errors as GraphQL errors
                raise GraphQLError([{"message": f"HTTP {e.response.status_code}"}]) from e

            # Update rate limiter with response headers
            if self._rate_limiter:
                headers_dict = dict(response.headers)
                self._rate_limiter.update(headers_dict, APIType.GRAPHQL)

            # Check for errors in response
            if not response.is_success:
                logger.error(
                    "GraphQL request failed: status=%d, response=%s",
                    response.status_code,
                    response.data,
                )
                raise GraphQLError([{"message": f"HTTP {response.status_code}"}])

            # Parse GraphQL response
            if not isinstance(response.data, dict):
                raise GraphQLError([{"message": "Invalid GraphQL response format"}])

            # Check for GraphQL errors
            if "errors" in response.data:
                errors = response.data["errors"]
                logger.error("GraphQL errors: %s", errors)
                raise GraphQLError(errors)

            # Return data payload
            data = response.data.get("data")
            if data is None:
                raise GraphQLError([{"message": "Missing data in GraphQL response"}])

            return cast("dict[str, Any]", data)

        finally:
            # Always release rate limiter
            if self._rate_limiter:
                self._rate_limiter.release()

    async def paginate(
        self,
        query: str,
        variables: dict[str, Any],
        path_to_connection: list[str],
        page_size: int = 100,
    ) -> AsyncIterator[dict[str, Any]]:
        """Auto-paginate through GraphQL connection.

        Args:
            query: GraphQL query with $after and $first variables.
            variables: Base variables (without after/first).
            path_to_connection: Path to connection object in response.
            page_size: Number of items per page.

        Yields:
            Individual items from paginated results.
        """
        has_next_page = True
        after_cursor: str | None = None

        while has_next_page:
            # Add pagination variables
            page_vars = {**variables, "after": after_cursor, "first": page_size}

            # Execute query
            data = await self.execute(query, page_vars)

            # Navigate to connection object
            connection = data
            for key in path_to_connection:
                connection = connection.get(key, {})

            # Extract page info and edges
            page_info = connection.get("pageInfo", {})
            edges = connection.get("edges", [])

            # Yield individual nodes
            for edge in edges:
                if node := edge.get("node"):
                    yield node

            # Check for next page
            has_next_page = page_info.get("hasNextPage", False)
            after_cursor = page_info.get("endCursor")

            logger.debug(
                "Paginated %d items, hasNextPage=%s, cursor=%s",
                len(edges),
                has_next_page,
                after_cursor,
            )

    async def query_repository_info(
        self,
        owner: str,
        repo: str,
    ) -> dict[str, Any]:
        """Query repository information.

        Args:
            owner: Repository owner (user or org).
            repo: Repository name.

        Returns:
            Repository data dictionary.
        """
        logger.debug("Querying repository info: %s/%s", owner, repo)

        data = await self.execute(
            REPOSITORY_INFO_QUERY,
            {"owner": owner, "name": repo},
        )

        return cast("dict[str, Any]", data.get("repository", {}))

    async def query_pull_requests(
        self,
        owner: str,
        repo: str,
        after_cursor: str | None = None,
        first: int = 100,
    ) -> dict[str, Any]:
        """Query pull requests for a repository.

        Args:
            owner: Repository owner.
            repo: Repository name.
            after_cursor: Cursor for pagination.
            first: Number of items to fetch.

        Returns:
            Pull requests connection data.
        """
        logger.debug(
            "Querying pull requests: %s/%s (after=%s, first=%d)",
            owner,
            repo,
            after_cursor,
            first,
        )

        variables = {
            "owner": owner,
            "name": repo,
            "after": after_cursor,
            "first": first,
        }

        data = await self.execute(PULL_REQUESTS_QUERY, variables)

        repository = data.get("repository", {})
        return cast("dict[str, Any]", repository.get("pullRequests", {}))

    async def paginate_pull_requests(
        self,
        owner: str,
        repo: str,
        page_size: int = 100,
    ) -> AsyncIterator[dict[str, Any]]:
        """Paginate through all pull requests for a repository.

        Args:
            owner: Repository owner.
            repo: Repository name.
            page_size: Number of items per page.

        Yields:
            Individual pull request nodes.
        """
        logger.info("Paginating pull requests: %s/%s", owner, repo)

        async for pr in self.paginate(
            PULL_REQUESTS_QUERY,
            {"owner": owner, "name": repo},
            ["repository", "pullRequests"],
            page_size,
        ):
            yield pr

    async def query_issues(
        self,
        owner: str,
        repo: str,
        after_cursor: str | None = None,
        first: int = 100,
    ) -> dict[str, Any]:
        """Query issues for a repository.

        Args:
            owner: Repository owner.
            repo: Repository name.
            after_cursor: Cursor for pagination.
            first: Number of items to fetch.

        Returns:
            Issues connection data.
        """
        logger.debug(
            "Querying issues: %s/%s (after=%s, first=%d)",
            owner,
            repo,
            after_cursor,
            first,
        )

        variables = {
            "owner": owner,
            "name": repo,
            "after": after_cursor,
            "first": first,
        }

        data = await self.execute(ISSUES_QUERY, variables)

        repository = data.get("repository", {})
        return cast("dict[str, Any]", repository.get("issues", {}))

    async def paginate_issues(
        self,
        owner: str,
        repo: str,
        page_size: int = 100,
    ) -> AsyncIterator[dict[str, Any]]:
        """Paginate through all issues for a repository.

        Args:
            owner: Repository owner.
            repo: Repository name.
            page_size: Number of items per page.

        Yields:
            Individual issue nodes.
        """
        logger.info("Paginating issues: %s/%s", owner, repo)

        async for issue in self.paginate(
            ISSUES_QUERY,
            {"owner": owner, "name": repo},
            ["repository", "issues"],
            page_size,
        ):
            yield issue

    async def query_user_info(self, username: str) -> dict[str, Any]:
        """Query user profile information.

        Args:
            username: GitHub username.

        Returns:
            User data dictionary.
        """
        logger.debug("Querying user info: %s", username)

        data = await self.execute(USER_INFO_QUERY, {"login": username})

        return cast("dict[str, Any]", data.get("user", {}))

    async def query_org_info(self, org: str) -> dict[str, Any]:
        """Query organization profile information.

        Args:
            org: GitHub organization login.

        Returns:
            Organization data dictionary.
        """
        logger.debug("Querying org info: %s", org)

        data = await self.execute(ORGANIZATION_INFO_QUERY, {"login": org})

        return cast("dict[str, Any]", data.get("organization", {}))
