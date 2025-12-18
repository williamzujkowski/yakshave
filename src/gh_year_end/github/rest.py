"""GitHub REST API client with pagination and rate limiting.

Provides high-level methods for common GitHub REST API endpoints with automatic
pagination, rate limiting, and error handling.
"""

import logging
import re
from collections.abc import AsyncIterator
from typing import Any, cast
from urllib.parse import urlparse

from gh_year_end.github.http import GitHubClient, GitHubResponse
from gh_year_end.github.ratelimit import AdaptiveRateLimiter, APIType

logger = logging.getLogger(__name__)


class RestClient:
    """GitHub REST API client with pagination and rate limiting.

    Wraps GitHubClient to provide:
    - Automatic pagination following Link headers
    - Integration with AdaptiveRateLimiter
    - High-level methods for common endpoints
    - Memory-efficient async iteration
    """

    def __init__(
        self,
        http_client: GitHubClient,
        rate_limiter: AdaptiveRateLimiter | None = None,
    ) -> None:
        """Initialize REST API client.

        Args:
            http_client: GitHubClient instance for HTTP requests.
            rate_limiter: AdaptiveRateLimiter for throttling. If None, no rate limiting.
        """
        self._http = http_client
        self._rate_limiter = rate_limiter

    def _parse_link_header(self, link_header: str | None) -> dict[str, str]:
        """Parse Link header to extract pagination URLs.

        Args:
            link_header: Link header value from response.

        Returns:
            Dict mapping rel type to URL (e.g., {"next": "url", "last": "url"}).
        """
        if not link_header:
            return {}

        links = {}
        # Link header format: <url>; rel="next", <url>; rel="last"
        for part in link_header.split(","):
            match = re.match(r'<([^>]+)>;\s*rel="([^"]+)"', part.strip())
            if match:
                url, rel = match.groups()
                links[rel] = url

        return links

    async def _paginate(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> AsyncIterator[tuple[list[Any], dict[str, Any]]]:
        """Paginate through API results following Link headers.

        Args:
            path: API endpoint path.
            params: Query parameters.

        Yields:
            Tuple of (items list, metadata dict) for each page.
        """
        current_path = path
        current_params = params or {}
        page_num = 1

        while True:
            # Acquire rate limit permission
            if self._rate_limiter:
                await self._rate_limiter.acquire(APIType.REST)

            try:
                # Make request
                response: GitHubResponse = await self._http.get(
                    current_path,
                    params=current_params if page_num == 1 else None,
                )

                # Update rate limiter
                if self._rate_limiter:
                    self._rate_limiter.update(
                        dict(response.headers),
                        APIType.REST,
                    )

                # Handle 404 - return empty for missing resources
                if response.status_code == 404:
                    logger.debug("Resource not found (404): %s", current_path)
                    return

                # Ensure success
                if not response.is_success:
                    logger.error(
                        "Request failed: %s %s - status %d",
                        "GET",
                        current_path,
                        response.status_code,
                    )
                    return

                # Extract data
                data = response.data
                if not isinstance(data, list):
                    # Single object response - wrap in list
                    data = [data] if data else []

                # Build metadata
                metadata = {
                    "endpoint": path,
                    "page": page_num,
                    "status_code": response.status_code,
                    "url": response.url,
                }

                if response.rate_limit:
                    metadata["rate_limit"] = {
                        "limit": response.rate_limit.limit,
                        "remaining": response.rate_limit.remaining,
                        "reset": response.rate_limit.reset.isoformat(),
                    }

                yield data, metadata

                # Check for next page
                link_header = response.headers.get("link")
                links = self._parse_link_header(link_header)

                if "next" not in links:
                    # No more pages
                    break

                # Parse next URL to extract path and params
                next_url = links["next"]
                parsed = urlparse(next_url)
                current_path = parsed.path
                # GitHub's Link header includes full URL with query string
                # We'll use the full path from the next URL
                current_params = {}
                page_num += 1

                logger.debug("Following pagination to page %d", page_num)

            finally:
                # Always release rate limiter
                if self._rate_limiter:
                    self._rate_limiter.release()

    async def list_org_repos(
        self,
        org: str,
        repo_type: str = "all",
    ) -> AsyncIterator[tuple[list[Any], dict[str, Any]]]:
        """List all repositories for an organization.

        Args:
            org: Organization name.
            repo_type: Type of repos: "all", "public", "private", "forks", "sources", "member".

        Yields:
            Tuple of (repos list, metadata dict) for each page.
        """
        path = f"/orgs/{org}/repos"
        params = {
            "type": repo_type,
            "per_page": 100,
            "sort": "full_name",
        }

        logger.info("Fetching repositories for org: %s (type=%s)", org, repo_type)

        async for items, metadata in self._paginate(path, params):
            yield items, metadata

    async def list_user_repos(
        self,
        username: str,
        repo_type: str = "owner",
    ) -> AsyncIterator[tuple[list[Any], dict[str, Any]]]:
        """List all repositories for a user.

        Args:
            username: GitHub username.
            repo_type: Type of repos: "all", "owner", "member".

        Yields:
            Tuple of (repos list, metadata dict) for each page.
        """
        path = f"/users/{username}/repos"
        params = {
            "type": repo_type,
            "per_page": 100,
            "sort": "full_name",
        }

        logger.info("Fetching repositories for user: %s (type=%s)", username, repo_type)

        async for items, metadata in self._paginate(path, params):
            yield items, metadata

    async def get_repo(self, owner: str, repo: str) -> dict[str, Any] | None:
        """Get single repository details.

        Args:
            owner: Repository owner.
            repo: Repository name.

        Returns:
            Repository data dict, or None if not found.
        """
        path = f"/repos/{owner}/{repo}"

        if self._rate_limiter:
            await self._rate_limiter.acquire(APIType.REST)

        try:
            response = await self._http.get(path)

            if self._rate_limiter:
                self._rate_limiter.update(dict(response.headers), APIType.REST)

            if response.status_code == 404:
                return None

            if response.is_success:
                return cast("dict[str, Any]", response.data)

            logger.error("Failed to fetch repo %s/%s: %d", owner, repo, response.status_code)
            return None

        finally:
            if self._rate_limiter:
                self._rate_limiter.release()

    async def list_pulls(
        self,
        owner: str,
        repo: str,
        state: str = "all",
        since: str | None = None,
    ) -> AsyncIterator[tuple[list[Any], dict[str, Any]]]:
        """List pull requests for a repository.

        Args:
            owner: Repository owner.
            repo: Repository name.
            state: PR state: "open", "closed", "all".
            since: ISO 8601 timestamp to filter PRs updated after this date.

        Yields:
            Tuple of (PRs list, metadata dict) for each page.
        """
        path = f"/repos/{owner}/{repo}/pulls"
        params: dict[str, Any] = {
            "state": state,
            "per_page": 100,
            "sort": "updated",
            "direction": "desc",
        }

        if since:
            # Note: GitHub doesn't support 'since' for PRs directly
            # We'll fetch and filter client-side if needed
            # For now, just fetch all and let caller filter
            pass

        logger.info(
            "Fetching pull requests for %s/%s (state=%s, since=%s)",
            owner,
            repo,
            state,
            since or "none",
        )

        async for items, metadata in self._paginate(path, params):
            yield items, metadata

    async def list_issues(
        self,
        owner: str,
        repo: str,
        state: str = "all",
        since: str | None = None,
    ) -> AsyncIterator[tuple[list[Any], dict[str, Any]]]:
        """List issues for a repository.

        Args:
            owner: Repository owner.
            repo: Repository name.
            state: Issue state: "open", "closed", "all".
            since: ISO 8601 timestamp to filter issues updated after this date.

        Yields:
            Tuple of (issues list, metadata dict) for each page.
        """
        path = f"/repos/{owner}/{repo}/issues"
        params: dict[str, Any] = {
            "state": state,
            "per_page": 100,
            "sort": "updated",
            "direction": "desc",
        }

        if since:
            params["since"] = since

        logger.info(
            "Fetching issues for %s/%s (state=%s, since=%s)",
            owner,
            repo,
            state,
            since or "none",
        )

        async for items, metadata in self._paginate(path, params):
            yield items, metadata

    async def list_reviews(
        self,
        owner: str,
        repo: str,
        pull_number: int,
    ) -> AsyncIterator[tuple[list[Any], dict[str, Any]]]:
        """List reviews for a pull request.

        Args:
            owner: Repository owner.
            repo: Repository name.
            pull_number: Pull request number.

        Yields:
            Tuple of (reviews list, metadata dict) for each page.
        """
        path = f"/repos/{owner}/{repo}/pulls/{pull_number}/reviews"
        params = {"per_page": 100}

        logger.debug("Fetching reviews for %s/%s#%d", owner, repo, pull_number)

        async for items, metadata in self._paginate(path, params):
            yield items, metadata

    async def list_issue_comments(
        self,
        owner: str,
        repo: str,
        issue_number: int,
    ) -> AsyncIterator[tuple[list[Any], dict[str, Any]]]:
        """List comments on an issue.

        Args:
            owner: Repository owner.
            repo: Repository name.
            issue_number: Issue number.

        Yields:
            Tuple of (comments list, metadata dict) for each page.
        """
        path = f"/repos/{owner}/{repo}/issues/{issue_number}/comments"
        params = {"per_page": 100}

        logger.debug("Fetching comments for issue %s/%s#%d", owner, repo, issue_number)

        async for items, metadata in self._paginate(path, params):
            yield items, metadata

    async def list_review_comments(
        self,
        owner: str,
        repo: str,
        pull_number: int,
    ) -> AsyncIterator[tuple[list[Any], dict[str, Any]]]:
        """List review comments on a pull request.

        Args:
            owner: Repository owner.
            repo: Repository name.
            pull_number: Pull request number.

        Yields:
            Tuple of (review comments list, metadata dict) for each page.
        """
        path = f"/repos/{owner}/{repo}/pulls/{pull_number}/comments"
        params = {"per_page": 100}

        logger.debug("Fetching review comments for %s/%s#%d", owner, repo, pull_number)

        async for items, metadata in self._paginate(path, params):
            yield items, metadata

    async def list_commits(
        self,
        owner: str,
        repo: str,
        since: str | None = None,
        until: str | None = None,
    ) -> AsyncIterator[tuple[list[Any], dict[str, Any]]]:
        """List commits for a repository.

        Args:
            owner: Repository owner.
            repo: Repository name.
            since: ISO 8601 timestamp to filter commits after this date.
            until: ISO 8601 timestamp to filter commits before this date.

        Yields:
            Tuple of (commits list, metadata dict) for each page.
        """
        path = f"/repos/{owner}/{repo}/commits"
        params: dict[str, Any] = {"per_page": 100}

        if since:
            params["since"] = since
        if until:
            params["until"] = until

        logger.info(
            "Fetching commits for %s/%s (since=%s, until=%s)",
            owner,
            repo,
            since or "none",
            until or "none",
        )

        async for items, metadata in self._paginate(path, params):
            yield items, metadata

    async def get_rate_limit(self) -> dict[str, Any] | None:
        """Get current rate limit status.

        Returns:
            Rate limit data dict with resources breakdown, or None on error.
        """
        path = "/rate_limit"

        if self._rate_limiter:
            await self._rate_limiter.acquire(APIType.REST)

        try:
            response = await self._http.get(path)

            if self._rate_limiter:
                self._rate_limiter.update(dict(response.headers), APIType.REST)

            if response.is_success:
                return cast("dict[str, Any]", response.data)

            logger.error("Failed to fetch rate limit: %d", response.status_code)
            return None

        finally:
            if self._rate_limiter:
                self._rate_limiter.release()
