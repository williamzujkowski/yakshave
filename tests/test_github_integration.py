"""Integration tests for async GitHub client methods using respx.

Tests async HTTP calls with mocked httpx responses.
Uses respx to mock GitHub API endpoints and verify correct behavior.
"""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from gh_year_end.github.auth import GitHubAuth
from gh_year_end.github.graphql import GraphQLClient, GraphQLError
from gh_year_end.github.http import GitHubClient, GitHubHTTPError, RateLimitExceeded
from gh_year_end.github.ratelimit import AdaptiveRateLimiter
from gh_year_end.github.rest import RestClient

# Valid test token format (meets minimum length requirement)
TEST_TOKEN = "ghp_test1234567890abcdefghijklmnopqrst"


class TestGitHubClientAsync:
    """Tests for GitHubClient async request methods."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_request_get_success(self) -> None:
        """Test successful GET request with rate limit headers."""
        # Mock GitHub API response
        route = respx.get("https://api.github.com/user").mock(
            return_value=httpx.Response(
                200,
                json={"login": "testuser", "id": 12345},
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                    "x-ratelimit-used": "1",
                    "x-ratelimit-resource": "core",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as client:
            response = await client.get("/user")

            assert response.is_success
            assert response.status_code == 200
            assert response.data["login"] == "testuser"
            assert response.data["id"] == 12345
            assert response.rate_limit is not None
            assert response.rate_limit.limit == 5000
            assert response.rate_limit.remaining == 4999

            # Verify rate limit state was updated
            assert client.rate_limit_state.requests_made == 1
            assert client.rate_limit_state.last_rate_limit is not None

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_request_post_with_json(self) -> None:
        """Test POST request with JSON payload."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={"data": {"viewer": {"login": "testuser"}}},
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4998",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as client:
            response = await client.post("/graphql", json={"query": "{ viewer { login } }"})

            assert response.is_success
            assert response.status_code == 200
            assert response.data["data"]["viewer"]["login"] == "testuser"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_request_404_not_found(self) -> None:
        """Test 404 response handling."""
        route = respx.get("https://api.github.com/repos/owner/nonexistent").mock(
            return_value=httpx.Response(
                404,
                json={"message": "Not Found"},
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as client:
            response = await client.get("/repos/owner/nonexistent")

            # 404 is not raised, but is_success returns False
            assert not response.is_success
            assert response.status_code == 404

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_request_429_rate_limit_retry(self) -> None:
        """Test rate limit handling with retry-after header."""
        # First request returns 429, second succeeds
        route = respx.get("https://api.github.com/user").mock(
            side_effect=[
                httpx.Response(
                    429,
                    json={"message": "API rate limit exceeded"},
                    headers={
                        "retry-after": "1",
                        "x-ratelimit-limit": "5000",
                        "x-ratelimit-remaining": "0",
                        "x-ratelimit-reset": "1234567890",
                    },
                ),
                httpx.Response(
                    200,
                    json={"login": "testuser"},
                    headers={
                        "x-ratelimit-limit": "5000",
                        "x-ratelimit-remaining": "5000",
                        "x-ratelimit-reset": "1234567950",
                    },
                ),
            ]
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as client:
            response = await client.get("/user")

            # Should succeed after retry
            assert response.is_success
            assert response.status_code == 200
            assert response.data["login"] == "testuser"

        # Should have made 2 requests (initial + retry)
        assert route.call_count == 2

    @pytest.mark.asyncio
    @respx.mock
    async def test_request_500_server_error_retry(self) -> None:
        """Test server error retry with exponential backoff."""
        # First request fails with 500, second succeeds
        route = respx.get("https://api.github.com/user").mock(
            side_effect=[
                httpx.Response(500, json={"message": "Internal Server Error"}),
                httpx.Response(
                    200,
                    json={"login": "testuser"},
                    headers={
                        "x-ratelimit-limit": "5000",
                        "x-ratelimit-remaining": "4999",
                        "x-ratelimit-reset": "1234567890",
                    },
                ),
            ]
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as client:
            response = await client.get("/user")

            assert response.is_success
            assert response.status_code == 200

        # Should have made 2 requests
        assert route.call_count == 2

    @pytest.mark.asyncio
    @respx.mock
    async def test_request_max_retries_exceeded(self) -> None:
        """Test max retries exceeded raises GitHubHTTPError."""
        # Return 500 for all requests
        route = respx.get("https://api.github.com/user").mock(
            return_value=httpx.Response(500, json={"message": "Internal Server Error"})
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth, max_retries=2) as client:
            with pytest.raises(GitHubHTTPError, match="Max retries"):
                await client.get("/user")

        # Should have made initial + 2 retries = 3 requests
        assert route.call_count == 3

    @pytest.mark.asyncio
    @respx.mock
    async def test_request_timeout_retry(self) -> None:
        """Test timeout exception triggers retry."""
        # First request times out, second succeeds
        route = respx.get("https://api.github.com/user").mock(
            side_effect=[
                httpx.TimeoutException("Request timeout"),
                httpx.Response(
                    200,
                    json={"login": "testuser"},
                    headers={
                        "x-ratelimit-limit": "5000",
                        "x-ratelimit-remaining": "4999",
                        "x-ratelimit-reset": "1234567890",
                    },
                ),
            ]
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as client:
            response = await client.get("/user")

            assert response.is_success
            assert response.data["login"] == "testuser"

        assert route.call_count == 2

    @pytest.mark.asyncio
    @respx.mock
    async def test_request_client_error_raises(self) -> None:
        """Test 4xx client errors (except 404) raise exception."""
        route = respx.get("https://api.github.com/user").mock(
            return_value=httpx.Response(
                401,
                json={"message": "Bad credentials"},
                headers={
                    "x-ratelimit-limit": "60",
                    "x-ratelimit-remaining": "59",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token="ghp_invalid12345678901234567890")
        async with GitHubClient(auth=auth) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await client.get("/user")

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_request_rate_limit_exceeded_exception(self) -> None:
        """Test RateLimitExceeded raised when max retries exhausted."""
        future_reset = int(datetime.now(UTC).timestamp()) + 3600
        route = respx.get("https://api.github.com/user").mock(
            return_value=httpx.Response(
                403,
                json={"message": "API rate limit exceeded"},
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "0",
                    "x-ratelimit-reset": str(future_reset),
                    "x-ratelimit-used": "5000",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth, max_retries=1) as client:
            with pytest.raises(RateLimitExceeded, match="Rate limit exceeded"):
                await client.get("/user")

        # Should make initial + 1 retry = 2 requests
        assert route.call_count == 2


class TestRestClientPagination:
    """Tests for RestClient pagination methods."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_paginate_single_page(self) -> None:
        """Test pagination with single page of results."""
        route = respx.get("https://api.github.com/orgs/testorg/repos").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"id": 1, "name": "repo1", "full_name": "testorg/repo1"},
                    {"id": 2, "name": "repo2", "full_name": "testorg/repo2"},
                ],
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            items = []
            async for page_items, metadata in rest.list_org_repos("testorg"):
                items.extend(page_items)
                assert metadata["status_code"] == 200
                assert metadata["page"] >= 1

            assert len(items) == 2
            assert items[0]["name"] == "repo1"
            assert items[1]["name"] == "repo2"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_paginate_multiple_pages(self) -> None:
        """Test pagination across multiple pages with Link headers."""
        # First page with Link header
        respx.get("https://api.github.com/orgs/testorg/repos").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"id": 1, "name": "repo1", "full_name": "testorg/repo1"},
                ],
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                    "link": '<https://api.github.com/orgs/testorg/repos?page=2>; rel="next"',
                },
            )
        )

        # Second page without Link header (last page)
        respx.get("https://api.github.com/orgs/testorg/repos?page=2").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"id": 2, "name": "repo2", "full_name": "testorg/repo2"},
                ],
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4998",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            pages = []
            async for page_items, metadata in rest.list_org_repos("testorg"):
                pages.append((page_items, metadata))

            assert len(pages) == 2
            assert len(pages[0][0]) == 1
            assert len(pages[1][0]) == 1
            assert pages[0][0][0]["name"] == "repo1"
            assert pages[1][0][0]["name"] == "repo2"

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_user_repos(self) -> None:
        """Test listing user repositories."""
        route = respx.get("https://api.github.com/users/testuser/repos").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"id": 1, "name": "repo1", "full_name": "testuser/repo1"},
                ],
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            items = []
            async for page_items, _ in rest.list_user_repos("testuser"):
                items.extend(page_items)

            assert len(items) == 1
            assert items[0]["name"] == "repo1"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_repo(self) -> None:
        """Test getting single repository details."""
        route = respx.get("https://api.github.com/repos/owner/repo").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": 123,
                    "name": "repo",
                    "full_name": "owner/repo",
                    "description": "Test repo",
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            repo = await rest.get_repo("owner", "repo")

            assert repo is not None
            assert repo["id"] == 123
            assert repo["name"] == "repo"
            assert repo["description"] == "Test repo"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_repo_not_found(self) -> None:
        """Test getting non-existent repository returns None."""
        route = respx.get("https://api.github.com/repos/owner/nonexistent").mock(
            return_value=httpx.Response(
                404,
                json={"message": "Not Found"},
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            repo = await rest.get_repo("owner", "nonexistent")

            assert repo is None

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_pulls(self) -> None:
        """Test listing pull requests."""
        route = respx.get("https://api.github.com/repos/owner/repo/pulls").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": 1,
                        "number": 42,
                        "title": "Test PR",
                        "state": "open",
                    },
                ],
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            items = []
            async for page_items, _ in rest.list_pulls("owner", "repo"):
                items.extend(page_items)

            assert len(items) == 1
            assert items[0]["number"] == 42
            assert items[0]["title"] == "Test PR"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_issues(self) -> None:
        """Test listing issues."""
        route = respx.get("https://api.github.com/repos/owner/repo/issues").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": 1,
                        "number": 10,
                        "title": "Test Issue",
                        "state": "open",
                    },
                ],
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            items = []
            async for page_items, _ in rest.list_issues("owner", "repo"):
                items.extend(page_items)

            assert len(items) == 1
            assert items[0]["number"] == 10

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_rate_limit(self) -> None:
        """Test getting rate limit status."""
        route = respx.get("https://api.github.com/rate_limit").mock(
            return_value=httpx.Response(
                200,
                json={
                    "resources": {
                        "core": {
                            "limit": 5000,
                            "remaining": 4999,
                            "reset": 1234567890,
                            "used": 1,
                        },
                        "graphql": {
                            "limit": 5000,
                            "remaining": 5000,
                            "reset": 1234567890,
                            "used": 0,
                        },
                    },
                    "rate": {
                        "limit": 5000,
                        "remaining": 4999,
                        "reset": 1234567890,
                        "used": 1,
                    },
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            rate_limit = await rest.get_rate_limit()

            assert rate_limit is not None
            assert rate_limit["resources"]["core"]["limit"] == 5000
            assert rate_limit["resources"]["core"]["remaining"] == 4999

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_rest_client_with_rate_limiter(self) -> None:
        """Test RestClient integration with AdaptiveRateLimiter."""
        route = respx.get("https://api.github.com/orgs/testorg/repos").mock(
            return_value=httpx.Response(
                200,
                json=[{"id": 1, "name": "repo1"}],
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        rate_limiter = AdaptiveRateLimiter()

        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client, rate_limiter)

            items = []
            async for page_items, _ in rest.list_org_repos("testorg"):
                items.extend(page_items)

            assert len(items) == 1

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_reviews(self) -> None:
        """Test listing reviews for a pull request."""
        route = respx.get("https://api.github.com/repos/owner/repo/pulls/42/reviews").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": 1,
                        "user": {"login": "reviewer1"},
                        "body": "LGTM",
                        "state": "APPROVED",
                        "submitted_at": "2024-01-01T12:00:00Z",
                    },
                    {
                        "id": 2,
                        "user": {"login": "reviewer2"},
                        "body": "Needs changes",
                        "state": "CHANGES_REQUESTED",
                        "submitted_at": "2024-01-02T12:00:00Z",
                    },
                ],
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            items = []
            async for page_items, _ in rest.list_reviews("owner", "repo", 42):
                items.extend(page_items)

            assert len(items) == 2
            assert items[0]["state"] == "APPROVED"
            assert items[1]["state"] == "CHANGES_REQUESTED"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_issue_comments(self) -> None:
        """Test listing comments on an issue."""
        route = respx.get("https://api.github.com/repos/owner/repo/issues/10/comments").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": 1,
                        "user": {"login": "commenter1"},
                        "body": "Great idea!",
                        "created_at": "2024-01-01T12:00:00Z",
                    },
                    {
                        "id": 2,
                        "user": {"login": "commenter2"},
                        "body": "I agree",
                        "created_at": "2024-01-02T12:00:00Z",
                    },
                ],
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            items = []
            async for page_items, _ in rest.list_issue_comments("owner", "repo", 10):
                items.extend(page_items)

            assert len(items) == 2
            assert items[0]["body"] == "Great idea!"
            assert items[1]["user"]["login"] == "commenter2"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_review_comments(self) -> None:
        """Test listing review comments on a pull request."""
        route = respx.get("https://api.github.com/repos/owner/repo/pulls/42/comments").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": 1,
                        "user": {"login": "reviewer1"},
                        "body": "Consider refactoring this",
                        "path": "src/main.py",
                        "position": 10,
                        "created_at": "2024-01-01T12:00:00Z",
                    },
                ],
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            items = []
            async for page_items, _ in rest.list_review_comments("owner", "repo", 42):
                items.extend(page_items)

            assert len(items) == 1
            assert items[0]["path"] == "src/main.py"
            assert items[0]["body"] == "Consider refactoring this"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_commits(self) -> None:
        """Test listing commits for a repository."""
        route = respx.get("https://api.github.com/repos/owner/repo/commits").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "sha": "abc123",
                        "commit": {
                            "author": {
                                "name": "Test Author",
                                "email": "test@example.com",
                                "date": "2024-01-01T12:00:00Z",
                            },
                            "message": "Initial commit",
                        },
                        "author": {"login": "testuser"},
                    },
                    {
                        "sha": "def456",
                        "commit": {
                            "author": {
                                "name": "Test Author",
                                "email": "test@example.com",
                                "date": "2024-01-02T12:00:00Z",
                            },
                            "message": "Second commit",
                        },
                        "author": {"login": "testuser"},
                    },
                ],
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            items = []
            async for page_items, _ in rest.list_commits(
                "owner", "repo", since="2024-01-01T00:00:00Z", until="2024-12-31T23:59:59Z"
            ):
                items.extend(page_items)

            assert len(items) == 2
            assert items[0]["sha"] == "abc123"
            assert items[1]["commit"]["message"] == "Second commit"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_repository_tree(self) -> None:
        """Test getting repository tree for a commit SHA."""
        route = respx.get("https://api.github.com/repos/owner/repo/git/trees/abc123").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sha": "abc123",
                    "url": "https://api.github.com/repos/owner/repo/git/trees/abc123",
                    "tree": [
                        {
                            "path": "README.md",
                            "mode": "100644",
                            "type": "blob",
                            "sha": "file123",
                            "size": 1024,
                        },
                        {
                            "path": "src",
                            "mode": "040000",
                            "type": "tree",
                            "sha": "tree456",
                        },
                    ],
                    "truncated": False,
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            tree = await rest.get_repository_tree("owner", "repo", "abc123", recursive=True)

            assert tree is not None
            assert tree["sha"] == "abc123"
            assert len(tree["tree"]) == 2
            assert tree["tree"][0]["path"] == "README.md"
            assert tree["tree"][1]["type"] == "tree"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_repository_tree_not_found(self) -> None:
        """Test getting repository tree for non-existent SHA."""
        route = respx.get("https://api.github.com/repos/owner/repo/git/trees/invalid").mock(
            return_value=httpx.Response(
                404,
                json={"message": "Not Found"},
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            tree = await rest.get_repository_tree("owner", "repo", "invalid")

            assert tree is None

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_branch_protection(self) -> None:
        """Test getting branch protection rules."""
        route = respx.get(
            "https://api.github.com/repos/owner/repo/branches/main/protection"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "url": "https://api.github.com/repos/owner/repo/branches/main/protection",
                    "required_status_checks": {
                        "strict": True,
                        "contexts": ["ci/test"],
                    },
                    "required_pull_request_reviews": {
                        "dismiss_stale_reviews": True,
                        "require_code_owner_reviews": True,
                        "required_approving_review_count": 2,
                    },
                    "enforce_admins": {"enabled": True},
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            protection, status = await rest.get_branch_protection("owner", "repo", "main")

            assert status == 200
            assert protection is not None
            assert protection["required_status_checks"]["strict"] is True
            assert protection["required_pull_request_reviews"]["required_approving_review_count"] == 2

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_branch_protection_not_set(self) -> None:
        """Test getting branch protection when not set (404)."""
        route = respx.get(
            "https://api.github.com/repos/owner/repo/branches/main/protection"
        ).mock(
            return_value=httpx.Response(
                404,
                json={"message": "Branch not protected"},
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            protection, status = await rest.get_branch_protection("owner", "repo", "main")

            assert status == 404
            assert protection is None

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_branch_protection_no_permission(self) -> None:
        """Test getting branch protection with no permission (403)."""
        route = respx.get(
            "https://api.github.com/repos/owner/repo/branches/main/protection"
        ).mock(
            return_value=httpx.Response(
                403,
                json={"message": "Forbidden"},
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            protection, status = await rest.get_branch_protection("owner", "repo", "main")

            assert status == 403
            assert protection is None

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_check_vulnerability_alerts_enabled(self) -> None:
        """Test checking vulnerability alerts when enabled (204)."""
        route = respx.get(
            "https://api.github.com/repos/owner/repo/vulnerability-alerts"
        ).mock(
            return_value=httpx.Response(
                204,
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            enabled = await rest.check_vulnerability_alerts("owner", "repo")

            assert enabled is True

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_check_vulnerability_alerts_disabled(self) -> None:
        """Test checking vulnerability alerts when disabled (404)."""
        route = respx.get(
            "https://api.github.com/repos/owner/repo/vulnerability-alerts"
        ).mock(
            return_value=httpx.Response(
                404,
                json={"message": "Not Found"},
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            enabled = await rest.check_vulnerability_alerts("owner", "repo")

            assert enabled is False

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_check_vulnerability_alerts_no_permission(self) -> None:
        """Test checking vulnerability alerts with no permission (403)."""
        route = respx.get(
            "https://api.github.com/repos/owner/repo/vulnerability-alerts"
        ).mock(
            return_value=httpx.Response(
                403,
                json={"message": "Forbidden"},
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            enabled = await rest.check_vulnerability_alerts("owner", "repo")

            assert enabled is None

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_repo_security_analysis(self) -> None:
        """Test getting repository with security analysis field."""
        route = respx.get("https://api.github.com/repos/owner/repo").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": 123,
                    "name": "repo",
                    "full_name": "owner/repo",
                    "security_and_analysis": {
                        "secret_scanning": {"status": "enabled"},
                        "secret_scanning_push_protection": {"status": "enabled"},
                        "dependabot_security_updates": {"status": "enabled"},
                    },
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            repo = await rest.get_repo_security_analysis("owner", "repo")

            assert repo is not None
            assert repo["security_and_analysis"]["secret_scanning"]["status"] == "enabled"
            assert (
                repo["security_and_analysis"]["dependabot_security_updates"]["status"]
                == "enabled"
            )

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_repo_security_analysis_not_found(self) -> None:
        """Test getting security analysis for non-existent repo."""
        route = respx.get("https://api.github.com/repos/owner/nonexistent").mock(
            return_value=httpx.Response(
                404,
                json={"message": "Not Found"},
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            rest = RestClient(http_client)

            repo = await rest.get_repo_security_analysis("owner", "nonexistent")

            assert repo is None

        assert route.called


class TestGraphQLClient:
    """Tests for GraphQLClient execute and paginate methods."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_execute_query_success(self) -> None:
        """Test successful GraphQL query execution."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "viewer": {
                            "login": "testuser",
                            "id": "MDQ6VXNlcjEyMzQ1",
                            "name": "Test User",
                        }
                    }
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            data = await graphql.execute("{ viewer { login id name } }")

            assert data["viewer"]["login"] == "testuser"
            assert data["viewer"]["id"] == "MDQ6VXNlcjEyMzQ1"
            assert data["viewer"]["name"] == "Test User"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_execute_query_with_variables(self) -> None:
        """Test GraphQL query with variables."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "repository": {
                            "name": "testrepo",
                            "owner": {"login": "testowner"},
                        }
                    }
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            query = """
            query($owner: String!, $name: String!) {
              repository(owner: $owner, name: $name) {
                name
                owner { login }
              }
            }
            """
            variables = {"owner": "testowner", "name": "testrepo"}

            data = await graphql.execute(query, variables)

            assert data["repository"]["name"] == "testrepo"
            assert data["repository"]["owner"]["login"] == "testowner"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_execute_query_with_errors(self) -> None:
        """Test GraphQL query that returns errors."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "errors": [
                        {
                            "message": "Field 'invalid' doesn't exist on type 'Repository'",
                            "type": "NOT_FOUND",
                        }
                    ]
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            with pytest.raises(GraphQLError, match="doesn't exist"):
                await graphql.execute("{ repository { invalid } }")

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_execute_query_http_error(self) -> None:
        """Test GraphQL query with HTTP error."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                401,
                json={"message": "Bad credentials"},
                headers={
                    "x-ratelimit-limit": "60",
                    "x-ratelimit-remaining": "59",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token="ghp_invalid12345678901234567890")
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            with pytest.raises(GraphQLError, match="HTTP 401"):
                await graphql.execute("{ viewer { login } }")

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_paginate_query(self) -> None:
        """Test GraphQL pagination through connection."""
        # First page
        respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "repository": {
                            "pullRequests": {
                                "pageInfo": {
                                    "hasNextPage": True,
                                    "endCursor": "cursor123",
                                },
                                "edges": [
                                    {
                                        "node": {
                                            "id": "PR_1",
                                            "number": 1,
                                            "title": "First PR",
                                        }
                                    },
                                ],
                            }
                        }
                    }
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "repository": {
                            "pullRequests": {
                                "pageInfo": {
                                    "hasNextPage": False,
                                    "endCursor": "cursor456",
                                },
                                "edges": [
                                    {
                                        "node": {
                                            "id": "PR_2",
                                            "number": 2,
                                            "title": "Second PR",
                                        }
                                    },
                                ],
                            }
                        }
                    }
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4998",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            query = """
            query($owner: String!, $name: String!, $after: String, $first: Int!) {
              repository(owner: $owner, name: $name) {
                pullRequests(first: $first, after: $after) {
                  pageInfo {
                    hasNextPage
                    endCursor
                  }
                  edges {
                    node {
                      id
                      number
                      title
                    }
                  }
                }
              }
            }
            """

            nodes = []
            async for node in graphql.paginate(
                query,
                {"owner": "owner", "name": "repo"},
                ["repository", "pullRequests"],
                page_size=1,
            ):
                nodes.append(node)

            assert len(nodes) == 2
            assert nodes[0]["number"] == 1
            assert nodes[0]["title"] == "First PR"
            assert nodes[1]["number"] == 2
            assert nodes[1]["title"] == "Second PR"

    @pytest.mark.asyncio
    @respx.mock
    async def test_query_repository_info(self) -> None:
        """Test querying repository information."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "repository": {
                            "id": "R_123",
                            "name": "testrepo",
                            "nameWithOwner": "owner/testrepo",
                            "description": "Test repository",
                            "isPrivate": False,
                            "stargazerCount": 42,
                        }
                    }
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            repo = await graphql.query_repository_info("owner", "testrepo")

            assert repo["id"] == "R_123"
            assert repo["name"] == "testrepo"
            assert repo["nameWithOwner"] == "owner/testrepo"
            assert repo["stargazerCount"] == 42

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_query_user_info(self) -> None:
        """Test querying user information."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "user": {
                            "id": "U_123",
                            "login": "testuser",
                            "name": "Test User",
                            "email": "test@example.com",
                            "bio": "Test bio",
                        }
                    }
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            user = await graphql.query_user_info("testuser")

            assert user["id"] == "U_123"
            assert user["login"] == "testuser"
            assert user["name"] == "Test User"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_graphql_with_rate_limiter(self) -> None:
        """Test GraphQLClient integration with AdaptiveRateLimiter."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={"data": {"viewer": {"login": "testuser"}}},
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        rate_limiter = AdaptiveRateLimiter()

        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client, rate_limiter)

            data = await graphql.execute("{ viewer { login } }")

            assert data["viewer"]["login"] == "testuser"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_query_pull_requests(self) -> None:
        """Test querying pull requests for a repository."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "repository": {
                            "pullRequests": {
                                "pageInfo": {"hasNextPage": False, "endCursor": "cursor123"},
                                "totalCount": 2,
                                "edges": [
                                    {
                                        "node": {
                                            "id": "PR_1",
                                            "number": 1,
                                            "title": "First PR",
                                            "state": "MERGED",
                                        }
                                    },
                                    {
                                        "node": {
                                            "id": "PR_2",
                                            "number": 2,
                                            "title": "Second PR",
                                            "state": "OPEN",
                                        }
                                    },
                                ],
                            }
                        }
                    }
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            result = await graphql.query_pull_requests("owner", "repo")

            assert result["totalCount"] == 2
            assert len(result["edges"]) == 2
            assert result["edges"][0]["node"]["title"] == "First PR"
            assert result["pageInfo"]["hasNextPage"] is False

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_query_issues(self) -> None:
        """Test querying issues for a repository."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "repository": {
                            "issues": {
                                "pageInfo": {"hasNextPage": False, "endCursor": "cursor456"},
                                "totalCount": 3,
                                "edges": [
                                    {
                                        "node": {
                                            "id": "I_1",
                                            "number": 1,
                                            "title": "Bug report",
                                            "state": "CLOSED",
                                        }
                                    },
                                    {
                                        "node": {
                                            "id": "I_2",
                                            "number": 2,
                                            "title": "Feature request",
                                            "state": "OPEN",
                                        }
                                    },
                                ],
                            }
                        }
                    }
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            result = await graphql.query_issues("owner", "repo")

            assert result["totalCount"] == 3
            assert len(result["edges"]) == 2
            assert result["edges"][0]["node"]["title"] == "Bug report"
            assert result["edges"][1]["node"]["state"] == "OPEN"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_paginate_pull_requests(self) -> None:
        """Test paginating through all pull requests."""
        # First page
        respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "repository": {
                            "pullRequests": {
                                "pageInfo": {"hasNextPage": True, "endCursor": "cursor1"},
                                "edges": [
                                    {
                                        "node": {
                                            "id": "PR_1",
                                            "number": 1,
                                            "title": "First PR",
                                        }
                                    }
                                ],
                            }
                        }
                    }
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "repository": {
                            "pullRequests": {
                                "pageInfo": {"hasNextPage": False, "endCursor": "cursor2"},
                                "edges": [
                                    {
                                        "node": {
                                            "id": "PR_2",
                                            "number": 2,
                                            "title": "Second PR",
                                        }
                                    }
                                ],
                            }
                        }
                    }
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4998",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            prs = []
            async for pr in graphql.paginate_pull_requests("owner", "repo", page_size=1):
                prs.append(pr)

            assert len(prs) == 2
            assert prs[0]["number"] == 1
            assert prs[1]["number"] == 2

    @pytest.mark.asyncio
    @respx.mock
    async def test_paginate_issues(self) -> None:
        """Test paginating through all issues."""
        # First page
        respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "repository": {
                            "issues": {
                                "pageInfo": {"hasNextPage": True, "endCursor": "cursor1"},
                                "edges": [
                                    {
                                        "node": {
                                            "id": "I_1",
                                            "number": 1,
                                            "title": "First Issue",
                                        }
                                    }
                                ],
                            }
                        }
                    }
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "repository": {
                            "issues": {
                                "pageInfo": {"hasNextPage": False, "endCursor": "cursor2"},
                                "edges": [
                                    {
                                        "node": {
                                            "id": "I_2",
                                            "number": 2,
                                            "title": "Second Issue",
                                        }
                                    }
                                ],
                            }
                        }
                    }
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4998",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            issues = []
            async for issue in graphql.paginate_issues("owner", "repo", page_size=1):
                issues.append(issue)

            assert len(issues) == 2
            assert issues[0]["number"] == 1
            assert issues[1]["number"] == 2

    @pytest.mark.asyncio
    @respx.mock
    async def test_query_org_info(self) -> None:
        """Test querying organization information."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "organization": {
                            "id": "O_123",
                            "login": "testorg",
                            "name": "Test Organization",
                            "email": "contact@testorg.com",
                            "description": "A test organization",
                            "websiteUrl": "https://testorg.com",
                            "location": "San Francisco, CA",
                            "isVerified": True,
                            "members": {"totalCount": 50},
                            "repositories": {"totalCount": 100},
                            "teams": {"totalCount": 10},
                        }
                    }
                },
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            org = await graphql.query_org_info("testorg")

            assert org["id"] == "O_123"
            assert org["login"] == "testorg"
            assert org["name"] == "Test Organization"
            assert org["isVerified"] is True
            assert org["members"]["totalCount"] == 50

        assert route.called


class TestDiscoverRepos:
    """Tests for discover_repos function."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_discover_repos_org_mode(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        """Test repository discovery in org mode."""
        from gh_year_end.collect.discovery import discover_repos
        from gh_year_end.config import Config
        from gh_year_end.storage.paths import PathManager

        # Mock org repos endpoint
        route = respx.get("https://api.github.com/orgs/testorg/repos").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": 1,
                        "name": "repo1",
                        "full_name": "testorg/repo1",
                        "description": "Test repo 1",
                        "default_branch": "main",
                        "fork": False,
                        "archived": False,
                        "private": False,
                        "visibility": "public",
                        "created_at": "2024-01-01T00:00:00Z",
                        "updated_at": "2024-01-02T00:00:00Z",
                        "pushed_at": "2024-01-03T00:00:00Z",
                        "language": "Python",
                        "stargazers_count": 10,
                        "forks_count": 2,
                        "open_issues_count": 1,
                        "size": 1024,
                    },
                ],
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "testorg"},
                    "discovery": {
                        "include_forks": False,
                        "include_archived": False,
                        "visibility": "public",
                    },
                    "windows": {
                        "year": 2024,
                        "since": "2024-01-01T00:00:00Z",
                        "until": "2025-01-01T00:00:00Z",
                    },
                },
                "storage": {"root": str(tmp_path)},
                "report": {"title": "Test", "output_dir": str(tmp_path / "site")},
            }
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        paths = PathManager(config)
        paths.ensure_directories()

        async with GitHubClient(auth=auth) as client:
            repos = await discover_repos(config, client, paths)

            assert len(repos) == 1
            assert repos[0]["name"] == "repo1"
            assert repos[0]["full_name"] == "testorg/repo1"
            assert repos[0]["language"] == "Python"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_discover_repos_filters_forks(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        """Test that forks are filtered when include_forks is False."""
        from gh_year_end.collect.discovery import discover_repos
        from gh_year_end.config import Config
        from gh_year_end.storage.paths import PathManager

        route = respx.get("https://api.github.com/orgs/testorg/repos").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": 1,
                        "name": "original",
                        "full_name": "testorg/original",
                        "fork": False,
                        "archived": False,
                        "visibility": "public",
                        "default_branch": "main",
                    },
                    {
                        "id": 2,
                        "name": "forked",
                        "full_name": "testorg/forked",
                        "fork": True,
                        "archived": False,
                        "visibility": "public",
                        "default_branch": "main",
                    },
                ],
                headers={
                    "x-ratelimit-limit": "5000",
                    "x-ratelimit-remaining": "4999",
                    "x-ratelimit-reset": "1234567890",
                },
            )
        )

        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "testorg"},
                    "discovery": {
                        "include_forks": False,
                        "include_archived": False,
                        "visibility": "public",
                    },
                    "windows": {
                        "year": 2024,
                        "since": "2024-01-01T00:00:00Z",
                        "until": "2025-01-01T00:00:00Z",
                    },
                },
                "storage": {"root": str(tmp_path)},
                "report": {"title": "Test", "output_dir": str(tmp_path / "site")},
            }
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        paths = PathManager(config)
        paths.ensure_directories()

        async with GitHubClient(auth=auth) as client:
            repos = await discover_repos(config, client, paths)

            # Should only have the non-forked repo
            assert len(repos) == 1
            assert repos[0]["name"] == "original"

        assert route.called
