"""Unit tests for GitHub GraphQL client module.

Tests GraphQL client initialization, query execution, pagination,
error handling, and integration with rate limiter.
"""

import httpx
import pytest
import respx

from gh_year_end.config import RateLimitConfig
from gh_year_end.github.auth import GitHubAuth
from gh_year_end.github.graphql import (
    ISSUES_QUERY,
    ORGANIZATION_INFO_QUERY,
    PULL_REQUESTS_QUERY,
    REPOSITORY_INFO_QUERY,
    USER_INFO_QUERY,
    GraphQLClient,
    GraphQLError,
)
from gh_year_end.github.http import GitHubClient
from gh_year_end.github.ratelimit import AdaptiveRateLimiter


def create_rate_limiter() -> AdaptiveRateLimiter:
    """Create a rate limiter with default config for testing."""
    config = RateLimitConfig()
    return AdaptiveRateLimiter(config)


TEST_TOKEN = "ghp_test1234567890abcdefghijklmnopqrst"


class TestGraphQLError:
    """Tests for GraphQLError exception."""

    def test_error_with_single_message(self) -> None:
        """Test GraphQLError with single error message."""
        errors = [{"message": "Field not found"}]
        error = GraphQLError(errors)

        assert "Field not found" in str(error)
        assert error.errors == errors

    def test_error_with_multiple_messages(self) -> None:
        """Test GraphQLError with multiple error messages."""
        errors = [
            {"message": "Field not found"},
            {"message": "Invalid argument"},
        ]
        error = GraphQLError(errors)

        assert "Field not found" in str(error)
        assert "Invalid argument" in str(error)
        assert error.errors == errors

    def test_error_without_message(self) -> None:
        """Test GraphQLError with missing message field."""
        errors = [{"type": "NOT_FOUND"}]
        error = GraphQLError(errors)

        assert "Unknown error" in str(error)


class TestGraphQLClientInit:
    """Tests for GraphQLClient initialization."""

    def test_init_with_http_client(self) -> None:
        """Test client initialization with HTTP client."""
        auth = GitHubAuth(token=TEST_TOKEN)
        http_client = GitHubClient(auth=auth)
        graphql = GraphQLClient(http_client)

        assert graphql._http == http_client
        assert graphql._rate_limiter is None

    def test_init_with_rate_limiter(self) -> None:
        """Test client initialization with rate limiter."""
        auth = GitHubAuth(token=TEST_TOKEN)
        http_client = GitHubClient(auth=auth)
        rate_limiter = create_rate_limiter()
        graphql = GraphQLClient(http_client, rate_limiter)

        assert graphql._http == http_client
        assert graphql._rate_limiter == rate_limiter


class TestGraphQLClientExecute:
    """Tests for GraphQLClient execute method."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_execute_simple_query(self) -> None:
        """Test executing a simple GraphQL query."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "viewer": {
                            "login": "testuser",
                            "id": "MDQ6VXNlcjEyMzQ1",
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
            data = await graphql.execute("{ viewer { login id } }")

            assert data["viewer"]["login"] == "testuser"
            assert data["viewer"]["id"] == "MDQ6VXNlcjEyMzQ1"

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_execute_query_with_variables(self) -> None:
        """Test executing query with variables."""
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
    async def test_execute_query_with_graphql_errors(self) -> None:
        """Test query execution with GraphQL errors."""
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
    async def test_execute_query_with_http_error(self) -> None:
        """Test query execution with HTTP error."""
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

            # HTTP client raises HTTPStatusError for 4xx/5xx responses
            with pytest.raises((GraphQLError, httpx.HTTPStatusError)):
                await graphql.execute("{ viewer { login } }")

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_execute_query_with_invalid_response(self) -> None:
        """Test query execution with invalid response format."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                text="Not a JSON response",
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

            with pytest.raises(GraphQLError, match="Invalid GraphQL response format"):
                await graphql.execute("{ viewer { login } }")

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_execute_query_with_missing_data(self) -> None:
        """Test query execution with missing data field."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={},
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

            with pytest.raises(GraphQLError, match="Missing data"):
                await graphql.execute("{ viewer { login } }")

        assert route.called


class TestGraphQLClientPagination:
    """Tests for GraphQLClient paginate method."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_paginate_single_page(self) -> None:
        """Test pagination with single page of results."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "repository": {
                            "pullRequests": {
                                "pageInfo": {
                                    "hasNextPage": False,
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
        )

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            query = """
            query($owner: String!, $name: String!, $after: String, $first: Int!) {
              repository(owner: $owner, name: $name) {
                pullRequests(first: $first, after: $after) {
                  pageInfo { hasNextPage endCursor }
                  edges { node { id number title } }
                }
              }
            }
            """

            nodes = []
            async for node in graphql.paginate(
                query,
                {"owner": "owner", "name": "repo"},
                ["repository", "pullRequests"],
                page_size=100,
            ):
                nodes.append(node)

            assert len(nodes) == 1
            assert nodes[0]["number"] == 1

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_paginate_multiple_pages(self) -> None:
        """Test pagination across multiple pages."""
        responses = [
            httpx.Response(
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
            ),
            httpx.Response(
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
            ),
        ]
        respx.post("https://api.github.com/graphql").side_effect = responses

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            query = """
            query($owner: String!, $name: String!, $after: String, $first: Int!) {
              repository(owner: $owner, name: $name) {
                pullRequests(first: $first, after: $after) {
                  pageInfo { hasNextPage endCursor }
                  edges { node { id number title } }
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
            assert nodes[1]["number"] == 2

    @pytest.mark.asyncio
    @respx.mock
    async def test_paginate_empty_results(self) -> None:
        """Test pagination with no results."""
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "repository": {
                            "pullRequests": {
                                "pageInfo": {"hasNextPage": False, "endCursor": None},
                                "edges": [],
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

            query = """
            query($owner: String!, $name: String!, $after: String, $first: Int!) {
              repository(owner: $owner, name: $name) {
                pullRequests(first: $first, after: $after) {
                  pageInfo { hasNextPage endCursor }
                  edges { node { id number title } }
                }
              }
            }
            """

            nodes = []
            async for node in graphql.paginate(
                query,
                {"owner": "owner", "name": "repo"},
                ["repository", "pullRequests"],
                page_size=100,
            ):
                nodes.append(node)

            assert len(nodes) == 0

        assert route.called


class TestGraphQLClientWithRateLimiter:
    """Tests for GraphQLClient integration with rate limiter."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_execute_with_rate_limiter(self) -> None:
        """Test query execution with rate limiter."""
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
        rate_limiter = create_rate_limiter()

        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client, rate_limiter)
            data = await graphql.execute("{ viewer { login } }")

            assert data["viewer"]["login"] == "testuser"

        assert route.called


class TestGraphQLClientPrebuiltQueries:
    """Tests for GraphQLClient prebuilt query methods."""

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
            assert repo["stargazerCount"] == 42

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_query_pull_requests(self) -> None:
        """Test querying pull requests."""
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

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_query_issues(self) -> None:
        """Test querying issues."""
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
                            "isVerified": True,
                            "members": {"totalCount": 50},
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
            assert org["isVerified"] is True
            assert org["members"]["totalCount"] == 50

        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_paginate_pull_requests(self) -> None:
        """Test paginating through all pull requests."""
        responses = [
            httpx.Response(
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
            ),
            httpx.Response(
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
            ),
        ]
        respx.post("https://api.github.com/graphql").side_effect = responses

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
        responses = [
            httpx.Response(
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
            ),
            httpx.Response(
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
            ),
        ]
        respx.post("https://api.github.com/graphql").side_effect = responses

        auth = GitHubAuth(token=TEST_TOKEN)
        async with GitHubClient(auth=auth) as http_client:
            graphql = GraphQLClient(http_client)

            issues = []
            async for issue in graphql.paginate_issues("owner", "repo", page_size=1):
                issues.append(issue)

            assert len(issues) == 2
            assert issues[0]["number"] == 1
            assert issues[1]["number"] == 2


class TestGraphQLQueryTemplates:
    """Tests for GraphQL query template validation."""

    def test_repository_info_query_defined(self) -> None:
        """Test REPOSITORY_INFO_QUERY is defined."""
        assert REPOSITORY_INFO_QUERY is not None
        assert "repository" in REPOSITORY_INFO_QUERY
        assert "$owner: String!" in REPOSITORY_INFO_QUERY
        assert "$name: String!" in REPOSITORY_INFO_QUERY

    def test_pull_requests_query_defined(self) -> None:
        """Test PULL_REQUESTS_QUERY is defined."""
        assert PULL_REQUESTS_QUERY is not None
        assert "pullRequests" in PULL_REQUESTS_QUERY
        assert "$after: String" in PULL_REQUESTS_QUERY
        assert "pageInfo" in PULL_REQUESTS_QUERY

    def test_issues_query_defined(self) -> None:
        """Test ISSUES_QUERY is defined."""
        assert ISSUES_QUERY is not None
        assert "issues" in ISSUES_QUERY
        assert "$after: String" in ISSUES_QUERY
        assert "pageInfo" in ISSUES_QUERY

    def test_user_info_query_defined(self) -> None:
        """Test USER_INFO_QUERY is defined."""
        assert USER_INFO_QUERY is not None
        assert "user" in USER_INFO_QUERY
        assert "$login: String!" in USER_INFO_QUERY

    def test_organization_info_query_defined(self) -> None:
        """Test ORGANIZATION_INFO_QUERY is defined."""
        assert ORGANIZATION_INFO_QUERY is not None
        assert "organization" in ORGANIZATION_INFO_QUERY
        assert "$login: String!" in ORGANIZATION_INFO_QUERY
