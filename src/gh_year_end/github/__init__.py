"""GitHub API clients and utilities."""

from gh_year_end.github.auth import (
    AuthenticationError,
    GitHubAuth,
    get_auth_headers,
    load_github_token,
)
from gh_year_end.github.graphql import (
    GraphQLClient,
    GraphQLError,
)
from gh_year_end.github.http import (
    GitHubClient,
    GitHubHTTPError,
    GitHubResponse,
    HTTPRateLimitState,
    RateLimitExceeded,
    RateLimitInfo,
)
from gh_year_end.github.ratelimit import (
    AdaptiveRateLimiter,
    APIType,
    RateLimitSample,
    RateLimitState,
)
from gh_year_end.github.rest import RestClient

__all__ = [
    "APIType",
    # Adaptive Rate Limiter
    "AdaptiveRateLimiter",
    # Auth
    "AuthenticationError",
    "GitHubAuth",
    # HTTP Client
    "GitHubClient",
    "GitHubHTTPError",
    "GitHubResponse",
    # GraphQL Client
    "GraphQLClient",
    "GraphQLError",
    "HTTPRateLimitState",
    "RateLimitExceeded",
    "RateLimitInfo",
    "RateLimitSample",
    "RateLimitState",
    # REST API Client
    "RestClient",
    "get_auth_headers",
    "load_github_token",
]
