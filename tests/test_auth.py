"""Tests for GitHub authentication module."""

import os
from unittest.mock import patch

import pytest

from gh_year_end.github.auth import (
    AuthenticationError,
    GitHubAuth,
    get_auth_headers,
    load_github_token,
)


class TestGitHubAuthValidTokens:
    """Tests for GitHubAuth initialization with valid tokens."""

    def test_valid_ghp_token(self) -> None:
        """Test initialization with valid ghp_ prefix token."""
        token = "ghp_" + "a" * 36
        auth = GitHubAuth(token=token)
        assert auth.token == token

    def test_valid_gho_token(self) -> None:
        """Test initialization with valid gho_ prefix token."""
        token = "gho_" + "b" * 36
        auth = GitHubAuth(token=token)
        assert auth.token == token

    def test_valid_ghu_token(self) -> None:
        """Test initialization with valid ghu_ prefix token."""
        token = "ghu_" + "c" * 36
        auth = GitHubAuth(token=token)
        assert auth.token == token

    def test_valid_ghs_token(self) -> None:
        """Test initialization with valid ghs_ prefix token."""
        token = "ghs_" + "d" * 36
        auth = GitHubAuth(token=token)
        assert auth.token == token

    def test_valid_classic_token(self) -> None:
        """Test initialization with valid classic token (40 hex chars)."""
        token = "a" * 40
        auth = GitHubAuth(token=token)
        assert auth.token == token

    def test_valid_classic_token_mixed_case(self) -> None:
        """Test initialization with classic token using valid hex chars."""
        # Classic tokens must be lowercase hex only (exactly 40 chars)
        token = "abc123def456abc789def012abc345def6789abc"
        auth = GitHubAuth(token=token)
        assert auth.token == token


class TestGitHubAuthInvalidTokens:
    """Tests for GitHubAuth initialization with invalid tokens."""

    def test_missing_token_no_env(self) -> None:
        """Test that AuthenticationError is raised when token is missing."""
        with (
            patch.dict(os.environ, {}, clear=True),
            pytest.raises(AuthenticationError, match="GitHub token not found"),
        ):
            GitHubAuth(token=None)

    def test_empty_token(self) -> None:
        """Test that AuthenticationError is raised for empty token."""
        with pytest.raises(AuthenticationError, match="GitHub token not found"):
            GitHubAuth(token="")

    def test_invalid_prefix(self) -> None:
        """Test that AuthenticationError is raised for invalid prefix."""
        with pytest.raises(AuthenticationError, match="Invalid token format"):
            GitHubAuth(token="invalid_prefix_token123456")

    def test_invalid_classic_token_non_hex(self) -> None:
        """Test that AuthenticationError is raised for non-hex classic token."""
        # Contains 'g' which is not a valid hex character
        token = "g" * 40
        with pytest.raises(AuthenticationError, match="Invalid token format"):
            GitHubAuth(token=token)

    def test_invalid_classic_token_wrong_length(self) -> None:
        """Test that AuthenticationError is raised for wrong length classic token."""
        token = "a" * 39  # One char short
        with pytest.raises(AuthenticationError, match="Invalid token format"):
            GitHubAuth(token=token)

    def test_token_too_short(self) -> None:
        """Test that AuthenticationError is raised for token that appears too short."""
        token = "ghp_short"
        with pytest.raises(AuthenticationError, match="Token appears too short"):
            GitHubAuth(token=token)


class TestGitHubAuthEnvironmentVariable:
    """Tests for token loading from environment variable."""

    def test_load_from_environment_variable(self) -> None:
        """Test that token is loaded from GITHUB_TOKEN environment variable."""
        token = "ghp_" + "e" * 36
        with patch.dict(os.environ, {"GITHUB_TOKEN": token}):
            auth = GitHubAuth(token=None)
            assert auth.token == token

    def test_explicit_token_overrides_env(self) -> None:
        """Test that explicit token takes precedence over environment variable."""
        env_token = "ghp_" + "f" * 36
        explicit_token = "ghp_" + "g" * 36
        with patch.dict(os.environ, {"GITHUB_TOKEN": env_token}):
            auth = GitHubAuth(token=explicit_token)
            assert auth.token == explicit_token


class TestGitHubAuthHeaders:
    """Tests for authorization header generation."""

    def test_get_authorization_header(self) -> None:
        """Test get_authorization_header returns correct dict."""
        token = "ghp_" + "h" * 36
        auth = GitHubAuth(token=token)
        header = auth.get_authorization_header()

        assert isinstance(header, dict)
        assert "Authorization" in header
        assert header["Authorization"] == f"token {token}"

    def test_get_headers_alias(self) -> None:
        """Test get_headers alias returns same result as get_authorization_header."""
        token = "ghp_" + "i" * 36
        auth = GitHubAuth(token=token)

        header1 = auth.get_authorization_header()
        header2 = auth.get_headers()

        assert header1 == header2


class TestLoadGitHubToken:
    """Tests for load_github_token convenience function."""

    def test_load_github_token_explicit(self) -> None:
        """Test load_github_token with explicit token."""
        token = "ghp_" + "j" * 36
        result = load_github_token(token=token)
        assert result == token

    def test_load_github_token_from_env(self) -> None:
        """Test load_github_token from environment variable."""
        token = "ghp_" + "k" * 36
        with patch.dict(os.environ, {"GITHUB_TOKEN": token}):
            result = load_github_token(token=None)
            assert result == token

    def test_load_github_token_invalid(self) -> None:
        """Test load_github_token raises error for invalid token."""
        with pytest.raises(AuthenticationError):
            load_github_token(token="invalid")


class TestGetAuthHeaders:
    """Tests for get_auth_headers convenience function."""

    def test_get_auth_headers_explicit(self) -> None:
        """Test get_auth_headers with explicit token."""
        token = "ghp_" + "l" * 36
        headers = get_auth_headers(token=token)

        assert isinstance(headers, dict)
        assert headers["Authorization"] == f"token {token}"

    def test_get_auth_headers_from_env(self) -> None:
        """Test get_auth_headers from environment variable."""
        token = "ghp_" + "m" * 36
        with patch.dict(os.environ, {"GITHUB_TOKEN": token}):
            headers = get_auth_headers(token=None)
            assert headers["Authorization"] == f"token {token}"
