"""GitHub authentication module.

Handles loading and validating GitHub API tokens from environment variables
or the GitHub CLI.
"""

import logging
import os
import re
import subprocess

logger = logging.getLogger(__name__)


class AuthenticationError(Exception):
    """Raised when authentication fails or token is invalid."""


def _get_gh_cli_token() -> str | None:
    """Try to get token from GitHub CLI.

    Returns:
        Token from `gh auth token` or None if not available.
    """
    try:
        result = subprocess.run(
            ["gh", "auth", "token"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode == 0:
            token = result.stdout.strip()
            if token:
                logger.info("Using GitHub token from gh CLI")
                return token
        else:
            logger.debug(
                "gh CLI returned non-zero exit code (%d). "
                "Ensure you are authenticated with 'gh auth login'",
                result.returncode,
            )
    except FileNotFoundError:
        logger.debug(
            "gh CLI not found. Install from https://cli.github.com/ "
            "or set GITHUB_TOKEN environment variable"
        )
    except subprocess.TimeoutExpired:
        logger.debug("gh CLI command timed out after 5 seconds")
    return None


class GitHubAuth:
    """GitHub authentication manager.

    Loads and validates GitHub tokens from:
    1. Explicit token parameter
    2. GITHUB_TOKEN environment variable
    3. GitHub CLI (`gh auth token`)

    Token prefix formats:
    - ghp_: Personal access token (classic)
    - gho_: OAuth access token
    - ghu_: User-to-server token
    - ghs_: Server-to-server token
    - Classic tokens: 40 character hex string (no prefix)
    """

    # Valid GitHub token prefixes
    VALID_PREFIXES = ("ghp_", "gho_", "ghu_", "ghs_")

    # Pattern for classic tokens (40 hex characters)
    CLASSIC_TOKEN_PATTERN = re.compile(r"^[a-f0-9]{40}$")

    def __init__(self, token: str | None = None) -> None:
        """Initialize GitHub authentication.

        Args:
            token: GitHub token. If None, loads from GITHUB_TOKEN env var
                   or GitHub CLI.

        Raises:
            AuthenticationError: If token is missing or invalid.
        """
        # Try sources in order: explicit token, env var, gh CLI
        loaded_token = None
        token_source = None

        if token:
            loaded_token = token
            token_source = "explicit parameter"
        elif "GITHUB_TOKEN" in os.environ:
            loaded_token = os.environ["GITHUB_TOKEN"]
            token_source = "GITHUB_TOKEN environment variable"
        else:
            loaded_token = _get_gh_cli_token()
            if loaded_token:
                token_source = "gh CLI"

        if not loaded_token:
            raise AuthenticationError(
                "GitHub token not found. Set GITHUB_TOKEN environment variable, "
                "pass token explicitly, or authenticate with `gh auth login`."
            )

        if token_source and token_source != "gh CLI":
            # gh CLI logs its own success message
            logger.info("Using GitHub token from %s", token_source)

        self._token: str = loaded_token
        self._validate_token()

    def _validate_token(self) -> None:
        """Validate token format.

        Raises:
            AuthenticationError: If token format is invalid.
        """
        token = self._token
        if not token:
            raise AuthenticationError("Token is empty")

        # Check for valid prefix
        has_valid_prefix = any(token.startswith(prefix) for prefix in self.VALID_PREFIXES)

        # Check if it's a classic token (40 hex chars)
        is_classic = bool(self.CLASSIC_TOKEN_PATTERN.match(token))

        if not has_valid_prefix and not is_classic:
            raise AuthenticationError(
                f"Invalid token format. Expected prefix {self.VALID_PREFIXES} "
                "or 40-character hex string (classic token)"
            )

        # Additional validation: new-style tokens should have sufficient length
        if has_valid_prefix and len(token) < 20:
            raise AuthenticationError("Token appears too short to be valid")

    @property
    def token(self) -> str:
        """Get the GitHub token.

        Returns:
            The validated GitHub token.
        """
        return self._token

    def get_authorization_header(self) -> dict[str, str]:
        """Get the Authorization header for API requests.

        Returns:
            Dictionary with Authorization header.
        """
        return {"Authorization": f"token {self._token}"}

    def get_headers(self) -> dict[str, str]:
        """Get all authentication headers for API requests.

        Alias for get_authorization_header() for convenience.

        Returns:
            Dictionary with Authorization header.
        """
        return self.get_authorization_header()


def load_github_token(token: str | None = None) -> str:
    """Load and validate GitHub token.

    Convenience function for getting a validated token.

    Args:
        token: GitHub token. If None, loads from GITHUB_TOKEN env var.

    Returns:
        Validated GitHub token.

    Raises:
        AuthenticationError: If token is missing or invalid.
    """
    auth = GitHubAuth(token)
    return auth.token


def get_auth_headers(token: str | None = None) -> dict[str, str]:
    """Get authentication headers for GitHub API requests.

    Convenience function for getting auth headers.

    Args:
        token: GitHub token. If None, loads from GITHUB_TOKEN env var.

    Returns:
        Dictionary with Authorization header.

    Raises:
        AuthenticationError: If token is missing or invalid.
    """
    auth = GitHubAuth(token)
    return auth.get_authorization_header()
