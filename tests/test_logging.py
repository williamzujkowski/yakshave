"""Tests for logging with secret redaction."""

import logging

import pytest

from gh_year_end.logging import SecretRedactingFilter, get_logger, setup_logging


class TestSecretRedaction:
    """Tests for secret redaction filter."""

    @pytest.fixture
    def filter(self) -> SecretRedactingFilter:
        """Create a redaction filter."""
        return SecretRedactingFilter()

    def test_redact_ghp_token(self, filter: SecretRedactingFilter) -> None:
        """Test redacting ghp_ tokens (20+ chars after prefix)."""
        text = "Found ghp_abcdefghijklmnopqrstuvwxyz12"
        result = filter._redact(text)
        assert "ghp_" not in result
        assert "[REDACTED_GH_TOKEN]" in result

    def test_redact_gho_token(self, filter: SecretRedactingFilter) -> None:
        """Test redacting gho_ tokens (36 chars after prefix)."""
        text = "Using token gho_abcdefghijklmnopqrstuvwxyz12"
        result = filter._redact(text)
        assert "gho_" not in result
        assert "[REDACTED_GH_TOKEN]" in result

    def test_redact_bearer_token(self, filter: SecretRedactingFilter) -> None:
        """Test redacting Bearer tokens."""
        text = "Bearer my-secret-token-here"
        result = filter._redact(text)
        assert "my-secret-token-here" not in result
        assert "Bearer [REDACTED]" in result

    def test_redact_authorization_header(self, filter: SecretRedactingFilter) -> None:
        """Test redacting Authorization headers."""
        text = "Headers: {'Authorization: token123'}"
        result = filter._redact(text)
        assert "token123" not in result
        assert "[REDACTED]" in result

    def test_redact_token_param(self, filter: SecretRedactingFilter) -> None:
        """Test redacting token= parameters."""
        text = "Request with token=secret123 param"
        result = filter._redact(text)
        assert "secret123" not in result
        assert "[REDACTED]" in result

    def test_redact_api_key(self, filter: SecretRedactingFilter) -> None:
        """Test redacting api_key parameters."""
        text = "Using api_key=myapikey123"
        result = filter._redact(text)
        assert "myapikey123" not in result
        assert "[REDACTED]" in result

    def test_preserve_normal_text(self, filter: SecretRedactingFilter) -> None:
        """Test that normal text is preserved."""
        text = "Fetching data from repository owner/repo"
        result = filter._redact(text)
        assert result == text

    def test_multiple_secrets(self, filter: SecretRedactingFilter) -> None:
        """Test redacting multiple secrets in one string."""
        text = "ghp_abcdefghijklmnopqrstuvwxyz123456 and token=secret"
        result = filter._redact(text)
        assert "ghp_" not in result
        assert "secret" not in result


class TestLoggingSetup:
    """Tests for logging setup."""

    def test_setup_logging_default(self) -> None:
        """Test default logging setup."""
        setup_logging(verbose=False)
        logger = get_logger("test")
        assert logger.level == logging.NOTSET  # Inherits from root

    def test_get_logger(self) -> None:
        """Test getting a named logger."""
        logger = get_logger("test.module")
        assert logger.name == "test.module"
