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

    def test_filter_with_args(self, filter: SecretRedactingFilter) -> None:
        """Test filter method with log record containing args."""
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Token: %s, Key: %s",
            args=("ghp_abcdefghijklmnopqrstuvwxyz123", "api_key=secret123"),
            exc_info=None,
        )
        result = filter.filter(record)
        assert result is True
        assert isinstance(record.args, tuple)
        assert "[REDACTED_GH_TOKEN]" in str(record.args[0])
        assert "[REDACTED]" in str(record.args[1])

    def test_filter_with_non_string_args(self, filter: SecretRedactingFilter) -> None:
        """Test filter method with non-string args (should be preserved)."""
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Count: %d, Valid: %s",
            args=(42, True),
            exc_info=None,
        )
        result = filter.filter(record)
        assert result is True
        assert record.args == (42, True)

    def test_filter_message_only(self, filter: SecretRedactingFilter) -> None:
        """Test filter method with message only (no args)."""
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Using token ghp_abcdefghijklmnopqrstuvwxyz123",
            args=(),
            exc_info=None,
        )
        result = filter.filter(record)
        assert result is True
        # Check the message was redacted
        assert "ghp_" not in record.msg

    def test_redact_ghu_token(self, filter: SecretRedactingFilter) -> None:
        """Test redacting ghu_ tokens."""
        text = "User token ghu_abcdefghijklmnopqrstuvwxyz12"
        result = filter._redact(text)
        assert "ghu_" not in result
        assert "[REDACTED_GH_TOKEN]" in result

    def test_redact_ghs_token(self, filter: SecretRedactingFilter) -> None:
        """Test redacting ghs_ tokens."""
        text = "Secret token ghs_abcdefghijklmnopqrstuvwxyz12"
        result = filter._redact(text)
        assert "ghs_" not in result
        assert "[REDACTED_GH_TOKEN]" in result

    def test_redact_github_pat(self, filter: SecretRedactingFilter) -> None:
        """Test redacting github_pat_ tokens."""
        text = "PAT: github_pat_11ABCDEFG0123456789_abcdefghijklmnopqrstuvwxyz"
        result = filter._redact(text)
        assert "github_pat_" not in result
        assert "[REDACTED_GH_PAT]" in result

    def test_redact_case_insensitive_auth(self, filter: SecretRedactingFilter) -> None:
        """Test case-insensitive authorization header redaction."""
        text = "authorization: Bearer token123"
        result = filter._redact(text)
        assert "token123" not in result
        assert "[REDACTED]" in result

    def test_redact_token_colon(self, filter: SecretRedactingFilter) -> None:
        """Test redacting token: format."""
        text = "TOKEN: mysecrettoken"
        result = filter._redact(text)
        assert "mysecrettoken" not in result
        assert "[REDACTED]" in result

    def test_redact_api_key_variations(self, filter: SecretRedactingFilter) -> None:
        """Test various api_key formats."""
        test_cases = [
            ("api_key=secret", "secret"),
            ("api-key=secret", "secret"),
            ("apikey=secret", "secret"),
            ("API_KEY: secret", "secret"),
        ]
        for text, secret in test_cases:
            result = filter._redact(text)
            assert secret not in result
            assert "[REDACTED]" in result


class TestLoggingSetup:
    """Tests for logging setup."""

    def test_setup_logging_default(self) -> None:
        """Test default logging setup with INFO level."""
        # Create a fresh logger for testing
        test_logger = logging.getLogger("test_setup_default")
        test_logger.handlers.clear()
        test_logger.setLevel(logging.NOTSET)

        setup_logging(verbose=False)

        # Check that library loggers are set to WARNING
        assert logging.getLogger("httpx").level == logging.WARNING
        assert logging.getLogger("httpcore").level == logging.WARNING
        assert logging.getLogger("urllib3").level == logging.WARNING

    def test_setup_logging_verbose(self) -> None:
        """Test verbose logging setup with DEBUG level."""
        # Create a fresh logger for testing
        test_logger = logging.getLogger("test_setup_verbose")
        test_logger.handlers.clear()
        test_logger.setLevel(logging.NOTSET)

        setup_logging(verbose=True)

        # Library loggers should still be WARNING even in verbose mode
        assert logging.getLogger("httpx").level == logging.WARNING
        assert logging.getLogger("httpcore").level == logging.WARNING
        assert logging.getLogger("urllib3").level == logging.WARNING

    def test_setup_logging_adds_redaction_filter(self) -> None:
        """Test that setup_logging adds SecretRedactingFilter to handlers."""
        # Clear existing handlers
        root = logging.getLogger()
        for handler in root.handlers[:]:
            root.removeHandler(handler)

        # Add a new handler
        handler = logging.StreamHandler()
        root.addHandler(handler)

        # Setup logging (should add filter to existing handlers)
        setup_logging(verbose=False)

        # Check that filter was added
        has_redaction_filter = any(isinstance(f, SecretRedactingFilter) for f in handler.filters)
        assert has_redaction_filter

    def test_library_loggers_set_to_warning(self) -> None:
        """Test that library loggers are set to WARNING level."""
        setup_logging(verbose=True)

        # Even with verbose=True, library loggers should be WARNING
        assert logging.getLogger("httpx").level == logging.WARNING
        assert logging.getLogger("httpcore").level == logging.WARNING
        assert logging.getLogger("urllib3").level == logging.WARNING

    def test_get_logger(self) -> None:
        """Test getting a named logger."""
        logger = get_logger("test.module")
        assert logger.name == "test.module"
        assert isinstance(logger, logging.Logger)

    def test_logging_with_redaction_integration(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test end-to-end logging with secret redaction."""
        # Setup logging
        setup_logging(verbose=False)
        logger = get_logger("test.redaction")

        # Clear caplog and log a message with a secret
        caplog.clear()
        with caplog.at_level(logging.INFO):
            logger.info("Using token ghp_abcdefghijklmnopqrstuvwxyz123456")

        # Verify redaction occurred
        assert "ghp_" not in caplog.text
        assert "[REDACTED" in caplog.text

    def test_json_format_configuration(self) -> None:
        """Test that json_format parameter configures JSON logging."""
        # This test verifies the function accepts the parameter
        # and runs without errors
        setup_logging(verbose=False, json_format=True)
        setup_logging(verbose=True, json_format=False)

        # Both calls should succeed without exception
        assert True
