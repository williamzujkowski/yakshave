"""Logging configuration with secret redaction."""

import logging
import re
from typing import ClassVar


class SecretRedactingFilter(logging.Filter):
    """Filter that redacts secrets from log messages."""

    # Patterns that should be redacted
    SECRET_PATTERNS: ClassVar[list[tuple[re.Pattern[str], str]]] = [
        # GitHub tokens (ghp_, gho_, ghu_, ghs_ followed by 20+ alphanumeric chars)
        (re.compile(r"ghp_[a-zA-Z0-9]{20,}"), "[REDACTED_GH_TOKEN]"),
        (re.compile(r"gho_[a-zA-Z0-9]{20,}"), "[REDACTED_GH_TOKEN]"),
        (re.compile(r"ghu_[a-zA-Z0-9]{20,}"), "[REDACTED_GH_TOKEN]"),
        (re.compile(r"ghs_[a-zA-Z0-9]{20,}"), "[REDACTED_GH_TOKEN]"),
        # GitHub fine-grained PAT
        (re.compile(r"github_pat_[a-zA-Z0-9_]+"), "[REDACTED_GH_PAT]"),
        # Generic Bearer tokens
        (re.compile(r"Bearer\s+[a-zA-Z0-9_\-\.]+"), "Bearer [REDACTED]"),
        # Authorization headers
        (re.compile(r"(Authorization:\s*)[^\s,\]]+", re.IGNORECASE), r"\1[REDACTED]"),
        # Generic tokens in key=value format
        (re.compile(r"(token[=:]\s*)[^\s,\]]+", re.IGNORECASE), r"\1[REDACTED]"),
        # API keys
        (re.compile(r"(api[_-]?key[=:]\s*)[^\s,\]]+", re.IGNORECASE), r"\1[REDACTED]"),
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter and redact secrets from log record."""
        record.msg = self._redact(str(record.msg))
        if record.args:
            record.args = tuple(
                self._redact(str(arg)) if isinstance(arg, str) else arg for arg in record.args
            )
        return True

    def _redact(self, text: str) -> str:
        """Redact secrets from text."""
        for pattern, replacement in self.SECRET_PATTERNS:
            text = pattern.sub(replacement, text)
        return text


def setup_logging(
    verbose: bool = False,
    json_format: bool = False,
) -> None:
    """Configure logging for the application.

    Args:
        verbose: Enable debug level logging.
        json_format: Use JSON format for logs (useful for structured logging).
    """
    level = logging.DEBUG if verbose else logging.INFO

    # Create formatter
    if json_format:
        format_str = '{"time": "%(asctime)s", "level": "%(levelname)s", "name": "%(name)s", "message": "%(message)s"}'
    else:
        format_str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

    # Configure root logger
    logging.basicConfig(
        level=level,
        format=format_str,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Add secret redaction filter to all handlers
    root_logger = logging.getLogger()
    redaction_filter = SecretRedactingFilter()

    for handler in root_logger.handlers:
        handler.addFilter(redaction_filter)

    # Set library loggers to WARNING to reduce noise
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name.

    Args:
        name: Logger name, typically __name__ of the calling module.

    Returns:
        Configured logger instance.
    """
    return logging.getLogger(name)
