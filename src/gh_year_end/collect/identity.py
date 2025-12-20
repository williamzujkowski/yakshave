"""Bot detection and identity resolution."""

import re
from dataclasses import dataclass


@dataclass
class BotDetectionResult:
    """Result of bot detection.

    Attributes:
        is_bot: Whether the user is classified as a bot.
        reason: Explanation for bot classification. None if human.
    """

    is_bot: bool
    reason: str | None


class BotDetector:
    """Detects bots based on configured patterns and overrides.

    Bot detection rules:
    1. If login is in include_overrides, treat as human
    2. If type is "Bot", treat as bot
    3. If login matches any exclude_patterns, treat as bot
    4. Otherwise, treat as human

    Attributes:
        exclude_patterns: Compiled regex patterns for bot detection.
        include_overrides: Set of logins to treat as humans.
    """

    def __init__(self, exclude_patterns: list[str], include_overrides: list[str]) -> None:
        """Initialize bot detector.

        Args:
            exclude_patterns: Regex patterns to match bot logins.
            include_overrides: Logins to force as human even if matching patterns.
        """
        self.exclude_patterns = [re.compile(pattern) for pattern in exclude_patterns]
        self.include_overrides = set(include_overrides)

    def detect(self, login: str, user_type: str) -> BotDetectionResult:
        """Detect if a user is a bot.

        Args:
            login: GitHub login/username.
            user_type: GitHub user type (User, Bot, Organization).

        Returns:
            BotDetectionResult with is_bot flag and optional reason.
        """
        # Rule 1: Override takes precedence
        if login in self.include_overrides:
            return BotDetectionResult(is_bot=False, reason=None)

        # Rule 2: Type is Bot
        if user_type == "Bot":
            return BotDetectionResult(is_bot=True, reason="type is Bot")

        # Rule 3: Check against exclude patterns
        for pattern in self.exclude_patterns:
            if pattern.match(login):
                return BotDetectionResult(
                    is_bot=True,
                    reason=f"matches pattern: {pattern.pattern}",
                )

        # Rule 4: Default to human
        return BotDetectionResult(is_bot=False, reason=None)
