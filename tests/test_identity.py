"""Tests for bot detection and identity resolution."""

from gh_year_end.collect.identity import BotDetector


class TestBotDetector:
    """Tests for BotDetector class."""

    def test_detect_bot_by_type(self) -> None:
        """Test that type 'Bot' is detected as bot."""
        detector = BotDetector(exclude_patterns=[], include_overrides=[])
        result = detector.detect("some-bot", "Bot")

        assert result.is_bot is True
        assert result.reason == "type is Bot"

    def test_detect_bot_by_pattern_suffix(self) -> None:
        """Test that [bot] suffix pattern matches bots."""
        detector = BotDetector(
            exclude_patterns=[r".*\[bot\]$"],
            include_overrides=[],
        )

        result = detector.detect("dependabot[bot]", "User")
        assert result.is_bot is True
        assert "matches pattern" in result.reason
        assert r".*\[bot\]$" in result.reason

    def test_detect_bot_by_exact_name(self) -> None:
        """Test that exact name pattern matches bots."""
        detector = BotDetector(
            exclude_patterns=[r"^dependabot$", r"^renovate\[bot\]$"],
            include_overrides=[],
        )

        result = detector.detect("dependabot", "User")
        assert result.is_bot is True
        assert "matches pattern" in result.reason
        assert "^dependabot$" in result.reason

    def test_detect_human_user(self) -> None:
        """Test that normal users are not detected as bots."""
        detector = BotDetector(
            exclude_patterns=[r".*\[bot\]$", r"^dependabot$"],
            include_overrides=[],
        )

        result = detector.detect("alice", "User")
        assert result.is_bot is False
        assert result.reason is None

    def test_include_override_takes_precedence(self) -> None:
        """Test that include_overrides take precedence over patterns."""
        detector = BotDetector(
            exclude_patterns=[r".*\[bot\]$"],
            include_overrides=["special-bot[bot]"],
        )

        result = detector.detect("special-bot[bot]", "User")
        assert result.is_bot is False
        assert result.reason is None

    def test_include_override_with_bot_type(self) -> None:
        """Test that include_overrides take precedence over type."""
        detector = BotDetector(
            exclude_patterns=[],
            include_overrides=["special-bot"],
        )

        result = detector.detect("special-bot", "Bot")
        assert result.is_bot is False
        assert result.reason is None

    def test_multiple_patterns(self) -> None:
        """Test detection with multiple exclude patterns."""
        detector = BotDetector(
            exclude_patterns=[
                r".*\[bot\]$",
                r"^dependabot$",
                r"^renovate\[bot\]$",
                r"^github-actions\[bot\]$",
            ],
            include_overrides=[],
        )

        # Test each pattern
        assert detector.detect("dependabot[bot]", "User").is_bot is True
        assert detector.detect("dependabot", "User").is_bot is True
        assert detector.detect("renovate[bot]", "User").is_bot is True
        assert detector.detect("github-actions[bot]", "User").is_bot is True
        assert detector.detect("alice", "User").is_bot is False

    def test_organization_type(self) -> None:
        """Test that Organization type is not treated as bot."""
        detector = BotDetector(
            exclude_patterns=[r".*\[bot\]$"],
            include_overrides=[],
        )

        result = detector.detect("my-org", "Organization")
        assert result.is_bot is False
        assert result.reason is None

    def test_empty_patterns_and_overrides(self) -> None:
        """Test detector with no patterns or overrides."""
        detector = BotDetector(exclude_patterns=[], include_overrides=[])

        assert detector.detect("alice", "User").is_bot is False
        assert detector.detect("some-bot[bot]", "User").is_bot is False
        assert detector.detect("bot", "Bot").is_bot is True

    def test_pattern_case_sensitivity(self) -> None:
        """Test that patterns are case-sensitive by default."""
        detector = BotDetector(
            exclude_patterns=[r"^dependabot$"],
            include_overrides=[],
        )

        # Exact match
        assert detector.detect("dependabot", "User").is_bot is True

        # Different case should not match
        assert detector.detect("Dependabot", "User").is_bot is False
        assert detector.detect("DEPENDABOT", "User").is_bot is False

    def test_real_world_bot_names(self) -> None:
        """Test detection of real-world bot names."""
        detector = BotDetector(
            exclude_patterns=[
                r".*\[bot\]$",
                r"^dependabot$",
                r"^renovate\[bot\]$",
            ],
            include_overrides=[],
        )

        # Common GitHub bots
        bots = [
            "dependabot[bot]",
            "renovate[bot]",
            "github-actions[bot]",
            "codecov[bot]",
            "sonarcloud[bot]",
            "dependabot",
        ]

        for bot in bots:
            result = detector.detect(bot, "User")
            assert result.is_bot is True, f"{bot} should be detected as bot"

        # Humans
        humans = [
            "alice",
            "bob",
            "charlie-smith",
            "user123",
        ]

        for human in humans:
            result = detector.detect(human, "User")
            assert result.is_bot is False, f"{human} should not be detected as bot"

    def test_empty_login(self) -> None:
        """Test that empty login is handled correctly."""
        detector = BotDetector(
            exclude_patterns=[r".*\[bot\]$"],
            include_overrides=[],
        )

        result = detector.detect("", "User")
        assert result.is_bot is False
        assert result.reason is None

    def test_special_characters_in_login(self) -> None:
        """Test logins with special characters."""
        detector = BotDetector(
            exclude_patterns=[r".*\[bot\]$"],
            include_overrides=[],
        )

        # Hyphens and numbers are common in usernames
        result = detector.detect("user-name-123", "User")
        assert result.is_bot is False
        assert result.reason is None

        # Bot with special characters
        result = detector.detect("auto-deploy[bot]", "User")
        assert result.is_bot is True
        assert "matches pattern" in result.reason

    def test_pattern_matching_order(self) -> None:
        """Test that first matching pattern is reported in reason."""
        detector = BotDetector(
            exclude_patterns=[
                r"^dependabot$",
                r".*\[bot\]$",
            ],
            include_overrides=[],
        )

        # Should match first pattern
        result = detector.detect("dependabot", "User")
        assert result.is_bot is True
        assert "^dependabot$" in result.reason

        # Should match second pattern
        result = detector.detect("other[bot]", "User")
        assert result.is_bot is True
        assert r".*\[bot\]$" in result.reason
