"""Tests for configuration loading and validation."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from gh_year_end.config import Config, load_config

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestConfigLoading:
    """Tests for config loading."""

    def test_load_valid_config(self) -> None:
        """Test loading a valid configuration file."""
        config = load_config(FIXTURES_DIR / "valid_config.yaml")

        assert config.github.target.mode == "org"
        assert config.github.target.name == "test-org"
        assert config.github.windows.year == 2025
        assert config.rate_limit.max_concurrency == 4
        assert config.identity.humans_only is True

    def test_load_missing_file(self) -> None:
        """Test that loading a missing file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_config(Path("/nonexistent/config.yaml"))

    def test_load_invalid_date_boundaries(self) -> None:
        """Test that invalid date boundaries raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            load_config(FIXTURES_DIR / "invalid_date_config.yaml")

        assert "since must be before until" in str(exc_info.value)


class TestConfigValidation:
    """Tests for config validation rules."""

    def test_valid_target_mode_org(self) -> None:
        """Test that 'org' is a valid target mode."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test"},
                    "windows": {
                        "year": 2025,
                        "since": "2025-01-01T00:00:00Z",
                        "until": "2026-01-01T00:00:00Z",
                    },
                }
            }
        )
        assert config.github.target.mode == "org"

    def test_valid_target_mode_user(self) -> None:
        """Test that 'user' is a valid target mode."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "user", "name": "testuser"},
                    "windows": {
                        "year": 2025,
                        "since": "2025-01-01T00:00:00Z",
                        "until": "2026-01-01T00:00:00Z",
                    },
                }
            }
        )
        assert config.github.target.mode == "user"

    def test_invalid_target_mode(self) -> None:
        """Test that invalid target mode raises ValidationError."""
        with pytest.raises(ValidationError):
            Config.model_validate(
                {
                    "github": {
                        "target": {"mode": "invalid", "name": "test"},
                        "windows": {
                            "year": 2025,
                            "since": "2025-01-01T00:00:00Z",
                            "until": "2026-01-01T00:00:00Z",
                        },
                    }
                }
            )

    def test_default_values(self) -> None:
        """Test that default values are applied correctly."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test"},
                    "windows": {
                        "year": 2025,
                        "since": "2025-01-01T00:00:00Z",
                        "until": "2026-01-01T00:00:00Z",
                    },
                }
            }
        )

        # Check defaults
        assert config.rate_limit.strategy == "adaptive"
        assert config.rate_limit.max_concurrency == 4
        assert config.identity.humans_only is True
        assert config.storage.root == Path("./data")
        assert config.collection.enable.pulls is True

    def test_custom_since_date_allowed(self) -> None:
        """Test that custom since dates are now allowed."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test"},
                    "windows": {
                        "year": 2025,
                        "since": "2025-02-01T00:00:00Z",  # Custom start date
                        "until": "2026-01-01T00:00:00Z",
                    },
                }
            }
        )
        assert config.github.windows.since.month == 2

    def test_custom_until_date_allowed(self) -> None:
        """Test that custom until dates are now allowed."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test"},
                    "windows": {
                        "year": 2025,
                        "since": "2025-01-01T00:00:00Z",
                        "until": "2025-12-31T00:00:00Z",  # Custom end date
                    },
                }
            }
        )
        assert config.github.windows.until.month == 12

    def test_rate_limit_bounds(self) -> None:
        """Test rate limit configuration bounds."""
        with pytest.raises(ValidationError):
            Config.model_validate(
                {
                    "github": {
                        "target": {"mode": "org", "name": "test"},
                        "windows": {
                            "year": 2025,
                            "since": "2025-01-01T00:00:00Z",
                            "until": "2026-01-01T00:00:00Z",
                        },
                    },
                    "rate_limit": {
                        "max_concurrency": 100,  # Too high
                    },
                }
            )

    def test_windows_default_year(self) -> None:
        """Test that year defaults to 2025."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test"},
                    "windows": {},
                }
            }
        )
        assert config.github.windows.year == 2025

    def test_windows_auto_calculate_since_until(self) -> None:
        """Test that since/until are auto-calculated from year."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test"},
                    "windows": {"year": 2024},
                }
            }
        )
        assert config.github.windows.year == 2024
        assert config.github.windows.since.year == 2024
        assert config.github.windows.since.month == 1
        assert config.github.windows.since.day == 1
        assert config.github.windows.until.year == 2025
        assert config.github.windows.until.month == 1
        assert config.github.windows.until.day == 1

    def test_windows_explicit_since_until_still_work(self) -> None:
        """Test that explicit since/until values still work."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test"},
                    "windows": {
                        "year": 2025,
                        "since": "2025-01-01T00:00:00Z",
                        "until": "2026-01-01T00:00:00Z",
                    },
                }
            }
        )
        assert config.github.windows.year == 2025
        assert config.github.windows.since.year == 2025
        assert config.github.windows.until.year == 2026

    def test_windows_validates_since_before_until(self) -> None:
        """Test that since must be before until."""
        with pytest.raises(ValidationError) as exc_info:
            Config.model_validate(
                {
                    "github": {
                        "target": {"mode": "org", "name": "test"},
                        "windows": {
                            "year": 2025,
                            "since": "2026-01-01T00:00:00Z",
                            "until": "2025-01-01T00:00:00Z",
                        },
                    }
                }
            )
        assert "since must be before until" in str(exc_info.value)

    def test_windows_custom_date_range_within_year(self) -> None:
        """Test that custom date ranges work (e.g., Q4 only)."""
        config = Config.model_validate(
            {
                "github": {
                    "target": {"mode": "org", "name": "test"},
                    "windows": {
                        "year": 2025,
                        "since": "2025-10-01T00:00:00Z",
                        "until": "2026-01-01T00:00:00Z",
                    },
                }
            }
        )
        assert config.github.windows.since.month == 10
        assert config.github.windows.until.year == 2026
