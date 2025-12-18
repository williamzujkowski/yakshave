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

        assert "since must be" in str(exc_info.value)


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

    def test_year_boundary_validation_wrong_since(self) -> None:
        """Test that wrong since date raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            Config.model_validate(
                {
                    "github": {
                        "target": {"mode": "org", "name": "test"},
                        "windows": {
                            "year": 2025,
                            "since": "2025-02-01T00:00:00Z",  # Wrong
                            "until": "2026-01-01T00:00:00Z",
                        },
                    }
                }
            )
        assert "since must be" in str(exc_info.value)

    def test_year_boundary_validation_wrong_until(self) -> None:
        """Test that wrong until date raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            Config.model_validate(
                {
                    "github": {
                        "target": {"mode": "org", "name": "test"},
                        "windows": {
                            "year": 2025,
                            "since": "2025-01-01T00:00:00Z",
                            "until": "2025-12-31T00:00:00Z",  # Wrong
                        },
                    }
                }
            )
        assert "until must be" in str(exc_info.value)

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
