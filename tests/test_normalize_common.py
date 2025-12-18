"""Tests for common normalization utilities."""

import json
from datetime import UTC
from pathlib import Path

import pytest

from gh_year_end.normalize.common import (
    determine_pr_state,
    extract_labels,
    get_repo_id,
    get_user_id,
    normalize_timestamp,
    read_jsonl_data,
    safe_len,
)


class TestReadJsonlData:
    """Tests for read_jsonl_data function."""

    def test_reads_enveloped_data(self, tmp_path: Path) -> None:
        """Test reading data from enveloped JSONL."""
        test_file = tmp_path / "test.jsonl"
        with test_file.open("w") as f:
            envelope = {
                "timestamp": "2025-01-01T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/test",
                "request_id": "123",
                "page": 1,
                "data": {"id": 1, "name": "test"},
            }
            f.write(json.dumps(envelope) + "\n")

        data_list = list(read_jsonl_data(test_file))
        assert len(data_list) == 1
        assert data_list[0] == {"id": 1, "name": "test"}

    def test_handles_missing_data_field(self, tmp_path: Path) -> None:
        """Test handling of envelope without data field."""
        test_file = tmp_path / "test.jsonl"
        with test_file.open("w") as f:
            envelope = {"timestamp": "2025-01-01T00:00:00Z"}
            f.write(json.dumps(envelope) + "\n")

        data_list = list(read_jsonl_data(test_file))
        assert len(data_list) == 0

    def test_handles_invalid_json(self, tmp_path: Path) -> None:
        """Test handling of invalid JSON lines."""
        test_file = tmp_path / "test.jsonl"
        with test_file.open("w") as f:
            f.write("invalid json\n")
            f.write('{"data": {"valid": "record"}}\n')

        data_list = list(read_jsonl_data(test_file))
        assert len(data_list) == 1
        assert data_list[0] == {"valid": "record"}

    def test_raises_on_missing_file(self, tmp_path: Path) -> None:
        """Test that FileNotFoundError is raised for missing files."""
        missing_file = tmp_path / "missing.jsonl"
        with pytest.raises(FileNotFoundError):
            list(read_jsonl_data(missing_file))


class TestNormalizeTimestamp:
    """Tests for normalize_timestamp function."""

    def test_parses_github_timestamp(self) -> None:
        """Test parsing GitHub ISO 8601 timestamp."""
        ts = "2025-01-15T10:30:00Z"
        result = normalize_timestamp(ts)
        assert result is not None
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30
        assert result.second == 0
        assert result.tzinfo == UTC

    def test_returns_none_for_none_input(self) -> None:
        """Test that None input returns None."""
        assert normalize_timestamp(None) is None

    def test_returns_none_for_invalid_timestamp(self) -> None:
        """Test that invalid timestamp returns None."""
        assert normalize_timestamp("not a timestamp") is None
        assert normalize_timestamp("") is None


class TestSafeLen:
    """Tests for safe_len function."""

    def test_returns_length_of_text(self) -> None:
        """Test returning length of valid text."""
        assert safe_len("hello") == 5
        assert safe_len("test string") == 11

    def test_returns_zero_for_none(self) -> None:
        """Test returning 0 for None input."""
        assert safe_len(None) == 0

    def test_returns_zero_for_empty_string(self) -> None:
        """Test returning 0 for empty string."""
        assert safe_len("") == 0


class TestExtractLabels:
    """Tests for extract_labels function."""

    def test_extracts_label_names(self) -> None:
        """Test extracting label names from GitHub labels array."""
        labels = [{"name": "bug"}, {"name": "enhancement"}]
        result = extract_labels(labels)
        # Should be sorted and comma-separated
        assert result == "bug,enhancement"

    def test_returns_empty_for_none(self) -> None:
        """Test returning empty string for None input."""
        assert extract_labels(None) == ""

    def test_returns_empty_for_empty_list(self) -> None:
        """Test returning empty string for empty list."""
        assert extract_labels([]) == ""

    def test_sorts_labels(self) -> None:
        """Test that labels are sorted alphabetically."""
        labels = [{"name": "z-last"}, {"name": "a-first"}, {"name": "m-middle"}]
        result = extract_labels(labels)
        assert result == "a-first,m-middle,z-last"

    def test_handles_missing_name_field(self) -> None:
        """Test handling labels with missing name field."""
        labels = [{"name": "bug"}, {}, {"name": "feature"}]
        result = extract_labels(labels)
        assert result == "bug,feature"


class TestGetRepoId:
    """Tests for get_repo_id function."""

    def test_extracts_node_id_from_repo(self) -> None:
        """Test extracting node_id from repo dict."""
        repo = {"node_id": "R_12345", "name": "test-repo"}
        assert get_repo_id(repo) == "R_12345"

    def test_returns_none_for_none_input(self) -> None:
        """Test returning None for None input."""
        assert get_repo_id(None) is None

    def test_returns_none_for_missing_node_id(self) -> None:
        """Test returning None when node_id is missing."""
        repo = {"name": "test-repo"}
        assert get_repo_id(repo) is None


class TestGetUserId:
    """Tests for get_user_id function."""

    def test_extracts_node_id_from_user(self) -> None:
        """Test extracting node_id from user dict."""
        user = {"node_id": "U_12345", "login": "testuser"}
        assert get_user_id(user) == "U_12345"

    def test_returns_none_for_none_input(self) -> None:
        """Test returning None for None input."""
        assert get_user_id(None) is None

    def test_returns_none_for_missing_node_id(self) -> None:
        """Test returning None when node_id is missing."""
        user = {"login": "testuser"}
        assert get_user_id(user) is None


class TestDeterminePrState:
    """Tests for determine_pr_state function."""

    def test_returns_merged_when_merged_at_exists(self) -> None:
        """Test returning 'merged' when merged_at is present."""
        pr = {"state": "closed", "merged_at": "2025-01-15T10:00:00Z"}
        assert determine_pr_state(pr) == "merged"

    def test_returns_open_for_open_pr(self) -> None:
        """Test returning 'open' for open PR."""
        pr = {"state": "open", "merged_at": None}
        assert determine_pr_state(pr) == "open"

    def test_returns_closed_for_closed_unmerged_pr(self) -> None:
        """Test returning 'closed' for closed but not merged PR."""
        pr = {"state": "closed", "merged_at": None}
        assert determine_pr_state(pr) == "closed"

    def test_defaults_to_open_when_state_missing(self) -> None:
        """Test defaulting to 'open' when state field is missing."""
        pr = {"merged_at": None}
        assert determine_pr_state(pr) == "open"
