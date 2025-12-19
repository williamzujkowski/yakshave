"""Tests for data validator."""

import json
import tempfile
from pathlib import Path

from gh_year_end.storage.validator import (
    DataValidator,
    ValidationError,
    ValidationResult,
    validate_collection,
)


class TestValidationResult:
    """Tests for ValidationResult class."""

    def test_initial_state(self) -> None:
        """Test initial state of ValidationResult."""
        result = ValidationResult()
        assert result.valid is True
        assert result.total_records == 0
        assert result.valid_records == 0
        assert result.errors == []
        assert result.warnings == []

    def test_add_recoverable_error(self) -> None:
        """Test adding a recoverable error keeps valid=True."""
        result = ValidationResult()
        error = ValidationError(
            file_path=Path("test.jsonl"),
            line_number=1,
            error_type="TEST",
            message="test error",
            recoverable=True,
        )
        result.add_error(error)
        assert result.valid is True
        assert len(result.errors) == 1

    def test_add_non_recoverable_error(self) -> None:
        """Test adding a non-recoverable error sets valid=False."""
        result = ValidationResult()
        error = ValidationError(
            file_path=Path("test.jsonl"),
            line_number=None,
            error_type="TEST",
            message="critical error",
            recoverable=False,
        )
        result.add_error(error)
        assert result.valid is False
        assert len(result.errors) == 1

    def test_add_warning(self) -> None:
        """Test adding a warning."""
        result = ValidationResult()
        result.add_warning("test warning")
        assert result.valid is True
        assert len(result.warnings) == 1
        assert result.warnings[0] == "test warning"

    def test_merge_results(self) -> None:
        """Test merging validation results."""
        result1 = ValidationResult(total_records=10, valid_records=8)
        result1.add_warning("warning1")

        result2 = ValidationResult(total_records=5, valid_records=5)
        result2.add_warning("warning2")

        result1.merge(result2)
        assert result1.total_records == 15
        assert result1.valid_records == 13
        assert len(result1.warnings) == 2


class TestValidationError:
    """Tests for ValidationError class."""

    def test_str_with_line_number(self) -> None:
        """Test string representation with line number."""
        error = ValidationError(
            file_path=Path("test.jsonl"),
            line_number=42,
            error_type="INVALID_JSON",
            message="unexpected token",
        )
        assert "test.jsonl:42" in str(error)
        assert "INVALID_JSON" in str(error)
        assert "unexpected token" in str(error)

    def test_str_without_line_number(self) -> None:
        """Test string representation without line number (no line:number format)."""
        error = ValidationError(
            file_path=Path("test.jsonl"),
            line_number=None,
            error_type="FILE_ERROR",
            message="file not found",
        )
        # Should not have :N format (where N is a number)
        assert ":1" not in str(error)
        assert "test.jsonl" in str(error)
        assert "FILE_ERROR" in str(error)


class TestDataValidator:
    """Tests for DataValidator class."""

    def test_validate_nonexistent_file(self) -> None:
        """Test validating a file that doesn't exist."""
        validator = DataValidator()
        result = validator.validate_jsonl(Path("/nonexistent/file.jsonl"))
        assert result.valid is False
        assert len(result.errors) == 1
        assert result.errors[0].error_type == "FILE_NOT_FOUND"

    def test_validate_empty_file(self) -> None:
        """Test validating an empty file."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            temp_path = Path(f.name)

        try:
            validator = DataValidator()
            result = validator.validate_jsonl(temp_path)
            assert result.valid is True
            assert result.total_records == 0
            assert len(result.warnings) == 1
        finally:
            temp_path.unlink()

    def test_validate_valid_jsonl(self) -> None:
        """Test validating a valid JSONL file."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False, mode="w") as f:
            record = {
                "request_id": "abc123",
                "timestamp": "2024-01-01T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/owner/repo/pulls",
                "data": {"id": 1, "number": 100, "state": "open"},
            }
            f.write(json.dumps(record) + "\n")
            f.write(json.dumps(record) + "\n")
            temp_path = Path(f.name)

        try:
            validator = DataValidator()
            result = validator.validate_jsonl(temp_path)
            assert result.valid is True
            assert result.total_records == 2
            assert result.valid_records == 2
            assert len(result.errors) == 0
        finally:
            temp_path.unlink()

    def test_validate_invalid_json(self) -> None:
        """Test validating a file with invalid JSON."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False, mode="w") as f:
            # Use valid envelope structure for first and third lines
            record = {
                "request_id": "abc123",
                "timestamp": "2024-01-01T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/owner/repo/pulls",
                "data": {"id": 1},
            }
            f.write(json.dumps(record) + "\n")
            f.write("not valid json\n")  # Invalid JSON
            f.write(json.dumps(record) + "\n")
            temp_path = Path(f.name)

        try:
            validator = DataValidator()
            result = validator.validate_jsonl(temp_path)
            assert result.total_records == 3
            assert result.valid_records == 2
            # Only the invalid JSON line should have an error
            invalid_json_errors = [e for e in result.errors if e.error_type == "INVALID_JSON"]
            assert len(invalid_json_errors) == 1
            assert invalid_json_errors[0].line_number == 2
        finally:
            temp_path.unlink()

    def test_validate_missing_envelope_fields(self) -> None:
        """Test validating records missing required envelope fields."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False, mode="w") as f:
            # Missing 'data' field
            record = {
                "request_id": "abc123",
                "timestamp": "2024-01-01T00:00:00Z",
                "source": "github_rest",
                "endpoint": "/repos/owner/repo/pulls",
            }
            f.write(json.dumps(record) + "\n")
            temp_path = Path(f.name)

        try:
            validator = DataValidator()
            result = validator.validate_jsonl(temp_path)
            assert result.total_records == 1
            assert len(result.errors) == 1
            assert result.errors[0].error_type == "MISSING_ENVELOPE_FIELDS"
        finally:
            temp_path.unlink()

    def test_repair_truncated(self) -> None:
        """Test repairing truncated JSONL files."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False, mode="w") as f:
            f.write('{"valid": "record1"}\n')
            f.write('{"valid": "record2"}\n')
            f.write('{"truncated": "rec')  # Truncated record
            temp_path = Path(f.name)

        try:
            validator = DataValidator()
            removed = validator.repair_truncated(temp_path)
            assert removed == 1

            # Verify file now only contains valid records
            with temp_path.open() as f:
                lines = f.readlines()
            assert len(lines) == 2
        finally:
            temp_path.unlink()


class TestValidateCollection:
    """Tests for validate_collection function."""

    def test_validate_nonexistent_directory(self) -> None:
        """Test validating a nonexistent directory."""
        result = validate_collection(Path("/nonexistent/dir"))
        assert result.valid is False
        assert len(result.errors) == 1
        assert result.errors[0].error_type == "DIRECTORY_NOT_FOUND"

    def test_validate_empty_directory(self) -> None:
        """Test validating an empty directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = validate_collection(Path(temp_dir))
            assert result.valid is True
            assert len(result.warnings) == 1
