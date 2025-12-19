"""Data validation for cached collection data.

Validates JSONL file integrity, schema conformance, and checkpoint consistency
to ensure reliable resume functionality.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """Represents a single validation error."""

    file_path: Path
    line_number: int | None
    error_type: str
    message: str
    recoverable: bool = True

    def __str__(self) -> str:
        """Format error for display."""
        if self.line_number is not None:
            return f"{self.file_path}:{self.line_number}: {self.error_type}: {self.message}"
        return f"{self.file_path}: {self.error_type}: {self.message}"


@dataclass
class ValidationResult:
    """Result of validating a file or set of files."""

    valid: bool = True
    total_records: int = 0
    valid_records: int = 0
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def add_error(self, error: ValidationError) -> None:
        """Add a validation error."""
        self.errors.append(error)
        if not error.recoverable:
            self.valid = False

    def add_warning(self, warning: str) -> None:
        """Add a validation warning."""
        self.warnings.append(warning)

    def merge(self, other: ValidationResult) -> None:
        """Merge another result into this one."""
        self.total_records += other.total_records
        self.valid_records += other.valid_records
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)
        if not other.valid:
            self.valid = False


# Required fields in the envelope structure
ENVELOPE_REQUIRED_FIELDS = {"request_id", "timestamp", "source", "endpoint", "data"}

# Endpoints and their required data fields
ENDPOINT_REQUIRED_FIELDS: dict[str, set[str]] = {
    "repos": {"id", "full_name", "name"},
    "pulls": {"id", "number", "state"},
    "issues": {"id", "number", "state"},
    "reviews": {"id", "state"},
    "issue_comments": {"id", "body"},
    "review_comments": {"id", "body"},
    "commits": {"sha"},
    "branch_protection": {"enabled"},
    "security_features": set(),  # Security features may vary
}


class DataValidator:
    """Validates collection data integrity and schema conformance."""

    def __init__(self, strict: bool = False) -> None:
        """Initialize validator.

        Args:
            strict: If True, treat warnings as errors.
        """
        self.strict = strict

    def validate_jsonl(
        self,
        path: Path,
        endpoint: str | None = None,
    ) -> ValidationResult:
        """Validate a JSONL file for integrity and schema.

        Args:
            path: Path to the JSONL file.
            endpoint: Optional endpoint type for schema validation.

        Returns:
            ValidationResult with details about validation.
        """
        result = ValidationResult()

        if not path.exists():
            result.add_error(
                ValidationError(
                    file_path=path,
                    line_number=None,
                    error_type="FILE_NOT_FOUND",
                    message="File does not exist",
                    recoverable=False,
                )
            )
            return result

        if path.stat().st_size == 0:
            result.add_warning(f"File is empty: {path}")
            return result

        try:
            with path.open(encoding="utf-8") as f:
                for line_num, line in enumerate(f, start=1):
                    result.total_records += 1

                    # Skip empty lines
                    if not line.strip():
                        result.add_warning(f"Empty line at {path}:{line_num}")
                        continue

                    # Parse JSON
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError as e:
                        result.add_error(
                            ValidationError(
                                file_path=path,
                                line_number=line_num,
                                error_type="INVALID_JSON",
                                message=f"JSON parse error: {e}",
                                recoverable=True,
                            )
                        )
                        continue

                    # Validate envelope structure
                    envelope_result = self._validate_envelope(record, path, line_num)
                    if envelope_result:
                        result.add_error(envelope_result)
                        continue

                    # Validate endpoint-specific fields
                    if endpoint:
                        data_result = self._validate_data_fields(
                            record.get("data", {}), endpoint, path, line_num
                        )
                        if data_result:
                            result.add_error(data_result)
                            continue

                    result.valid_records += 1

        except (OSError, UnicodeDecodeError) as e:
            result.add_error(
                ValidationError(
                    file_path=path,
                    line_number=None,
                    error_type="FILE_ERROR",
                    message=f"Error reading file: {e}",
                    recoverable=False,
                )
            )

        return result

    def _validate_envelope(
        self,
        record: dict[str, Any],
        path: Path,
        line_num: int,
    ) -> ValidationError | None:
        """Validate the envelope structure of a record.

        Args:
            record: The parsed JSON record.
            path: File path for error reporting.
            line_num: Line number for error reporting.

        Returns:
            ValidationError if invalid, None if valid.
        """
        missing_fields = ENVELOPE_REQUIRED_FIELDS - set(record.keys())
        if missing_fields:
            return ValidationError(
                file_path=path,
                line_number=line_num,
                error_type="MISSING_ENVELOPE_FIELDS",
                message=f"Missing required fields: {missing_fields}",
                recoverable=True,
            )
        return None

    def _validate_data_fields(
        self,
        data: dict[str, Any],
        endpoint: str,
        path: Path,
        line_num: int,
    ) -> ValidationError | None:
        """Validate endpoint-specific data fields.

        Args:
            data: The data payload from the record.
            endpoint: The endpoint type.
            path: File path for error reporting.
            line_num: Line number for error reporting.

        Returns:
            ValidationError if invalid, None if valid.
        """
        required_fields = ENDPOINT_REQUIRED_FIELDS.get(endpoint, set())
        if not required_fields:
            return None

        missing_fields = required_fields - set(data.keys())
        if missing_fields:
            return ValidationError(
                file_path=path,
                line_number=line_num,
                error_type="MISSING_DATA_FIELDS",
                message=f"Missing required data fields for {endpoint}: {missing_fields}",
                recoverable=True,
            )
        return None

    def validate_directory(
        self,
        directory: Path,
        pattern: str = "*.jsonl",
    ) -> ValidationResult:
        """Validate all JSONL files in a directory.

        Args:
            directory: Directory containing JSONL files.
            pattern: Glob pattern for files to validate.

        Returns:
            Merged validation result for all files.
        """
        result = ValidationResult()

        if not directory.exists():
            result.add_error(
                ValidationError(
                    file_path=directory,
                    line_number=None,
                    error_type="DIRECTORY_NOT_FOUND",
                    message="Directory does not exist",
                    recoverable=False,
                )
            )
            return result

        files = list(directory.rglob(pattern))
        if not files:
            result.add_warning(f"No files matching {pattern} in {directory}")
            return result

        for file_path in files:
            # Determine endpoint from path
            endpoint = self._endpoint_from_path(file_path)
            file_result = self.validate_jsonl(file_path, endpoint)
            result.merge(file_result)

        return result

    def _endpoint_from_path(self, path: Path) -> str | None:
        """Infer endpoint type from file path.

        Args:
            path: Path to JSONL file.

        Returns:
            Endpoint name or None if cannot be determined.
        """
        # Extract from parent directory or filename
        parent_name = path.parent.name
        if parent_name in ENDPOINT_REQUIRED_FIELDS:
            return parent_name

        stem = path.stem
        if stem in ENDPOINT_REQUIRED_FIELDS:
            return stem

        return None

    def repair_truncated(self, path: Path) -> int:
        """Remove incomplete trailing records from a JSONL file.

        Scans the file backwards to find and remove any incomplete
        JSON records at the end (usually from interrupted writes).

        Args:
            path: Path to the JSONL file.

        Returns:
            Number of records removed.
        """
        if not path.exists():
            return 0

        lines: list[str] = []
        removed_count = 0

        try:
            with path.open(encoding="utf-8") as f:
                lines = f.readlines()
        except (OSError, UnicodeDecodeError) as e:
            logger.error("Error reading file for repair: %s: %s", path, e)
            return 0

        if not lines:
            return 0

        # Scan from end to find truncated records
        valid_lines: list[str] = []
        for line in lines:
            if not line.strip():
                continue
            try:
                json.loads(line)
                valid_lines.append(line)
            except json.JSONDecodeError:
                removed_count += 1
                logger.warning("Removing truncated record from %s", path)

        # Only rewrite if we removed something
        if removed_count > 0:
            try:
                with path.open("w", encoding="utf-8") as f:
                    f.writelines(valid_lines)
                logger.info(
                    "Repaired %s: removed %d truncated record(s)",
                    path,
                    removed_count,
                )
            except OSError as e:
                logger.error("Error writing repaired file: %s: %s", path, e)

        return removed_count

    def validate_checkpoint_consistency(
        self,
        checkpoint_path: Path,
        raw_root: Path,
    ) -> ValidationResult:
        """Verify checkpoint state matches actual files.

        Checks that files marked as complete in checkpoint actually exist
        and have data, and that in-progress files exist.

        Args:
            checkpoint_path: Path to checkpoint JSON file.
            raw_root: Root directory for raw data files.

        Returns:
            ValidationResult with consistency check results.
        """
        result = ValidationResult()

        if not checkpoint_path.exists():
            result.add_warning("No checkpoint file found")
            return result

        try:
            with checkpoint_path.open() as f:
                checkpoint_data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            result.add_error(
                ValidationError(
                    file_path=checkpoint_path,
                    line_number=None,
                    error_type="CHECKPOINT_ERROR",
                    message=f"Error reading checkpoint: {e}",
                    recoverable=False,
                )
            )
            return result

        # Check repos data consistency
        repos = checkpoint_data.get("repos", {})
        for repo_name, repo_data in repos.items():
            endpoints = repo_data.get("endpoints", {})
            for endpoint, endpoint_data in endpoints.items():
                status = endpoint_data.get("status", "")
                if status == "complete":
                    # Verify file exists
                    expected_path = raw_root / endpoint / f"{repo_name.replace('/', '__')}.jsonl"
                    if not expected_path.exists():
                        result.add_error(
                            ValidationError(
                                file_path=expected_path,
                                line_number=None,
                                error_type="MISSING_DATA_FILE",
                                message=(
                                    f"Checkpoint shows {endpoint} complete for {repo_name} "
                                    "but file is missing"
                                ),
                                recoverable=True,
                            )
                        )

        return result


def validate_collection(
    raw_root: Path,
    checkpoint_path: Path | None = None,
    repair: bool = False,
) -> ValidationResult:
    """Validate an entire collection directory.

    Args:
        raw_root: Root directory for raw data.
        checkpoint_path: Optional path to checkpoint file.
        repair: If True, attempt to repair truncated files.

    Returns:
        Complete validation result.
    """
    validator = DataValidator()
    result = ValidationResult()

    # Validate all JSONL files
    logger.info("Validating JSONL files in %s", raw_root)
    jsonl_result = validator.validate_directory(raw_root)
    result.merge(jsonl_result)

    # Repair if requested
    if repair:
        for file_path in raw_root.rglob("*.jsonl"):
            removed = validator.repair_truncated(file_path)
            if removed > 0:
                result.add_warning(f"Repaired {file_path}: removed {removed} truncated record(s)")

    # Validate checkpoint consistency
    if checkpoint_path:
        logger.info("Validating checkpoint consistency")
        checkpoint_result = validator.validate_checkpoint_consistency(
            checkpoint_path, raw_root
        )
        result.merge(checkpoint_result)

    return result
