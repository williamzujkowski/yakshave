"""JSONL writer for raw API responses with envelope structure."""

import asyncio
import json
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4


@dataclass
class EnvelopedRecord:
    """Envelope containing metadata and API response data.

    Attributes:
        timestamp: ISO 8601 timestamp of when record was written.
        source: API source type.
        endpoint: API endpoint that was called.
        request_id: UUID for this request.
        page: Page number for paginated results.
        data: Actual API response data.
    """

    timestamp: str
    source: Literal["github_rest", "github_graphql", "derived"]
    endpoint: str
    request_id: str
    page: int
    data: dict[str, Any]

    @classmethod
    def create(
        cls,
        source: Literal["github_rest", "github_graphql", "derived"],
        endpoint: str,
        data: dict[str, Any],
        page: int = 1,
        request_id: UUID | str | None = None,
    ) -> "EnvelopedRecord":
        """Create an enveloped record with current timestamp.

        Args:
            source: API source type.
            endpoint: API endpoint that was called.
            data: Actual API response data.
            page: Page number for paginated results.
            request_id: UUID for this request. Generates new UUID if None.

        Returns:
            EnvelopedRecord instance.
        """
        if request_id is None:
            request_id = uuid4()
        return cls(
            timestamp=datetime.now(UTC).isoformat(),
            source=source,
            endpoint=endpoint,
            request_id=str(request_id),
            page=page,
            data=data,
        )

    def to_json_line(self) -> str:
        """Convert record to JSONL line.

        Returns:
            JSON string with no trailing newline.
        """
        return json.dumps(asdict(self), separators=(",", ":"))


class JSONLWriter:
    """Synchronous JSONL writer for raw API responses.

    Writes records to JSONL files with automatic directory creation,
    buffering, and file appending for resumable collection.

    Example:
        writer = JSONLWriter(Path("data/repos.jsonl"))
        with writer:
            writer.write(
                source="github_rest",
                endpoint="/repos/org/repo",
                data={"id": 123, "name": "repo"},
            )
            writer.write_batch([
                EnvelopedRecord.create(...),
                EnvelopedRecord.create(...),
            ])
    """

    def __init__(
        self,
        path: Path,
        buffer_size: int = 100,
    ) -> None:
        """Initialize JSONL writer.

        Args:
            path: Path to JSONL file.
            buffer_size: Flush buffer after this many records.
        """
        self.path = path
        self.buffer_size = buffer_size
        self._file: Any = None
        self._buffer: list[str] = []
        self._record_count = 0

    def __enter__(self) -> "JSONLWriter":
        """Enter context manager."""
        self.open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager."""
        self.close()

    def open(self) -> None:
        """Open file for appending."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a")

    def close(self) -> None:
        """Flush buffer and close file."""
        if self._file is not None:
            self.flush()
            self._file.close()
            self._file = None

    def flush(self) -> None:
        """Flush buffered records to disk."""
        if self._buffer and self._file is not None:
            self._file.write("\n".join(self._buffer) + "\n")
            self._file.flush()
            self._buffer.clear()

    def write(
        self,
        source: Literal["github_rest", "github_graphql", "derived"],
        endpoint: str,
        data: dict[str, Any],
        page: int = 1,
        request_id: UUID | str | None = None,
    ) -> None:
        """Write single record to JSONL file.

        Args:
            source: API source type.
            endpoint: API endpoint that was called.
            data: Actual API response data.
            page: Page number for paginated results.
            request_id: UUID for this request. Generates new UUID if None.
        """
        record = EnvelopedRecord.create(
            source=source,
            endpoint=endpoint,
            data=data,
            page=page,
            request_id=request_id,
        )
        self._write_record(record)

    def write_batch(self, records: list[EnvelopedRecord]) -> None:
        """Write multiple records to JSONL file.

        Args:
            records: List of enveloped records to write.
        """
        for record in records:
            self._write_record(record)

    def _write_record(self, record: EnvelopedRecord) -> None:
        """Write single enveloped record to buffer.

        Args:
            record: Enveloped record to write.
        """
        self._buffer.append(record.to_json_line())
        self._record_count += 1

        if len(self._buffer) >= self.buffer_size:
            self.flush()

    @staticmethod
    def count_records(path: Path) -> int:
        """Count records in existing JSONL file.

        Args:
            path: Path to JSONL file.

        Returns:
            Number of records in file. Returns 0 if file doesn't exist.
        """
        if not path.exists():
            return 0

        count = 0
        with path.open() as f:
            for _ in f:
                count += 1
        return count

    @staticmethod
    def read_records(path: Path) -> Iterator[EnvelopedRecord]:
        """Read records from JSONL file.

        Args:
            path: Path to JSONL file.

        Yields:
            EnvelopedRecord instances.

        Raises:
            FileNotFoundError: If file doesn't exist.
        """
        with path.open() as f:
            for line in f:
                data = json.loads(line)
                yield EnvelopedRecord(**data)


class AsyncJSONLWriter:
    """Async JSONL writer for raw API responses.

    Thread-safe async writer with buffering and automatic flushing.
    Suitable for concurrent async collectors.

    Example:
        writer = AsyncJSONLWriter(Path("data/repos.jsonl"))
        async with writer:
            await writer.write(
                source="github_rest",
                endpoint="/repos/org/repo",
                data={"id": 123, "name": "repo"},
            )
            await writer.write_batch([
                EnvelopedRecord.create(...),
                EnvelopedRecord.create(...),
            ])
    """

    def __init__(
        self,
        path: Path,
        buffer_size: int = 100,
    ) -> None:
        """Initialize async JSONL writer.

        Args:
            path: Path to JSONL file.
            buffer_size: Flush buffer after this many records.
        """
        self.path = path
        self.buffer_size = buffer_size
        self._file: Any = None
        self._buffer: list[str] = []
        self._record_count = 0
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "AsyncJSONLWriter":
        """Enter async context manager."""
        await self.open()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit async context manager."""
        await self.close()

    async def open(self) -> None:
        """Open file for appending."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a")

    async def close(self) -> None:
        """Flush buffer and close file."""
        if self._file is not None:
            await self.flush()
            self._file.close()
            self._file = None

    async def flush(self) -> None:
        """Flush buffered records to disk."""
        async with self._lock:
            if self._buffer and self._file is not None:
                self._file.write("\n".join(self._buffer) + "\n")
                self._file.flush()
                self._buffer.clear()

    async def write(
        self,
        source: Literal["github_rest", "github_graphql", "derived"],
        endpoint: str,
        data: dict[str, Any],
        page: int = 1,
        request_id: UUID | str | None = None,
    ) -> None:
        """Write single record to JSONL file.

        Args:
            source: API source type.
            endpoint: API endpoint that was called.
            data: Actual API response data.
            page: Page number for paginated results.
            request_id: UUID for this request. Generates new UUID if None.
        """
        record = EnvelopedRecord.create(
            source=source,
            endpoint=endpoint,
            data=data,
            page=page,
            request_id=request_id,
        )
        await self._write_record(record)

    async def write_batch(self, records: list[EnvelopedRecord]) -> None:
        """Write multiple records to JSONL file.

        Args:
            records: List of enveloped records to write.
        """
        for record in records:
            await self._write_record(record)

    async def _write_record(self, record: EnvelopedRecord) -> None:
        """Write single enveloped record to buffer.

        Args:
            record: Enveloped record to write.
        """
        async with self._lock:
            self._buffer.append(record.to_json_line())
            self._record_count += 1

            # Flush without reacquiring lock
            if len(self._buffer) >= self.buffer_size and self._buffer and self._file is not None:
                self._file.write("\n".join(self._buffer) + "\n")
                self._file.flush()
                self._buffer.clear()

    @staticmethod
    async def count_records(path: Path) -> int:
        """Count records in existing JSONL file.

        Args:
            path: Path to JSONL file.

        Returns:
            Number of records in file. Returns 0 if file doesn't exist.
        """
        if not path.exists():
            return 0

        count = 0
        with path.open() as f:
            for _ in f:
                count += 1
        return count

    @staticmethod
    async def read_records(path: Path) -> AsyncIterator[EnvelopedRecord]:
        """Read records from JSONL file.

        Args:
            path: Path to JSONL file.

        Yields:
            EnvelopedRecord instances.

        Raises:
            FileNotFoundError: If file doesn't exist.
        """
        with path.open() as f:
            for line in f:
                data = json.loads(line)
                yield EnvelopedRecord(**data)


@contextmanager
def jsonl_writer(path: Path, buffer_size: int = 100) -> Iterator[JSONLWriter]:
    """Context manager for synchronous JSONL writer.

    Args:
        path: Path to JSONL file.
        buffer_size: Flush buffer after this many records.

    Yields:
        JSONLWriter instance.
    """
    writer = JSONLWriter(path, buffer_size=buffer_size)
    with writer:
        yield writer


@asynccontextmanager
async def async_jsonl_writer(
    path: Path,
    buffer_size: int = 100,
) -> AsyncIterator[AsyncJSONLWriter]:
    """Context manager for async JSONL writer.

    Args:
        path: Path to JSONL file.
        buffer_size: Flush buffer after this many records.

    Yields:
        AsyncJSONLWriter instance.
    """
    writer = AsyncJSONLWriter(path, buffer_size=buffer_size)
    async with writer:
        yield writer
