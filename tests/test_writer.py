"""Tests for JSONL writer module."""

import json
from pathlib import Path
from uuid import uuid4

import pytest

from gh_year_end.storage.writer import (
    AsyncJSONLWriter,
    EnvelopedRecord,
    JSONLWriter,
    async_jsonl_writer,
    jsonl_writer,
)


class TestEnvelopedRecord:
    """Tests for EnvelopedRecord dataclass."""

    def test_create_generates_valid_record(self) -> None:
        """Test EnvelopedRecord.create generates valid record."""
        record = EnvelopedRecord.create(
            source="github_rest",
            endpoint="/repos/owner/repo",
            data={"id": 123, "name": "test"},
        )

        assert record.source == "github_rest"
        assert record.endpoint == "/repos/owner/repo"
        assert record.data == {"id": 123, "name": "test"}
        assert record.page == 1
        assert isinstance(record.timestamp, str)
        assert isinstance(record.request_id, str)

    def test_create_with_custom_page(self) -> None:
        """Test EnvelopedRecord.create with custom page number."""
        record = EnvelopedRecord.create(
            source="github_rest",
            endpoint="/repos/owner/repo",
            data={},
            page=5,
        )

        assert record.page == 5

    def test_create_with_custom_request_id(self) -> None:
        """Test EnvelopedRecord.create with custom request_id."""
        custom_id = uuid4()
        record = EnvelopedRecord.create(
            source="github_rest",
            endpoint="/repos/owner/repo",
            data={},
            request_id=custom_id,
        )

        assert record.request_id == str(custom_id)

    def test_create_with_string_request_id(self) -> None:
        """Test EnvelopedRecord.create with string request_id."""
        custom_id = "test-request-id-123"
        record = EnvelopedRecord.create(
            source="github_rest",
            endpoint="/repos/owner/repo",
            data={},
            request_id=custom_id,
        )

        assert record.request_id == custom_id

    def test_create_graphql_source(self) -> None:
        """Test EnvelopedRecord.create with graphql source."""
        record = EnvelopedRecord.create(
            source="github_graphql",
            endpoint="/graphql",
            data={"query": "test"},
        )

        assert record.source == "github_graphql"

    def test_to_json_line_produces_valid_json(self) -> None:
        """Test to_json_line produces valid JSON."""
        record = EnvelopedRecord.create(
            source="github_rest",
            endpoint="/repos/owner/repo",
            data={"id": 123},
            request_id="test-id",
        )

        json_line = record.to_json_line()

        # Should be valid JSON
        parsed = json.loads(json_line)
        assert parsed["source"] == "github_rest"
        assert parsed["endpoint"] == "/repos/owner/repo"
        assert parsed["data"] == {"id": 123}
        assert parsed["request_id"] == "test-id"

    def test_to_json_line_no_trailing_newline(self) -> None:
        """Test to_json_line has no trailing newline."""
        record = EnvelopedRecord.create(
            source="github_rest",
            endpoint="/test",
            data={},
        )

        json_line = record.to_json_line()
        assert not json_line.endswith("\n")

    def test_to_json_line_compact_format(self) -> None:
        """Test to_json_line uses compact format (no spaces)."""
        record = EnvelopedRecord.create(
            source="github_rest",
            endpoint="/test",
            data={"key": "value"},
        )

        json_line = record.to_json_line()
        # Compact JSON should use , and : without spaces
        assert ", " not in json_line
        assert ": " not in json_line


class TestJSONLWriter:
    """Tests for synchronous JSONLWriter."""

    def test_write_creates_file(self, tmp_path: Path) -> None:
        """Test JSONLWriter creates file on write."""
        output_path = tmp_path / "test.jsonl"
        writer = JSONLWriter(output_path)

        with writer:
            writer.write(
                source="github_rest",
                endpoint="/test",
                data={"id": 1},
            )

        assert output_path.exists()

    def test_write_creates_parent_directories(self, tmp_path: Path) -> None:
        """Test JSONLWriter creates parent directories."""
        output_path = tmp_path / "nested" / "dir" / "test.jsonl"
        writer = JSONLWriter(output_path)

        with writer:
            writer.write(
                source="github_rest",
                endpoint="/test",
                data={"id": 1},
            )

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_write_single_record(self, tmp_path: Path) -> None:
        """Test writing single record."""
        output_path = tmp_path / "test.jsonl"
        writer = JSONLWriter(output_path)

        with writer:
            writer.write(
                source="github_rest",
                endpoint="/repos/owner/repo",
                data={"id": 123, "name": "test"},
            )

        # Read back and verify
        with output_path.open() as f:
            lines = f.readlines()

        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["source"] == "github_rest"
        assert record["data"]["id"] == 123

    def test_write_multiple_records(self, tmp_path: Path) -> None:
        """Test writing multiple records."""
        output_path = tmp_path / "test.jsonl"
        writer = JSONLWriter(output_path)

        with writer:
            for i in range(5):
                writer.write(
                    source="github_rest",
                    endpoint=f"/test/{i}",
                    data={"id": i},
                )

        # Read back and verify
        with output_path.open() as f:
            lines = f.readlines()

        assert len(lines) == 5
        for i, line in enumerate(lines):
            record = json.loads(line)
            assert record["data"]["id"] == i

    def test_write_batch(self, tmp_path: Path) -> None:
        """Test write_batch writes multiple records."""
        output_path = tmp_path / "test.jsonl"
        writer = JSONLWriter(output_path)

        records = [EnvelopedRecord.create("github_rest", f"/test/{i}", {"id": i}) for i in range(3)]

        with writer:
            writer.write_batch(records)

        # Read back and verify
        with output_path.open() as f:
            lines = f.readlines()

        assert len(lines) == 3

    def test_buffered_writes_flush_on_close(self, tmp_path: Path) -> None:
        """Test buffered writes flush when file is closed."""
        output_path = tmp_path / "test.jsonl"
        writer = JSONLWriter(output_path, buffer_size=100)

        with writer:
            # Write a few records (less than buffer size)
            for i in range(5):
                writer.write("github_rest", f"/test/{i}", {"id": i})

        # After context manager exits, file should be flushed
        with output_path.open() as f:
            lines = f.readlines()

        assert len(lines) == 5

    def test_buffered_writes_auto_flush(self, tmp_path: Path) -> None:
        """Test buffered writes auto-flush when buffer is full."""
        output_path = tmp_path / "test.jsonl"
        # Set small buffer size
        writer = JSONLWriter(output_path, buffer_size=2)

        with writer:
            # Write 3 records (exceeds buffer)
            for i in range(3):
                writer.write("github_rest", f"/test/{i}", {"id": i})

            # Manually flush to ensure all records are written
            writer.flush()

        with output_path.open() as f:
            lines = f.readlines()

        assert len(lines) == 3

    def test_count_records_existing_file(self, tmp_path: Path) -> None:
        """Test count_records counts records correctly."""
        output_path = tmp_path / "test.jsonl"

        # Write some records
        writer = JSONLWriter(output_path)
        with writer:
            for i in range(10):
                writer.write("github_rest", f"/test/{i}", {"id": i})

        # Count records
        count = JSONLWriter.count_records(output_path)
        assert count == 10

    def test_count_records_nonexistent_file(self, tmp_path: Path) -> None:
        """Test count_records returns 0 for nonexistent file."""
        output_path = tmp_path / "nonexistent.jsonl"
        count = JSONLWriter.count_records(output_path)
        assert count == 0

    def test_read_records_yields_records(self, tmp_path: Path) -> None:
        """Test read_records yields EnvelopedRecord instances."""
        output_path = tmp_path / "test.jsonl"

        # Write records
        writer = JSONLWriter(output_path)
        with writer:
            for i in range(3):
                writer.write("github_rest", f"/test/{i}", {"id": i})

        # Read records
        records = list(JSONLWriter.read_records(output_path))

        assert len(records) == 3
        for i, record in enumerate(records):
            assert isinstance(record, EnvelopedRecord)
            assert record.data["id"] == i

    def test_read_records_nonexistent_file(self, tmp_path: Path) -> None:
        """Test read_records raises FileNotFoundError for missing file."""
        output_path = tmp_path / "nonexistent.jsonl"

        with pytest.raises(FileNotFoundError):
            list(JSONLWriter.read_records(output_path))

    def test_append_to_existing_file(self, tmp_path: Path) -> None:
        """Test writing appends to existing file."""
        output_path = tmp_path / "test.jsonl"

        # Write first batch
        writer1 = JSONLWriter(output_path)
        with writer1:
            writer1.write("github_rest", "/test/1", {"id": 1})

        # Write second batch
        writer2 = JSONLWriter(output_path)
        with writer2:
            writer2.write("github_rest", "/test/2", {"id": 2})

        # Should have both records
        count = JSONLWriter.count_records(output_path)
        assert count == 2


class TestAsyncJSONLWriter:
    """Tests for asynchronous AsyncJSONLWriter."""

    @pytest.mark.asyncio
    async def test_async_write_creates_file(self, tmp_path: Path) -> None:
        """Test AsyncJSONLWriter creates file on write."""
        output_path = tmp_path / "test.jsonl"
        writer = AsyncJSONLWriter(output_path)

        async with writer:
            await writer.write(
                source="github_rest",
                endpoint="/test",
                data={"id": 1},
            )

        assert output_path.exists()

    @pytest.mark.asyncio
    async def test_async_write_single_record(self, tmp_path: Path) -> None:
        """Test async writing single record."""
        output_path = tmp_path / "test.jsonl"
        writer = AsyncJSONLWriter(output_path)

        async with writer:
            await writer.write(
                source="github_rest",
                endpoint="/repos/owner/repo",
                data={"id": 123, "name": "test"},
            )

        # Read back and verify
        with output_path.open() as f:
            lines = f.readlines()

        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["data"]["id"] == 123

    @pytest.mark.asyncio
    async def test_async_write_multiple_records(self, tmp_path: Path) -> None:
        """Test async writing multiple records."""
        output_path = tmp_path / "test.jsonl"
        writer = AsyncJSONLWriter(output_path)

        async with writer:
            for i in range(5):
                await writer.write(
                    source="github_rest",
                    endpoint=f"/test/{i}",
                    data={"id": i},
                )

        # Read back and verify
        with output_path.open() as f:
            lines = f.readlines()

        assert len(lines) == 5

    @pytest.mark.asyncio
    async def test_async_write_batch(self, tmp_path: Path) -> None:
        """Test async write_batch."""
        output_path = tmp_path / "test.jsonl"
        writer = AsyncJSONLWriter(output_path)

        records = [EnvelopedRecord.create("github_rest", f"/test/{i}", {"id": i}) for i in range(3)]

        async with writer:
            await writer.write_batch(records)

        # Read back and verify
        with output_path.open() as f:
            lines = f.readlines()

        assert len(lines) == 3

    @pytest.mark.asyncio
    async def test_async_buffered_writes_flush_on_close(self, tmp_path: Path) -> None:
        """Test async buffered writes flush when file is closed."""
        output_path = tmp_path / "test.jsonl"
        writer = AsyncJSONLWriter(output_path, buffer_size=100)

        async with writer:
            # Write a few records (less than buffer size)
            for i in range(5):
                await writer.write("github_rest", f"/test/{i}", {"id": i})

        # After context manager exits, file should be flushed
        with output_path.open() as f:
            lines = f.readlines()

        assert len(lines) == 5

    @pytest.mark.asyncio
    async def test_async_count_records(self, tmp_path: Path) -> None:
        """Test async count_records."""
        output_path = tmp_path / "test.jsonl"

        # Write some records
        writer = AsyncJSONLWriter(output_path)
        async with writer:
            for i in range(10):
                await writer.write("github_rest", f"/test/{i}", {"id": i})

        # Count records
        count = await AsyncJSONLWriter.count_records(output_path)
        assert count == 10

    @pytest.mark.asyncio
    async def test_async_read_records(self, tmp_path: Path) -> None:
        """Test async read_records yields EnvelopedRecord instances."""
        output_path = tmp_path / "test.jsonl"

        # Write records
        writer = AsyncJSONLWriter(output_path)
        async with writer:
            for i in range(3):
                await writer.write("github_rest", f"/test/{i}", {"id": i})

        # Read records
        records = []
        async for record in AsyncJSONLWriter.read_records(output_path):
            records.append(record)

        assert len(records) == 3
        for i, record in enumerate(records):
            assert isinstance(record, EnvelopedRecord)
            assert record.data["id"] == i

    @pytest.mark.asyncio
    async def test_async_buffered_writes_auto_flush(self, tmp_path: Path) -> None:
        """Test async buffered writes auto-flush when buffer is full."""
        output_path = tmp_path / "test.jsonl"
        # Set small buffer size
        writer = AsyncJSONLWriter(output_path, buffer_size=2)

        async with writer:
            # Write 3 records (exceeds buffer)
            for i in range(3):
                await writer.write("github_rest", f"/test/{i}", {"id": i})

        # After close, all records should be written
        with output_path.open() as f:
            lines = f.readlines()

        assert len(lines) == 3

    @pytest.mark.asyncio
    async def test_async_count_records_nonexistent_file(self, tmp_path: Path) -> None:
        """Test async count_records returns 0 for nonexistent file."""
        output_path = tmp_path / "nonexistent.jsonl"
        count = await AsyncJSONLWriter.count_records(output_path)
        assert count == 0

    @pytest.mark.asyncio
    async def test_async_append_to_existing_file(self, tmp_path: Path) -> None:
        """Test async writing appends to existing file."""
        output_path = tmp_path / "test.jsonl"

        # Write first batch
        writer1 = AsyncJSONLWriter(output_path)
        async with writer1:
            await writer1.write("github_rest", "/test/1", {"id": 1})

        # Write second batch
        writer2 = AsyncJSONLWriter(output_path)
        async with writer2:
            await writer2.write("github_rest", "/test/2", {"id": 2})

        # Should have both records
        count = await AsyncJSONLWriter.count_records(output_path)
        assert count == 2

    @pytest.mark.asyncio
    async def test_async_read_records_nonexistent_file(self, tmp_path: Path) -> None:
        """Test async read_records raises FileNotFoundError for missing file."""
        output_path = tmp_path / "nonexistent.jsonl"

        with pytest.raises(FileNotFoundError):
            async for _ in AsyncJSONLWriter.read_records(output_path):
                pass

    @pytest.mark.asyncio
    async def test_async_creates_parent_directories(self, tmp_path: Path) -> None:
        """Test AsyncJSONLWriter creates parent directories."""
        output_path = tmp_path / "nested" / "dir" / "test.jsonl"
        writer = AsyncJSONLWriter(output_path)

        async with writer:
            await writer.write(
                source="github_rest",
                endpoint="/test",
                data={"id": 1},
            )

        assert output_path.exists()
        assert output_path.parent.exists()


class TestContextManagers:
    """Tests for context manager functions."""

    def test_jsonl_writer_context_manager(self, tmp_path: Path) -> None:
        """Test jsonl_writer context manager function."""
        output_path = tmp_path / "test.jsonl"

        with jsonl_writer(output_path) as writer:
            writer.write(
                source="github_rest",
                endpoint="/test",
                data={"id": 1},
            )

        assert output_path.exists()

        # Verify record was written
        with output_path.open() as f:
            lines = f.readlines()

        assert len(lines) == 1

    def test_jsonl_writer_context_manager_with_buffer_size(self, tmp_path: Path) -> None:
        """Test jsonl_writer context manager with custom buffer size."""
        output_path = tmp_path / "test.jsonl"

        with jsonl_writer(output_path, buffer_size=50) as writer:
            for i in range(5):
                writer.write("github_rest", f"/test/{i}", {"id": i})

        # Verify all records were written
        count = JSONLWriter.count_records(output_path)
        assert count == 5

    @pytest.mark.asyncio
    async def test_async_jsonl_writer_context_manager(self, tmp_path: Path) -> None:
        """Test async_jsonl_writer context manager function."""
        output_path = tmp_path / "test.jsonl"

        async with async_jsonl_writer(output_path) as writer:
            await writer.write(
                source="github_rest",
                endpoint="/test",
                data={"id": 1},
            )

        assert output_path.exists()

        # Verify record was written
        with output_path.open() as f:
            lines = f.readlines()

        assert len(lines) == 1

    @pytest.mark.asyncio
    async def test_async_jsonl_writer_context_manager_with_buffer_size(
        self, tmp_path: Path
    ) -> None:
        """Test async_jsonl_writer context manager with custom buffer size."""
        output_path = tmp_path / "test.jsonl"

        async with async_jsonl_writer(output_path, buffer_size=50) as writer:
            for i in range(5):
                await writer.write("github_rest", f"/test/{i}", {"id": i})

        # Verify all records were written
        count = await AsyncJSONLWriter.count_records(output_path)
        assert count == 5


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_write_with_custom_page_and_request_id(self, tmp_path: Path) -> None:
        """Test writing with custom page number and request ID."""
        output_path = tmp_path / "test.jsonl"
        request_id = uuid4()

        with jsonl_writer(output_path) as writer:
            writer.write(
                source="github_rest",
                endpoint="/test",
                data={"id": 1},
                page=3,
                request_id=request_id,
            )

        # Read back and verify
        records = list(JSONLWriter.read_records(output_path))
        assert len(records) == 1
        assert records[0].page == 3
        assert records[0].request_id == str(request_id)

    @pytest.mark.asyncio
    async def test_async_write_with_custom_page_and_request_id(self, tmp_path: Path) -> None:
        """Test async writing with custom page number and request ID."""
        output_path = tmp_path / "test.jsonl"
        request_id = uuid4()

        async with async_jsonl_writer(output_path) as writer:
            await writer.write(
                source="github_rest",
                endpoint="/test",
                data={"id": 1},
                page=3,
                request_id=request_id,
            )

        # Read back and verify
        records = []
        async for record in AsyncJSONLWriter.read_records(output_path):
            records.append(record)

        assert len(records) == 1
        assert records[0].page == 3
        assert records[0].request_id == str(request_id)

    def test_enveloped_record_derived_source(self) -> None:
        """Test EnvelopedRecord with 'derived' source type."""
        record = EnvelopedRecord.create(
            source="derived",
            endpoint="/internal/metrics",
            data={"metric": "value"},
        )

        assert record.source == "derived"

    def test_manual_open_close(self, tmp_path: Path) -> None:
        """Test manual open and close without context manager."""
        output_path = tmp_path / "test.jsonl"
        writer = JSONLWriter(output_path)

        writer.open()
        writer.write("github_rest", "/test", {"id": 1})
        writer.close()

        assert output_path.exists()
        count = JSONLWriter.count_records(output_path)
        assert count == 1

    @pytest.mark.asyncio
    async def test_async_manual_open_close(self, tmp_path: Path) -> None:
        """Test async manual open and close without context manager."""
        output_path = tmp_path / "test.jsonl"
        writer = AsyncJSONLWriter(output_path)

        await writer.open()
        await writer.write("github_rest", "/test", {"id": 1})
        await writer.close()

        assert output_path.exists()
        count = await AsyncJSONLWriter.count_records(output_path)
        assert count == 1

    def test_empty_data_dict(self, tmp_path: Path) -> None:
        """Test writing record with empty data dictionary."""
        output_path = tmp_path / "test.jsonl"

        with jsonl_writer(output_path) as writer:
            writer.write("github_rest", "/test", {})

        records = list(JSONLWriter.read_records(output_path))
        assert len(records) == 1
        assert records[0].data == {}

    def test_write_batch_empty_list(self, tmp_path: Path) -> None:
        """Test write_batch with empty list."""
        output_path = tmp_path / "test.jsonl"

        with jsonl_writer(output_path) as writer:
            writer.write_batch([])

        # File should exist but be empty
        assert output_path.exists()
        count = JSONLWriter.count_records(output_path)
        assert count == 0

    @pytest.mark.asyncio
    async def test_async_write_batch_empty_list(self, tmp_path: Path) -> None:
        """Test async write_batch with empty list."""
        output_path = tmp_path / "test.jsonl"

        async with async_jsonl_writer(output_path) as writer:
            await writer.write_batch([])

        # File should exist but be empty
        assert output_path.exists()
        count = await AsyncJSONLWriter.count_records(output_path)
        assert count == 0

    def test_count_records_empty_file(self, tmp_path: Path) -> None:
        """Test count_records on an empty file."""
        output_path = tmp_path / "empty.jsonl"
        output_path.touch()  # Create empty file

        count = JSONLWriter.count_records(output_path)
        assert count == 0

    @pytest.mark.asyncio
    async def test_async_count_records_empty_file(self, tmp_path: Path) -> None:
        """Test async count_records on an empty file."""
        output_path = tmp_path / "empty.jsonl"
        output_path.touch()  # Create empty file

        count = await AsyncJSONLWriter.count_records(output_path)
        assert count == 0
