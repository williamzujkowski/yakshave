"""Tests for Parquet writer."""

from pathlib import Path

import pyarrow as pa

from gh_year_end.storage.parquet_writer import ParquetWriter, read_parquet, write_parquet


def test_write_and_read_parquet(tmp_path: Path) -> None:
    """Test writing and reading Parquet files."""
    # Create test data
    data = [
        {"user_id": "u1", "login": "alice", "year": 2025},
        {"user_id": "u2", "login": "bob", "year": 2025},
        {"user_id": "u3", "login": "charlie", "year": 2025},
    ]

    schema = pa.schema(
        [
            pa.field("user_id", pa.string()),
            pa.field("login", pa.string()),
            pa.field("year", pa.int32()),
        ]
    )

    table = pa.Table.from_pylist(data, schema=schema)

    # Write to Parquet
    output_path = tmp_path / "test.parquet"
    write_parquet(table, output_path)

    # Verify file exists
    assert output_path.exists()

    # Read back
    read_table = read_parquet(output_path)

    # Verify data
    assert read_table.num_rows == 3
    assert read_table.schema == schema


def test_parquet_writer_with_sorting(tmp_path: Path) -> None:
    """Test that sorting produces deterministic output."""
    # Create unsorted data
    data = [
        {"user_id": "u3", "login": "charlie", "year": 2025},
        {"user_id": "u1", "login": "alice", "year": 2025},
        {"user_id": "u2", "login": "bob", "year": 2025},
    ]

    schema = pa.schema(
        [
            pa.field("user_id", pa.string()),
            pa.field("login", pa.string()),
            pa.field("year", pa.int32()),
        ]
    )

    table = pa.Table.from_pylist(data, schema=schema)

    # Write with sorting
    output_path = tmp_path / "sorted.parquet"
    write_parquet(table, output_path, sort_by=["user_id"])

    # Read back
    read_table = read_parquet(output_path)

    # Verify sorted order
    user_ids = read_table.column("user_id").to_pylist()
    assert user_ids == ["u1", "u2", "u3"]


def test_parquet_writer_with_metadata(tmp_path: Path) -> None:
    """Test that metadata is attached correctly."""
    data = [{"value": 42}]
    schema = pa.schema([pa.field("value", pa.int32())])
    table = pa.Table.from_pylist(data, schema=schema)

    # Write with metadata
    output_path = tmp_path / "metadata.parquet"
    metadata = {"version": "1.0", "source": "test"}
    write_parquet(table, output_path, metadata=metadata)

    # Read back and check metadata
    read_table = read_parquet(output_path)
    table_metadata = read_table.schema.metadata

    assert table_metadata is not None
    assert b"version" in table_metadata
    assert table_metadata[b"version"] == b"1.0"


def test_count_rows(tmp_path: Path) -> None:
    """Test counting rows in Parquet file."""
    # Create test data
    data = [{"id": i} for i in range(100)]
    schema = pa.schema([pa.field("id", pa.int32())])
    table = pa.Table.from_pylist(data, schema=schema)

    # Write to Parquet
    output_path = tmp_path / "count.parquet"
    write_parquet(table, output_path)

    # Count rows
    writer = ParquetWriter()
    count = writer.count_rows(output_path)
    assert count == 100


def test_count_rows_nonexistent_file(tmp_path: Path) -> None:
    """Test counting rows in nonexistent file returns 0."""
    writer = ParquetWriter()
    nonexistent = tmp_path / "nonexistent.parquet"
    count = writer.count_rows(nonexistent)
    assert count == 0


def test_parquet_writer_compression_options(tmp_path: Path) -> None:
    """Test different compression codecs."""
    data = [{"value": i} for i in range(10)]
    schema = pa.schema([pa.field("value", pa.int32())])
    table = pa.Table.from_pylist(data, schema=schema)

    # Test snappy (default)
    snappy_path = tmp_path / "snappy.parquet"
    write_parquet(table, snappy_path, compression="snappy")
    assert snappy_path.exists()

    # Test gzip
    gzip_path = tmp_path / "gzip.parquet"
    write_parquet(table, gzip_path, compression="gzip")
    assert gzip_path.exists()


def test_deterministic_output(tmp_path: Path) -> None:
    """Test that writing same data twice produces identical files."""
    data = [
        {"user_id": "u3", "login": "charlie", "year": 2025},
        {"user_id": "u1", "login": "alice", "year": 2025},
        {"user_id": "u2", "login": "bob", "year": 2025},
    ]

    schema = pa.schema(
        [
            pa.field("user_id", pa.string()),
            pa.field("login", pa.string()),
            pa.field("year", pa.int32()),
        ]
    )

    table = pa.Table.from_pylist(data, schema=schema)

    # Write twice with sorting
    path1 = tmp_path / "file1.parquet"
    path2 = tmp_path / "file2.parquet"

    write_parquet(table, path1, sort_by=["user_id"])
    write_parquet(table, path2, sort_by=["user_id"])

    # Read both files
    table1 = read_parquet(path1)
    table2 = read_parquet(path2)

    # Verify they are identical
    assert table1.equals(table2)
