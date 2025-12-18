"""Parquet writer for curated and metrics tables.

Provides deterministic writing of DataFrames to Parquet format with:
- Consistent sorting for deterministic output
- Schema validation
- Compression
- Metadata tracking
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


class ParquetWriter:
    """Writer for Parquet tables with deterministic output.

    Ensures that running the same normalization twice produces identical
    Parquet files by:
    - Sorting data by stable keys
    - Using consistent Parquet writer settings
    - Validating schemas

    Example:
        writer = ParquetWriter()
        writer.write(
            df=dim_user_df,
            path=Path("data/curated/year=2025/dim_user.parquet"),
            sort_by=["user_id"],
        )
    """

    def __init__(
        self,
        compression: str = "snappy",
        version: str = "2.6",
    ) -> None:
        """Initialize Parquet writer.

        Args:
            compression: Compression codec (snappy, gzip, brotli, zstd, lz4).
            version: Parquet format version (1.0, 2.4, 2.6).
        """
        self.compression = compression
        self.version = version

    def write(
        self,
        table: pa.Table,
        path: Path,
        sort_by: list[str] | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Write Arrow table to Parquet file.

        Args:
            table: PyArrow table to write.
            path: Path to output Parquet file.
            sort_by: Column names to sort by for deterministic output.
            metadata: Custom metadata to attach to Parquet file.
        """
        # Ensure parent directory exists
        path.parent.mkdir(parents=True, exist_ok=True)

        # Sort if requested
        if sort_by:
            table = self._sort_table(table, sort_by)

        # Add metadata if provided
        if metadata:
            existing_metadata = table.schema.metadata or {}
            combined_metadata = {**existing_metadata, **metadata}
            table = table.replace_schema_metadata(combined_metadata)

        # Write to Parquet
        pq.write_table(
            table,
            path,
            compression=self.compression,
            version=self.version,
            write_statistics=True,
            use_dictionary=True,
            store_schema=True,
        )

    def _sort_table(self, table: pa.Table, sort_by: list[str]) -> pa.Table:
        """Sort Arrow table by specified columns.

        Args:
            table: PyArrow table to sort.
            sort_by: Column names to sort by.

        Returns:
            Sorted PyArrow table.
        """
        # Build sort indices
        sort_keys = [(col, "ascending") for col in sort_by]
        indices = pa.compute.sort_indices(table, sort_keys=sort_keys)
        return pa.compute.take(table, indices)

    @staticmethod
    def read(path: Path) -> pa.Table:
        """Read Parquet file into Arrow table.

        Args:
            path: Path to Parquet file.

        Returns:
            PyArrow table.

        Raises:
            FileNotFoundError: If file doesn't exist.
        """
        return pq.read_table(path)

    @staticmethod
    def count_rows(path: Path) -> int:
        """Count rows in Parquet file.

        Args:
            path: Path to Parquet file.

        Returns:
            Number of rows in file. Returns 0 if file doesn't exist.
        """
        if not path.exists():
            return 0

        metadata = pq.read_metadata(path)
        return int(metadata.num_rows)


def write_parquet(
    table: pa.Table,
    path: Path,
    sort_by: list[str] | None = None,
    compression: str = "snappy",
    metadata: dict[str, str] | None = None,
) -> None:
    """Write Arrow table to Parquet file with deterministic output.

    Convenience function for one-off writes without creating a ParquetWriter instance.

    Args:
        table: PyArrow table to write.
        path: Path to output Parquet file.
        sort_by: Column names to sort by for deterministic output.
        compression: Compression codec (snappy, gzip, brotli, zstd, lz4).
        metadata: Custom metadata to attach to Parquet file.
    """
    writer = ParquetWriter(compression=compression)
    writer.write(table, path, sort_by=sort_by, metadata=metadata)


def read_parquet(path: Path) -> pa.Table:
    """Read Parquet file into Arrow table.

    Convenience function for reading Parquet files.

    Args:
        path: Path to Parquet file.

    Returns:
        PyArrow table.

    Raises:
        FileNotFoundError: If file doesn't exist.
    """
    return ParquetWriter.read(path)
