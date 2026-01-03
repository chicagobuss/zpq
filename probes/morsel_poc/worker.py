"""
MorselWorker - Processes row groups and submits to coordinator.

Each worker handles a row group (morsel) through the pipeline:
1. Decode columns from input
2. Apply filter (selection vector)
3. Encode to Parquet row group bytes
4. Submit to coordinator for upload

This is a simplified implementation that uses mock encoding.
The real Zig implementation will use actual Parquet encoding.
"""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import time

from .coordinator import MorselCoordinator, RowGroupMetadata, ColumnChunkMetadata


@dataclass
class ColumnData:
    """Column data extracted from a row group."""
    column_index: int
    name: str
    values: List[Any]
    null_mask: Optional[List[bool]] = None


@dataclass
class SelectionVector:
    """Indicates which rows passed the filter."""
    indices: List[int]  # Row indices that passed

    @property
    def count(self) -> int:
        return len(self.indices)

    def is_empty(self) -> bool:
        return len(self.indices) == 0


class MockEncoder:
    """
    Mock Parquet encoder for testing.

    Real implementation will:
    - Determine encoding per column (PLAIN, RLE_DICTIONARY, etc.)
    - Compress with configured codec (SNAPPY, ZSTD, etc.)
    - Build proper page headers and column chunks
    """

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self.compression = "none"  # Could be snappy, zstd, etc.

    def encode_row_group(
        self,
        columns: Dict[int, ColumnData],
        row_group_index: int,
    ) -> tuple[bytes, RowGroupMetadata]:
        """
        Encode columns into Parquet row group bytes.

        Args:
            columns: Column data by column index
            row_group_index: Index of this row group

        Returns:
            (encoded_bytes, metadata)
        """
        # Mock encoding: just serialize column data
        # Real implementation would produce proper Parquet bytes

        encoded_chunks = []
        column_metadata = []
        current_offset = 0
        total_rows = 0

        for col_idx in sorted(columns.keys()):
            col_data = columns[col_idx]

            # Mock encode: just pickle-ish representation
            chunk_bytes = self._encode_column(col_data)
            encoded_chunks.append(chunk_bytes)

            column_metadata.append(ColumnChunkMetadata(
                column_index=col_idx,
                relative_offset=current_offset,
                compressed_size=len(chunk_bytes),
                uncompressed_size=len(chunk_bytes),  # Same for no compression
                num_values=len(col_data.values),
            ))

            current_offset += len(chunk_bytes)
            total_rows = max(total_rows, len(col_data.values))

        # Concatenate all chunks
        row_group_bytes = b"".join(encoded_chunks)

        metadata = RowGroupMetadata(
            row_group_index=row_group_index,
            num_rows=total_rows,
            total_byte_size=len(row_group_bytes),
            columns=column_metadata,
        )

        return row_group_bytes, metadata

    def _encode_column(self, col_data: ColumnData) -> bytes:
        """Encode a single column. Mock implementation."""
        import json
        return json.dumps({
            "name": col_data.name,
            "values": col_data.values[:100],  # Truncate for mock
            "count": len(col_data.values),
        }).encode("utf-8")


class MockDecoder:
    """
    Mock Parquet decoder for testing.

    Real implementation will use BatchReader to decode columns.
    """

    def decode_column(
        self,
        row_group_data: bytes,
        column_index: int,
        column_name: str,
    ) -> ColumnData:
        """Decode a column from row group bytes. Mock implementation."""
        # Generate mock data
        return ColumnData(
            column_index=column_index,
            name=column_name,
            values=list(range(1000)),  # Mock 1000 values
        )


class MorselWorker:
    """
    Processes a single row group (morsel) through the pipeline.

    Lifecycle:
    1. Receive row group assignment from pipeline
    2. Decode filter column(s)
    3. Evaluate filter to produce selection vector
    4. If matches > 0: decode remaining columns with selection
    5. Encode selected rows to new row group
    6. Submit to coordinator
    """

    def __init__(
        self,
        coordinator: MorselCoordinator,
        encoder: MockEncoder,
        worker_id: int = 0,
    ):
        self.coordinator = coordinator
        self.encoder = encoder
        self.worker_id = worker_id

        # Stats
        self.stats = {
            "row_groups_processed": 0,
            "rows_input": 0,
            "rows_output": 0,
            "encode_time_ms": 0,
        }

    def process(
        self,
        row_group_data: bytes,
        row_group_index: int,
        selected_columns: List[int],
        column_names: List[str],
        selection: Optional[SelectionVector] = None,
        upload_fn=None,
    ) -> int:
        """
        Process a row group and submit to coordinator.

        Args:
            row_group_data: Raw row group bytes (for real impl, would be source)
            row_group_index: Index of this row group
            selected_columns: Column indices to include in output
            column_names: Column names for metadata
            selection: Optional selection vector from filter evaluation
            upload_fn: Upload function to pass to coordinator

        Returns:
            Number of rows in output
        """
        print(f"[Worker {self.worker_id}] Processing row group {row_group_index}")

        decoder = MockDecoder()

        # Decode selected columns
        columns: Dict[int, ColumnData] = {}
        for col_idx, col_name in zip(selected_columns, column_names):
            col_data = decoder.decode_column(row_group_data, col_idx, col_name)

            # Apply selection if provided
            if selection and not selection.is_empty():
                col_data = self._apply_selection(col_data, selection)

            columns[col_idx] = col_data
            self.stats["rows_input"] += len(col_data.values)

        # Check if any rows to output
        if columns:
            first_col = next(iter(columns.values()))
            num_rows = len(first_col.values)
        else:
            num_rows = 0

        if num_rows == 0:
            print(f"[Worker {self.worker_id}] Row group {row_group_index}: 0 rows after filter, skipping")
            return 0

        # Encode to Parquet row group
        encode_start = time.time()
        encoded_bytes, metadata = self.encoder.encode_row_group(columns, row_group_index)
        self.stats["encode_time_ms"] += (time.time() - encode_start) * 1000

        print(f"[Worker {self.worker_id}] Encoded {num_rows} rows, {len(encoded_bytes)} bytes")

        # Submit to coordinator
        self.coordinator.submit_morsel(encoded_bytes, metadata, upload_fn=upload_fn)

        # Update stats
        self.stats["row_groups_processed"] += 1
        self.stats["rows_output"] += num_rows

        return num_rows

    def _apply_selection(
        self,
        col_data: ColumnData,
        selection: SelectionVector,
    ) -> ColumnData:
        """Apply selection vector to column data."""
        selected_values = [col_data.values[i] for i in selection.indices if i < len(col_data.values)]

        selected_nulls = None
        if col_data.null_mask:
            selected_nulls = [col_data.null_mask[i] for i in selection.indices if i < len(col_data.null_mask)]

        return ColumnData(
            column_index=col_data.column_index,
            name=col_data.name,
            values=selected_values,
            null_mask=selected_nulls,
        )

    def get_stats(self) -> dict:
        return self.stats.copy()


class FilterEvaluator:
    """
    Evaluates filter expressions against column data.

    Real implementation will use EncodedFilter for byte-level comparison.
    """

    def __init__(self, column_index: int, column_name: str, predicate: str, value: Any):
        self.column_index = column_index
        self.column_name = column_name
        self.predicate = predicate  # "=", "<", ">", "<=", ">="
        self.value = value

    def evaluate(self, col_data: ColumnData) -> SelectionVector:
        """
        Evaluate filter and return selection vector.

        Args:
            col_data: Column data to filter

        Returns:
            SelectionVector with indices of matching rows
        """
        matching_indices = []

        for i, val in enumerate(col_data.values):
            if self._matches(val):
                matching_indices.append(i)

        return SelectionVector(indices=matching_indices)

    def _matches(self, value: Any) -> bool:
        """Check if value matches the predicate."""
        if self.predicate == "=":
            return value == self.value
        elif self.predicate == "<":
            return value < self.value
        elif self.predicate == ">":
            return value > self.value
        elif self.predicate == "<=":
            return value <= self.value
        elif self.predicate == ">=":
            return value >= self.value
        else:
            raise ValueError(f"Unknown predicate: {self.predicate}")


if __name__ == "__main__":
    from .coordinator import MorselCoordinator, MockS3Client

    print("Testing MorselWorker...")

    # Setup
    s3 = MockS3Client(upload_delay_ms=5)
    coordinator = MorselCoordinator(
        s3_client=s3,
        bucket="test-bucket",
        key="output/test.parquet",
        max_in_flight=4,
    )

    schema = {"columns": ["id", "category", "value"]}
    encoder = MockEncoder(schema)
    worker = MorselWorker(coordinator, encoder, worker_id=0)

    coordinator.start(schema, num_row_groups=3)

    # Create upload function
    def sync_upload(part_number: int, data: bytes) -> str:
        response = s3.upload_part(
            Bucket="test-bucket",
            Key="output/test.parquet",
            UploadId=coordinator.upload_id,
            PartNumber=part_number,
            Body=data,
        )
        return response["ETag"]

    # Process row groups
    for rg_idx in range(3):
        row_group_data = f"mock_data_{rg_idx}".encode()

        # Mock filter: select half the rows
        selection = SelectionVector(indices=list(range(0, 1000, 2)))

        worker.process(
            row_group_data=row_group_data,
            row_group_index=rg_idx,
            selected_columns=[0, 1, 2],
            column_names=["id", "category", "value"],
            selection=selection,
            upload_fn=sync_upload,
        )

    # Wait for completion
    success = coordinator.wait_for_completion(timeout=10)

    print(f"\nResult: {'SUCCESS' if success else 'FAILED'}")
    print(f"Worker stats: {worker.get_stats()}")
    print(f"Coordinator stats: {coordinator.get_stats()}")
