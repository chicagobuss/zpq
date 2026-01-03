"""
Pipeline - Orchestrates the full morsel-parallel Parquet processing pipeline.

This module ties together:
- Input parsing (row group enumeration, schema extraction)
- Row group stat-based pruning
- Filter evaluation
- Morsel workers
- Coordinator for parallel S3 upload

The pipeline implements two-phase column fetching:
1. Phase 1: Fetch filter column only, evaluate filter
2. Phase 2: If matches > 0, fetch remaining columns and encode
"""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from .coordinator import MorselCoordinator, MockS3Client, CoordinatorState
from .worker import MorselWorker, MockEncoder, FilterEvaluator, SelectionVector, ColumnData


@dataclass
class RowGroupInfo:
    """Information about a row group from file metadata."""
    index: int
    num_rows: int
    total_byte_size: int
    column_chunks: List[Dict[str, Any]]

    # Stats for predicate pushdown
    min_values: Dict[int, Any] = None  # column_index -> min value
    max_values: Dict[int, Any] = None  # column_index -> max value

    def might_contain(self, filter_col: int, value: Any) -> bool:
        """Check if row group might contain matching rows based on stats."""
        if self.min_values is None or self.max_values is None:
            return True  # No stats, must scan

        if filter_col not in self.min_values:
            return True

        min_val = self.min_values[filter_col]
        max_val = self.max_values[filter_col]

        # For equality filter: value must be in [min, max]
        return min_val <= value <= max_val


@dataclass
class ParquetSchema:
    """Simplified Parquet schema."""
    columns: List[Dict[str, Any]]  # [{"name": "col1", "type": "INT64"}, ...]

    def get_column_index(self, name: str) -> Optional[int]:
        for i, col in enumerate(self.columns):
            if col["name"] == name:
                return i
        return None

    def get_column_names(self, indices: List[int]) -> List[str]:
        return [self.columns[i]["name"] for i in indices]


@dataclass
class FilterExpression:
    """Parsed filter expression."""
    column_name: str
    predicate: str  # "=", "<", ">", etc.
    value: Any


class MockParquetReader:
    """
    Mock Parquet file reader for testing.

    Real implementation will use zpq's file reader with S3 source.
    """

    def __init__(self, path: str):
        self.path = path

        # Mock schema
        self.schema = ParquetSchema(columns=[
            {"name": "id", "type": "INT64"},
            {"name": "category", "type": "STRING"},
            {"name": "value", "type": "DOUBLE"},
        ])

        # Mock row groups
        self.row_groups = [
            RowGroupInfo(
                index=i,
                num_rows=10000,
                total_byte_size=1_000_000,
                column_chunks=[
                    {"column_index": 0, "offset": 0, "size": 300_000},
                    {"column_index": 1, "offset": 300_000, "size": 400_000},
                    {"column_index": 2, "offset": 700_000, "size": 300_000},
                ],
                min_values={0: i * 10000, 1: f"cat_{i:03d}", 2: float(i)},
                max_values={0: (i + 1) * 10000 - 1, 1: f"cat_{i:03d}", 2: float(i + 1)},
            )
            for i in range(10)  # 10 row groups
        ]

    def get_row_group_data(self, index: int) -> bytes:
        """Fetch row group data. Mock implementation."""
        return f"row_group_{index}_data".encode() * 10000

    def prefetch_columns(self, row_group_index: int, column_indices: List[int]) -> None:
        """Prefetch specific columns for a row group. Mock implementation."""
        pass

    def decode_column(
        self,
        row_group_index: int,
        column_index: int,
    ) -> ColumnData:
        """Decode a column from a row group. Mock implementation."""
        col_name = self.schema.columns[column_index]["name"]
        num_rows = self.row_groups[row_group_index].num_rows

        # Generate mock data based on column type
        if col_name == "id":
            base = row_group_index * num_rows
            values = list(range(base, base + num_rows))
        elif col_name == "category":
            values = [f"cat_{row_group_index:03d}"] * num_rows
        else:
            values = [float(i) for i in range(num_rows)]

        return ColumnData(
            column_index=column_index,
            name=col_name,
            values=values,
        )


class Pipeline:
    """
    Orchestrates parallel Parquet processing with morsel architecture.

    Features:
    - Row group pruning via statistics
    - Two-phase column fetching
    - Parallel morsel processing
    - Parallel S3 multipart upload
    """

    def __init__(
        self,
        s3_client: Any,
        num_workers: int = 4,
        max_in_flight_parts: int = 8,
    ):
        self.s3 = s3_client
        self.num_workers = num_workers
        self.max_in_flight_parts = max_in_flight_parts

        # Stats
        self.stats = {
            "row_groups_total": 0,
            "row_groups_pruned": 0,
            "row_groups_processed": 0,
            "rows_input": 0,
            "rows_output": 0,
            "processing_time_ms": 0,
        }

    def run(
        self,
        input_path: str,
        output_bucket: str,
        output_key: str,
        filter_expr: Optional[FilterExpression] = None,
        selected_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Execute the full pipeline.

        Args:
            input_path: Path to input Parquet file (or S3 URI)
            output_bucket: Output S3 bucket
            output_key: Output S3 key
            filter_expr: Optional filter expression
            selected_columns: Optional list of column names to include

        Returns:
            Dictionary with stats and result info
        """
        start_time = time.time()

        print(f"[Pipeline] Starting: {input_path} -> s3://{output_bucket}/{output_key}")

        # Parse input file
        reader = MockParquetReader(input_path)
        schema = reader.schema

        # Resolve selected columns
        if selected_columns:
            column_indices = [
                schema.get_column_index(name)
                for name in selected_columns
            ]
            column_indices = [i for i in column_indices if i is not None]
        else:
            column_indices = list(range(len(schema.columns)))

        column_names = schema.get_column_names(column_indices)

        # Resolve filter column
        filter_col_idx = None
        if filter_expr:
            filter_col_idx = schema.get_column_index(filter_expr.column_name)

        # Prune row groups based on statistics
        row_groups_to_process = []
        for rg in reader.row_groups:
            self.stats["row_groups_total"] += 1

            if filter_expr and filter_col_idx is not None:
                if not rg.might_contain(filter_col_idx, filter_expr.value):
                    self.stats["row_groups_pruned"] += 1
                    print(f"[Pipeline] Pruned row group {rg.index} via stats")
                    continue

            row_groups_to_process.append(rg)

        print(f"[Pipeline] Processing {len(row_groups_to_process)}/{len(reader.row_groups)} row groups")

        if len(row_groups_to_process) == 0:
            print("[Pipeline] No row groups to process after pruning")
            return {
                "success": True,
                "rows_written": 0,
                "stats": self.stats,
            }

        # Initialize coordinator
        coordinator = MorselCoordinator(
            s3_client=self.s3,
            bucket=output_bucket,
            key=output_key,
            max_in_flight=self.max_in_flight_parts,
        )

        # Start multipart upload
        # Note: We may write fewer row groups than input due to filtering
        # but we need to start with an upper bound
        coordinator.start(
            schema={"columns": column_names},
            num_row_groups=len(row_groups_to_process),
        )

        # Create upload function
        def sync_upload(part_number: int, data: bytes) -> str:
            response = self.s3.upload_part(
                Bucket=output_bucket,
                Key=output_key,
                UploadId=coordinator.upload_id,
                PartNumber=part_number,
                Body=data,
            )
            return response["ETag"]

        # Create workers
        encoder = MockEncoder({"columns": column_names})
        workers = [
            MorselWorker(coordinator, encoder, worker_id=i)
            for i in range(self.num_workers)
        ]

        # Process row groups
        # In real implementation, this would use thread pool or async
        rows_written = 0

        for i, rg in enumerate(row_groups_to_process):
            worker = workers[i % len(workers)]

            # Two-phase column fetching
            selection = None

            if filter_expr and filter_col_idx is not None:
                # Phase 1: Fetch and evaluate filter column
                reader.prefetch_columns(rg.index, [filter_col_idx])
                filter_col_data = reader.decode_column(rg.index, filter_col_idx)

                evaluator = FilterEvaluator(
                    column_index=filter_col_idx,
                    column_name=filter_expr.column_name,
                    predicate=filter_expr.predicate,
                    value=filter_expr.value,
                )
                selection = evaluator.evaluate(filter_col_data)

                print(f"[Pipeline] Row group {rg.index}: {selection.count}/{rg.num_rows} rows match filter")

                if selection.is_empty():
                    print(f"[Pipeline] Row group {rg.index}: No matches, skipping")
                    # Need to account for this in coordinator
                    coordinator.total_row_groups -= 1
                    if coordinator.total_row_groups == coordinator._submitted_count:
                        # All expected row groups submitted (even if some were skipped)
                        with coordinator._lock:
                            if coordinator._state == CoordinatorState.UPLOADING:
                                coordinator._state = CoordinatorState.DRAINING
                    continue

            # Phase 2: Fetch remaining columns and process
            remaining_cols = [c for c in column_indices if c != filter_col_idx]
            if remaining_cols:
                reader.prefetch_columns(rg.index, remaining_cols)

            row_group_data = reader.get_row_group_data(rg.index)

            num_rows = worker.process(
                row_group_data=row_group_data,
                row_group_index=rg.index,
                selected_columns=column_indices,
                column_names=column_names,
                selection=selection,
                upload_fn=sync_upload,
            )

            rows_written += num_rows
            self.stats["row_groups_processed"] += 1
            self.stats["rows_output"] += num_rows

        # Wait for completion
        print("[Pipeline] Waiting for upload completion...")
        success = coordinator.wait_for_completion(timeout=60)

        self.stats["processing_time_ms"] = (time.time() - start_time) * 1000

        if success:
            print(f"[Pipeline] Complete! Wrote {rows_written} rows")
        else:
            print(f"[Pipeline] Failed! Error: {coordinator.error}")

        return {
            "success": success,
            "rows_written": rows_written,
            "stats": self.stats,
            "coordinator_stats": coordinator.get_stats(),
            "worker_stats": [w.get_stats() for w in workers],
        }


def run_parallel_pipeline(
    s3_client: Any,
    input_path: str,
    output_bucket: str,
    output_key: str,
    filter_expr: Optional[FilterExpression] = None,
    selected_columns: Optional[List[str]] = None,
    num_workers: int = 4,
) -> Dict[str, Any]:
    """
    Convenience function to run the pipeline.

    This version processes row groups truly in parallel using ThreadPoolExecutor.
    """
    start_time = time.time()

    print(f"[Pipeline] Starting parallel pipeline: {input_path} -> s3://{output_bucket}/{output_key}")

    # Parse input file
    reader = MockParquetReader(input_path)
    schema = reader.schema

    # Resolve columns
    if selected_columns:
        column_indices = [schema.get_column_index(name) for name in selected_columns]
        column_indices = [i for i in column_indices if i is not None]
    else:
        column_indices = list(range(len(schema.columns)))

    column_names = schema.get_column_names(column_indices)

    # Resolve filter
    filter_col_idx = None
    if filter_expr:
        filter_col_idx = schema.get_column_index(filter_expr.column_name)

    # Prune row groups
    row_groups = [
        rg for rg in reader.row_groups
        if not filter_expr or filter_col_idx is None or rg.might_contain(filter_col_idx, filter_expr.value)
    ]

    print(f"[Pipeline] Processing {len(row_groups)}/{len(reader.row_groups)} row groups in parallel")

    if not row_groups:
        return {"success": True, "rows_written": 0}

    # Initialize coordinator
    coordinator = MorselCoordinator(
        s3_client=s3_client,
        bucket=output_bucket,
        key=output_key,
        max_in_flight=8,
    )
    coordinator.start({"columns": column_names}, len(row_groups))

    def sync_upload(part_number: int, data: bytes) -> str:
        response = s3_client.upload_part(
            Bucket=output_bucket,
            Key=output_key,
            UploadId=coordinator.upload_id,
            PartNumber=part_number,
            Body=data,
        )
        return response["ETag"]

    # Process row groups in parallel
    encoder = MockEncoder({"columns": column_names})
    total_rows = 0

    def process_row_group(rg: RowGroupInfo, worker_id: int) -> int:
        """Process a single row group."""
        worker = MorselWorker(coordinator, encoder, worker_id=worker_id)

        # Two-phase fetch
        selection = None
        if filter_expr and filter_col_idx is not None:
            filter_col_data = reader.decode_column(rg.index, filter_col_idx)
            evaluator = FilterEvaluator(
                filter_col_idx,
                filter_expr.column_name,
                filter_expr.predicate,
                filter_expr.value,
            )
            selection = evaluator.evaluate(filter_col_data)

            if selection.is_empty():
                # Adjust coordinator expectation
                with coordinator._lock:
                    coordinator.total_row_groups -= 1
                    if (coordinator._state == CoordinatorState.UPLOADING and
                        coordinator.total_row_groups == coordinator._submitted_count):
                        coordinator._state = CoordinatorState.DRAINING
                return 0

        row_group_data = reader.get_row_group_data(rg.index)
        return worker.process(
            row_group_data=row_group_data,
            row_group_index=rg.index,
            selected_columns=column_indices,
            column_names=column_names,
            selection=selection,
            upload_fn=sync_upload,
        )

    # Fan out to thread pool
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {
            executor.submit(process_row_group, rg, i % num_workers): rg
            for i, rg in enumerate(row_groups)
        }

        for future in as_completed(futures):
            rg = futures[future]
            try:
                rows = future.result()
                total_rows += rows
            except Exception as e:
                print(f"[Pipeline] Error processing row group {rg.index}: {e}")
                coordinator.abort()
                raise

    # Wait for upload completion
    success = coordinator.wait_for_completion(timeout=60)

    elapsed = (time.time() - start_time) * 1000
    print(f"[Pipeline] Complete in {elapsed:.1f}ms, {total_rows} rows written")

    return {
        "success": success,
        "rows_written": total_rows,
        "elapsed_ms": elapsed,
        "coordinator_stats": coordinator.get_stats(),
    }


if __name__ == "__main__":
    print("Testing Pipeline...")

    s3 = MockS3Client(upload_delay_ms=10)

    # Test without filter
    print("\n=== Test 1: No filter ===")
    result = run_parallel_pipeline(
        s3_client=s3,
        input_path="s3://bucket/input.parquet",
        output_bucket="bucket",
        output_key="output/result.parquet",
        num_workers=4,
    )
    print(f"Result: {result['success']}, {result['rows_written']} rows")

    # Test with filter that matches some row groups
    print("\n=== Test 2: Filter matches some ===")
    s3 = MockS3Client(upload_delay_ms=10)  # Fresh client
    result = run_parallel_pipeline(
        s3_client=s3,
        input_path="s3://bucket/input.parquet",
        output_bucket="bucket",
        output_key="output/filtered.parquet",
        filter_expr=FilterExpression(
            column_name="category",
            predicate="=",
            value="cat_005",  # Only matches row group 5
        ),
        num_workers=4,
    )
    print(f"Result: {result['success']}, {result['rows_written']} rows")

    # Test with filter that matches nothing
    print("\n=== Test 3: Filter matches none ===")
    s3 = MockS3Client(upload_delay_ms=10)
    result = run_parallel_pipeline(
        s3_client=s3,
        input_path="s3://bucket/input.parquet",
        output_bucket="bucket",
        output_key="output/empty.parquet",
        filter_expr=FilterExpression(
            column_name="category",
            predicate="=",
            value="cat_999",  # Matches nothing
        ),
        num_workers=4,
    )
    print(f"Result: {result['success']}, {result['rows_written']} rows")
