#!/usr/bin/env python3
"""
Generate benchmark Parquet files with comprehensive type and feature coverage.

Creates 1MB, 10MB, and 100MB files with:
- All primitive types (int8-64, uint8-64, float, double, bool)
- String columns (plain + dictionary encoded)
- Binary columns
- Temporal types (timestamp, date, time)
- Nullable vs required columns
- Various compression codecs (snappy, gzip, zstd, uncompressed)
- Multiple row groups
- Sorted columns (for predicate pushdown testing)
- High/low cardinality dictionary columns

Usage:
    python tools/testdata/generate_benchmarks.py [--output-dir data/benchmark]
"""

import argparse
import os
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def generate_strings(n: int, min_len: int = 5, max_len: int = 50) -> list[str]:
    """Generate random strings."""
    return [
        "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(min_len, max_len)))
        for _ in range(n)
    ]


def generate_dictionary_strings(n: int, cardinality: int) -> list[str]:
    """Generate strings with controlled cardinality for dictionary encoding."""
    vocab = [f"category_{i:04d}" for i in range(cardinality)]
    return [random.choice(vocab) for _ in range(n)]


def generate_timestamps(n: int, start: datetime = None) -> list[datetime]:
    """Generate random timestamps."""
    if start is None:
        start = datetime(2020, 1, 1)
    return [start + timedelta(seconds=random.randint(0, 365 * 24 * 3600 * 4)) for _ in range(n)]


def create_table(num_rows: int, sorted_fraction: float = 0.1) -> pa.Table:
    """
    Create a PyArrow table with comprehensive column types.

    Args:
        num_rows: Number of rows to generate
        sorted_fraction: Fraction of rows that are sorted (for predicate pushdown)
    """
    random.seed(42)
    np.random.seed(42)

    # Sorted index for some columns
    sorted_idx = np.arange(num_rows)

    # --- Integer types (signed) ---
    int8_vals = np.random.randint(-128, 127, num_rows, dtype=np.int8)
    int16_vals = np.random.randint(-32768, 32767, num_rows, dtype=np.int16)
    int32_sorted = sorted_idx.astype(np.int32)  # Sorted for predicate pushdown
    int32_random = np.random.randint(-2**30, 2**30, num_rows, dtype=np.int32)
    int64_sorted = sorted_idx.astype(np.int64) * 1000  # Sorted
    int64_random = np.random.randint(-2**60, 2**60, num_rows, dtype=np.int64)

    # --- Integer types (unsigned) ---
    uint8_vals = np.random.randint(0, 255, num_rows, dtype=np.uint8)
    uint16_vals = np.random.randint(0, 65535, num_rows, dtype=np.uint16)
    uint32_vals = np.random.randint(0, 2**31, num_rows, dtype=np.uint32)
    uint64_vals = np.random.randint(0, 2**62, num_rows, dtype=np.uint64)

    # --- Floating point ---
    float32_vals = np.random.randn(num_rows).astype(np.float32) * 1000
    float64_vals = np.random.randn(num_rows) * 1e6
    float64_sorted = np.sort(np.random.randn(num_rows)) * 1000  # Sorted

    # --- Boolean ---
    bool_vals = np.random.choice([True, False], num_rows)
    bool_sparse = np.random.choice([True, False], num_rows, p=[0.01, 0.99])  # 1% true

    # --- Strings ---
    string_random = generate_strings(num_rows, 10, 100)
    string_dict_low = generate_dictionary_strings(num_rows, cardinality=10)  # Low cardinality
    string_dict_high = generate_dictionary_strings(num_rows, cardinality=1000)  # High cardinality
    string_sorted = [f"row_{i:010d}" for i in range(num_rows)]  # Sorted

    # --- Binary ---
    binary_vals = [os.urandom(random.randint(10, 100)) for _ in range(num_rows)]

    # --- Temporal ---
    timestamps = generate_timestamps(num_rows)
    timestamp_sorted = sorted(timestamps)
    dates = [ts.date() for ts in timestamps]

    # --- Nullable columns (10% null) ---
    null_mask = np.random.choice([True, False], num_rows, p=[0.1, 0.9])
    int32_nullable = np.where(null_mask, None, int32_random)
    float64_nullable = np.where(null_mask, None, float64_vals)
    string_nullable = [None if null_mask[i] else string_random[i] for i in range(num_rows)]

    # --- Sparse nulls (99% null - tests RLE efficiency) ---
    sparse_null_mask = np.random.choice([True, False], num_rows, p=[0.99, 0.01])
    int32_sparse = np.where(sparse_null_mask, None, int32_random)

    # Build the table
    table = pa.table({
        # Signed integers
        "int8": pa.array(int8_vals, type=pa.int8()),
        "int16": pa.array(int16_vals, type=pa.int16()),
        "int32_sorted": pa.array(int32_sorted, type=pa.int32()),
        "int32_random": pa.array(int32_random, type=pa.int32()),
        "int64_sorted": pa.array(int64_sorted, type=pa.int64()),
        "int64_random": pa.array(int64_random, type=pa.int64()),

        # Unsigned integers
        "uint8": pa.array(uint8_vals, type=pa.uint8()),
        "uint16": pa.array(uint16_vals, type=pa.uint16()),
        "uint32": pa.array(uint32_vals, type=pa.uint32()),
        "uint64": pa.array(uint64_vals, type=pa.uint64()),

        # Floating point
        "float32": pa.array(float32_vals, type=pa.float32()),
        "float64": pa.array(float64_vals, type=pa.float64()),
        "float64_sorted": pa.array(float64_sorted, type=pa.float64()),

        # Boolean
        "bool": pa.array(bool_vals, type=pa.bool_()),
        "bool_sparse": pa.array(bool_sparse, type=pa.bool_()),

        # Strings
        "string_random": pa.array(string_random, type=pa.string()),
        "string_dict_low": pa.array(string_dict_low, type=pa.string()),
        "string_dict_high": pa.array(string_dict_high, type=pa.string()),
        "string_sorted": pa.array(string_sorted, type=pa.string()),

        # Binary
        "binary": pa.array(binary_vals, type=pa.binary()),

        # Temporal
        "timestamp": pa.array(timestamps, type=pa.timestamp("us")),
        "timestamp_sorted": pa.array(timestamp_sorted, type=pa.timestamp("us")),
        "date": pa.array(dates, type=pa.date32()),

        # Nullable
        "int32_nullable": pa.array(int32_nullable, type=pa.int32()),
        "float64_nullable": pa.array(float64_nullable, type=pa.float64()),
        "string_nullable": pa.array(string_nullable, type=pa.string()),
        "int32_sparse": pa.array(int32_sparse, type=pa.int32()),
    })

    return table


def estimate_rows_for_size(target_mb: float) -> int:
    """Estimate number of rows needed for target file size."""
    # Rough estimate: ~200 bytes per row with our schema
    bytes_per_row = 200
    target_bytes = target_mb * 1024 * 1024
    return int(target_bytes / bytes_per_row)


def write_parquet(
    table: pa.Table,
    path: Path,
    compression: str = "snappy",
    row_group_size: int = None,
    use_dictionary: bool = True,
    write_statistics: bool = True,
) -> None:
    """Write table to Parquet with specified options."""
    if row_group_size is None:
        row_group_size = len(table) // 4  # 4 row groups by default

    pq.write_table(
        table,
        path,
        compression=compression,
        row_group_size=row_group_size,
        use_dictionary=use_dictionary,
        write_statistics=write_statistics,
        # Enable page index for predicate pushdown
        write_page_index=True,
        data_page_size=1024 * 1024,  # 1MB pages
    )


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark Parquet files")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/benchmark"),
        help="Output directory for generated files",
    )
    parser.add_argument(
        "--sizes",
        type=str,
        default="1,10,100",
        help="Comma-separated list of target sizes in MB",
    )
    args = parser.parse_args()

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    sizes = [int(s) for s in args.sizes.split(",")]

    for size_mb in sizes:
        print(f"\n{'=' * 60}")
        print(f"Generating {size_mb}MB benchmark file...")
        print(f"{'=' * 60}")

        num_rows = estimate_rows_for_size(size_mb)
        print(f"  Target rows: {num_rows:,}")

        # Generate table
        print("  Generating data...")
        table = create_table(num_rows)
        print(f"  Columns: {len(table.column_names)}")
        print(f"  Rows: {len(table):,}")

        # Write with different compression options
        compressions = ["snappy", "zstd", "gzip", "none"]

        for compression in compressions:
            filename = f"benchmark_{size_mb}mb_{compression}.parquet"
            filepath = output_dir / filename

            print(f"  Writing {filename}...")
            write_parquet(
                table,
                filepath,
                compression=compression,
                row_group_size=num_rows // 4,
            )

            actual_size = filepath.stat().st_size / (1024 * 1024)
            print(f"    Actual size: {actual_size:.2f} MB")

        # Also write a "default" version with snappy (most common)
        default_path = output_dir / f"benchmark_{size_mb}mb.parquet"
        if not default_path.exists():
            import shutil
            shutil.copy(output_dir / f"benchmark_{size_mb}mb_snappy.parquet", default_path)
            print(f"  Created default: benchmark_{size_mb}mb.parquet")

    print(f"\n{'=' * 60}")
    print("Summary of generated files:")
    print(f"{'=' * 60}")
    for f in sorted(output_dir.glob("*.parquet")):
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  {f.name}: {size_mb:.2f} MB")

    # Print schema info
    print(f"\n{'=' * 60}")
    print("Schema (28 columns):")
    print(f"{'=' * 60}")
    sample = pq.read_table(output_dir / f"benchmark_{sizes[0]}mb.parquet")
    for i, field in enumerate(sample.schema):
        print(f"  {i+1:2}. {field.name}: {field.type}")


if __name__ == "__main__":
    main()
