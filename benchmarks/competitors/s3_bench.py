import os
import sys
import time

import pyarrow.fs
import pyarrow.parquet as pq

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

try:
    import duckdb

    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False

MODE = sys.argv[1] if len(sys.argv) > 1 else "all"
ITERATIONS = int(os.environ.get("ITERATIONS", "3"))

# Column projection - configurable via env or first 3 columns of file
COLUMNS_ENV = os.environ.get("COLUMNS")
COLUMNS = COLUMNS_ENV.split(",") if COLUMNS_ENV else None  # None = auto-detect

# S3 path from environment - no defaults to avoid leaking bucket names
PATH = os.environ.get("ZPQ_TEST_S3_PATH")
if not PATH:
    print("Error: ZPQ_TEST_S3_PATH environment variable required")
    print("Example: ZPQ_TEST_S3_PATH=s3://bucket/key.parquet")
    sys.exit(1)

# Helper to strip s3:// for pyarrow.fs
if PATH.startswith("s3://"):
    PATH_FS = PATH[5:]
else:
    PATH_FS = PATH

region = os.environ.get("AWS_REGION", "us-west-2")

print(f"Benchmarking S3 Read: {PATH} (Mode: {MODE})")

# Shared S3 Filesystem for PyArrow (C++ Native)
fs = pyarrow.fs.S3FileSystem(region=region)
file_info = fs.get_file_info(PATH_FS)
file_size_mb = file_info.size / (1024 * 1024)
print(f"File Size: {file_size_mb:.2f} MB")

# Auto-detect columns if not specified (use first 3)
if COLUMNS is None:
    schema = pq.read_schema(PATH_FS, filesystem=fs)
    COLUMNS = [f.name for f in schema][:3]
print(f"Columns: {COLUMNS}")

# --- PyArrow ---
print("\n--- PyArrow (C++ Backend) ---")

if MODE in ["projection", "all"]:
    times = []
    for i in range(ITERATIONS):
        start = time.time()
        table = pq.read_table(PATH_FS, filesystem=fs, columns=COLUMNS)
        elapsed = (time.time() - start) * 1000
        times.append(elapsed)
        print(f"  Run {i + 1}: {elapsed:.2f}ms")
    print(
        f"  Summary: min={min(times):.2f}ms max={max(times):.2f}ms avg={sum(times) / len(times):.2f}ms"
    )

if MODE in ["scan"]:
    start = time.time()
    table = pq.read_table(PATH_FS, filesystem=fs)
    end = time.time()
    print(f"[Full Scan] Time: {end - start:.4f}s")
    print(f"  Rows: {table.num_rows}, Columns: {table.num_columns}")

# --- Polars ---
print("\n--- Polars (Rust Backend) ---")
if HAS_POLARS:
    storage_options = {
        "aws_region": region,
    }
    for key in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]:
        val = os.environ.get(key)
        if val:
            storage_options[key.lower()] = val

    if MODE in ["projection", "all"]:
        times = []
        for i in range(ITERATIONS):
            start = time.time()
            df = pl.read_parquet(PATH, columns=COLUMNS, storage_options=storage_options)
            elapsed = (time.time() - start) * 1000
            times.append(elapsed)
            print(f"  Run {i + 1}: {elapsed:.2f}ms")
        print(
            f"  Summary: min={min(times):.2f}ms max={max(times):.2f}ms avg={sum(times) / len(times):.2f}ms"
        )

    if MODE in ["scan"]:
        start = time.time()
        df = pl.read_parquet(PATH, storage_options=storage_options)
        end = time.time()
        print(f"[Full Scan] Time: {end - start:.4f}s")
        print(f"  Rows: {df.height}, Columns: {df.width}")
else:
    print("Skipped (uv run --with polars)")

# --- DuckDB ---
print("\n--- DuckDB ---")
if HAS_DUCKDB:
    if MODE in ["projection", "all"]:
        times = []
        for i in range(ITERATIONS):
            start = time.time()
            result = duckdb.sql(f"SELECT {', '.join(COLUMNS)} FROM '{PATH}'").fetchall()
            elapsed = (time.time() - start) * 1000
            times.append(elapsed)
            print(f"  Run {i + 1}: {elapsed:.2f}ms")
        print(
            f"  Summary: min={min(times):.2f}ms max={max(times):.2f}ms avg={sum(times) / len(times):.2f}ms"
        )

    if MODE in ["scan"]:
        start = time.time()
        result = duckdb.sql(f"SELECT * FROM '{PATH}'").fetchall()
        end = time.time()
        print(f"[Full Scan] Time: {end - start:.4f}s")
else:
    print("Skipped (uv run --with duckdb)")
