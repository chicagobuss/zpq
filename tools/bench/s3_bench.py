import time
import sys
import os
import s3fs
import pyarrow.parquet as pq
import pyarrow.fs

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

MODE = sys.argv[1] if len(sys.argv) > 1 else "all"

PABUCKET = "production-sample-bucket"
KEY = "sample-data.parquet"
PATH = os.environ.get("ZPQ_TEST_S3_PATH", f"s3://{PABUCKET}/{KEY}")

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

# --- PyArrow ---
print("\n--- 2. PyArrow (C++ Backend) ---")

if MODE in ["meta", "all"]:
    # 1. Metadata
    start = time.time()
    meta = pq.read_metadata(PATH_FS, filesystem=fs)
    end = time.time()
    print(f"[Metadata] Time: {end - start:.4f}s")

if MODE in ["scan", "all"]:
    # 2. Full Scan
    start = time.time()
    table = pq.read_table(PATH_FS, filesystem=fs)
    end = time.time()
    print(f"[Full Scan] Time: {end - start:.4f}s ({file_size_mb / (end - start):.2f} MB/s)")
    print(f"  Rows: {table.num_rows}, Columns: {table.num_columns}")

# --- Polars ---
print("\n--- 3. Polars (Rust Backend) ---")
if HAS_POLARS:
    storage_options = {
        "aws_region": region,
    }
    for key in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]:
        val = os.environ.get(key)
        if val:
            storage_options[key.lower()] = val

    if MODE in ["meta", "all"]:
        # Polars doesn't have explicit "read_metadata", but we can fetch schema
        start = time.time()
        # scan_parquet is lazy, fetching schema is fast
        schema = pl.scan_parquet(PATH, storage_options=storage_options).schema
        end = time.time()
        print(f"[Metadata (Schema)] Time: {end - start:.4f}s")

    if MODE in ["scan", "all"]:
        # Full Scan
        start = time.time()
        df = pl.read_parquet(PATH, storage_options=storage_options)
        end = time.time()
        print(f"[Full Scan] Time: {end - start:.4f}s ({file_size_mb / (end - start):.2f} MB/s)")
        print(f"  Rows: {df.height}, Columns: {df.width}")
else:
    print("Skipped (pip install polars)")
