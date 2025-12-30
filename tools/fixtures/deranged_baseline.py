import time
import os
import pyarrow.parquet as pq
import polars as pl
import duckdb

def test_pyarrow(path):
    print("\n--- PyArrow ---")
    start = time.time()
    table = pq.read_table(path)
    print(f"Read {len(table)} rows in {(time.time() - start)*1000:.2f}ms")
    # Verify a few rows
    df = table.to_pandas()
    print(df.head(3))

def test_polars(path):
    print("\n--- Polars ---")
    start = time.time()
    df = pl.read_parquet(path)
    print(f"Read {len(df)} rows in {(time.time() - start)*1000:.2f}ms")
    print(df.head(3))

def test_duckdb(path):
    print("\n--- DuckDB ---")
    start = time.time()
    res = duckdb.query(f"SELECT * FROM read_parquet('{path}')").fetchall()
    print(f"Read {len(res)} rows in {(time.time() - start)*1000:.2f}ms")
    print(res[:3])

if __name__ == "__main__":
    path = "data/deranged.parquet"
    if os.path.exists(path):
        test_pyarrow(path)
        test_polars(path)
        test_duckdb(path)
    else:
        print(f"File not found: {path}")

