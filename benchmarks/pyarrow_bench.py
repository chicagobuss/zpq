import pyarrow.parquet as pq
import time
import sys
import os

if len(sys.argv) < 2:
    print("Usage: python pyarrow_bench.py <parquet_file>")
    sys.exit(1)

file_path = sys.argv[1]
file_size_mb = os.path.getsize(file_path) / (1024 * 1024)

start = time.time()
# read_table reads all columns and rows into memory, decoding everything
table = pq.read_table(file_path)
end = time.time()

elapsed = end - start
num_rows = table.num_rows
num_cols = table.num_columns
total_values = num_rows * num_cols # Approximation, ignoring nulls logic in count

print(f"PyArrow: Read {num_rows} rows x {num_cols} cols in {elapsed:.4f}s")
print(f"Throughput: {file_size_mb / elapsed:.2f} MB/s (file size)")

