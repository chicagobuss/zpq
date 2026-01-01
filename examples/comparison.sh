#!/usr/bin/env bash
# Compare zpq vs PyArrow vs Polars on a parquet file
#
# Usage: ./examples/comparison.sh path/to/file.parquet
#
# Requires: zpq built, Python with pyarrow and polars

set -euo pipefail

FILE="${1:-}"
if [[ -z "$FILE" ]]; then
    echo "Usage: $0 <parquet-file>"
    exit 1
fi

echo "=== Benchmarking: $FILE ==="
echo ""

# zpq
echo "--- zpq ---"
time ./zig-out/bin/zpq "$FILE" --schema
echo ""

# PyArrow
echo "--- PyArrow ---"
time python3 -c "
import pyarrow.parquet as pq
table = pq.read_table('$FILE')
print(f'Rows: {table.num_rows}, Columns: {table.num_columns}')
"
echo ""

# Polars
echo "--- Polars ---"
time python3 -c "
import polars as pl
df = pl.read_parquet('$FILE')
print(f'Rows: {df.height}, Columns: {df.width}')
"
