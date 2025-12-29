#!/bin/bash
# Run local decode benchmark using file from .env
# Usage: ./benchmarks/run_local.sh [num_columns] [iterations]

set -e

# Source environment if .env exists
if [ -f .env ]; then
    source .env
fi

FILE="${ZPQ_BENCH_FILE:-data/sample.parquet}"
COLS="${1:-3}"
ITERS="${2:-5}"

if [ ! -f "$FILE" ]; then
    echo "Error: Benchmark file not found: $FILE"
    echo "Set ZPQ_BENCH_FILE in .env or provide a valid path"
    exit 1
fi

echo "=== ZPQ Local Decode Benchmark ==="
echo "File: $FILE"
echo "Columns: $COLS"
echo "Iterations: $ITERS"
echo ""

# Build if needed
zig build bench-decode 2>/dev/null

# Run ZPQ
echo "--- ZPQ ---"
./zig-out/bin/bench-decode-full "$FILE" "$COLS" "$ITERS"
echo ""

# Run competitors if requested
if [ "${RUN_COMPETITORS:-false}" = "true" ]; then
    echo "--- Competitors ---"
    uv run --with pyarrow --with polars --with duckdb python benchmarks/competitors/s3_bench.py "$FILE" "$COLS" "$ITERS"
fi
