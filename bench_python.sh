#!/bin/bash
set -e
if [ -f .env.staging.bot ]; then
    export $(grep -v '^#' .env.staging.bot | xargs)
fi
export AWS_REGION=us-west-2

echo "Running Python Benchmarks (PyArrow + Polars)..."
# Removed s3fs, using pyarrow.fs
uv run --with pyarrow --with polars python3 tools/bench/s3_bench.py
